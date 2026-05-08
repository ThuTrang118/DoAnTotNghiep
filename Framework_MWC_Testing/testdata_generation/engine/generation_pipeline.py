from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from testdata_generation.engine.exporters import DataExporter, export_step1_to_excel, export_step2_to_excel
from testdata_generation.engine.feature_item_schema import (
    build_default_testcase_id,
    get_feature_item_fields,
    normalize_feature_name,
)
from testdata_generation.engine.llm_output_parser import LLMOutputParser
from testdata_generation.engine.prompt_loader import PromptLoader
from testdata_generation.engine.validators import (
    ConditionsValidator,
    Step2DecisionTableValidator,
    Step3FinalValidator,
)


class GenerationPipeline:
    """
    Pipeline sinh dữ liệu kiểm thử tự động theo mô hình 3 bước:

    Step 1:
        AI phân tích EP + BVA -> coverage_items

    Step 2:
        AI phân tích Decision Table -> decision_rules trung gian

    Step 3:
        AI map decision_rules + Step1 -> final testcases

    Nguyên tắc production:
    - Fail fast: sai ở bước nào dừng ngay ở bước đó
    - Không export processed data nếu Step 3 chưa pass
    - Luôn lưu raw output để debug
    - Luôn lưu invalid json nếu parse được nhưng validate fail
    """

    FEATURE_PATTERN = re.compile(
        r"^\s*CHỨC NĂNG\s*:\s*(.+?)\s*$",
        re.IGNORECASE | re.MULTILINE,
    )

    ALLOWED_EXPORT_FORMATS = {
        "csv", "json", "xlsx", "xls", "yaml", "yml", "xml", "db"
    }

    STEP1_SEVERE_WARNING_MARKERS = (
        "No coverage items found for fields",
        "missing coverage",
        "may be over-grouped",
        "must include exact boundary points",
        "must include range boundary points",
        "unsupported feature",
        "duplicate id",
        "boundary.reference",
        "boundary.point",
        "at least 1 invalid",
        "at least 1 valid",
    )

    STEP2_SEVERE_WARNING_MARKERS = (
        "unused coverage",
        "missing coverage",
        "not found in Step 1",
        "no happy path",
        "happy_path",
        "single-fault",
        "single_fault",
        "boundary",
        "missing decision rule",
        "expected",
    )

    STEP3_SEVERE_WARNING_MARKERS = (
        "unused coverage",
        "missing coverage",
        "not found in Step 1",
        "not found in Step 2",
        "no happy path",
        "happy path",
        "single-fault",
        "boundary",
        "missing testcase",
        "expected",
    )

    EMPTY_LLM_OUTPUT_MARKER = "[[EMPTY_LLM_OUTPUT]]"

    def __init__(self, llm_client, base_dir: Path, verbose: bool = True) -> None:
        self.llm_client = llm_client
        self.base_dir = Path(base_dir).resolve()
        self.verbose = verbose

        self.output_root = self.base_dir / "output"
        self.output_root.mkdir(parents=True, exist_ok=True)

        self.parser = LLMOutputParser()
        self.prompt_loader = PromptLoader(input_dir=self.base_dir / "input")

        self.step1_validator = ConditionsValidator()
        self.step2_validator = Step2DecisionTableValidator()
        self.step3_validator = Step3FinalValidator()

    # ==========================================================================
    # LOGGING
    # ==========================================================================
    def _log(self, message: str) -> None:
        if not self.verbose:
            return
        now = time.strftime("%H:%M:%S")
        print(f"[{now}] {message}", flush=True)

    @staticmethod
    def _format_seconds(seconds: float) -> str:
        return f"{seconds:.2f}s"

    # ==========================================================================
    # RUN OUTPUT DIR
    # ==========================================================================
    def _build_run_output_dir(self, feature: str) -> Path:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        run_dir = self.output_root / f"{feature}_{timestamp}"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def _resolve_existing_run_dir(self, run_name: str) -> Path:
        if not isinstance(run_name, str) or not run_name.strip():
            raise RuntimeError("Run name is required for this step.")

        run_name = run_name.strip().strip('\"').strip("'")
        run_dir = Path(run_name)

        if not run_dir.is_absolute():
            run_dir = self.output_root / run_name

        run_dir = run_dir.resolve()

        if not run_dir.exists():
            raise FileNotFoundError(f"Run directory not found: {run_dir}")
        if not run_dir.is_dir():
            raise RuntimeError(f"Run path is not a directory: {run_dir}")

        return run_dir

    def _load_run_json(self, run_dir: Path, filename: str) -> Dict[str, Any]:
        path = run_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Required run artifact not found: {path}")
        if not path.is_file():
            raise RuntimeError(f"Run artifact path is not a file: {path}")

        with path.open("r", encoding="utf-8") as f:
            data = __import__("json").load(f)

        if not isinstance(data, dict):
            raise RuntimeError(f"{filename} must contain a JSON object: {path}")

        return data

    def _prepare_loaded_step1_data(self, step1_data: Dict[str, Any], feature: str) -> Dict[str, Any]:
        feature_key = normalize_feature_name(feature)
        loaded_feature = normalize_feature_name(str(step1_data.get("feature", feature_key)))
        if loaded_feature != feature_key:
            raise RuntimeError(
                f"Feature mismatch: command feature='{feature_key}' but step1.json feature='{loaded_feature}'."
            )

        step1_data = self._force_step1_feature(step1_data, feature_key)
        step1_data = self._normalize_step1_data(step1_data)
        step1_data = self._rebuild_step1_summary(step1_data)

        self._hard_check_step1_structure(step1_data)
        self.step1_validator.validate_or_raise(step1_data)
        return step1_data

    def _prepare_loaded_step2_data(self, dt_data: Dict[str, Any], feature: str, step1_data: Dict[str, Any] | None = None) -> Dict[str, Any]:
        feature_key = normalize_feature_name(feature)
        loaded_feature = normalize_feature_name(str(dt_data.get("feature", feature_key)))
        if loaded_feature != feature_key:
            raise RuntimeError(
                f"Feature mismatch: command feature='{feature_key}' but step2_dt.json feature='{loaded_feature}'."
            )

        dt_data = self._force_step2_feature(dt_data, feature_key)
        dt_data = self._normalize_step2_data(dt_data)
        dt_data = self._align_step2_with_expected_contract(dt_data, step1_data, feature_key)
        dt_data = self._rebuild_step2_summary(dt_data)

        self._hard_check_step2_structure(dt_data)
        self.step2_validator.validate_or_raise(dt_data, step1_data=step1_data)
        return dt_data


    def _compact_step1_for_step2(self, step1_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rút gọn Step 1 trước khi đưa vào prompt Step 2.

        Step 2 không cần toàn bộ EP/BVA, representative_value, boundary metadata.
        Step 2 chỉ cần:
        - danh sách field,
        - các outcome nghiệp vụ đã có ở Step 1,
        """
        items = step1_data.get("coverage_items", [])
        fields: List[str] = []
        outcome_map: Dict[str, Dict[str, Any]] = {}

        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue

                cov_id = self._clean_text(item.get("id"))
                field = self._clean_text(item.get("field"))
                validity = self._clean_text(item.get("validity"))
                expected = self._clean_text(item.get("expected_class"))
                rule = self._clean_text(item.get("rule"))

                if field and field not in fields:
                    fields.append(field)
                if not cov_id or not expected:
                    continue

                key = expected
                if key not in outcome_map:
                    outcome_map[key] = {
                        "expected": expected,
                        "fields": [],
                        "rules": [],
                        "validity": validity,
                    }

                if field and field not in outcome_map[key]["fields"]:
                    outcome_map[key]["fields"].append(field)
                if rule and rule not in outcome_map[key]["rules"]:
                    outcome_map[key]["rules"].append(rule)

        return {
            "feature": self._clean_text(step1_data.get("feature")),
            "fields": fields,
            "business_outcomes": list(outcome_map.values()),
        }

    # ==========================================================================
    # PUBLIC API
    # ==========================================================================
    def generate(self, feature: str, formats: List[str]) -> Tuple[str, List[str]]:
        """Backward-compatible alias: chạy liền mạch cả 3 step."""
        return self.generate_all(feature, formats)

    def generate_all(self, feature: str, formats: List[str]) -> Tuple[str, List[str]]:
        total_start = time.perf_counter()

        feature_name = normalize_feature_name(feature)
        self._validate_generate_inputs(feature_name, formats)

        run_dir = self._build_run_output_dir(feature_name)
        exporter = DataExporter(run_dir=run_dir)
        processed_feature_dir = exporter.processed_dir / feature_name

        self._log(f"Bắt đầu pipeline cho feature: '{feature_name}'")
        self._log(f"Thư mục run artifacts: {run_dir}")
        self._log(f"Thư mục processed data: {processed_feature_dir}")
        self._log(f"Định dạng export yêu cầu: {formats}")

        step1_data = self._generate_step1(feature_name, exporter)
        dt_data = self._generate_step2_decision_table(feature_name, exporter, step1_data=step1_data)
        final_json_path, final_data = self._generate_step3_final(
            feature_name,
            step1_data,
            dt_data,
            exporter,
        )

        processed_files = self._export_processed_files(feature_name, final_data, formats, exporter)

        total_elapsed = time.perf_counter() - total_start
        self._log(f"Hoàn tất pipeline trong {self._format_seconds(total_elapsed)}")

        return str(final_json_path), processed_files

    def generate_step1(self, feature: str) -> str:
        """Chạy riêng Step 1 và tạo run mới."""
        total_start = time.perf_counter()

        feature_name = normalize_feature_name(feature)
        if not feature_name:
            raise RuntimeError("Feature is empty.")

        run_dir = self._build_run_output_dir(feature_name)
        exporter = DataExporter(run_dir=run_dir)

        self._log(f"Bắt đầu STEP 1 cho feature: '{feature_name}'")
        self._log(f"Thư mục run artifacts: {run_dir}")

        self._generate_step1(feature_name, exporter)

        total_elapsed = time.perf_counter() - total_start
        self._log(f"STEP 1 standalone hoàn tất trong {self._format_seconds(total_elapsed)}")
        return str(run_dir)

    def generate_step2(self, feature: str, run_name: str) -> str:
        """Chạy riêng Step 2 bằng step1.json trong run được chỉ định."""
        total_start = time.perf_counter()

        feature_name = normalize_feature_name(feature)
        run_dir = self._resolve_existing_run_dir(run_name)
        exporter = DataExporter(run_dir=run_dir)

        self._log(f"Bắt đầu STEP 2 cho feature: '{feature_name}'")
        self._log(f"Run được chỉ định: {run_dir}")

        step1_data_raw = self._load_run_json(run_dir, "step1.json")
        step1_data = self._prepare_loaded_step1_data(step1_data_raw, feature_name)

        # Ghi lại step1.json đã normalize để đảm bảo summary/count sạch cho các bước sau.
        step1_json_path = exporter.write_raw_json(step1_data, filename="step1.json")
        self._export_step1_excel_safely(step1_json_path)

        dt_data = self._generate_step2_decision_table(feature_name, exporter, step1_data=step1_data)

        total_elapsed = time.perf_counter() - total_start
        self._log(f"STEP 2 standalone hoàn tất trong {self._format_seconds(total_elapsed)}")
        return str(run_dir / "step2_dt.json")

    def generate_step3(self, feature: str, run_name: str, formats: List[str]) -> Tuple[str, List[str]]:
        """Chạy riêng Step 3 bằng step1.json và step2_dt.json trong run được chỉ định."""
        total_start = time.perf_counter()

        feature_name = normalize_feature_name(feature)
        self._validate_generate_inputs(feature_name, formats)

        run_dir = self._resolve_existing_run_dir(run_name)
        exporter = DataExporter(run_dir=run_dir)

        self._log(f"Bắt đầu STEP 3 cho feature: '{feature_name}'")
        self._log(f"Run được chỉ định: {run_dir}")
        self._log(f"Định dạng export yêu cầu: {formats}")

        step1_data_raw = self._load_run_json(run_dir, "step1.json")
        step1_data = self._prepare_loaded_step1_data(step1_data_raw, feature_name)

        dt_data_raw = self._load_run_json(run_dir, "step2_dt.json")
        dt_data = self._prepare_loaded_step2_data(dt_data_raw, feature_name, step1_data)

        # Ghi lại các artifact đã normalize để đảm bảo các file trong run đồng bộ.
        step1_json_path = exporter.write_raw_json(step1_data, filename="step1.json")
        self._export_step1_excel_safely(step1_json_path)
        exporter.write_raw_json(dt_data, filename="step2_dt.json")
        self._export_step2_excel_safely(dt_data, exporter)

        final_json_path, final_data = self._generate_step3_final(
            feature_name,
            step1_data,
            dt_data,
            exporter,
        )
        processed_files = self._export_processed_files(feature_name, final_data, formats, exporter)

        total_elapsed = time.perf_counter() - total_start
        self._log(f"STEP 3 standalone hoàn tất trong {self._format_seconds(total_elapsed)}")
        return str(final_json_path), processed_files

    # ==========================================================================
    # INPUT VALIDATION
    # ==========================================================================
    def _validate_generate_inputs(self, feature: str, formats: List[str]) -> None:
        if not isinstance(feature, str) or not feature.strip():
            raise RuntimeError("Feature is empty.")

        if not isinstance(formats, list) or not formats:
            raise RuntimeError("Formats must be a non-empty list.")

        normalized_formats = []
        for fmt in formats:
            fmt_clean = str(fmt).strip().lower()
            if not fmt_clean:
                continue
            if fmt_clean not in self.ALLOWED_EXPORT_FORMATS:
                raise RuntimeError(
                    f"Unsupported export format: '{fmt_clean}'. "
                    f"Allowed formats: {sorted(self.ALLOWED_EXPORT_FORMATS)}"
                )
            normalized_formats.append(fmt_clean)

        if not normalized_formats:
            raise RuntimeError("No valid export formats provided.")

    # ==========================================================================
    # FEATURE EXTRACTION / NORMALIZATION
    # ==========================================================================
    def _extract_feature_from_spec(self, prompt: str) -> str:
        if not isinstance(prompt, str) or not prompt.strip():
            raise RuntimeError("Prompt is empty. Cannot extract feature from specification.")

        match = self.FEATURE_PATTERN.search(prompt)
        if not match:
            raise RuntimeError(
                "Cannot extract feature from specification. "
                "Expected a line like: 'CHỨC NĂNG: ...'"
            )

        raw_feature = match.group(1).strip()
        if not raw_feature:
            raise RuntimeError("Extracted feature from specification is empty.")

        return raw_feature

    def _resolve_feature_key_from_prompt(self, prompt: str, fallback_feature: str) -> str:
        try:
            raw_feature = self._extract_feature_from_spec(prompt)
            return normalize_feature_name(raw_feature)
        except Exception:
            return normalize_feature_name(fallback_feature)

    # ==========================================================================
    # COMMON NORMALIZERS
    # ==========================================================================
    @staticmethod
    def _clean_text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_priority(value: Any) -> str:
        raw = str(value or "").strip().lower()
        mapping = {
            "high": "High",
            "medium": "Medium",
            "low": "Low",
        }
        return mapping.get(raw, str(value or "").strip())

    @staticmethod
    def _normalize_technique(value: Any) -> str:
        raw = str(value or "").strip().upper()
        if raw in {"EP", "BVA"}:
            return raw
        return str(value or "").strip()

    @staticmethod
    def _normalize_validity(value: Any) -> str:
        raw = str(value or "").strip().lower()
        if raw in {"valid", "invalid"}:
            return raw
        return str(value or "").strip()

    @staticmethod
    def _normalize_partition_type(value: Any) -> Any:
        if value is None:
            return None
        raw = str(value).strip().lower()
        if raw in {"valid", "invalid"}:
            return raw
        return value

    @staticmethod
    def _normalize_boundary_point(value: Any) -> str:
        raw = str(value or "").strip().upper()
        allowed = {
            "MIN-1", "MIN", "MIN+1",
            "MAX-1", "MAX", "MAX+1",
            "N-1", "N", "N+1",
        }
        return raw if raw in allowed else str(value or "").strip()

    @staticmethod
    def _normalize_boundary_kind(value: Any) -> str:
        raw = str(value or "").strip().lower()
        allowed = {"range", "exact"}
        return raw if raw in allowed else str(value or "").strip()

    @staticmethod
    def _normalize_step2_rule_type(value: Any) -> str:
        raw = str(value or "").strip().lower()
        allowed = {"happy_path", "single_fault", "boundary", "boundary_valid", "business_rule"}
        return raw if raw in allowed else str(value or "").strip()

    @staticmethod
    def _dedupe_string_list(values: Any) -> List[str]:
        if not isinstance(values, list):
            return []

        out: List[str] = []
        seen = set()

        for v in values:
            if not isinstance(v, str):
                continue
            s = v.strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)

        return out

    @staticmethod
    def _dedupe_conditions(values: Any) -> List[Dict[str, str]]:
        if not isinstance(values, list):
            return []

        out: List[Dict[str, str]] = []
        seen = set()

        for item in values:
            if not isinstance(item, dict):
                continue

            field = str(item.get("field", "")).strip()
            state = str(item.get("state", "")).strip()

            if not field or not state:
                continue

            key = (field, state)
            if key in seen:
                continue

            seen.add(key)
            out.append({"field": field, "state": state})

        return out

    # ==========================================================================
    # RAW OUTPUT HELPERS
    # ==========================================================================
    def _save_raw_output(self, exporter: DataExporter, filename: str, raw_output: Any) -> Path:
        path = exporter._get_run_file_path(filename)

        if raw_output is None:
            text = self.EMPTY_LLM_OUTPUT_MARKER
        else:
            text = str(raw_output)
            if not text.strip():
                text = self.EMPTY_LLM_OUTPUT_MARKER

        path.write_text(text, encoding="utf-8")
        return path

    def _raise_if_llm_output_empty(self, raw_output: Any, step_name: str, raw_txt_path: Path) -> None:
        if raw_output is None or not str(raw_output).strip():
            raise RuntimeError(
                f"{step_name} LLM returned empty output. "
                f"Raw output marker saved at: {raw_txt_path}"
            )

    # ==========================================================================
    # STEP 1 NORMALIZATION / HARD CHECK
    # ==========================================================================

    def _rebuild_step1_summary(self, step1_data: Dict[str, Any]) -> Dict[str, Any]:
        items = step1_data.get("coverage_items", [])
        ep_count = 0
        bva_count = 0

        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                technique = item.get("technique")
                if technique == "EP":
                    ep_count += 1
                elif technique == "BVA":
                    bva_count += 1

        step1_data["coverage_summary"] = {
            "EP_count": ep_count,
            "BVA_count": bva_count,
            "TOTAL": ep_count + bva_count,
        }
        return step1_data

    def _force_step1_feature(self, step1_data: Dict[str, Any], feature_key: str) -> Dict[str, Any]:
        step1_data["feature"] = feature_key
        return step1_data

    def _normalize_step1_data(self, step1_data: Dict[str, Any]) -> Dict[str, Any]:
        items = step1_data.get("coverage_items")
        if not isinstance(items, list):
            step1_data["coverage_items"] = []
            return step1_data

        normalized_items: List[Dict[str, Any]] = []

        for item in items:
            if not isinstance(item, dict):
                continue

            normalized = dict(item)
            normalized["id"] = self._clean_text(normalized.get("id"))
            normalized["field"] = self._clean_text(normalized.get("field"))
            normalized["technique"] = self._normalize_technique(normalized.get("technique"))
            normalized["description"] = self._clean_text(normalized.get("description"))
            normalized["validity"] = self._normalize_validity(normalized.get("validity"))
            normalized["partition_type"] = self._normalize_partition_type(normalized.get("partition_type"))
            normalized["rule"] = self._clean_text(normalized.get("rule"))
            normalized["expected_class"] = self._clean_text(normalized.get("expected_class"))

            rep = normalized.get("representative_value")
            normalized["representative_value"] = "" if rep is None else str(rep)

            boundary = normalized.get("boundary")
            if isinstance(boundary, dict):
                boundary = dict(boundary)
                boundary["kind"] = self._normalize_boundary_kind(boundary.get("kind"))
                boundary["point"] = self._normalize_boundary_point(boundary.get("point"))
                normalized["boundary"] = boundary
            else:
                normalized["boundary"] = None

            if normalized["technique"] == "EP":
                normalized["boundary"] = None

            if normalized["technique"] == "BVA":
                normalized["partition_type"] = None

            normalized_items.append(normalized)

        step1_data["coverage_items"] = self._ensure_required_empty_coverage(normalized_items)
        return step1_data


    # --------------------------------------------------------------------------
    # STEP 1 REQUIRED/EMPTY COVERAGE REPAIR
    # --------------------------------------------------------------------------
    REQUIRED_RULE_MARKERS = (
        "bắt buộc",
        "bat buoc",
        "required",
        "not empty",
        "không được để trống",
        "khong duoc de trong",
        "không rỗng",
        "khong rong",
    )

    EMPTY_EXPECTED_MARKERS = (
        "điền",
        "dien",
        "trống",
        "trong",
        "rỗng",
        "rong",
        "required",
        "empty",
        "blank",
    )

    @classmethod
    def _is_required_rule(cls, value: Any) -> bool:
        text = str(value or "").strip().lower()
        return any(marker in text for marker in cls.REQUIRED_RULE_MARKERS)

    @staticmethod
    def _is_empty_representative(value: Any) -> bool:
        return value is None or str(value) == ""

    @classmethod
    def _looks_like_empty_expected(cls, value: Any) -> bool:
        text = str(value or "").strip().lower()
        return bool(text) and any(marker in text for marker in cls.EMPTY_EXPECTED_MARKERS)

    @staticmethod
    def _next_step1_coverage_id(items: List[Dict[str, Any]]) -> str:
        numeric_ids: List[int] = []
        for item in items:
            raw_id = str(item.get("id", "")).strip()
            if raw_id.isdigit():
                numeric_ids.append(int(raw_id))
        if numeric_ids:
            return str(max(numeric_ids) + 1)

        counter = len(items) + 1
        existing = {str(item.get("id", "")).strip() for item in items}
        while f"AUTO_{counter}" in existing:
            counter += 1
        return f"AUTO_{counter}"

    def _find_required_empty_expected_class(self, items: List[Dict[str, Any]], field: str) -> str:
        # Ưu tiên message rỗng đúng field, sau đó message rỗng của field khác.
        for same_field_only in (True, False):
            for item in items:
                if same_field_only and self._clean_text(item.get("field")) != field:
                    continue
                if (
                    self._clean_text(item.get("technique")).upper() == "EP"
                    and self._clean_text(item.get("validity")).lower() == "invalid"
                    and self._is_required_rule(item.get("rule"))
                    and self._is_empty_representative(item.get("representative_value"))
                ):
                    expected = self._clean_text(item.get("expected_class"))
                    if expected:
                        return expected

        # Nếu AI chỉ để message bắt buộc ở rule/expected khác, vẫn tận dụng đúng message đó.
        for item in items:
            if self._is_required_rule(item.get("rule")) and self._looks_like_empty_expected(item.get("expected_class")):
                expected = self._clean_text(item.get("expected_class"))
                if expected:
                    return expected

        return "Vui lòng điền vào trường này"

    def _ensure_required_empty_coverage(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Bổ sung coverage EP invalid cho trường bắt buộc nhưng bị thiếu case rỗng.

        Đây là luật chung cho mọi feature/field: nếu một field có rule bắt buộc nhập
        thì lớp rỗng là một phân vùng không hợp lệ riêng theo EP, không phụ thuộc
        vào tên field hoặc feature cụ thể.
        """
        if not items:
            return items

        required_fields: Set[str] = set()
        has_empty_invalid: Set[str] = set()

        for item in items:
            field = self._clean_text(item.get("field"))
            if not field:
                continue

            if self._is_required_rule(item.get("rule")) or self._is_required_rule(item.get("description")):
                required_fields.add(field)

            if (
                self._clean_text(item.get("technique")).upper() == "EP"
                and self._clean_text(item.get("validity")).lower() == "invalid"
                and self._is_empty_representative(item.get("representative_value"))
            ):
                has_empty_invalid.add(field)

        missing_fields = [field for field in sorted(required_fields) if field not in has_empty_invalid]
        if not missing_fields:
            return items

        out = list(items)
        for field in missing_fields:
            out.append(
                {
                    "id": self._next_step1_coverage_id(out),
                    "field": field,
                    "technique": "EP",
                    "description": f"{field} rỗng",
                    "validity": "invalid",
                    "partition_type": "invalid",
                    "boundary": None,
                    "representative_value": "",
                    "rule": "Bắt buộc nhập",
                    "expected_class": self._find_required_empty_expected_class(out, field),
                }
            )

        return out

    def _hard_check_step1_structure(self, step1_data: Dict[str, Any]) -> None:
        if not isinstance(step1_data, dict):
            raise RuntimeError("Step1 output must be a JSON object.")

        if not self._clean_text(step1_data.get("feature")):
            raise RuntimeError("Step1 missing 'feature'.")

        if not self._clean_text(step1_data.get("description")):
            raise RuntimeError("Step1 missing 'description'.")

        items = step1_data.get("coverage_items")
        if not isinstance(items, list) or not items:
            raise RuntimeError("Step1 must contain non-empty 'coverage_items'.")

        ids = set()
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                raise RuntimeError(f"Step1 coverage_items[{idx}] must be an object.")

            item_id = self._clean_text(item.get("id"))
            if not item_id:
                raise RuntimeError(f"Step1 coverage_items[{idx}] missing 'id'.")
            if item_id in ids:
                raise RuntimeError(f"Step1 duplicate coverage id: '{item_id}'.")
            ids.add(item_id)

            if not self._clean_text(item.get("field")):
                raise RuntimeError(f"Step1 coverage_items[{idx}] missing 'field'.")
            if not self._clean_text(item.get("technique")):
                raise RuntimeError(f"Step1 coverage_items[{idx}] missing 'technique'.")
            if not self._clean_text(item.get("description")):
                raise RuntimeError(f"Step1 coverage_items[{idx}] missing 'description'.")
            if not self._clean_text(item.get("validity")):
                raise RuntimeError(f"Step1 coverage_items[{idx}] missing 'validity'.")
            if not self._clean_text(item.get("rule")):
                raise RuntimeError(f"Step1 coverage_items[{idx}] missing 'rule'.")
            if not self._clean_text(item.get("expected_class")):
                raise RuntimeError(f"Step1 coverage_items[{idx}] missing 'expected_class'.")

    def _raise_if_step1_warnings_are_severe(self, warnings: List[str]) -> None:
        severe = [
            w for w in warnings
            if any(marker.lower() in w.lower() for marker in self.STEP1_SEVERE_WARNING_MARKERS)
        ]
        if severe:
            raise RuntimeError(
                "Step1 validation produced severe warnings:\n- " + "\n- ".join(severe)
            )

    # ==========================================================================
    # STEP 2 NORMALIZATION / HARD CHECK
    # ==========================================================================
    def _force_step2_feature(self, dt_data: Dict[str, Any], feature_key: str) -> Dict[str, Any]:
        dt_data["feature"] = feature_key
        return dt_data

    def _rebuild_step2_summary(self, dt_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rebuild summary cho Step 2 Reduced Decision Logic Table.

        Giữ full_combination_count vì validator/schema hiện tại vẫn dùng để tham chiếu
        số tổ hợp lý thuyết 2^n, nhưng Step 2 KHÔNG sinh full_decision_rules.
        """
        conditions = dt_data.get("conditions", [])
        actions = dt_data.get("actions", [])
        reduced_rules = dt_data.get("decision_rules", [])

        condition_count = len(conditions) if isinstance(conditions, list) else 0
        action_count = len(actions) if isinstance(actions, list) else 0
        reduced_rule_count = len(reduced_rules) if isinstance(reduced_rules, list) else 0

        dt_data["decision_summary"] = {
            "condition_count": condition_count,
            "action_count": action_count,
            "full_combination_count": 2 ** condition_count if condition_count > 0 else 0,
            "reduced_rule_count": reduced_rule_count,
        }
        return dt_data

    @staticmethod
    def _normalize_dt_state(value: Any) -> str:
        raw = str(value or "").strip().upper()
        return raw if raw in {"Y", "N", "-"} else str(value or "").strip()

    def _parse_legacy_condition_states(self, value: Any) -> Dict[str, str]:
        """
        Hỗ trợ output cũ từ LLM:
        - conditions: ["C1=Y", "C2=N", ...]
        - conditions: {"C1": "Y", "C2": "N"}

        Nếu không parse phần này, Step 2 sẽ mất Y/N và bị fill toàn '-' trong Excel/JSON.
        """
        out: Dict[str, str] = {}

        if isinstance(value, dict):
            for key, state in value.items():
                cid = self._clean_text(key)
                normalized_state = self._normalize_dt_state(state)
                if cid and normalized_state in {"Y", "N", "-"}:
                    out[cid] = normalized_state
            return out

        if isinstance(value, list):
            for item in value:
                text = self._clean_text(item)
                if not text:
                    continue
                match = re.match(r"^\s*(C\d+)\s*[=:]\s*([YNyn-])\s*$", text)
                if match:
                    out[match.group(1)] = self._normalize_dt_state(match.group(2))
            return out

        return out

    def _normalize_step2_rule(self, rule: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize 1 decision_rule rút gọn. Không xử lý full table ở Step 2."""
        normalized = dict(rule)
        normalized["id"] = self._clean_text(normalized.get("id"))

        rule_type = self._clean_text(normalized.get("type")).lower()
        type_aliases = {
            "validation_fault": "single_fault",
            "single-fault": "single_fault",
            "single fault": "single_fault",
            "boundary_valid": "boundary",
            "boundary_invalid": "boundary",
            "valid_boundary": "boundary",
            "invalid_boundary": "boundary",
            "business": "business_rule",
            "business rule": "business_rule",
            "success": "happy_path",
            "happy path": "happy_path",
        }
        normalized["type"] = type_aliases.get(rule_type, rule_type)

        states = normalized.get("condition_states")
        if not isinstance(states, dict):
            # Nhiều model vẫn trả schema cũ: "conditions": ["C1=Y", "C2=N"].
            # Parse lại để không mất trạng thái Y/N khi export Excel và khi Step 3 map.
            states = self._parse_legacy_condition_states(normalized.get("conditions"))
        normalized["condition_states"] = {
            self._clean_text(k): self._normalize_dt_state(v)
            for k, v in states.items()
            if self._clean_text(k) and self._normalize_dt_state(v) in {"Y", "N", "-"}
        }

        action_refs = self._dedupe_string_list(normalized.get("action_refs"))
        if not action_refs and self._clean_text(normalized.get("action")):
            action_refs = [self._clean_text(normalized.get("action"))]
        normalized["action_refs"] = action_refs

        # Step 2 không được giữ các key trung gian/cũ.
        for key in (
            "action",
            "coverage_refs",
            "conditions",
            "optimization_note",
            "combination_note",
            "description",
            "name",
        ):
            normalized.pop(key, None)

        normalized["expected"] = self._clean_text(normalized.get("expected"))
        normalized["reduction_note"] = self._clean_text(normalized.get("reduction_note"))
        return normalized

    def _extract_step2_expected_contracts(self, feature: str) -> List[str]:
        """
        Lấy danh sách Expected chuẩn từ đặc tả nghiệp vụ.
        Hàm này chỉ đọc các dòng/mệnh đề có dạng Expected = ... và không chứa luật riêng
        cho bất kỳ field hoặc feature cụ thể nào.
        """
        try:
            spec = self.prompt_loader.load_feature_description(feature)
        except Exception:
            return []

        contracts: List[str] = []
        pattern = re.compile(
            r"Expected\s*=\s*(?:\"([^\"\r\n]+)\"|'([^'\r\n]+)'|([^\r\n]+))",
            flags=re.IGNORECASE,
        )
        for match in pattern.finditer(spec):
            value = next((g for g in match.groups() if g is not None), "")
            value = self._clean_text(value).strip('"').strip("'")
            if value and value not in contracts:
                contracts.append(value)
        return contracts

    def _extract_step2_expected_sources(
        self,
        feature: str,
        step1_data: Dict[str, Any] | None,
    ) -> List[str]:
        """
        Nguồn expected chuẩn dùng cho Step 2:
        - ưu tiên Expected trong đặc tả nghiệp vụ;
        - bổ sung expected_class từ Step 1 nếu có.
        """
        values: List[str] = []

        for value in self._extract_step2_expected_contracts(feature):
            if value and value not in values:
                values.append(value)

        if isinstance(step1_data, dict):
            items = step1_data.get("coverage_items", [])
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    expected = self._clean_text(item.get("expected_class"))
                    if expected and expected not in values:
                        values.append(expected)
        return values

    @staticmethod
    def _step2_text_tokens(value: Any) -> Set[str]:
        text = str(value or "").strip().lower()
        return set(re.findall(r"[\wÀ-ỹ]+", text, flags=re.UNICODE))

    def _step2_similarity(self, left: Any, right: Any) -> float:
        left_text = self._clean_text(left).lower()
        right_text = self._clean_text(right).lower()
        if not left_text or not right_text:
            return 0.0
        if left_text == right_text:
            return 1.0
        if left_text in right_text or right_text in left_text:
            return 0.85

        left_tokens = self._step2_text_tokens(left_text)
        right_tokens = self._step2_text_tokens(right_text)
        if not left_tokens or not right_tokens:
            return 0.0
        overlap = len(left_tokens & right_tokens)
        union = len(left_tokens | right_tokens)
        return overlap / union if union else 0.0

    def _canonical_step2_expected(self, value: Any, expected_sources: List[str]) -> str:
        """Chuẩn hóa expected về đúng một giá trị trong expected_sources nếu đủ tin cậy."""
        raw = self._clean_text(value)
        if not raw:
            return ""
        if raw in expected_sources:
            return raw

        best_value = ""
        best_score = 0.0
        for candidate in expected_sources:
            score = self._step2_similarity(raw, candidate)
            if score > best_score:
                best_score = score
                best_value = candidate
        return best_value if best_score >= 0.5 else raw

    def _choose_step2_success_expected(self, feature: str, expected_sources: List[str]) -> str:
        """Chọn expected cho happy path từ hợp đồng Expected trong đặc tả."""
        contracts = self._extract_step2_expected_contracts(feature)
        if contracts:
            return contracts[-1]
        return expected_sources[-1] if expected_sources else "Dữ liệu hợp lệ"

    def _ensure_step2_action(
        self,
        actions: List[Dict[str, Any]],
        expected: str,
        name: str,
    ) -> str:
        expected = self._clean_text(expected)
        for action in actions:
            if self._clean_text(action.get("expected")) == expected:
                if not self._clean_text(action.get("name")):
                    action["name"] = name
                return self._clean_text(action.get("id"))

        used_numbers: List[int] = []
        for action in actions:
            aid = self._clean_text(action.get("id"))
            m = re.match(r"^A(\d+)$", aid)
            if m:
                used_numbers.append(int(m.group(1)))
        next_id = f"A{(max(used_numbers) + 1) if used_numbers else 1}"
        actions.append({
            "id": next_id,
            "name": name or f"Kết quả: {expected}",
            "expected": expected,
        })
        return next_id

    def _step2_rule_note(
        self,
        rule_type: str,
        states: Dict[str, str],
        condition_map: Dict[str, Dict[str, Any]],
        expected: str,
    ) -> str:
        changed = [cid for cid, state in states.items() if state == "N"]
        if rule_type == "happy_path":
            return "Các điều kiện chính đều thỏa mãn nên rule dẫn tới expected thành công."
        if len(changed) == 1:
            cid = changed[0]
            name = self._clean_text(condition_map.get(cid, {}).get("name")) or cid
            return f"{cid}=N ({name}) quyết định expected của rule."
        if changed:
            return f"Tổ hợp {', '.join(changed)} quyết định expected của rule."
        return "Rule được giữ vì tạo ra một expected nghiệp vụ riêng."

    def _infer_step2_expected_for_condition(
        self,
        condition: Dict[str, Any],
        step1_data: Dict[str, Any] | None,
        expected_sources: List[str],
    ) -> str:
        """
        Suy ra expected cho một condition ở trạng thái N bằng cách so khớp condition
        với các coverage invalid của Step 1. Không dùng tên field hoặc feature cố định.
        """
        if not isinstance(step1_data, dict):
            return ""

        source_fields = condition.get("source_fields")
        if not isinstance(source_fields, list):
            source_fields = []
        source_fields = [self._clean_text(field) for field in source_fields if self._clean_text(field)]

        condition_text = " ".join([
            self._clean_text(condition.get("name")),
            self._clean_text(condition.get("meaning_when_n")),
        ])

        items = step1_data.get("coverage_items", [])
        if not isinstance(items, list):
            return ""

        best_expected = ""
        best_score = 0.0
        for item in items:
            if not isinstance(item, dict):
                continue
            if self._clean_text(item.get("validity")).lower() != "invalid":
                continue
            expected = self._clean_text(item.get("expected_class"))
            if not expected:
                continue

            item_field = self._clean_text(item.get("field"))
            field_score = 0.35 if item_field in source_fields else 0.0
            item_text = " ".join([
                self._clean_text(item.get("description")),
                self._clean_text(item.get("rule")),
                expected,
            ])
            text_score = self._step2_similarity(condition_text, item_text)
            score = field_score + text_score

            if score > best_score:
                best_score = score
                best_expected = expected

        if not best_expected or best_score < 0.35:
            return ""
        return self._canonical_step2_expected(best_expected, expected_sources)

    # --------------------------------------------------------------------------
    # STEP 2 CONDITION DEPENDENCY REPAIR
    # --------------------------------------------------------------------------
    PRESENCE_CONDITION_MARKERS = (
        "không rỗng",
        "khong rong",
        "không trống",
        "khong trong",
        "được nhập",
        "duoc nhap",
        "bắt buộc",
        "bat buoc",
        "required",
        "not empty",
        "not blank",
    )

    @classmethod
    def _is_step2_presence_condition(cls, condition: Dict[str, Any]) -> bool:
        """
        Nhận diện condition tiền đề kiểu "field đã được nhập / không rỗng".

        Luật này dùng chung cho mọi feature: không dựa vào tên field cụ thể,
        chỉ dựa vào ý nghĩa của condition và source_fields.
        """
        if not isinstance(condition, dict):
            return False

        source_fields = condition.get("source_fields")
        if not isinstance(source_fields, list) or len([f for f in source_fields if str(f).strip()]) != 1:
            return False

        text = " ".join([
            str(condition.get("name", "")),
            str(condition.get("meaning_when_y", "")),
            str(condition.get("meaning_when_n", "")),
        ]).strip().lower()

        return any(marker in text for marker in cls.PRESENCE_CONDITION_MARKERS)

    def _build_step2_presence_condition_by_field(
        self,
        conditions: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        """Map field -> condition id của điều kiện tiền đề không rỗng/được nhập."""
        out: Dict[str, str] = {}
        for condition in conditions:
            if not isinstance(condition, dict):
                continue
            if not self._is_step2_presence_condition(condition):
                continue

            cid = self._clean_text(condition.get("id"))
            source_fields = condition.get("source_fields")
            if not cid or not isinstance(source_fields, list):
                continue

            field = next((self._clean_text(f) for f in source_fields if self._clean_text(f)), "")
            if field and field not in out:
                out[field] = cid
        return out

    def _repair_step2_condition_dependencies(
        self,
        states: Dict[str, str],
        condition_map: Dict[str, Dict[str, Any]],
        presence_condition_by_field: Dict[str, str],
    ) -> Dict[str, str]:
        """
        Sửa mâu thuẫn trong bảng quyết định rút gọn.

        Nếu một condition kiểm tra sâu hơn của field đang = N thì field đó phải
        có dữ liệu trước, nên condition tiền đề "field không rỗng" phải = Y.
        Với condition quan hệ nhiều field, tất cả field liên quan phải có tiền đề = Y.
        """
        if not isinstance(states, dict):
            return {}

        repaired = dict(states)

        for cid, state in list(repaired.items()):
            if self._normalize_dt_state(state) != "N":
                continue

            condition = condition_map.get(cid, {})
            if self._is_step2_presence_condition(condition):
                continue

            source_fields = condition.get("source_fields")
            if not isinstance(source_fields, list):
                continue

            for field in source_fields:
                field_clean = self._clean_text(field)
                if not field_clean:
                    continue

                presence_cid = presence_condition_by_field.get(field_clean)
                if not presence_cid or presence_cid == cid:
                    continue

                # Ép tiền đề thành Y. Đây là sửa logic, không tạo thêm lỗi mới.
                repaired[presence_cid] = "Y"

        return repaired

    def _align_step2_with_expected_contract(
        self,
        dt_data: Dict[str, Any],
        step1_data: Dict[str, Any] | None,
        feature: str,
    ) -> Dict[str, Any]:
        """
        Căn Step 2 về hợp đồng expected dùng chung:
        - action.expected phải bám theo Expected trong đặc tả hoặc expected_class Step 1;
        - rule.expected phải trùng action.expected;
        - đảm bảo có happy_path;
        - bổ sung single_fault khi có thể suy ra expected từ Step 1;
        - không chứa luật riêng cho field/feature cụ thể.
        """
        conditions = dt_data.get("conditions", [])
        actions = dt_data.get("actions", [])
        rules = dt_data.get("decision_rules", [])
        if not isinstance(conditions, list):
            conditions = []
        if not isinstance(actions, list):
            actions = []
        if not isinstance(rules, list):
            rules = []

        expected_sources = self._extract_step2_expected_sources(feature, step1_data)
        condition_ids = [self._clean_text(c.get("id")) for c in conditions if isinstance(c, dict) and self._clean_text(c.get("id"))]
        condition_map = {self._clean_text(c.get("id")): c for c in conditions if isinstance(c, dict) and self._clean_text(c.get("id"))}
        presence_condition_by_field = self._build_step2_presence_condition_by_field(conditions)
        all_y_states = {cid: "Y" for cid in condition_ids}

        normalized_actions: List[Dict[str, Any]] = []
        for idx, action in enumerate(actions, start=1):
            if not isinstance(action, dict):
                continue
            aid = self._clean_text(action.get("id")) or f"A{idx}"
            name = self._clean_text(action.get("name")) or f"Action {idx}"
            expected_raw = self._clean_text(action.get("expected") or action.get("description") or name)
            expected = self._canonical_step2_expected(expected_raw, expected_sources)
            normalized_actions.append({"id": aid, "name": name, "expected": expected})

        actions = normalized_actions
        action_expected = {
            self._clean_text(action.get("id")): self._clean_text(action.get("expected"))
            for action in actions
            if self._clean_text(action.get("id"))
        }

        success_expected = self._choose_step2_success_expected(feature, expected_sources)
        success_action_id = self._ensure_step2_action(actions, success_expected, "Kết quả xử lý thành công")
        action_expected[success_action_id] = success_expected

        expected_by_condition: Dict[str, str] = {}
        action_by_condition: Dict[str, str] = {}
        for cid in condition_ids:
            expected = self._infer_step2_expected_for_condition(condition_map[cid], step1_data, expected_sources)
            if expected:
                expected_by_condition[cid] = expected
                action_by_condition[cid] = self._ensure_step2_action(actions, expected, f"Kết quả: {expected}")

        repaired_rules: List[Dict[str, Any]] = []
        seen_keys: Set[Tuple[str, str]] = set()

        def normalize_states(raw_states: Any) -> Dict[str, str]:
            states = raw_states if isinstance(raw_states, dict) else {}
            return {cid: self._normalize_dt_state(states.get(cid, "-")) for cid in condition_ids}

        for rule in rules:
            if not isinstance(rule, dict):
                continue
            states = normalize_states(rule.get("condition_states"))
            states = self._repair_step2_condition_dependencies(states, condition_map, presence_condition_by_field)
            all_y = bool(condition_ids) and all(states.get(cid) == "Y" for cid in condition_ids)
            n_conditions = [cid for cid, state in states.items() if state == "N"]
            action_refs = self._dedupe_string_list(rule.get("action_refs"))

            if all_y:
                rule_type = "happy_path"
                expected = success_expected
                action_refs = [success_action_id]
                key = (rule_type, "happy")
            elif len(n_conditions) == 1:
                cid = n_conditions[0]
                expected = ""
                if action_refs:
                    expected = action_expected.get(action_refs[0], "")
                if not expected:
                    expected = expected_by_condition.get(cid, "")
                expected = self._canonical_step2_expected(expected, expected_sources)
                if not expected:
                    continue
                action_id = self._ensure_step2_action(actions, expected, f"Kết quả: {expected}")
                action_refs = [action_id]
                rule_type = "single_fault"
                key = (rule_type, cid)
            else:
                expected = ""
                if action_refs:
                    expected = action_expected.get(action_refs[0], "")
                expected = self._canonical_step2_expected(expected or rule.get("expected"), expected_sources)
                if not expected:
                    continue
                action_id = self._ensure_step2_action(actions, expected, f"Kết quả: {expected}")
                action_refs = [action_id]
                rule_type = self._clean_text(rule.get("type")) or "business_rule"
                key = (rule_type, "|".join(f"{k}={v}" for k, v in sorted(states.items())))

            if key in seen_keys:
                continue
            seen_keys.add(key)
            repaired_rules.append({
                "id": "",
                "type": rule_type,
                "condition_states": states,
                "action_refs": action_refs,
                "expected": expected,
                "reduction_note": self._step2_rule_note(rule_type, states, condition_map, expected),
            })

        if ("happy_path", "happy") not in seen_keys:
            repaired_rules.insert(0, {
                "id": "",
                "type": "happy_path",
                "condition_states": dict(all_y_states),
                "action_refs": [success_action_id],
                "expected": success_expected,
                "reduction_note": self._step2_rule_note("happy_path", all_y_states, condition_map, success_expected),
            })
            seen_keys.add(("happy_path", "happy"))

        for cid, expected in expected_by_condition.items():
            key = ("single_fault", cid)
            if key in seen_keys:
                continue
            states = dict(all_y_states)
            states[cid] = "N"
            states = self._repair_step2_condition_dependencies(states, condition_map, presence_condition_by_field)
            action_id = action_by_condition[cid]
            repaired_rules.append({
                "id": "",
                "type": "single_fault",
                "condition_states": states,
                "action_refs": [action_id],
                "expected": expected,
                "reduction_note": self._step2_rule_note("single_fault", states, condition_map, expected),
            })
            seen_keys.add(key)

        for idx, rule in enumerate(repaired_rules, start=1):
            rule["id"] = f"DT_{idx:03d}"

        dt_data["actions"] = actions
        dt_data["decision_rules"] = repaired_rules
        return dt_data

    def _strict_check_step2_ai_output(self, dt_data: Dict[str, Any]) -> None:
        """
        Kiểm tra Step 2 theo schema strict sau normalize.

        Step 2 chỉ là Reduced Decision Logic Table:
        - không full_decision_rules
        - không reduction_steps
        - không coverage_refs
        - không testcase/testdata
        """
        required_top_keys = {
            "feature",
            "description",
            "decision_summary",
            "conditions",
            "actions",
            "decision_rules",
        }
        forbidden_top_keys = {
            "full_decision_rules",
            "reduction_steps",
            "testcases",
            "items",
            "coverage_refs",
            "final_output",
        }

        actual_top_keys = set(dt_data.keys())
        missing = sorted(required_top_keys - actual_top_keys)
        forbidden = sorted(actual_top_keys & forbidden_top_keys)
        extra = sorted(actual_top_keys - required_top_keys - forbidden_top_keys)
        errors: List[str] = []

        if missing:
            errors.append(f"Missing top-level keys: {missing}")
        if forbidden:
            errors.append(f"Forbidden top-level keys in Step 2: {forbidden}")
        if extra:
            errors.append(f"Unexpected top-level keys: {extra}")

        if not isinstance(dt_data.get("decision_summary"), dict):
            errors.append("decision_summary must be an object.")
        if not isinstance(dt_data.get("conditions"), list) or not dt_data.get("conditions"):
            errors.append("conditions must be a non-empty list.")
        if not isinstance(dt_data.get("actions"), list) or not dt_data.get("actions"):
            errors.append("actions must be a non-empty list.")
        if not isinstance(dt_data.get("decision_rules"), list) or not dt_data.get("decision_rules"):
            errors.append("decision_rules must be a non-empty list.")

        condition_ids: Set[str] = set()
        if isinstance(dt_data.get("conditions"), list):
            for idx, cond in enumerate(dt_data["conditions"], start=1):
                prefix = f"conditions[{idx}]"
                if not isinstance(cond, dict):
                    errors.append(f"{prefix} must be an object.")
                    continue

                allowed = {"id", "name", "source_fields", "values", "meaning_when_y", "meaning_when_n"}
                extra_keys = sorted(set(cond.keys()) - allowed)
                if extra_keys:
                    errors.append(f"{prefix} contains unexpected keys: {extra_keys}.")

                cid = self._clean_text(cond.get("id"))
                if not cid:
                    errors.append(f"{prefix}.id is missing or empty.")
                elif not re.match(r"^C\d+$", cid):
                    errors.append(f"{prefix}.id must match C1, C2, ... got '{cid}'.")
                else:
                    if cid in condition_ids:
                        errors.append(f"Duplicate condition id: {cid}")
                    condition_ids.add(cid)

                for key in ("name", "meaning_when_y", "meaning_when_n"):
                    if not self._clean_text(cond.get(key)):
                        errors.append(f"{prefix}.{key} is missing or empty.")

                source_fields = cond.get("source_fields")
                if not isinstance(source_fields, list) or not source_fields:
                    errors.append(f"{prefix}.source_fields must be a non-empty list.")

                values = cond.get("values")
                if not isinstance(values, list) or set(values) != {"Y", "N"}:
                    errors.append(f"{prefix}.values must be exactly ['Y', 'N'] or ['N', 'Y'].")

        action_ids: Set[str] = set()
        action_expected: Dict[str, str] = {}
        if isinstance(dt_data.get("actions"), list):
            for idx, action in enumerate(dt_data["actions"], start=1):
                prefix = f"actions[{idx}]"
                if not isinstance(action, dict):
                    errors.append(f"{prefix} must be an object.")
                    continue

                allowed = {"id", "name", "expected"}
                extra_keys = sorted(set(action.keys()) - allowed)
                if extra_keys:
                    errors.append(f"{prefix} contains unexpected keys: {extra_keys}.")

                aid = self._clean_text(action.get("id"))
                if not aid:
                    errors.append(f"{prefix}.id is missing or empty.")
                elif not re.match(r"^A\d+$", aid):
                    errors.append(f"{prefix}.id must match A1, A2, ... got '{aid}'.")
                else:
                    if aid in action_ids:
                        errors.append(f"Duplicate action id: {aid}")
                    action_ids.add(aid)
                    action_expected[aid] = self._clean_text(action.get("expected"))

                for key in ("name", "expected"):
                    if not self._clean_text(action.get(key)):
                        errors.append(f"{prefix}.{key} is missing or empty.")

        allowed_rule_types = {"happy_path", "single_fault", "boundary", "business_rule"}
        happy_path_count = 0
        rule_ids: Set[str] = set()

        if isinstance(dt_data.get("decision_rules"), list):
            for idx, rule in enumerate(dt_data["decision_rules"], start=1):
                prefix = f"decision_rules[{idx}]"
                if not isinstance(rule, dict):
                    errors.append(f"{prefix} must be an object.")
                    continue

                allowed = {"id", "type", "condition_states", "action_refs", "expected", "reduction_note"}
                extra_keys = sorted(set(rule.keys()) - allowed)
                if extra_keys:
                    errors.append(f"{prefix} contains unexpected keys: {extra_keys}.")

                rid = self._clean_text(rule.get("id"))
                if not rid:
                    errors.append(f"{prefix}.id is missing or empty.")
                elif not re.match(r"^DT_\d{3,}$", rid):
                    errors.append(f"{prefix}.id must match DT_001, DT_002, ... got '{rid}'.")
                else:
                    if rid in rule_ids:
                        errors.append(f"Duplicate decision rule id: {rid}")
                    rule_ids.add(rid)

                rule_type = self._clean_text(rule.get("type"))
                if rule_type not in allowed_rule_types:
                    errors.append(f"{prefix}.type must be one of {sorted(allowed_rule_types)}, got '{rule_type}'.")
                if rule_type == "happy_path":
                    happy_path_count += 1

                states = rule.get("condition_states")
                if not isinstance(states, dict) or not states:
                    errors.append(f"{prefix}.condition_states must be a non-empty object.")
                else:
                    state_keys = {self._clean_text(k) for k in states.keys()}
                    missing_states = sorted(condition_ids - state_keys)
                    extra_states = sorted(state_keys - condition_ids)
                    if missing_states:
                        errors.append(f"{prefix}.condition_states missing condition ids: {missing_states}.")
                    if extra_states:
                        errors.append(f"{prefix}.condition_states contains unknown condition ids: {extra_states}.")
                    for cid, state in states.items():
                        if self._normalize_dt_state(state) not in {"Y", "N", "-"}:
                            errors.append(f"{prefix}.condition_states[{cid}] must be Y/N/-.")

                    if rule_type == "happy_path" and condition_ids:
                        not_y = [cid for cid in condition_ids if states.get(cid) != "Y"]
                        if not_y:
                            errors.append(f"{prefix} happy_path must have all condition_states = Y. Not Y: {sorted(not_y)}")

                    if rule_type == "single_fault":
                        n_count = sum(1 for v in states.values() if self._normalize_dt_state(v) == "N")
                        if n_count != 1:
                            errors.append(f"{prefix} single_fault must contain exactly one N, got {n_count}.")

                action_refs = rule.get("action_refs")
                if not isinstance(action_refs, list) or not action_refs:
                    errors.append(f"{prefix}.action_refs must be a non-empty list.")
                else:
                    for ref in action_refs:
                        ref_clean = self._clean_text(ref)
                        if ref_clean not in action_ids:
                            errors.append(f"{prefix}.action_refs references unknown action id '{ref_clean}'.")

                expected = self._clean_text(rule.get("expected"))
                if not expected:
                    errors.append(f"{prefix}.expected is missing or empty.")
                elif isinstance(action_refs, list) and action_refs:
                    first_ref = self._clean_text(action_refs[0])
                    action_exp = action_expected.get(first_ref, "")
                    if action_exp and expected != action_exp:
                        errors.append(f"{prefix}.expected must equal expected of action_refs[0] ({first_ref}).")

                if not self._clean_text(rule.get("reduction_note")):
                    errors.append(f"{prefix}.reduction_note is missing or empty.")

        if happy_path_count != 1:
            errors.append(f"Step2 must contain exactly 1 happy_path rule, got {happy_path_count}.")

        if errors:
            raise RuntimeError("Step2 output does not match strict schema:\n- " + "\n- ".join(errors))

    def _normalize_step2_data(self, dt_data: Dict[str, Any]) -> Dict[str, Any]:
        # =========================================================
        # STEP 2 WHITELIST FILTER
        # Chỉ giữ đúng schema chuẩn.
        # AI sinh thêm key nào cũng tự loại bỏ.
        # =========================================================
        allowed_top_keys = {
            "feature",
            "description",
            "decision_summary",
            "conditions",
            "actions",
            "decision_rules",
        }

        if isinstance(dt_data, dict):
            dt_data = {
                key: value
                for key, value in dt_data.items()
                if key in allowed_top_keys
            }

        # Normalize conditions
        raw_conditions = dt_data.get("conditions")
        conditions: List[Dict[str, Any]] = []
        if isinstance(raw_conditions, list):
            for idx, cond in enumerate(raw_conditions, start=1):
                if not isinstance(cond, dict):
                    continue
                normalized = {
                    "id": self._clean_text(cond.get("id")) or f"C{idx}",
                    "name": self._clean_text(cond.get("name") or cond.get("description")),
                    "source_fields": [],
                    "values": ["Y", "N"],
                    "meaning_when_y": self._clean_text(cond.get("meaning_when_y")),
                    "meaning_when_n": self._clean_text(cond.get("meaning_when_n")),
                }

                source_fields = cond.get("source_fields")
                if isinstance(source_fields, list):
                    normalized["source_fields"] = [
                        self._clean_text(x) for x in source_fields if self._clean_text(x)
                    ]
                elif self._clean_text(source_fields):
                    normalized["source_fields"] = [self._clean_text(source_fields)]

                if not normalized["name"]:
                    normalized["name"] = f"Điều kiện {normalized['id']}"
                if not normalized["source_fields"]:
                    normalized["source_fields"] = ["Nghiệp vụ"]
                if not normalized["meaning_when_y"]:
                    normalized["meaning_when_y"] = f"{normalized['name']} được thỏa mãn"
                if not normalized["meaning_when_n"]:
                    normalized["meaning_when_n"] = f"{normalized['name']} không được thỏa mãn"

                conditions.append(normalized)
        dt_data["conditions"] = conditions

        # Normalize actions
        raw_actions = dt_data.get("actions")
        actions: List[Dict[str, Any]] = []
        if isinstance(raw_actions, list):
            for idx, action in enumerate(raw_actions, start=1):
                if not isinstance(action, dict):
                    continue
                name = self._clean_text(action.get("name") or action.get("description"))
                expected = self._clean_text(action.get("expected") or action.get("description") or name)
                actions.append({
                    "id": self._clean_text(action.get("id")) or f"A{idx}",
                    "name": name or expected or f"Hành động {idx}",
                    "expected": expected,
                })
        dt_data["actions"] = actions

        action_expected_map = {
            self._clean_text(action.get("id")): self._clean_text(action.get("expected"))
            for action in actions
            if self._clean_text(action.get("id"))
        }

        condition_ids = [self._clean_text(c.get("id")) for c in conditions if self._clean_text(c.get("id"))]

        # Normalize decision rules
        raw_rules = dt_data.get("decision_rules")
        rules: List[Dict[str, Any]] = []
        if isinstance(raw_rules, list):
            for idx, rule in enumerate(raw_rules, start=1):
                if not isinstance(rule, dict):
                    continue

                normalized_rule = self._normalize_step2_rule(rule)

                if not re.match(r"^DT_\d{3,}$", normalized_rule.get("id", "")):
                    normalized_rule["id"] = f"DT_{idx:03d}"

                states = normalized_rule.get("condition_states")
                if not isinstance(states, dict):
                    states = {}
                # Bảo đảm đủ condition id để Step 3 map được ổn định.
                normalized_states: Dict[str, str] = {}
                for cid in condition_ids:
                    normalized_states[cid] = self._normalize_dt_state(states.get(cid, "-"))
                normalized_rule["condition_states"] = normalized_states

                state_values = list(normalized_states.values())
                n_count = state_values.count("N")
                all_y = bool(state_values) and all(v == "Y" for v in state_values)

                if normalized_rule.get("type") not in {"happy_path", "single_fault", "boundary", "business_rule"}:
                    if all_y:
                        normalized_rule["type"] = "happy_path"
                    elif n_count == 1:
                        normalized_rule["type"] = "single_fault"
                    else:
                        normalized_rule["type"] = "business_rule"

                if not normalized_rule.get("action_refs") and actions:
                    normalized_rule["action_refs"] = [actions[0]["id"]]

                if not normalized_rule.get("expected") and normalized_rule.get("action_refs"):
                    normalized_rule["expected"] = action_expected_map.get(normalized_rule["action_refs"][0], "")

                if not normalized_rule.get("reduction_note"):
                    normalized_rule["reduction_note"] = "Rule được chuẩn hóa theo trạng thái điều kiện và expected được tham chiếu."

                rules.append(normalized_rule)

        dt_data["decision_rules"] = rules
        dt_data = self._rebuild_step2_summary(dt_data)
        return dt_data

    def _hard_check_step2_structure(self, dt_data: Dict[str, Any]) -> None:
        if not isinstance(dt_data, dict):
            raise RuntimeError("Step2 DT output must be a JSON object.")

        if not self._clean_text(dt_data.get("feature")):
            raise RuntimeError("Step2 missing 'feature'.")

        if not self._clean_text(dt_data.get("description")):
            raise RuntimeError("Step2 missing 'description'.")

        for forbidden_key in ("full_decision_rules", "reduction_steps", "coverage_refs", "testcases", "items"):
            if forbidden_key in dt_data:
                raise RuntimeError(f"Step2 must not contain '{forbidden_key}'.")

        conditions = dt_data.get("conditions")
        if not isinstance(conditions, list) or not conditions:
            raise RuntimeError("Step2 must contain non-empty 'conditions'.")

        actions = dt_data.get("actions")
        if not isinstance(actions, list) or not actions:
            raise RuntimeError("Step2 must contain non-empty 'actions'.")

        rules = dt_data.get("decision_rules")
        if not isinstance(rules, list) or not rules:
            raise RuntimeError("Step2 must contain non-empty 'decision_rules'.")

        condition_ids = set()
        for idx, cond in enumerate(conditions, start=1):
            if not isinstance(cond, dict):
                raise RuntimeError(f"Step2 conditions[{idx}] must be an object.")
            cid = self._clean_text(cond.get("id"))
            if not cid:
                raise RuntimeError(f"Step2 conditions[{idx}] missing 'id'.")
            if cid in condition_ids:
                raise RuntimeError(f"Step2 duplicate condition id: '{cid}'.")
            condition_ids.add(cid)
            if not self._clean_text(cond.get("name")):
                raise RuntimeError(f"Step2 conditions[{idx}] missing 'name'.")
            if not isinstance(cond.get("source_fields"), list) or not cond.get("source_fields"):
                raise RuntimeError(f"Step2 conditions[{idx}] missing non-empty 'source_fields'.")
            if not self._clean_text(cond.get("meaning_when_y")):
                raise RuntimeError(f"Step2 conditions[{idx}] missing 'meaning_when_y'.")
            if not self._clean_text(cond.get("meaning_when_n")):
                raise RuntimeError(f"Step2 conditions[{idx}] missing 'meaning_when_n'.")

        action_ids = set()
        for idx, action in enumerate(actions, start=1):
            if not isinstance(action, dict):
                raise RuntimeError(f"Step2 actions[{idx}] must be an object.")
            aid = self._clean_text(action.get("id"))
            if not aid:
                raise RuntimeError(f"Step2 actions[{idx}] missing 'id'.")
            if aid in action_ids:
                raise RuntimeError(f"Step2 duplicate action id: '{aid}'.")
            action_ids.add(aid)
            if not self._clean_text(action.get("name")):
                raise RuntimeError(f"Step2 actions[{idx}] missing 'name'.")
            if not self._clean_text(action.get("expected")):
                raise RuntimeError(f"Step2 actions[{idx}] missing 'expected'.")

        happy_path_count = 0
        rule_ids = set()
        for idx, rule in enumerate(rules, start=1):
            if not isinstance(rule, dict):
                raise RuntimeError(f"Step2 decision_rules[{idx}] must be an object.")

            rid = self._clean_text(rule.get("id"))
            if not rid:
                raise RuntimeError(f"Step2 decision_rules[{idx}] missing 'id'.")
            if rid in rule_ids:
                raise RuntimeError(f"Step2 duplicate decision rule id: '{rid}'.")
            rule_ids.add(rid)

            rule_type = self._clean_text(rule.get("type"))
            if not rule_type:
                raise RuntimeError(f"Step2 decision_rules[{idx}] missing 'type'.")
            if rule_type == "happy_path":
                happy_path_count += 1

            states = rule.get("condition_states")
            if not isinstance(states, dict) or not states:
                raise RuntimeError(f"Step2 decision_rules[{idx}] must have non-empty 'condition_states'.")

            state_keys = {self._clean_text(k) for k in states.keys()}
            if state_keys != condition_ids:
                raise RuntimeError(
                    f"Step2 decision_rules[{idx}].condition_states must contain exactly all condition ids. "
                    f"Missing={sorted(condition_ids - state_keys)}, Extra={sorted(state_keys - condition_ids)}"
                )

            action_refs = rule.get("action_refs")
            if not isinstance(action_refs, list) or not action_refs:
                raise RuntimeError(f"Step2 decision_rules[{idx}] must have non-empty 'action_refs'.")
            for ref in action_refs:
                if self._clean_text(ref) not in action_ids:
                    raise RuntimeError(f"Step2 decision_rules[{idx}] references unknown action id: {ref}")

            if not self._clean_text(rule.get("expected")):
                raise RuntimeError(f"Step2 decision_rules[{idx}] missing 'expected'.")
            if not self._clean_text(rule.get("reduction_note")):
                raise RuntimeError(f"Step2 decision_rules[{idx}] missing 'reduction_note'.")

        if happy_path_count != 1:
            raise RuntimeError(f"Step2 must contain exactly 1 happy_path rule, got {happy_path_count}.")

    # ==========================================================================
    # STEP 3 NORMALIZATION / HARD CHECK
    # ==========================================================================
    def _force_step3_feature(self, step3_data: Dict[str, Any], feature_key: str) -> Dict[str, Any]:
        step3_data["feature"] = feature_key
        return step3_data

    def _rebuild_step3_summary(self, step3_data: Dict[str, Any]) -> Dict[str, Any]:
        testcases = step3_data.get("testcases", [])
        total = len(testcases) if isinstance(testcases, list) else 0
        step3_data["testcase_summary"] = {"total_testcases": total}
        return step3_data

    def _normalize_step3_data(self, feature: str, step3_data: Dict[str, Any]) -> Dict[str, Any]:
        testcases = step3_data.get("testcases")
        if not isinstance(testcases, list):
            step3_data["testcases"] = []
            return step3_data

        normalized_testcases: List[Dict[str, Any]] = []

        for idx, tc in enumerate(testcases, start=1):
            if not isinstance(tc, dict):
                continue

            normalized = dict(tc)

            tc_id = self._clean_text(normalized.get("id"))
            if not tc_id:
                tc_id = build_default_testcase_id(feature, idx)
            normalized["id"] = tc_id

            normalized["name"] = self._clean_text(normalized.get("name"))
            normalized["description"] = self._clean_text(normalized.get("description"))
            normalized["objective"] = self._clean_text(normalized.get("objective"))
            normalized["expected"] = self._clean_text(normalized.get("expected"))
            normalized["priority"] = self._normalize_priority(normalized.get("priority"))
            normalized.pop("coverage_refs", None)

            inputs = normalized.get("inputs")
            if not isinstance(inputs, dict):
                inputs = {}
            cleaned_inputs: Dict[str, Any] = {}
            for k, v in inputs.items():
                key = str(k).strip()
                if not key:
                    continue
                cleaned_inputs[key] = "" if v is None else v
            normalized["inputs"] = cleaned_inputs

            decision_basis = normalized.get("decision_basis")
            if not isinstance(decision_basis, dict):
                decision_basis = {}

            decision_basis["rule_id"] = self._clean_text(decision_basis.get("rule_id"))

            states = decision_basis.get("condition_states")
            if not isinstance(states, dict):
                states = {}
            decision_basis["condition_states"] = {
                self._clean_text(k): self._normalize_dt_state(v)
                for k, v in states.items()
                if self._clean_text(k)
            }

            decision_basis["reduction_note"] = self._clean_text(
                decision_basis.get("reduction_note") or decision_basis.get("optimization_note")
            )
            normalized["decision_basis"] = decision_basis

            normalized_testcases.append(normalized)

        step3_data["testcases"] = normalized_testcases
        return step3_data

    def _hard_check_step3_structure(self, step3_data: Dict[str, Any]) -> None:
        if not isinstance(step3_data, dict):
            raise RuntimeError("Step3 output must be a JSON object.")

        if not self._clean_text(step3_data.get("feature")):
            raise RuntimeError("Step3 missing 'feature'.")

        if not self._clean_text(step3_data.get("description")):
            raise RuntimeError("Step3 missing 'description'.")

        testcases = step3_data.get("testcases")
        if not isinstance(testcases, list) or not testcases:
            raise RuntimeError("Step3 must contain non-empty 'testcases'.")

        ids = set()
        for idx, tc in enumerate(testcases, start=1):
            if not isinstance(tc, dict):
                raise RuntimeError(f"Step3 testcases[{idx}] must be an object.")

            tc_id = self._clean_text(tc.get("id"))
            if not tc_id:
                raise RuntimeError(f"Step3 testcases[{idx}] missing 'id'.")
            if tc_id in ids:
                raise RuntimeError(f"Step3 duplicate testcase id: '{tc_id}'.")
            ids.add(tc_id)

            for key in ("name", "description", "objective", "expected"):
                if not self._clean_text(tc.get(key)):
                    raise RuntimeError(f"Step3 testcases[{idx}] missing '{key}'.")

            coverage_refs = tc.get("coverage_refs")
            if not isinstance(coverage_refs, list) or not coverage_refs:
                raise RuntimeError(f"Step3 testcases[{idx}] must have non-empty 'coverage_refs'.")

            inputs = tc.get("inputs")
            if not isinstance(inputs, dict) or not inputs:
                raise RuntimeError(f"Step3 testcases[{idx}] must have non-empty 'inputs'.")

            decision_basis = tc.get("decision_basis")
            if not isinstance(decision_basis, dict):
                raise RuntimeError(f"Step3 testcases[{idx}] missing 'decision_basis'.")

            rule_id = decision_basis.get("rule_id")
            if not isinstance(rule_id, str) or not rule_id.strip():
                raise RuntimeError(f"Step3 testcases[{idx}] decision_basis.rule_id must be non-empty.")

            condition_states = decision_basis.get("condition_states")
            if not isinstance(condition_states, dict) or not condition_states:
                raise RuntimeError(
                    f"Step3 testcases[{idx}] decision_basis.condition_states must be non-empty."
                )

    def _raise_if_step3_warnings_are_severe(self, warnings: List[str]) -> None:
        severe = [
            w for w in warnings
            if any(marker.lower() in w.lower() for marker in self.STEP3_SEVERE_WARNING_MARKERS)
        ]
        if severe:
            raise RuntimeError(
                "Step3 validation produced severe warnings:\n- " + "\n- ".join(severe)
            )

    def _ensure_all_step1_coverage_used(self, step3_data: Dict[str, Any], step1_data: Dict[str, Any]) -> None:
        step1_ids = {
            str(item.get("id")).strip()
            for item in step1_data.get("coverage_items", [])
            if isinstance(item, dict) and str(item.get("id", "")).strip()
        }

        used_ids = set()
        for tc in step3_data.get("testcases", []):
            if not isinstance(tc, dict):
                continue
            for ref in tc.get("coverage_refs", []):
                ref_clean = str(ref).strip()
                if ref_clean:
                    used_ids.add(ref_clean)

        missing = sorted(step1_ids - used_ids)
        if missing:
            raise RuntimeError(
                "Step3 is missing coverage from Step1:\n- " + "\n- ".join(missing)
            )

    # ==========================================================================
    # STEP 1
    # ==========================================================================
    def _generate_step1(self, feature: str, exporter: DataExporter) -> Dict[str, Any]:
        step_start = time.perf_counter()

        self._log("STEP 1: build prompt")
        step1_prompt = self.prompt_loader.build_step1_prompt(feature)
        feature_key = self._resolve_feature_key_from_prompt(step1_prompt, feature)
        self._log(f"STEP 1: feature chuẩn hóa = '{feature_key}'")
        self._log(f"STEP 1: độ dài prompt = {len(step1_prompt):,} ký tự")

        self._log("STEP 1: gọi AI để phân tích EP + BVA ...")
        llm_start = time.perf_counter()
        raw_output = self.llm_client.generate(step1_prompt)
        raw_txt_path = self._save_raw_output(exporter, "step1_raw.txt", raw_output)
        self._raise_if_llm_output_empty(raw_output, "Step1", raw_txt_path)
        llm_elapsed = time.perf_counter() - llm_start
        self._log(f"STEP 1: AI trả kết quả sau {self._format_seconds(llm_elapsed)}")
        self._log(f"STEP 1: lưu raw output tại {raw_txt_path}")

        self._log("STEP 1: parse JSON ...")
        parsed = self.parser.parse_json(raw_output)
        if not parsed.ok:
            raise RuntimeError(
                f"Step1 parse error: {parsed.error}. Raw output saved at: {raw_txt_path}"
            )

        step1_data = parsed.data
        if not isinstance(step1_data, dict):
            raise RuntimeError("Step1 output must be a JSON object.")

        self._log("STEP 1: normalize + rebuild summary ...")
        step1_data = self._force_step1_feature(step1_data, feature_key)
        step1_data = self._normalize_step1_data(step1_data)
        step1_data = self._rebuild_step1_summary(step1_data)

        self._log("STEP 1: hard-check structure ...")
        try:
            self._hard_check_step1_structure(step1_data)
        except Exception:
            self._log("STEP 1: hard-check failed, ghi step1_invalid.json")
            invalid_json_path = exporter.write_raw_json(step1_data, filename="step1_invalid.json")
            self._export_step1_excel_safely(invalid_json_path)
            raise

        try:
            self._log("STEP 1: validate output ...")
            result = self.step1_validator.validate_or_raise(step1_data)
            if result.warnings:
                self._log(f"STEP 1: có {len(result.warnings)} warning")
                self._raise_if_step1_warnings_are_severe(result.warnings)
        except Exception:
            self._log("STEP 1: validate failed, ghi step1_invalid.json")
            invalid_json_path = exporter.write_raw_json(step1_data, filename="step1_invalid.json")
            self._export_step1_excel_safely(invalid_json_path)
            raise

        self._log("STEP 1: ghi step1 JSON + Excel ...")
        step1_json_path = exporter.write_raw_json(step1_data, filename="step1.json")
        self._export_step1_excel_safely(step1_json_path)

        items = step1_data.get("coverage_items", [])
        self._log(f"STEP 1: số coverage_items = {len(items) if isinstance(items, list) else 0}")
        self._log(f"STEP 1: lưu JSON tại {step1_json_path}")

        step_elapsed = time.perf_counter() - step_start
        self._log(f"STEP 1: hoàn tất trong {self._format_seconds(step_elapsed)}")

        return step1_data

    # ==========================================================================
    # STEP 2: DECISION TABLE TRUNG GIAN
    # ==========================================================================
    def _generate_step2_decision_table(
        self,
        feature: str,
        exporter: DataExporter,
        step1_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        step_start = time.perf_counter()

        self._log("STEP 2: build prompt cho Reduced Decision Logic Table")
        compact_step1 = self._compact_step1_for_step2(step1_data) if isinstance(step1_data, dict) else None
        step2_prompt = self.prompt_loader.build_step2_prompt(feature, compact_step1)
        feature_key = self._resolve_feature_key_from_prompt(step2_prompt, feature)
        self._log(f"STEP 2: feature chuẩn hóa = '{feature_key}'")
        self._log(f"STEP 2: độ dài prompt = {len(step2_prompt):,} ký tự")

        self._log("STEP 2: gọi AI để sinh reduced decision rules ...")
        llm_start = time.perf_counter()
        raw_output = self.llm_client.generate(step2_prompt)
        raw_txt_path = self._save_raw_output(exporter, "step2_dt_raw.txt", raw_output)
        self._raise_if_llm_output_empty(raw_output, "Step2 DT", raw_txt_path)
        llm_elapsed = time.perf_counter() - llm_start
        self._log(f"STEP 2: AI trả kết quả sau {self._format_seconds(llm_elapsed)}")
        self._log(f"STEP 2: lưu raw output tại {raw_txt_path}")

        self._log("STEP 2: parse JSON ...")
        parsed = self.parser.parse_json(raw_output)
        if not parsed.ok:
            raise RuntimeError(
                f"Step2 DT parse error: {parsed.error}. Raw output saved at: {raw_txt_path}"
            )

        dt_data = parsed.data
        if not isinstance(dt_data, dict):
            raise RuntimeError("Step2 DT output must be a JSON object.")

        self._log("STEP 2: normalize + rebuild summary ...")
        dt_data = self._force_step2_feature(dt_data, feature_key)
        dt_data = self._normalize_step2_data(dt_data)
        dt_data = self._align_step2_with_expected_contract(dt_data, step1_data, feature_key)
        dt_data = self._rebuild_step2_summary(dt_data)

        self._log("STEP 2: strict schema check + validate ...")
        try:
            self._strict_check_step2_ai_output(dt_data)
            self._hard_check_step2_structure(dt_data)
            result = self.step2_validator.validate_or_raise(dt_data, step1_data=step1_data)
            if result.warnings:
                self._log(f"STEP 2: có {len(result.warnings)} warning")
                severe = [
                    w for w in result.warnings
                    if any(marker.lower() in w.lower() for marker in self.STEP2_SEVERE_WARNING_MARKERS)
                ]
                if severe:
                    raise RuntimeError(
                        "Step2 validation produced severe warnings:\n- " + "\n- ".join(severe)
                    )
        except Exception:
            self._log("STEP 2: validate failed, ghi step2_dt_invalid.json")
            invalid_json_path = exporter.write_raw_json(dt_data, filename="step2_dt_invalid.json")
            try:
                self._export_step2_excel_safely(dt_data, exporter)
            except Exception:
                pass
            self._log(f"STEP 2: invalid JSON tại {invalid_json_path}")
            raise

        dt_json_path = exporter.write_raw_json(dt_data, filename="step2_dt.json")
        self._export_step2_excel_safely(dt_data, exporter)
        self._log(f"STEP 2: số decision_rules = {len(dt_data.get('decision_rules', []))}")
        self._log(f"STEP 2: lưu JSON tại {dt_json_path}")

        step_elapsed = time.perf_counter() - step_start
        self._log(f"STEP 2: hoàn tất trong {self._format_seconds(step_elapsed)}")
        return dt_data

    # ==========================================================================
    # STEP 3 COVERAGE-PRESERVING BUILDER (CODE-ONLY MAPPING)
    # ==========================================================================
    def _get_step3_happy_rule(self, dt_data: Dict[str, Any]) -> Dict[str, Any]:
        rules = dt_data.get("decision_rules", [])
        if isinstance(rules, list):
            for rule in rules:
                if isinstance(rule, dict) and self._clean_text(rule.get("type")) == "happy_path":
                    return rule
        return {}

    def _build_condition_index_for_step3(self, dt_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        conditions = dt_data.get("conditions", [])
        if not isinstance(conditions, list):
            return out

        for cond in conditions:
            if not isinstance(cond, dict):
                continue
            cid = self._clean_text(cond.get("id"))
            if not cid:
                continue
            source_fields = cond.get("source_fields")
            if not isinstance(source_fields, list):
                source_fields = []
            out[cid] = {
                "id": cid,
                "name": self._clean_text(cond.get("name")),
                "source_fields": [self._clean_text(f) for f in source_fields if self._clean_text(f)],
                "meaning_when_y": self._clean_text(cond.get("meaning_when_y")),
                "meaning_when_n": self._clean_text(cond.get("meaning_when_n")),
            }
        return out

    def _build_step1_value_index_for_step3(
        self,
        feature: str,
        step1_data: Dict[str, Any],
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        fields = get_feature_item_fields(feature)
        index: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
            field: {"valid": [], "invalid": []}
            for field in fields
        }

        items = step1_data.get("coverage_items", [])
        if not isinstance(items, list):
            return index

        for item in items:
            if not isinstance(item, dict):
                continue
            field = self._clean_text(item.get("field"))
            validity = self._clean_text(item.get("validity")).lower()
            if field not in index or validity not in {"valid", "invalid"}:
                continue
            index[field][validity].append(item)
        return index

    def _representative_value(self, coverage_item: Dict[str, Any]) -> str:
        value = coverage_item.get("representative_value")
        return "" if value is None else str(value)

    def _coverage_text_for_step3(self, coverage_item: Dict[str, Any]) -> str:
        boundary = coverage_item.get("boundary") if isinstance(coverage_item.get("boundary"), dict) else {}
        return " ".join([
            self._clean_text(coverage_item.get("description")),
            self._clean_text(coverage_item.get("rule")),
            self._clean_text(coverage_item.get("expected_class")),
            self._clean_text(boundary.get("kind")),
            self._clean_text(boundary.get("point")),
        ]).lower()

    def _pick_valid_value_for_step3(
        self,
        field: str,
        value_index: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> str:
        candidates = value_index.get(field, {}).get("valid", [])
        if not candidates:
            return ""

        for item in candidates:
            if self._clean_text(item.get("technique")).upper() == "EP":
                return self._representative_value(item)

        for point in ("N", "MIN", "MAX", "MIN+1", "MAX-1"):
            for item in candidates:
                boundary = item.get("boundary") if isinstance(item.get("boundary"), dict) else {}
                if self._clean_text(boundary.get("point")).upper() == point:
                    return self._representative_value(item)

        return self._representative_value(candidates[0])

    def _build_default_valid_row_for_step3(
        self,
        feature: str,
        value_index: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> Dict[str, Any]:
        fields = get_feature_item_fields(feature)
        row: Dict[str, Any] = {}
        for field in fields:
            row[field] = self._pick_valid_value_for_step3(field, value_index)

        # Rule quan hệ phổ biến: nếu có Password và ConfirmPassword thì mặc định phải khớp nhau.
        if "Password" in row and "ConfirmPassword" in row:
            row["ConfirmPassword"] = row["Password"]
        return row

    def _condition_text_for_step3(self, condition: Dict[str, Any], state: str) -> str:
        state = self._clean_text(state).upper()
        if state == "Y":
            return " ".join([
                self._clean_text(condition.get("name")),
                self._clean_text(condition.get("meaning_when_y")),
            ]).lower()
        if state == "N":
            return " ".join([
                self._clean_text(condition.get("name")),
                self._clean_text(condition.get("meaning_when_n")),
            ]).lower()
        return self._clean_text(condition.get("name")).lower()

    def _filter_invalid_coverages_for_condition(
        self,
        *,
        field: str,
        expected: str,
        condition: Dict[str, Any],
        state: str,
        value_index: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> List[Dict[str, Any]]:
        """
        Lấy TẤT CẢ coverage invalid phù hợp với condition của Decision Table.
        Đây là phần giúp Step 3 bảo toàn EP/BVA coverage.
        """
        candidates = list(value_index.get(field, {}).get("invalid", []))
        if not candidates:
            return []

        expected_clean = self._clean_text(expected)
        expected_lower = expected_clean.lower()
        condition_text = self._condition_text_for_step3(condition, state)

        exact_expected_matches = [
            item for item in candidates
            if self._clean_text(item.get("expected_class")) == expected_clean
        ]
        if exact_expected_matches:
            candidates = exact_expected_matches

        empty_markers = (
            "rỗng", "trống", "không được để trống", "bắt buộc", "được nhập",
            "empty", "required", "blank",
        )
        duplicate_markers = ("tồn tại", "đã đăng ký", "trùng", "exist", "duplicate")
        mismatch_markers = ("không khớp", "khác", "không trùng", "mismatch", "not match")

        asks_empty = any(marker in condition_text for marker in empty_markers) or any(
            marker in expected_lower for marker in ("điền", "trống", "rỗng", "required", "empty", "blank")
        )
        asks_duplicate = any(marker in condition_text for marker in duplicate_markers) or any(
            marker in expected_lower for marker in duplicate_markers
        )
        asks_mismatch = any(marker in condition_text for marker in mismatch_markers) or any(
            marker in expected_lower for marker in mismatch_markers
        )

        if asks_empty:
            empty_items = [item for item in candidates if self._representative_value(item) == ""]
            if empty_items:
                return empty_items

        if asks_duplicate:
            duplicate_items = [
                item for item in candidates
                if any(marker in self._coverage_text_for_step3(item) for marker in duplicate_markers)
            ]
            if duplicate_items:
                return duplicate_items

        if asks_mismatch:
            mismatch_items = [
                item for item in candidates
                if any(marker in self._coverage_text_for_step3(item) for marker in mismatch_markers)
            ]
            if mismatch_items:
                return mismatch_items

        return candidates

    def _get_changed_conditions_for_rule(
        self,
        rule: Dict[str, Any],
        happy_states: Dict[str, Any],
    ) -> List[Tuple[str, str]]:
        states = rule.get("condition_states")
        if not isinstance(states, dict):
            return []

        changed: List[Tuple[str, str]] = []
        for cid, state in states.items():
            cid_clean = self._clean_text(cid)
            state_clean = self._normalize_dt_state(state)
            if not cid_clean or state_clean == "-":
                continue

            happy_state = self._normalize_dt_state(happy_states.get(cid_clean))
            if happy_state and state_clean == happy_state:
                continue

            changed.append((cid_clean, state_clean))
        return changed

    def _build_variants_for_changed_condition(
        self,
        *,
        condition_id: str,
        state: str,
        expected: str,
        fields: List[str],
        condition_index: Dict[str, Dict[str, Any]],
        value_index: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> List[Dict[str, Any]]:
        condition = condition_index.get(condition_id, {})
        source_fields = condition.get("source_fields", [])
        if not isinstance(source_fields, list):
            source_fields = []

        variants: List[Dict[str, Any]] = []
        for field in source_fields:
            field_clean = self._clean_text(field)
            if field_clean not in fields:
                continue

            coverages = self._filter_invalid_coverages_for_condition(
                field=field_clean,
                expected=expected,
                condition=condition,
                state=state,
                value_index=value_index,
            )

            for coverage in coverages:
                if not isinstance(coverage, dict):
                    continue
                variants.append({
                    "field": field_clean,
                    "value": self._representative_value(coverage),
                    "coverage_id": self._clean_text(coverage.get("id")),
                    "coverage_description": self._clean_text(coverage.get("description")),
                })
        return variants

    def _resolve_expected_for_step3(self, expected: str, row: Dict[str, Any]) -> str:
        expected_clean = self._clean_text(expected)
        if expected_clean in row:
            return "" if row.get(expected_clean) is None else str(row.get(expected_clean))
        return expected_clean

    def _make_final_row_for_step3(
        self,
        *,
        feature: str,
        fields: List[str],
        row_values: Dict[str, Any],
        expected: str,
        index: int,
    ) -> Dict[str, Any]:
        final_row: Dict[str, Any] = {
            "Testcase": build_default_testcase_id(feature, index)
        }
        for field in fields:
            final_row[field] = row_values.get(field, "")
        final_row["Expected"] = self._resolve_expected_for_step3(expected, final_row)
        return final_row

    def _build_happy_path_rows_for_step3(
        self,
        *,
        feature: str,
        rule: Dict[str, Any],
        fields: List[str],
        value_index: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> List[Dict[str, Any]]:
        """
        Happy path sinh:
        - 1 dòng all-valid mặc định
        - thêm biến thể valid để giữ các coverage valid/BVA hợp lệ của Step 1
        """
        expected = self._clean_text(rule.get("expected"))
        base_row = self._build_default_valid_row_for_step3(feature, value_index)
        rows: List[Dict[str, Any]] = []

        def append_row(row_values: Dict[str, Any]) -> None:
            row = {field: row_values.get(field, "") for field in fields}
            row["Expected"] = self._resolve_expected_for_step3(expected, row)
            rows.append(row)

        append_row(dict(base_row))

        for field in fields:
            valid_items = value_index.get(field, {}).get("valid", [])
            for coverage in valid_items:
                if not isinstance(coverage, dict):
                    continue

                value = self._representative_value(coverage)
                if str(base_row.get(field, "")) == value:
                    continue

                row_values = dict(base_row)
                row_values[field] = value

                if field == "Password" and "ConfirmPassword" in row_values:
                    row_values["ConfirmPassword"] = value

                if field == "ConfirmPassword" and "Password" in row_values:
                    if row_values.get("ConfirmPassword") != row_values.get("Password"):
                        continue

                append_row(row_values)

        return rows

    def _build_final_items_fallback_from_step1_step2(
        self,
        feature: str,
        step1_data: Dict[str, Any],
        dt_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Coverage-preserving mapping giữa EP/BVA và Decision Table.

        Khác mapping 1-1 cũ, một decision_rule có thể sinh nhiều testcase nếu condition lỗi
        tương ứng với nhiều coverage_items của Step 1.
        """
        from itertools import product

        feature_key = normalize_feature_name(feature)
        fields = get_feature_item_fields(feature_key)
        value_index = self._build_step1_value_index_for_step3(feature_key, step1_data)
        condition_index = self._build_condition_index_for_step3(dt_data)

        happy_rule = self._get_step3_happy_rule(dt_data)
        happy_states = happy_rule.get("condition_states") if isinstance(happy_rule.get("condition_states"), dict) else {}

        rules = dt_data.get("decision_rules", [])
        if not isinstance(rules, list) or not rules:
            raise RuntimeError("Cannot build Step3 by code: Step2 decision_rules is empty.")

        rows: List[Dict[str, Any]] = []
        seen_signatures: Set[tuple] = set()

        def append_final_row(row: Dict[str, Any]) -> None:
            signature = tuple(
                (key, "" if row.get(key) is None else str(row.get(key)))
                for key in [*fields, "Expected"]
            )
            if signature in seen_signatures:
                return
            seen_signatures.add(signature)
            row["Testcase"] = build_default_testcase_id(feature_key, len(rows) + 1)
            rows.append(row)

        for rule in rules:
            if not isinstance(rule, dict):
                continue

            rule_type = self._clean_text(rule.get("type"))
            expected = self._clean_text(rule.get("expected"))

            if rule_type == "happy_path":
                happy_rows = self._build_happy_path_rows_for_step3(
                    feature=feature_key,
                    rule=rule,
                    fields=fields,
                    value_index=value_index,
                )
                for row_values in happy_rows:
                    final_row = self._make_final_row_for_step3(
                        feature=feature_key,
                        fields=fields,
                        row_values=row_values,
                        expected=expected,
                        index=len(rows) + 1,
                    )
                    append_final_row(final_row)
                continue

            changed_conditions = self._get_changed_conditions_for_rule(rule, happy_states)
            base_row = self._build_default_valid_row_for_step3(feature_key, value_index)

            if not changed_conditions:
                final_row = self._make_final_row_for_step3(
                    feature=feature_key,
                    fields=fields,
                    row_values=base_row,
                    expected=expected,
                    index=len(rows) + 1,
                )
                append_final_row(final_row)
                continue

            variant_groups: List[List[Dict[str, Any]]] = []
            for condition_id, state in changed_conditions:
                variants = self._build_variants_for_changed_condition(
                    condition_id=condition_id,
                    state=state,
                    expected=expected,
                    fields=fields,
                    condition_index=condition_index,
                    value_index=value_index,
                )

                if not variants:
                    variants = [{"field": "", "value": "", "coverage_id": "", "coverage_description": ""}]
                variant_groups.append(variants)

            for combination in product(*variant_groups):
                row_values = dict(base_row)

                for variant in combination:
                    field = self._clean_text(variant.get("field"))
                    if not field or field not in fields:
                        continue

                    row_values[field] = variant.get("value", "")

                    if (
                        field == "ConfirmPassword"
                        and "Password" in row_values
                        and row_values.get("ConfirmPassword") == row_values.get("Password")
                    ):
                        row_values["ConfirmPassword"] = str(row_values.get("Password", "")) + "x"

                final_row = self._make_final_row_for_step3(
                    feature=feature_key,
                    fields=fields,
                    row_values=row_values,
                    expected=expected,
                    index=len(rows) + 1,
                )
                append_final_row(final_row)

        if not rows:
            raise RuntimeError("Cannot build Step3 by code: no final items generated.")

        return {
            "feature": feature_key,
            "description": (
                "Bộ test data cuối cùng được sinh bằng coverage-preserving mapping "
                "giữa Step 1 EP/BVA và Step 2 Decision Table."
            ),
            "items": rows,
        }

    # ==========================================================================
    # STEP 3: FINAL TESTCASES
    # ==========================================================================
    def _generate_step3_final(
        self,
        feature: str,
        step1_data: Dict[str, Any],
        dt_data: Dict[str, Any],
        exporter: DataExporter,
    ) -> Tuple[Path, Dict[str, Any]]:
        """
        STEP 3 code-only:
        - Không gọi AI.
        - Sinh final.json bằng coverage-preserving mapping giữa Step 1 và Step 2.
        - Output schema phẳng: feature, description, items.
        """
        step_start = time.perf_counter()
        feature_key = normalize_feature_name(step1_data.get("feature", feature))

        self._log("STEP 3: bỏ gọi AI, sinh final testcases bằng coverage-preserving mapping Step1 + Step2 ...")

        final_data = self._build_final_items_fallback_from_step1_step2(
            feature=feature_key,
            step1_data=step1_data,
            dt_data=dt_data,
        )

        final_data = self._force_step3_feature(final_data, feature_key)
        final_data = self._normalize_step3_data(feature_key, final_data)

        self._log("STEP 3: hard-check output ...")
        self._hard_check_step3_structure(final_data, feature_key)

        exporter.write_raw_json(
            {
                "fallback_used": True,
                "mode": "code_only_coverage_preserving",
                "reason": "Step 3 sinh bằng code coverage-preserving mapping, không gọi AI.",
                "total_items": len(final_data.get("items", [])),
            },
            filename="final_fallback_info.json",
        )

        self._log("STEP 3: ghi final JSON ...")
        final_json_path = exporter.write_raw_json(final_data, filename="final.json")
        self._log(f"STEP 3: lưu JSON tại {final_json_path}")

        step_elapsed = time.perf_counter() - step_start
        self._log(f"STEP 3: hoàn tất trong {self._format_seconds(step_elapsed)}")

        return final_json_path, final_data

    # ==========================================================================
    # EXPORT PROCESSED
    # ==========================================================================
    def _export_processed_files(
        self,
        feature: str,
        final_data: Dict[str, Any],
        formats: List[str],
        exporter: DataExporter,
    ) -> List[str]:
        export_start = time.perf_counter()

        self._log("EXPORT: bắt đầu export processed files ...")

        items = final_data.get("items")
        if not isinstance(items, list) or not items:
            raise RuntimeError("Final JSON is invalid: 'items' must be a non-empty list.")

        # Tương thích exporter hiện tại nếu exporter vẫn nhận schema cũ id/inputs/expected.
        fields = get_feature_item_fields(feature)
        exporter_rows: List[Dict[str, Any]] = []
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            testcase_id = self._clean_text(item.get("Testcase")) or build_default_testcase_id(feature, idx)
            exporter_rows.append(
                {
                    "id": testcase_id,
                    "inputs": {
                        field: item.get(field, "")
                        for field in fields
                    },
                    "expected": item.get("Expected", ""),
                }
            )

        paths = exporter.export_feature_items(
            feature=feature,
            items=exporter_rows,
            formats=formats,
        )

        export_elapsed = time.perf_counter() - export_start
        self._log(f"EXPORT: hoàn tất trong {self._format_seconds(export_elapsed)}")
        for p in paths:
            self._log(f"EXPORT: {p}")

        return paths

    # ==========================================================================
    # SAFE EXCEL EXPORT FOR STEP 2
    # ==========================================================================
    def _export_step2_excel_safely(self, dt_data: Dict[str, Any], exporter: DataExporter) -> None:
        try:
            excel_path = exporter._get_run_file_path("step2_dt.xlsx")
            export_step2_to_excel(dt_data, excel_path)
            self._log(f"STEP 2: đã export Excel {excel_path}")
        except Exception as exc:
            self._log(f"Warning: Step2 Excel export failed: {exc}")

    # ==========================================================================
    # SAFE EXCEL EXPORT FOR STEP 1
    # ==========================================================================
    def _export_step1_excel_safely(self, json_path: Path) -> None:
        try:
            excel_path = json_path.with_name("step1.xlsx")
            export_step1_to_excel(json_path, excel_path)
            self._log(f"STEP 1: đã export Excel {excel_path}")
        except Exception as exc:
            self._log(f"Warning: Step1 Excel export failed: {exc}")

    # ===========================================================================
    # OVERRIDE STEP 3 FRAMEWORK-READY METHODS
    # Added at end of class to override older duplicate Step 3 methods above.
    # ===========================================================================
    def _normalize_framework_item(self, feature: str, item: Dict[str, Any], index: int) -> Dict[str, Any]:
        fields = get_feature_item_fields(feature)
        testcase_id = item.get("Testcase", item.get("testcase", item.get("id")))
        if not isinstance(testcase_id, str) or not testcase_id.strip():
            testcase_id = build_default_testcase_id(feature, index)

        inputs = item.get("inputs")
        if not isinstance(inputs, dict):
            inputs = item

        row: Dict[str, Any] = {"Testcase": testcase_id.strip()}
        for field in fields:
            value = inputs.get(field, item.get(field, ""))
            row[field] = "" if value is None else value

        expected = item.get("Expected", item.get("expected", ""))
        row["Expected"] = "" if expected is None else str(expected)
        return row

    def _normalize_step3_data(self, feature: str, step3_data: Dict[str, Any]) -> Dict[str, Any]:
        items = step3_data.get("items")
        if not isinstance(items, list):
            items = step3_data.get("testcases")

        if not isinstance(items, list):
            step3_data["items"] = []
            return step3_data

        normalized_items: List[Dict[str, Any]] = []
        seen_signatures: Set[tuple] = set()
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            row = self._normalize_framework_item(feature, item, idx)
            signature = tuple((k, str(v)) for k, v in row.items() if k != "Testcase")
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            row["Testcase"] = build_default_testcase_id(feature, len(normalized_items) + 1)
            normalized_items.append(row)

        step3_data["items"] = normalized_items
        step3_data.pop("testcases", None)
        step3_data.pop("testcase_summary", None)
        return step3_data

    def _hard_check_step3_structure(self, step3_data: Dict[str, Any], feature: str) -> None:
        if not isinstance(step3_data, dict):
            raise RuntimeError("Step3 output must be a JSON object.")
        if not self._clean_text(step3_data.get("feature")):
            raise RuntimeError("Step3 missing 'feature'.")
        if not self._clean_text(step3_data.get("description")):
            raise RuntimeError("Step3 missing 'description'.")

        items = step3_data.get("items")
        if not isinstance(items, list) or not items:
            raise RuntimeError("Step3 must contain non-empty 'items'.")

        expected_fields = get_feature_item_fields(feature)
        required_keys = ["Testcase", *expected_fields, "Expected"]
        allowed_keys = set(required_keys)
        forbidden_keys = {
            "id", "name", "description", "objective", "coverage_refs", "decision_basis",
            "inputs", "priority", "expected", "testcase",
        }

        testcase_ids: Set[str] = set()
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                raise RuntimeError(f"Step3 items[{idx}] must be an object.")
            for key in forbidden_keys:
                if key in item:
                    raise RuntimeError(f"Step3 items[{idx}] must not contain intermediate key '{key}'.")
            missing = [key for key in required_keys if key not in item]
            extra = sorted(set(item.keys()) - allowed_keys)
            if missing:
                raise RuntimeError(f"Step3 items[{idx}] missing required keys: {missing}.")
            if extra:
                raise RuntimeError(f"Step3 items[{idx}] contains unexpected keys: {extra}.")
            testcase_id = self._clean_text(item.get("Testcase"))
            if not testcase_id:
                raise RuntimeError(f"Step3 items[{idx}] missing 'Testcase'.")
            if testcase_id in testcase_ids:
                raise RuntimeError(f"Step3 duplicate Testcase: '{testcase_id}'.")
            testcase_ids.add(testcase_id)
            if not self._clean_text(item.get("Expected")):
                raise RuntimeError(f"Step3 items[{idx}] missing 'Expected'.")
