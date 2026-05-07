from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from testdata_generation.engine.exporters import DataExporter, export_step1_to_excel, export_step2_to_excel
from testdata_generation.engine.feature_item_schema import (
    build_default_testcase_id,
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
        vào tên field cụ thể như Phone/Register.
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
        conditions = dt_data.get("conditions", [])
        actions = dt_data.get("actions", [])
        full_rules = dt_data.get("full_decision_rules", [])
        reduced_rules = dt_data.get("decision_rules", [])

        condition_count = len(conditions) if isinstance(conditions, list) else 0
        action_count = len(actions) if isinstance(actions, list) else 0
        full_rule_count = len(full_rules) if isinstance(full_rules, list) else 0
        reduced_rule_count = len(reduced_rules) if isinstance(reduced_rules, list) else 0
        full_combination_count = 2 ** condition_count if condition_count > 0 else 0

        dt_data["decision_summary"] = {
            "condition_count": condition_count,
            "action_count": action_count,
            "full_combination_count": full_combination_count,
            "full_rule_count": full_rule_count,
            "reduced_rule_count": reduced_rule_count,
        }
        return dt_data

    @staticmethod
    def _normalize_dt_state(value: Any) -> str:
        raw = str(value or "").strip().upper()
        return raw if raw in {"Y", "N", "-"} else str(value or "").strip()

    def _normalize_step2_rule(self, rule: Dict[str, Any], *, full_table: bool) -> Dict[str, Any]:
        normalized = dict(rule)
        normalized["id"] = self._clean_text(normalized.get("id"))
        normalized["type"] = "full_combination" if full_table else self._normalize_step2_rule_type(normalized.get("type"))

        states = normalized.get("condition_states")
        if not isinstance(states, dict):
            states = {}
        normalized["condition_states"] = {
            self._clean_text(k): self._normalize_dt_state(v)
            for k, v in states.items()
            if self._clean_text(k)
        }

        action_refs = self._dedupe_string_list(normalized.get("action_refs"))
        if not action_refs and self._clean_text(normalized.get("action")):
            action_refs = [self._clean_text(normalized.get("action"))]
        normalized["action_refs"] = action_refs
        normalized.pop("action", None)

        normalized.pop("coverage_refs", None)
        normalized["expected"] = self._clean_text(normalized.get("expected") or normalized.get("description"))

        if full_table:
            normalized["combination_note"] = self._clean_text(
                normalized.get("combination_note") or normalized.get("reduction_note") or normalized.get("optimization_note")
            )
        else:
            normalized["reduction_note"] = self._clean_text(
                normalized.get("reduction_note") or normalized.get("optimization_note") or normalized.get("combination_note")
            )
        return normalized

    def _strict_check_step2_ai_output(self, dt_data: Dict[str, Any]) -> None:
        """Kiểm tra Step 2 theo schema strict, không tự sửa output AI."""
        required_top_keys = {
            "feature",
            "description",
            "decision_summary",
            "conditions",
            "actions",
            "decision_rules",
        }
        actual_top_keys = set(dt_data.keys())
        missing = sorted(required_top_keys - actual_top_keys)
        extra = sorted(actual_top_keys - required_top_keys)
        errors: List[str] = []

        if missing:
            errors.append(f"Missing top-level keys: {missing}")
        if extra:
            errors.append(f"Unexpected top-level keys: {extra}. Step 2 must not output full_decision_rules/reduction_steps or other extra keys.")

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
                if "description" in cond:
                    errors.append(f"{prefix} must use key 'name', not 'description'.")
                cid = self._clean_text(cond.get("id"))
                if cid:
                    condition_ids.add(cid)
                for key in ("id", "name", "source_fields", "values", "meaning_when_y", "meaning_when_n"):
                    if key not in cond:
                        errors.append(f"{prefix} missing key '{key}'.")

        action_ids: Set[str] = set()
        if isinstance(dt_data.get("actions"), list):
            for idx, action in enumerate(dt_data["actions"], start=1):
                prefix = f"actions[{idx}]"
                if not isinstance(action, dict):
                    errors.append(f"{prefix} must be an object.")
                    continue
                if "description" in action:
                    errors.append(f"{prefix} must use key 'name', not 'description'.")
                aid = self._clean_text(action.get("id"))
                if aid:
                    action_ids.add(aid)
                for key in ("id", "name", "expected"):
                    if key not in action:
                        errors.append(f"{prefix} missing key '{key}'.")

        if isinstance(dt_data.get("decision_rules"), list):
            for idx, rule in enumerate(dt_data["decision_rules"], start=1):
                prefix = f"decision_rules[{idx}]"
                if not isinstance(rule, dict):
                    errors.append(f"{prefix} must be an object.")
                    continue
                if "conditions" in rule:
                    errors.append(f"{prefix} must use key 'condition_states', not 'conditions'.")
                if "coverage_refs" in rule:
                    errors.append(f"{prefix} must not contain 'coverage_refs' according to Step 2 schema.")
                for key in ("id", "type", "condition_states", "action_refs", "expected", "reduction_note"):
                    if key not in rule:
                        errors.append(f"{prefix} missing key '{key}'.")
                states = rule.get("condition_states")
                if isinstance(states, dict) and condition_ids:
                    state_keys = {self._clean_text(k) for k in states.keys()}
                    missing_states = sorted(condition_ids - state_keys)
                    extra_states = sorted(state_keys - condition_ids)
                    if missing_states:
                        errors.append(f"{prefix}.condition_states missing condition ids: {missing_states}.")
                    if extra_states:
                        errors.append(f"{prefix}.condition_states contains unknown condition ids: {extra_states}.")
                refs = rule.get("coverage_refs")
                if isinstance(refs, list):
                    for ref in refs:
                        if not isinstance(ref, str):
                            errors.append(f"{prefix}.coverage_refs must contain string ids, got {type(ref).__name__}.")

        if errors:
            raise RuntimeError("Step2 output does not match strict schema:\n- " + "\n- ".join(errors))

    def _normalize_step2_data(self, dt_data: Dict[str, Any]) -> Dict[str, Any]:
        # Normalize conditions
        raw_conditions = dt_data.get("conditions")
        conditions: List[Dict[str, Any]] = []
        if isinstance(raw_conditions, list):
            for cond in raw_conditions:
                if not isinstance(cond, dict):
                    continue
                normalized = dict(cond)
                normalized["id"] = self._clean_text(normalized.get("id"))
                normalized["name"] = self._clean_text(normalized.get("name") or normalized.get("description"))
                normalized.pop("description", None)
                source_fields = normalized.get("source_fields")
                if not isinstance(source_fields, list):
                    source_fields = []
                normalized["source_fields"] = [
                    self._clean_text(x) for x in source_fields if self._clean_text(x)
                ]
                if not normalized["source_fields"]:
                    name_text = self._clean_text(normalized.get("name") or normalized.get("description"))
                    inferred_fields = []
                    for field_name in ("Username", "Phone", "Password", "ConfirmPassword"):
                        if field_name.lower() in name_text.lower():
                            inferred_fields.append(field_name)
                    normalized["source_fields"] = inferred_fields or ["Nghiệp vụ"]
                normalized["values"] = ["Y", "N"]
                normalized["meaning_when_y"] = self._clean_text(normalized.get("meaning_when_y"))
                normalized["meaning_when_n"] = self._clean_text(normalized.get("meaning_when_n"))
                if not normalized["meaning_when_y"] and normalized["name"]:
                    normalized["meaning_when_y"] = f"Điều kiện '{normalized['name']}' được thỏa mãn"
                if not normalized["meaning_when_n"] and normalized["name"]:
                    normalized["meaning_when_n"] = f"Điều kiện '{normalized['name']}' bị vi phạm"
                conditions.append(normalized)
        dt_data["conditions"] = conditions

        # Normalize actions
        raw_actions = dt_data.get("actions")
        actions: List[Dict[str, Any]] = []
        if isinstance(raw_actions, list):
            for action in raw_actions:
                if not isinstance(action, dict):
                    continue
                normalized = dict(action)
                normalized["id"] = self._clean_text(normalized.get("id"))
                description_fallback = self._clean_text(normalized.get("description"))
                normalized["name"] = self._clean_text(normalized.get("name") or description_fallback)
                normalized["expected"] = self._clean_text(normalized.get("expected") or description_fallback or normalized.get("name"))
                normalized.pop("description", None)
                actions.append(normalized)
        dt_data["actions"] = actions
        action_expected_map = {
            self._clean_text(action.get("id")): self._clean_text(action.get("expected"))
            for action in actions
            if self._clean_text(action.get("id"))
        }

        # Luồng Step 2 mới: AI chỉ sinh bảng rút gọn Bước 7.
        # Nếu AI lỡ sinh full_decision_rules/reduction_steps thì bỏ qua để tránh quá tải 2^n.
        dt_data["full_decision_rules"] = []
        dt_data["reduction_steps"] = []

        # Normalize decision_rules (Step 7 - reduced, kept for Step 3 compatibility)
        raw_rules = dt_data.get("decision_rules")
        rules: List[Dict[str, Any]] = []
        if isinstance(raw_rules, list):
            for idx, rule in enumerate(raw_rules, start=1):
                if not isinstance(rule, dict):
                    continue

                normalized_rule = self._normalize_step2_rule(rule, full_table=False)

                # Chống lỗi phổ biến từ AI: DR1/DR_001/R1 -> DT_001
                if not re.match(r"^DT_\d{3,}$", normalized_rule.get("id", "")):
                    normalized_rule["id"] = f"DT_{idx:03d}"

                states = normalized_rule.get("condition_states")
                if not isinstance(states, dict):
                    states = {}

                state_values = [str(v).strip().upper() for v in states.values()]
                n_count = state_values.count("N")
                all_y = bool(state_values) and all(v == "Y" for v in state_values)

                # Chống lỗi phổ biến từ AI: type = simplified/reduced/...
                if normalized_rule.get("type") not in {"happy_path", "single_fault", "boundary", "boundary_valid", "business_rule"}:
                    if all_y:
                        normalized_rule["type"] = "happy_path"
                    elif n_count == 1:
                        normalized_rule["type"] = "single_fault"
                    else:
                        normalized_rule["type"] = "business_rule"

                if not normalized_rule.get("expected") and normalized_rule.get("action_refs"):
                    normalized_rule["expected"] = action_expected_map.get(normalized_rule["action_refs"][0], "")

                if not normalized_rule.get("reduction_note"):
                    normalized_rule["reduction_note"] = "Bảng quyết định rút gọn Bước 7; các điều kiện '-' là không ảnh hưởng đến kết quả của rule này."

                rules.append(normalized_rule)

        dt_data["decision_rules"] = rules
        return dt_data

    def _hard_check_step2_structure(self, dt_data: Dict[str, Any]) -> None:
        if not isinstance(dt_data, dict):
            raise RuntimeError("Step2 DT output must be a JSON object.")

        if not self._clean_text(dt_data.get("feature")):
            raise RuntimeError("Step2 missing 'feature'.")

        if not self._clean_text(dt_data.get("description")):
            raise RuntimeError("Step2 missing 'description'.")

        conditions = dt_data.get("conditions")
        if not isinstance(conditions, list) or not conditions:
            raise RuntimeError("Step2 must contain non-empty 'conditions'.")

        actions = dt_data.get("actions")
        if not isinstance(actions, list) or not actions:
            raise RuntimeError("Step2 must contain non-empty 'actions'.")

        full_rules = dt_data.get("full_decision_rules")
        if full_rules is None:
            dt_data["full_decision_rules"] = []
            full_rules = []
        elif not isinstance(full_rules, list):
            raise RuntimeError("Step2 'full_decision_rules' must be a list if provided.")

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
            if not self._clean_text(action.get("expected")):
                raise RuntimeError(f"Step2 actions[{idx}] missing 'expected'.")

        full_rule_ids = set()
        for idx, rule in enumerate(full_rules, start=1):
            if not isinstance(rule, dict):
                raise RuntimeError(f"Step2 full_decision_rules[{idx}] must be an object.")

            rid = self._clean_text(rule.get("id"))
            if not rid:
                raise RuntimeError(f"Step2 full_decision_rules[{idx}] missing 'id'.")
            if rid in full_rule_ids:
                raise RuntimeError(f"Step2 duplicate full decision rule id: '{rid}'.")
            full_rule_ids.add(rid)

            states = rule.get("condition_states")
            if not isinstance(states, dict) or not states:
                raise RuntimeError(f"Step2 full_decision_rules[{idx}] must have non-empty 'condition_states'.")

            action_refs = rule.get("action_refs")
            if not isinstance(action_refs, list) or not action_refs:
                raise RuntimeError(f"Step2 full_decision_rules[{idx}] must have non-empty 'action_refs'.")

            if not self._clean_text(rule.get("expected")):
                raise RuntimeError(f"Step2 full_decision_rules[{idx}] missing 'expected'.")

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

            if not self._clean_text(rule.get("type")):
                raise RuntimeError(f"Step2 decision_rules[{idx}] missing 'type'.")

            states = rule.get("condition_states")
            if not isinstance(states, dict) or not states:
                raise RuntimeError(f"Step2 decision_rules[{idx}] must have non-empty 'condition_states'.")

            action_refs = rule.get("action_refs")
            if not isinstance(action_refs, list) or not action_refs:
                raise RuntimeError(f"Step2 decision_rules[{idx}] must have non-empty 'action_refs'.")

            if not self._clean_text(rule.get("expected")):
                raise RuntimeError(f"Step2 decision_rules[{idx}] missing 'expected'.")

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

        self._log("STEP 2: build prompt cho Decision Table trung gian")
        # Step 2 dùng feature spec + lý thuyết Decision Table + schema output.
        # Nếu có Step 1, truyền bản compact để AI hiểu field/ràng buộc/kết quả và giữ prompt ngắn.
        compact_step1 = self._compact_step1_for_step2(step1_data) if isinstance(step1_data, dict) else None
        step2_prompt = self.prompt_loader.build_step2_prompt(feature, compact_step1)
        feature_key = self._resolve_feature_key_from_prompt(step2_prompt, feature)
        self._log(f"STEP 2: feature chuẩn hóa = '{feature_key}'")
        self._log(f"STEP 2: độ dài prompt = {len(step2_prompt):,} ký tự")

        self._log("STEP 2: gọi AI để sinh decision_rules ...")
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

        self._log("STEP 2: strict schema check, không auto-repair ...")
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
        except Exception as exc:
            self._log(f"STEP 2: output chưa chuẩn, không ghi step2_dt_invalid.json. Lỗi: {exc}")
            raise

        dt_json_path = exporter.write_raw_json(dt_data, filename="step2_dt.json")
        self._export_step2_excel_safely(dt_data, exporter)
        self._log(f"STEP 2: số decision_rules = {len(dt_data.get('decision_rules', []))}")
        self._log(f"STEP 2: lưu JSON tại {dt_json_path}")

        step_elapsed = time.perf_counter() - step_start
        self._log(f"STEP 2: hoàn tất trong {self._format_seconds(step_elapsed)}")
        return dt_data

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
        step_start = time.perf_counter()

        self._log("STEP 3: build prompt cho final testcases")
        step3_prompt = self.prompt_loader.build_step3_prompt(feature, step1_data, dt_data)
        self._log(f"STEP 3: độ dài prompt = {len(step3_prompt):,} ký tự")

        self._log("STEP 3: gọi AI để sinh final testcases ...")
        llm_start = time.perf_counter()
        raw_output = self.llm_client.generate(step3_prompt)
        raw_txt_path = self._save_raw_output(exporter, "final_raw.txt", raw_output)
        self._raise_if_llm_output_empty(raw_output, "Step3 final", raw_txt_path)
        llm_elapsed = time.perf_counter() - llm_start
        self._log(f"STEP 3: AI trả kết quả sau {self._format_seconds(llm_elapsed)}")
        self._log(f"STEP 3: lưu raw output tại {raw_txt_path}")

        self._log("STEP 3: parse JSON ...")
        parsed = self.parser.parse_json(raw_output)
        if not parsed.ok:
            raise RuntimeError(
                f"Step3 final parse error: {parsed.error}. Raw output saved at: {raw_txt_path}"
            )

        final_data = parsed.data
        if not isinstance(final_data, dict):
            raise RuntimeError("Step3 final output must be a JSON object.")

        feature_key = normalize_feature_name(step1_data.get("feature", feature))

        self._log("STEP 3: normalize + rebuild summary ...")
        final_data = self._force_step3_feature(final_data, feature_key)
        final_data = self._normalize_step3_data(feature_key, final_data)
        final_data = self._rebuild_step3_summary(final_data)

        self._log("STEP 3: hard-check structure ...")
        try:
            self._hard_check_step3_structure(final_data)
        except Exception:
            self._log("STEP 3: hard-check failed, ghi final_invalid.json")
            exporter.write_raw_json(final_data, filename="final_invalid.json")
            raise

        try:
            self._log("STEP 3: validate output ...")
            result = self.step3_validator.validate_or_raise(final_data, step1_data, dt_data)
            if result.warnings:
                self._log(f"STEP 3: có {len(result.warnings)} warning")
                self._raise_if_step3_warnings_are_severe(result.warnings)

            self._log("STEP 3: kiểm tra coverage trace đầy đủ ...")
            self._ensure_all_step1_coverage_used(final_data, step1_data)
        except Exception:
            self._log("STEP 3: validate failed, ghi final_invalid.json")
            exporter.write_raw_json(final_data, filename="final_invalid.json")
            raise

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
        testcases = final_data.get("testcases")
        if not isinstance(testcases, list) or not testcases:
            raise RuntimeError("Final JSON is invalid: 'testcases' must be a non-empty list.")

        paths = exporter.export_feature_items(
            feature=feature,
            items=testcases,
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
