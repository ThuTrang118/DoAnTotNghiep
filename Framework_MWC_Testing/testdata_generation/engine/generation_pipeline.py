from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from testdata_generation.engine.exporters import DataExporter, export_step1_to_excel
from testdata_generation.engine.feature_item_schema import (
    build_default_testcase_id,
    normalize_feature_name,
)
from testdata_generation.engine.llm_output_parser import LLMOutputParser
from testdata_generation.engine.prompt_loader import PromptLoader
from testdata_generation.engine.validators import ConditionsValidator, FinalDTValidator


class GenerationPipeline:
    """
    Pipeline sinh dữ liệu kiểm thử tự động theo mô hình 2 bước:

    Step 1:
        AI phân tích EP + BVA -> coverage_items

    Step 2:
        AI phân tích Decision Table -> testcases cuối

    Nguyên tắc production:
    - Fail fast: sai ở bước nào dừng ngay ở bước đó
    - Không export processed data nếu Step 2 chưa pass
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
        "coverage_refs",
        "not found in Step 1",
        "no happy path",
        "happy path",
        "single-fault",
        "boundary",
        "missing testcase",
        "expected",
    )

    def __init__(self, llm_client, base_dir: Path, verbose: bool = True) -> None:
        self.llm_client = llm_client
        self.base_dir = Path(base_dir).resolve()
        self.verbose = verbose

        self.output_root = self.base_dir / "output"
        self.output_root.mkdir(parents=True, exist_ok=True)

        self.parser = LLMOutputParser()
        self.prompt_loader = PromptLoader(input_dir=self.base_dir / "input")

        self.step1_validator = ConditionsValidator()
        self.step2_validator = FinalDTValidator()

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

    # ==========================================================================
    # PUBLIC API
    # ==========================================================================
    def generate(self, feature: str, formats: List[str]) -> Tuple[str, List[str]]:
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

        # Step 1: fail ngay nếu sai
        step1_data = self._generate_step1(feature_name, exporter)

        # Step 2: fail ngay nếu sai
        final_json_path, step2_data = self._generate_step2(feature_name, step1_data, exporter)

        # Chỉ export khi Step 2 đã pass hoàn toàn
        processed_files = self._export_processed_files(feature_name, step2_data, formats, exporter)

        total_elapsed = time.perf_counter() - total_start
        self._log(f"Hoàn tất pipeline trong {self._format_seconds(total_elapsed)}")

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
        path.write_text("" if raw_output is None else str(raw_output), encoding="utf-8")
        return path

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

            # Đồng bộ EP/BVA bắt buộc
            if normalized["technique"] == "EP":
                normalized["boundary"] = None

            if normalized["technique"] == "BVA":
                normalized["partition_type"] = None

            normalized_items.append(normalized)

        step1_data["coverage_items"] = normalized_items
        return step1_data

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
    def _force_step2_feature(self, step2_data: Dict[str, Any], feature_key: str) -> Dict[str, Any]:
        step2_data["feature"] = feature_key
        return step2_data

    def _rebuild_step2_summary(self, step2_data: Dict[str, Any]) -> Dict[str, Any]:
        testcases = step2_data.get("testcases", [])
        total = len(testcases) if isinstance(testcases, list) else 0
        step2_data["testcase_summary"] = {"total_testcases": total}
        return step2_data

    def _normalize_step2_data(self, feature: str, step2_data: Dict[str, Any]) -> Dict[str, Any]:
        testcases = step2_data.get("testcases")
        if not isinstance(testcases, list):
            step2_data["testcases"] = []
            return step2_data

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
            normalized["coverage_refs"] = self._dedupe_string_list(normalized.get("coverage_refs"))

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

            decision_basis["conditions"] = self._dedupe_conditions(decision_basis.get("conditions"))
            decision_basis["optimization_note"] = self._clean_text(decision_basis.get("optimization_note"))
            normalized["decision_basis"] = decision_basis

            normalized_testcases.append(normalized)

        step2_data["testcases"] = normalized_testcases
        return step2_data

    def _hard_check_step2_structure(self, step2_data: Dict[str, Any]) -> None:
        if not isinstance(step2_data, dict):
            raise RuntimeError("Step2 output must be a JSON object.")

        if not self._clean_text(step2_data.get("feature")):
            raise RuntimeError("Step2 missing 'feature'.")

        if not self._clean_text(step2_data.get("description")):
            raise RuntimeError("Step2 missing 'description'.")

        testcases = step2_data.get("testcases")
        if not isinstance(testcases, list) or not testcases:
            raise RuntimeError("Step2 must contain non-empty 'testcases'.")

        ids = set()
        for idx, tc in enumerate(testcases, start=1):
            if not isinstance(tc, dict):
                raise RuntimeError(f"Step2 testcases[{idx}] must be an object.")

            tc_id = self._clean_text(tc.get("id"))
            if not tc_id:
                raise RuntimeError(f"Step2 testcases[{idx}] missing 'id'.")
            if tc_id in ids:
                raise RuntimeError(f"Step2 duplicate testcase id: '{tc_id}'.")
            ids.add(tc_id)

            for key in ("name", "description", "objective", "expected"):
                if not self._clean_text(tc.get(key)):
                    raise RuntimeError(f"Step2 testcases[{idx}] missing '{key}'.")

            coverage_refs = tc.get("coverage_refs")
            if not isinstance(coverage_refs, list) or not coverage_refs:
                raise RuntimeError(f"Step2 testcases[{idx}] must have non-empty 'coverage_refs'.")

            inputs = tc.get("inputs")
            if not isinstance(inputs, dict) or not inputs:
                raise RuntimeError(f"Step2 testcases[{idx}] must have non-empty 'inputs'.")

            decision_basis = tc.get("decision_basis")
            if not isinstance(decision_basis, dict):
                raise RuntimeError(f"Step2 testcases[{idx}] missing 'decision_basis'.")

            conditions = decision_basis.get("conditions")
            if not isinstance(conditions, list) or not conditions:
                raise RuntimeError(
                    f"Step2 testcases[{idx}] decision_basis.conditions must be non-empty."
                )

    def _raise_if_step2_warnings_are_severe(self, warnings: List[str]) -> None:
        severe = [
            w for w in warnings
            if any(marker.lower() in w.lower() for marker in self.STEP2_SEVERE_WARNING_MARKERS)
        ]
        if severe:
            raise RuntimeError(
                "Step2 validation produced severe warnings:\n- " + "\n- ".join(severe)
            )

    def _ensure_all_step1_coverage_used(self, step2_data: Dict[str, Any], step1_data: Dict[str, Any]) -> None:
        step1_ids = {
            str(item.get("id")).strip()
            for item in step1_data.get("coverage_items", [])
            if isinstance(item, dict) and str(item.get("id", "")).strip()
        }

        used_ids = set()
        for tc in step2_data.get("testcases", []):
            if not isinstance(tc, dict):
                continue
            for ref in tc.get("coverage_refs", []):
                ref_clean = str(ref).strip()
                if ref_clean:
                    used_ids.add(ref_clean)

        missing = sorted(step1_ids - used_ids)
        if missing:
            raise RuntimeError(
                "Step2 is missing coverage from Step1:\n- " + "\n- ".join(missing)
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
    # STEP 2
    # ==========================================================================
    def _generate_step2(
        self,
        feature: str,
        step1_data: Dict[str, Any],
        exporter: DataExporter,
    ) -> Tuple[Path, Dict[str, Any]]:
        step_start = time.perf_counter()

        self._log("STEP 2: build prompt")
        step2_prompt = self.prompt_loader.build_step2_prompt(feature, step1_data)
        self._log(f"STEP 2: độ dài prompt = {len(step2_prompt):,} ký tự")

        self._log("STEP 2: gọi AI để phân tích Decision Table ...")
        llm_start = time.perf_counter()
        raw_output = self.llm_client.generate(step2_prompt)
        raw_txt_path = self._save_raw_output(exporter, "final_raw.txt", raw_output)
        llm_elapsed = time.perf_counter() - llm_start
        self._log(f"STEP 2: AI trả kết quả sau {self._format_seconds(llm_elapsed)}")
        self._log(f"STEP 2: lưu raw output tại {raw_txt_path}")

        self._log("STEP 2: parse JSON ...")
        parsed = self.parser.parse_json(raw_output)
        if not parsed.ok:
            raise RuntimeError(
                f"Step2 parse error: {parsed.error}. Raw output saved at: {raw_txt_path}"
            )

        step2_data = parsed.data
        if not isinstance(step2_data, dict):
            raise RuntimeError("Step2 output must be a JSON object.")

        feature_key = normalize_feature_name(step1_data.get("feature", feature))

        self._log("STEP 2: normalize + rebuild summary ...")
        step2_data = self._force_step2_feature(step2_data, feature_key)
        step2_data = self._normalize_step2_data(feature_key, step2_data)
        step2_data = self._rebuild_step2_summary(step2_data)

        self._log("STEP 2: hard-check structure ...")
        try:
            self._hard_check_step2_structure(step2_data)
        except Exception:
            self._log("STEP 2: hard-check failed, ghi final_invalid.json")
            exporter.write_raw_json(step2_data, filename="final_invalid.json")
            raise

        try:
            self._log("STEP 2: validate output ...")
            result = self.step2_validator.validate_or_raise(step2_data, step1_data)
            if result.warnings:
                self._log(f"STEP 2: có {len(result.warnings)} warning")
                self._raise_if_step2_warnings_are_severe(result.warnings)

            self._log("STEP 2: kiểm tra coverage trace đầy đủ ...")
            self._ensure_all_step1_coverage_used(step2_data, step1_data)
        except Exception:
            self._log("STEP 2: validate failed, ghi final_invalid.json")
            exporter.write_raw_json(step2_data, filename="final_invalid.json")
            raise

        self._log("STEP 2: ghi final JSON ...")
        final_json_path = exporter.write_raw_json(step2_data, filename="final.json")

        testcases = step2_data.get("testcases", [])
        self._log(f"STEP 2: số testcases = {len(testcases) if isinstance(testcases, list) else 0}")
        self._log(f"STEP 2: lưu JSON tại {final_json_path}")

        step_elapsed = time.perf_counter() - step_start
        self._log(f"STEP 2: hoàn tất trong {self._format_seconds(step_elapsed)}")

        return final_json_path, step2_data

    # ==========================================================================
    # EXPORT PROCESSED
    # ==========================================================================
    def _export_processed_files(
        self,
        feature: str,
        step2_data: Dict[str, Any],
        formats: List[str],
        exporter: DataExporter,
    ) -> List[str]:
        export_start = time.perf_counter()

        self._log("EXPORT: bắt đầu export processed files ...")
        testcases = step2_data.get("testcases")
        if not isinstance(testcases, list) or not testcases:
            raise RuntimeError("Step2 JSON is invalid: 'testcases' must be a non-empty list.")

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
    # SAFE EXCEL EXPORT FOR STEP 1
    # ==========================================================================
    def _export_step1_excel_safely(self, json_path: Path) -> None:
        try:
            excel_path = json_path.with_name("step1.xlsx")
            export_step1_to_excel(json_path, excel_path)
            self._log(f"STEP 1: đã export Excel {excel_path}")
        except Exception as exc:
            self._log(f"Warning: Step1 Excel export failed: {exc}")