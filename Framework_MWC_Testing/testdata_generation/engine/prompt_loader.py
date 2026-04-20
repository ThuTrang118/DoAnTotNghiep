from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from testdata_generation.engine.feature_item_schema import normalize_feature_name


class PromptLoader:
    """
    Prompt loader cho pipeline 2 bước.

    Cấu trúc input chuẩn:

    input/
    ├── features/
    │   ├── login.txt
    │   ├── register.txt
    │   └── ...
    └── generation_rules/
        ├── step1_prompt.txt
        ├── step2_prompt.txt
        ├── intermediate_format.txt
        └── final_output_format.txt
    """

    STEP1_PROMPT_FILE = "step1_prompt.txt"
    STEP2_PROMPT_FILE = "step2_prompt.txt"
    INTERMEDIATE_FORMAT_FILE = "intermediate_format.txt"
    FINAL_OUTPUT_FORMAT_FILE = "final_output_format.txt"

    def __init__(self, input_dir: str | Path | None = None) -> None:
        if input_dir is None:
            self.input_dir = Path(__file__).resolve().parents[1] / "input"
        else:
            self.input_dir = Path(input_dir).resolve()

        self.features_dir = self.input_dir / "features"
        self.rules_dir = self.input_dir / "generation_rules"

    # ==========================================================================
    # INTERNAL
    # ==========================================================================
    @staticmethod
    def _normalize_text(text: str) -> str:
        return text.replace("\r\n", "\n").strip()

    def _read_required(self, path: Path) -> str:
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
        if not path.is_file():
            raise FileNotFoundError(f"Path is not a file: {path}")

        content = path.read_text(encoding="utf-8")
        content = self._normalize_text(content)

        if not content:
            raise ValueError(f"Required file is empty: {path}")

        return content

    def _feature_file_path(self, feature: str) -> Path:
        feature_name = normalize_feature_name(feature)
        return self.features_dir / f"{feature_name}.txt"

    def _rule_file_path(self, filename: str) -> Path:
        return self.rules_dir / filename

    @staticmethod
    def _json_dumps_compact(data: Dict[str, Any]) -> str:
        return json.dumps(data, ensure_ascii=False, indent=2)

    @staticmethod
    def _assert_step1_data_shape(step1_data: Any) -> None:
        if not isinstance(step1_data, dict):
            raise ValueError("step1_data must be a dict.")

        if not isinstance(step1_data.get("feature"), str) or not step1_data.get("feature", "").strip():
            raise ValueError("step1_data must contain non-empty 'feature'.")

        if not isinstance(step1_data.get("description"), str) or not step1_data.get("description", "").strip():
            raise ValueError("step1_data must contain non-empty 'description'.")

        coverage_items = step1_data.get("coverage_items")
        if not isinstance(coverage_items, list) or not coverage_items:
            raise ValueError("step1_data must contain non-empty 'coverage_items'.")

        coverage_summary = step1_data.get("coverage_summary")
        if not isinstance(coverage_summary, dict):
            raise ValueError("step1_data must contain 'coverage_summary' object.")

    def validate_required_sources(self, feature: str) -> Dict[str, str]:
        """
        Kiểm tra toàn bộ nguồn prompt/schema/spec trước khi chạy pipeline.
        Trả về map đường dẫn để debug/log.
        """
        feature_name = normalize_feature_name(feature)

        paths = {
            "feature_description": self._feature_file_path(feature_name),
            "step1_prompt": self._rule_file_path(self.STEP1_PROMPT_FILE),
            "step2_prompt": self._rule_file_path(self.STEP2_PROMPT_FILE),
            "intermediate_format": self._rule_file_path(self.INTERMEDIATE_FORMAT_FILE),
            "final_output_format": self._rule_file_path(self.FINAL_OUTPUT_FORMAT_FILE),
        }

        for key, path in paths.items():
            self._read_required(path)

        return {k: str(v) for k, v in paths.items()}

    # ==========================================================================
    # LOADERS
    # ==========================================================================
    def load_feature_description(self, feature: str) -> str:
        return self._read_required(self._feature_file_path(feature))

    def load_step1_prompt_template(self) -> str:
        return self._read_required(self._rule_file_path(self.STEP1_PROMPT_FILE))

    def load_step2_prompt_template(self) -> str:
        return self._read_required(self._rule_file_path(self.STEP2_PROMPT_FILE))

    def load_intermediate_format(self) -> str:
        return self._read_required(self._rule_file_path(self.INTERMEDIATE_FORMAT_FILE))

    def load_final_output_format(self) -> str:
        return self._read_required(self._rule_file_path(self.FINAL_OUTPUT_FORMAT_FILE))

    # ==========================================================================
    # BUILD STEP 1 PROMPT
    # ==========================================================================
    def build_step1_prompt(self, feature: str) -> str:
        """
        Build prompt cho Step 1:
        - role/rules từ step1_prompt.txt
        - spec từ features/<feature>.txt
        - schema từ intermediate_format.txt
        """
        feature_name = normalize_feature_name(feature)

        prompt_template = self.load_step1_prompt_template()
        feature_spec = self.load_feature_description(feature_name)
        intermediate_schema = self.load_intermediate_format()

        parts = [
            prompt_template,
            "================================================================================",
            "FEATURE SPECIFICATION",
            "================================================================================",
            feature_spec,
            "================================================================================",
            "OUTPUT SCHEMA",
            "================================================================================",
            intermediate_schema,
            "================================================================================",
            "OUTPUT CONTRACT",
            "================================================================================",
            "Chỉ trả về 1 JSON object hợp lệ theo OUTPUT SCHEMA.",
            "Không markdown.",
            "Không comment.",
            "Không giải thích.",
            "Không text ngoài JSON.",
        ]
        return "\n\n".join(parts).strip()

    # ==========================================================================
    # BUILD STEP 2 PROMPT
    # ==========================================================================
    def build_step2_prompt(self, feature: str, step1_data: Dict[str, Any]) -> str:
        """
        Build prompt cho Step 2:
        - role/rules từ step2_prompt.txt
        - spec từ features/<feature>.txt
        - Step 1 JSON locked input
        - schema từ final_output_format.txt
        """
        feature_name = normalize_feature_name(feature)
        self._assert_step1_data_shape(step1_data)

        prompt_template = self.load_step2_prompt_template()
        feature_spec = self.load_feature_description(feature_name)
        final_schema = self.load_final_output_format()
        step1_json = self._json_dumps_compact(step1_data)

        parts = [
            prompt_template,
            "================================================================================",
            "FEATURE SPECIFICATION",
            "================================================================================",
            feature_spec,
            "================================================================================",
            "STEP 1 COVERAGE OUTPUT (LOCKED INPUT - DO NOT CHANGE)",
            "================================================================================",
            step1_json,
            "================================================================================",
            "OUTPUT SCHEMA",
            "================================================================================",
            final_schema,
            "================================================================================",
            "OUTPUT CONTRACT",
            "================================================================================",
            "Chỉ trả về 1 JSON object hợp lệ theo OUTPUT SCHEMA.",
            "Phải dùng đúng Step 1 COVERAGE OUTPUT làm nguồn truy vết.",
            "Không được tạo coverage mới.",
            "Không được sửa coverage cũ.",
            "Không markdown.",
            "Không comment.",
            "Không giải thích.",
            "Không text ngoài JSON.",
        ]
        return "\n\n".join(parts).strip()

    # ==========================================================================
    # DEBUG / INSPECTION
    # ==========================================================================
    def describe_prompt_sources(self, feature: str) -> Dict[str, str]:
        feature_name = normalize_feature_name(feature)
        return {
            "feature_description": str(self._feature_file_path(feature_name)),
            "step1_prompt": str(self._rule_file_path(self.STEP1_PROMPT_FILE)),
            "step2_prompt": str(self._rule_file_path(self.STEP2_PROMPT_FILE)),
            "intermediate_format": str(self._rule_file_path(self.INTERMEDIATE_FORMAT_FILE)),
            "final_output_format": str(self._rule_file_path(self.FINAL_OUTPUT_FORMAT_FILE)),
        }

    def preview_step1_prompt(self, feature: str) -> str:
        return self.build_step1_prompt(feature)

    def preview_step2_prompt(self, feature: str, step1_data: Dict[str, Any]) -> str:
        return self.build_step2_prompt(feature, step1_data)