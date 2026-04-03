from __future__ import annotations

from pathlib import Path
from typing import Dict

from testdata_generation.engine.feature_item_schema import (
    build_item_fields_rules,
    build_item_fields_schema,
    build_item_fields_type_rules,
    normalize_feature_name,
)


class PromptLoader:
    def __init__(self, input_dir: str | Path | None = None) -> None:
        if input_dir is None:
            self.input_dir = Path(__file__).resolve().parents[1] / "input"
        else:
            self.input_dir = Path(input_dir).resolve()

    def _read_required(self, filename: str) -> Path:
        path = self.input_dir / filename
        if not path.exists():
            raise FileNotFoundError(str(path))
        return path

    def load_common_knowledge(self) -> str:
        return self._read_required("blackbox_techniques.txt").read_text(
            encoding="utf-8"
        ).strip()

    def load_feature_description(self, feature: str) -> str:
        feature_name = normalize_feature_name(feature)
        return self._read_required(f"{feature_name}.txt").read_text(
            encoding="utf-8"
        ).strip()

    def load_output_format_template(self) -> str:
        return self._read_required("output_format.txt").read_text(
            encoding="utf-8"
        ).strip()

    def load_output_format(self, feature: str) -> str:
        feature_name = normalize_feature_name(feature)
        template = self.load_output_format_template()

        item_fields_schema = build_item_fields_schema(feature_name)
        item_fields_rules = build_item_fields_rules(feature_name)
        item_fields_type_rules = build_item_fields_type_rules(feature_name)

        return (
            template
            .replace("{{ITEM_FIELDS_SCHEMA}}", item_fields_schema)
            .replace("{{ITEM_FIELDS_RULES}}", item_fields_rules)
            .replace("{{ITEM_FIELDS_TYPE_RULES}}", item_fields_type_rules)
        )

    def build_prompt(self, feature: str) -> str:
        feature_name = normalize_feature_name(feature)

        parts = [
            self.load_common_knowledge(),
            self.load_feature_description(feature_name),
            self.load_output_format(feature_name),
        ]
        return "\n\n".join(parts).strip()

    def describe_prompt_sources(self, feature: str) -> Dict[str, str]:
        feature_name = normalize_feature_name(feature)
        return {
            "blackbox_techniques": str(self._read_required("blackbox_techniques.txt")),
            "feature_description": str(self._read_required(f"{feature_name}.txt")),
            "output_format": str(self._read_required("output_format.txt")),
        }