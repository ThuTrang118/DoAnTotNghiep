from __future__ import annotations

from pathlib import Path
from typing import List

from testdata_generation.engine.prompt_loader import PromptLoader
from testdata_generation.engine.llm_output_parser import LLMOutputParser
from testdata_generation.engine.data_exporter import DataExporter
from testdata_generation.engine.feature_item_schema import (
    assign_testcase_ids,
    normalize_feature_name,
)


class GenerationPipeline:
    def __init__(self, llm_client, base_dir: Path):
        self.llm_client = llm_client
        self.base_dir = Path(base_dir).resolve()

        project_root = self.base_dir.parent

        self.parser = LLMOutputParser()
        self.exporter = DataExporter(
            raw_dir=project_root / "testdata_generation" / "output",
            processed_dir=project_root / "data" / "ai_processed",
        )
        self.prompt_loader = PromptLoader(input_dir=self.base_dir / "input")

    def generate(self, feature: str, formats: List[str]):
        feature = normalize_feature_name(feature)

        # 1. Build prompt
        full_prompt = self.prompt_loader.build_prompt(feature)

        # 2. Call LLM
        raw_text = self.llm_client.generate(full_prompt)

        # 3. Always save raw text first for debugging
        raw_text_path = self.exporter.write_raw_text(feature, raw_text, suffix="raw")

        # 4. Parse JSON
        parse_result = self.parser.parse_json(raw_text)
        if not parse_result.ok or parse_result.data is None:
            raise RuntimeError(
                "Cannot parse JSON from LLM output: "
                f"{parse_result.error}. "
                f"Raw text saved at: {raw_text_path}"
            )

        data = parse_result.data
        if not isinstance(data, dict):
            raise RuntimeError(
                f"LLM output must be a JSON object. Raw text saved at: {raw_text_path}"
            )

        items = data.get("items", [])
        if not isinstance(items, list):
            raise RuntimeError(
                f"Invalid JSON: 'items' must be a list. Raw text saved at: {raw_text_path}"
            )

        # 5. Assign Testcase IDs by feature prefix
        items = assign_testcase_ids(feature, items)
        data["items"] = items

        # 6. Save parsed JSON
        raw_json_path = self.exporter.write_raw_json(feature, data, suffix="raw")

        # 7. Export processed files
        processed_files = self.exporter.export_feature_items(
            feature=feature,
            items=items,
            formats=formats,
        )

        return str(raw_json_path), processed_files