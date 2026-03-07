# testdata_generation/engine/ai_testdata_generator.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from testdata_generation.engine.llm_client import OllamaClient
from testdata_generation.engine.llm_output_parser import LLMOutputParser
from testdata_generation.engine.schema_validator import DataSchemaValidator
from testdata_generation.engine.data_exporter import DataWriter


@dataclass
class GenerationResult:
    ok: bool
    feature: str

    raw_text_path: Optional[Path] = None
    raw_path: Optional[Path] = None
    processed_paths: Dict[str, Path] = field(default_factory=dict)
    rows: List[Dict[str, Any]] = field(default_factory=list)

    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class AITestDataGenerator:
    """
    1) AI sinh RAW text
    2) Lưu RAW evidence:
       - testdata_generation/output/<feature>_raw.txt
       - testdata_generation/output/<feature>_raw.json
    3) (Nếu có --formats) convert rows -> các định dạng processed (framework dùng)
       và lưu vào data/ai/
    """

    def __init__(
        self,
        client: OllamaClient,
        project_root: Path,
        raw_evidence_dir: Path,
        processed_dir: Path,
    ):
        self.client = client
        self.project_root = project_root
        self.raw_evidence_dir = raw_evidence_dir
        self.processed_dir = processed_dir

        self.parser = LLMOutputParser()
        self.validator = DataSchemaValidator()

        # FIX: DataWriter cần raw_dir và processed_dir
        self.writer = DataWriter(raw_dir=self.raw_evidence_dir, processed_dir=self.processed_dir)

    def generate(
        self,
        feature: str,
        prompt: str,
        system: Optional[str] = None,
        formats: Optional[Iterable[str]] = None,  # None=>ALL, []=>none
        yaml_ext: str = "yaml",
        llm_kwargs: Optional[Dict[str, Any]] = None,
    ) -> GenerationResult:
        llm_kwargs = llm_kwargs or {}
        feature = (feature or "").strip().lower()

        warnings: List[str] = []
        errors: List[str] = []

        # 1) Call LLM
        try:
            raw_text = self.client.generate_text(prompt=prompt, system=system, **llm_kwargs)
        except Exception as e:
            return GenerationResult(ok=False, feature=feature, errors=[f"LLM call failed: {e}"])

        # 2) Save RAW evidence text
        raw_text_path = self.writer.write_raw_text(feature, raw_text)

        # 3) Parse JSON from raw_text
        parsed = self.parser.parse_json(raw_text)
        if not parsed.ok or parsed.data is None:
            return GenerationResult(
                ok=False,
                feature=feature,
                raw_text_path=raw_text_path,
                raw_path=None,
                processed_paths={},
                rows=[],
                warnings=parsed.warnings or [],
                errors=parsed.errors or [parsed.error or "Cannot parse JSON from LLM output"],
            )

        payload = parsed.data
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            return GenerationResult(
                ok=False,
                feature=feature,
                raw_text_path=raw_text_path,
                raw_path=None,
                processed_paths={},
                rows=[],
                warnings=[],
                errors=["Parsed JSON must be an object with key 'items' as a list"],
            )

        # 4) Save RAW evidence json (EXACT payload, không tự ý cắt / dedup)
        raw_json_path = self.writer.write_raw_json(feature, payload)

        # 5) Normalize rows minimally (chỉ giữ dict)
        rows: List[Dict[str, Any]] = []
        dropped = 0
        for it in payload.get("items", []):
            if isinstance(it, dict):
                rows.append(it)
            else:
                dropped += 1
        if dropped:
            warnings.append(f"Dropped {dropped} non-object items from payload.items")

        normalized_payload = {"items": rows}

        # 6) Validate (warn-only; validator có thể clean key thừa như _source nếu bạn muốn)
        v = self.validator.validate(feature=feature, data=normalized_payload)
        if isinstance(v, dict):
            warnings.extend(v.get("warnings", []) or [])
            # Không hard-fail vì mục tiêu là evidence + convert
            # nhưng vẫn ghi nhận lỗi schema nếu có:
            errors.extend(v.get("errors", []) or [])
            cleaned_rows = v.get("data", []) or rows
        else:
            cleaned_rows = rows

        processed_paths: Dict[str, Path] = {}

        # 7) Convert processed ONLY if formats != []
        if formats != []:
            # DataWriter.write_formats sẽ ghi vào processed_dir
            processed_paths = self.writer.write_formats(
                feature=feature,
                rows=cleaned_rows,
                formats=None if formats is None else list(formats),
                yaml_ext=yaml_ext,
            )

        ok = len(cleaned_rows) > 0
        return GenerationResult(
            ok=ok,
            feature=feature,
            raw_text_path=raw_text_path,
            raw_path=raw_json_path,
            processed_paths=processed_paths,
            rows=cleaned_rows,
            warnings=warnings,
            errors=errors,
        )