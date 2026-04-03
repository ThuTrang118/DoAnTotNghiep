from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ParserResult:
    ok: bool
    data: Optional[Any] = None
    cleaned_text: str = ""
    error: str = ""
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "data": self.data,
            "cleaned_text": self.cleaned_text,
            "error": self.error,
            "warnings": self.warnings,
        }


class LLMOutputParser:
    """
    Parser chịu lỗi cho output từ LLM.

    Mục tiêu:
    - bỏ markdown fences
    - lấy khối JSON đầu tiên
    - bỏ text ngoài JSON
    - sửa một số lỗi pseudo-code phổ biến
    - chuẩn hoá dấu ngoặc kép kiểu smart quotes
    - trả về context gần vị trí lỗi nếu parse thất bại
    """

    def strip_code_fences(self, text: str) -> str:
        text = (text or "").strip()

        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

        return text.strip()

    def _normalize_quotes(self, text: str) -> tuple[str, List[str]]:
        warnings: List[str] = []
        fixed = (
            text.replace("“", '"')
            .replace("”", '"')
            .replace("‘", "'")
            .replace("’", "'")
        )
        if fixed != text:
            warnings.append("Normalized smart quotes.")
        return fixed, warnings

    def _extract_balanced_block(self, text: str, open_ch: str, close_ch: str) -> str:
        start = text.find(open_ch)
        if start < 0:
            return ""

        depth = 0
        in_string = False
        escape = False

        for i in range(start, len(text)):
            ch = text[i]

            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
                continue

            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]

        return ""

    def extract_json_block(self, text: str) -> str:
        text = (text or "").strip()
        if not text:
            return ""

        obj = self._extract_balanced_block(text, "{", "}")
        if obj:
            return obj.strip()

        arr = self._extract_balanced_block(text, "[", "]")
        if arr:
            return arr.strip()

        return ""

    def _expand_repeat_function(self, text: str, warnings: List[str]) -> str:
        pattern = re.compile(r'"([^"\\]|\\.)*"\.repeat\(\d+\)')

        def repl(match: re.Match) -> str:
            full = match.group(0)
            m = re.match(r'"((?:[^"\\]|\\.)*)"\.repeat\((\d+)\)', full)
            if not m:
                return full

            raw_str = m.group(1)
            count = int(m.group(2))

            try:
                decoded = bytes(raw_str, "utf-8").decode("unicode_escape")
            except Exception:
                decoded = raw_str

            expanded = decoded * count
            warnings.append(f"Expanded pseudo-code expression: {full}")
            return json.dumps(expanded, ensure_ascii=False)

        return pattern.sub(repl, text)

    def _expand_multiply_pattern(self, text: str, warnings: List[str]) -> str:
        pattern = re.compile(r'"([^"\\]|\\.)*"\s*\*\s*\d+')

        def repl(match: re.Match) -> str:
            full = match.group(0)
            m = re.match(r'"((?:[^"\\]|\\.)*)"\s*\*\s*(\d+)', full)
            if not m:
                return full

            raw_str = m.group(1)
            count = int(m.group(2))

            try:
                decoded = bytes(raw_str, "utf-8").decode("unicode_escape")
            except Exception:
                decoded = raw_str

            expanded = decoded * count
            warnings.append(f"Expanded pseudo-code expression: {full}")
            return json.dumps(expanded, ensure_ascii=False)

        return pattern.sub(repl, text)

    def repair_common_json_issues(self, text: str) -> tuple[str, List[str]]:
        warnings: List[str] = []
        text = (text or "").strip()

        # bỏ BOM nếu có
        text = text.lstrip("\ufeff")

        # chuẩn hóa dấu ngoặc kép
        text, quote_warnings = self._normalize_quotes(text)
        warnings.extend(quote_warnings)

        # sửa pseudo-code trước
        text = self._expand_repeat_function(text, warnings)
        text = self._expand_multiply_pattern(text, warnings)

        # bỏ comma thừa trước } hoặc ]
        fixed = re.sub(r",\s*([}\]])", r"\1", text)
        if fixed != text:
            warnings.append("Removed trailing comma before closing bracket.")
            text = fixed

        return text.strip(), warnings

    def parse_json(self, raw_text: str) -> ParserResult:
        if not isinstance(raw_text, str):
            return ParserResult(
                ok=False,
                data=None,
                cleaned_text="",
                error=f"raw_text must be str, got {type(raw_text).__name__}",
                warnings=[],
            )

        text = self.strip_code_fences(raw_text)
        block = self.extract_json_block(text)

        if not block:
            return ParserResult(
                ok=False,
                data=None,
                cleaned_text=text,
                error="Cannot find a valid JSON object/array in LLM output.",
                warnings=[],
            )

        repaired, warnings = self.repair_common_json_issues(block)

        try:
            parsed = json.loads(repaired)
            return ParserResult(
                ok=True,
                data=parsed,
                cleaned_text=repaired,
                error="",
                warnings=warnings,
            )
        except json.JSONDecodeError as exc:
            start = max(0, exc.pos - 120)
            end = min(len(repaired), exc.pos + 120)
            snippet = repaired[start:end]

            return ParserResult(
                ok=False,
                data=None,
                cleaned_text=repaired,
                error=(
                    f"JSON decode error: {exc}. "
                    f"Context near error: {snippet}"
                ),
                warnings=warnings,
            )


def parse_llm_json_output(raw_text: str) -> Any:
    parser = LLMOutputParser()
    result = parser.parse_json(raw_text)

    if not result.ok or result.data is None:
        raise ValueError(result.error or "Cannot parse JSON from LLM output.")

    return result.data