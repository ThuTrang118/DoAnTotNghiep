from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ParseResult:
    ok: bool
    data: Any = None
    cleaned_text: str = ""
    error: Optional[str] = None


class LLMOutputParser:
    """
    Robust JSON extractor + repair:
    - Strip ``` fences + lone 'json'
    - Remove JS-style comments (// and /* */) OUTSIDE strings
    - Extract first JSON object/array by bracket matching (string-aware)
    - Salvage: best-effort slice between first '{' and last '}' (or '[' and ']')
    - Repair common non-JSON patterns emitted by LLM, e.g.:
        "a" * 7 + "z"  -> "aaaaaaaz"
        "a" * 21       -> "aaaaaaaaaaaaaaaaaaaaa"
    - Remove trailing commas before ] or }
    """

    def parse_json(self, raw_text: str) -> ParseResult:
        if not raw_text or not raw_text.strip():
            return ParseResult(ok=False, error="Empty LLM output")

        cleaned = self._strip_code_fences(raw_text.strip())
        cleaned = self._remove_json_comments(cleaned)

        # 1) Try direct
        data = self._try_load_with_repair(cleaned)
        if data is not None:
            return ParseResult(ok=True, data=data, cleaned_text=cleaned)

        # 2) Extract first JSON block by bracket matching
        block = self._extract_first_json_block(cleaned)
        if block:
            block = self._remove_json_comments(block)
            data2 = self._try_load_with_repair(block)
            if data2 is not None:
                return ParseResult(ok=True, data=data2, cleaned_text=block)

        # 3) Salvage: slice between first open and last close
        salvaged = self._salvage_between_outer_braces(cleaned)
        if salvaged:
            salvaged = self._remove_json_comments(salvaged)
            data3 = self._try_load_with_repair(salvaged)
            if data3 is not None:
                return ParseResult(ok=True, data=data3, cleaned_text=salvaged)

        return ParseResult(
            ok=False,
            cleaned_text=cleaned,
            error="Cannot parse JSON from LLM output (no valid JSON object/array found)",
        )

    # -------------------------
    # Internals
    # -------------------------
    def _try_load_with_repair(self, text: str) -> Any:
        if not text:
            return None

        try:
            return json.loads(text)
        except Exception:
            pass

        repaired = self._repair_llm_json(text)
        try:
            return json.loads(repaired)
        except Exception:
            return None

    def _strip_code_fences(self, text: str) -> str:
        lines = []
        for ln in text.splitlines():
            if ln.strip().startswith("```"):
                continue
            lines.append(ln)
        t = "\n".join(lines).strip()

        lines2 = t.splitlines()
        while lines2 and not lines2[0].strip():
            lines2.pop(0)
        if lines2 and lines2[0].strip().lower() == "json":
            lines2.pop(0)
        return "\n".join(lines2).strip()

    def _extract_first_json_block(self, text: str) -> Optional[str]:
        if not text:
            return None

        i_obj = text.find("{")
        i_arr = text.find("[")
        if i_obj == -1 and i_arr == -1:
            return None

        if i_obj == -1:
            start = i_arr
            open_ch, close_ch = "[", "]"
        elif i_arr == -1:
            start = i_obj
            open_ch, close_ch = "{", "}"
        else:
            if i_arr < i_obj:
                start = i_arr
                open_ch, close_ch = "[", "]"
            else:
                start = i_obj
                open_ch, close_ch = "{", "}"

        depth = 0
        in_str = False
        esc = False

        for i in range(start, len(text)):
            ch = text[i]

            if in_str:
                if esc:
                    esc = False
                    continue
                if ch == "\\":
                    esc = True
                    continue
                if ch == '"':
                    in_str = False
                continue

            if ch == '"':
                in_str = True
                continue

            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return text[start:i + 1].strip()

        return None

    def _salvage_between_outer_braces(self, text: str) -> Optional[str]:
        if not text:
            return None

        first_obj = text.find("{")
        last_obj = text.rfind("}")
        if first_obj != -1 and last_obj != -1 and last_obj > first_obj:
            return text[first_obj:last_obj + 1].strip()

        first_arr = text.find("[")
        last_arr = text.rfind("]")
        if first_arr != -1 and last_arr != -1 and last_arr > first_arr:
            return text[first_arr:last_arr + 1].strip()

        return None

    def _repair_llm_json(self, s: str) -> str:
        out = s

        pattern_mul_plus = re.compile(
            r'"(?P<char>[^"\\])"\s*\*\s*(?P<n>\d+)\s*\+\s*"(?P<suf>[^"\\]*)"'
        )
        pattern_mul = re.compile(r'"(?P<char>[^"\\])"\s*\*\s*(?P<n>\d+)')

        changed = True
        while changed:
            changed = False

            def repl_mul_plus(m: re.Match) -> str:
                ch = m.group("char")
                n = int(m.group("n"))
                suf = m.group("suf")
                return '"' + (ch * n) + suf + '"'

            out2 = pattern_mul_plus.sub(repl_mul_plus, out)
            if out2 != out:
                out = out2
                changed = True

            def repl_mul(m: re.Match) -> str:
                ch = m.group("char")
                n = int(m.group("n"))
                return '"' + (ch * n) + '"'

            out3 = pattern_mul.sub(repl_mul, out)
            if out3 != out:
                out = out3
                changed = True

        out = re.sub(r",\s*([}\]])", r"\1", out)
        return out

    def _remove_json_comments(self, text: str) -> str:
        """
        Remove // line comments and /* block comments */ OUTSIDE of strings.
        This is needed because some LLMs output JS-style comments inside JSON.
        """
        if not text:
            return text

        out_chars = []
        i = 0
        n = len(text)
        in_str = False
        esc = False

        while i < n:
            ch = text[i]

            if in_str:
                out_chars.append(ch)
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                i += 1
                continue

            if ch == '"':
                in_str = True
                out_chars.append(ch)
                i += 1
                continue

            # // comment
            if ch == "/" and i + 1 < n and text[i + 1] == "/":
                i += 2
                while i < n and text[i] not in "\r\n":
                    i += 1
                continue

            # /* */ comment
            if ch == "/" and i + 1 < n and text[i + 1] == "*":
                i += 2
                while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                    i += 1
                i += 2 if i + 1 < n else 0
                continue

            out_chars.append(ch)
            i += 1

        return "".join(out_chars)
