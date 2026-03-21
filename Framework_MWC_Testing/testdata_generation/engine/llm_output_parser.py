from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Optional, List, Tuple


@dataclass
class ParseResult:
    ok: bool
    data: Any = None
    cleaned_text: str = ""
    error: Optional[str] = None


class LLMOutputParser:

    def parse_json(self, raw_text: str) -> ParseResult:
        if not raw_text or not raw_text.strip():
            return ParseResult(ok=False, error="Empty LLM output")

        cleaned = self._strip_code_fences(raw_text.strip())
        cleaned = self._remove_json_comments(cleaned)

        candidates: List[Tuple[str, str]] = []

        def add_candidate(name: str, text: Optional[str]) -> None:
            if text and text.strip():
                t = self._remove_json_comments(text.strip())
                candidates.append((name, t))

        add_candidate("direct", cleaned)
        add_candidate("items_root", self._salvage_root_object_with_items(cleaned))
        add_candidate("outer_braces", self._salvage_between_outer_braces(cleaned))
        add_candidate("truncated", self._salvage_truncated_json(cleaned))
        add_candidate("last_block", self._extract_last_json_block(cleaned))
        add_candidate("first_block", self._extract_first_json_block(cleaned))

        best_data: Any = None
        best_text = ""
        best_score = -1

        for _, candidate in candidates:
            data = self._try_load_with_repair(candidate)
            if data is None:
                continue

            score = self._score_parsed_json(data)
            if score > best_score:
                best_score = score
                best_data = data
                best_text = candidate
                if score >= 100:
                    break

        if best_score >= 0:
            return ParseResult(ok=True, data=best_data, cleaned_text=best_text)

        return ParseResult(
            ok=False,
            cleaned_text=cleaned,
            error="Cannot parse JSON from LLM output (no valid JSON object/array found)",
        )

    def _score_parsed_json(self, data: Any) -> int:
        if isinstance(data, dict):
            score = 10
            if isinstance(data.get("items"), list):
                score += 100
            if isinstance(data.get("plan"), dict):
                score += 20
            if any(k in data for k in ("items", "plan", "data", "result", "payload")):
                score += 5
            return score

        if isinstance(data, list):
            return 30

        return 0

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

    def _extract_last_json_block(self, text: str) -> Optional[str]:
        if not text:
            return None

        end_obj = text.rfind("}")
        end_arr = text.rfind("]")
        if end_obj == -1 and end_arr == -1:
            return None

        if end_obj > end_arr:
            end = end_obj
            open_ch, close_ch = "{", "}"
        else:
            end = end_arr
            open_ch, close_ch = "[", "]"

        depth = 0
        in_str = False
        esc = False

        for i in range(end, -1, -1):
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

            if ch == close_ch:
                depth += 1
            elif ch == open_ch:
                depth -= 1
                if depth == 0:
                    return text[i:end + 1].strip()

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

    def _salvage_root_object_with_items(self, text: str) -> Optional[str]:
        """
        Trường hợp root JSON có dạng { ..., "items": [ {...}, {...}, ... ] }
        nhưng bị cắt giữa chừng ở một item cuối.

        Ý tưởng:
        - giữ nguyên phần prefix trước dấu '[' của items
        - chỉ nhặt các object hoàn chỉnh trong mảng items
        - đóng lại ] và } để tạo root JSON hợp lệ
        """
        if not text or '"items"' not in text:
            return None

        items_key_pos = text.find('"items"')
        array_start = text.find('[', items_key_pos)
        if array_start == -1:
            return None

        prefix = text[: array_start + 1]
        suffix = "]}"

        objs: List[str] = []
        in_str = False
        esc = False
        depth = 0
        obj_start: Optional[int] = None

        for i in range(array_start + 1, len(text)):
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

            if ch == "{":
                if depth == 0:
                    obj_start = i
                depth += 1
            elif ch == "}":
                if depth > 0:
                    depth -= 1
                    if depth == 0 and obj_start is not None:
                        objs.append(text[obj_start:i + 1].strip())
                        obj_start = None
            elif ch == "]":
                break

        if not objs:
            return None

        return prefix + ", ".join(objs) + suffix

    def _salvage_truncated_json(self, text: str) -> Optional[str]:
        if not text:
            return None

        start_obj = text.find("{")
        start_arr = text.find("[")
        if start_obj == -1 and start_arr == -1:
            return None

        start = start_obj if (start_obj != -1 and (start_arr == -1 or start_obj < start_arr)) else start_arr
        candidate = text[start:].strip()
        if not candidate:
            return None

        stack: List[str] = []
        in_str = False
        esc = False
        last_safe_idx: Optional[int] = None

        for i, ch in enumerate(candidate):
            if in_str:
                if esc:
                    esc = False
                    continue
                if ch == "\\":
                    esc = True
                    continue
                if ch == '"':
                    in_str = False
                    last_safe_idx = i
                continue

            if ch == '"':
                in_str = True
                continue

            if ch in "{[":
                stack.append(ch)
            elif ch in "}]":
                if stack:
                    top = stack[-1]
                    if (top == "{" and ch == "}") or (top == "[" and ch == "]"):
                        stack.pop()
                last_safe_idx = i
            elif ch == ",":
                last_safe_idx = i

        if (in_str or stack) and last_safe_idx is not None:
            candidate = candidate[: last_safe_idx + 1].rstrip()

        candidate = re.sub(r",\s*$", "", candidate)

        stack = []
        in_str2 = False
        esc2 = False
        for ch in candidate:
            if in_str2:
                if esc2:
                    esc2 = False
                    continue
                if ch == "\\":
                    esc2 = True
                    continue
                if ch == '"':
                    in_str2 = False
                continue

            if ch == '"':
                in_str2 = True
                continue

            if ch in "{[":
                stack.append(ch)
            elif ch in "}]":
                if stack:
                    top = stack[-1]
                    if (top == "{" and ch == "}") or (top == "[" and ch == "]"):
                        stack.pop()

        if in_str2:
            return None

        if stack:
            closers = "".join("}" if op == "{" else "]" for op in reversed(stack))
            candidate = candidate + closers

        return candidate.strip()

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

            if ch == "/" and i + 1 < n and text[i + 1] == "/":
                i += 2
                while i < n and text[i] not in "\r\n":
                    i += 1
                continue

            if ch == "/" and i + 1 < n and text[i + 1] == "*":
                i += 2
                while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                    i += 1
                i += 2 if i + 1 < n else 0
                continue

            out_chars.append(ch)
            i += 1

        return "".join(out_chars)