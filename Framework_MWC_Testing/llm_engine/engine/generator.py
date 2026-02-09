# llm_engine/engine/generator.py
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from llm_engine.engine.llm_client import OllamaClient
from llm_engine.engine.parser import LLMOutputParser
from llm_engine.engine.validator import DataSchemaValidator
from llm_engine.engine.writer import DataWriter


@dataclass
class GenerationResult:
    ok: bool
    feature: str

    # evidence
    raw_path: Optional[Path] = None
    raw_text_path: Optional[Path] = None

    # outputs
    processed_paths: Dict[str, Path] = field(default_factory=dict)
    rows: List[Dict[str, Any]] = field(default_factory=list)

    # messages
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # debug
    raw_text: str = ""
    cleaned_text: str = ""


class AITestDataGenerator:
    """
    Pipeline:
      1) Call LLM -> raw_text
      2) Save raw evidence
      3) Parse JSON -> dict {"items":[...]}
      4) Normalize + enforce seed (login)
      5) Validate schema
      6) Save raw JSON clean {"items":[...]} (if writer supports)
      7) Write processed formats (json/csv/xlsx/xls/xml/yaml/yml/db)

    EXTRA (added):
      4.5) Validate business rules + deduplicate (login)
    """

    def __init__(self, client: Optional[OllamaClient] = None, project_root: Optional[Path] = None):
        self.client = client or OllamaClient()
        self.project_root = project_root or Path(__file__).resolve().parents[2]

        self.ai_data_dir = self.project_root / "data" / "ai_generated"
        self.parser = LLMOutputParser()
        self.validator = DataSchemaValidator()
        self.writer = DataWriter(self.ai_data_dir)

    # =========================
    # Helpers
    # =========================
    def _norm_str(self, x: Any) -> str:
        if x is None:
            return ""
        return str(x).strip()

    # =========================
    # Seed handling (LOGIN)
    # =========================
    def _load_login_seed_account(self) -> Tuple[str, str, str]:
        """
        Read: data/manual/LoginSeedAccounts.json
        Return: (username, password, display_name)
        display_name optional -> fallback username.
        """
        seed_path = self.project_root / "data" / "manual" / "LoginSeedAccounts.json"
        if not seed_path.exists():
            return "", "", ""

        try:
            data = json.loads(seed_path.read_text(encoding="utf-8"))
        except Exception:
            return "", "", ""

        accounts = data.get("valid_accounts", [])
        if not accounts:
            return "", "", ""

        acc = accounts[0]
        username = (acc.get("Username") or "").strip()
        password = (acc.get("Password") or "").strip()

        display_name = (
            (acc.get("DisplayName") or "").strip()
            or (acc.get("FullName") or "").strip()
            or username
        )

        if not username or not password:
            return "", "", ""

        return username, password, display_name

    def _replace_placeholders(self, value: Any, seed_u: str, seed_p: str, seed_display: str) -> Any:
        if not isinstance(value, str):
            return value

        s = value.strip()

        # placeholders you saw in outputs
        if s in {"<GIÁ_TRỊ_THẬT>", "GIÁ_TRỊ_THẬT", "<SEED_VALID_ACCOUNT Username>", "*SEED_VALID_ACCOUNT Username*"}:
            return seed_u or value
        if s in {"<SEED_VALID_ACCOUNT Password>", "*SEED_VALID_ACCOUNT Password*"}:
            return seed_p or value
        if s in {"<SEED_DISPLAY_NAME>", "<SEED_VALID_ACCOUNT Expected>"}:
            return seed_display or seed_u or value

        # inline replacements
        if seed_u:
            s = s.replace("<SEED_VALID_ACCOUNT Username>", seed_u)
        if seed_p:
            s = s.replace("<SEED_VALID_ACCOUNT Password>", seed_p)
        if seed_display:
            s = s.replace("<SEED_DISPLAY_NAME>", seed_display)

        return s

    def _enforce_login_success_case(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enforce LG01 to use seed account (NOT AI-generated).
        Also replace placeholders in other rows.
        """
        seed_u, seed_p, seed_display = self._load_login_seed_account()
        if not seed_u or not seed_p:
            return rows

        fixed: List[Dict[str, Any]] = []
        for r in rows:
            rr = dict(r)
            for k in list(rr.keys()):
                rr[k] = self._replace_placeholders(rr[k], seed_u, seed_p, seed_display)
            rr.setdefault("_source", "AI")
            fixed.append(rr)

        lg01 = {
            "Testcase": "LG01",
            "Username": seed_u,
            "Password": seed_p,
            "Expected": seed_display or seed_u,
            "_source": "AI",
        }

        replaced = False
        for i, r in enumerate(fixed):
            if str(r.get("Testcase", "")).strip().upper() == "LG01":
                fixed[i] = lg01
                replaced = True
                break
        if not replaced:
            fixed.insert(0, lg01)

        return fixed

    # =========================
    # LOGIN business rule validate + dedup (NEW)
    # =========================
    def _infer_login_expected(
        self,
        username: str,
        password: str,
        seed_u: str,
        seed_p: str,
        seed_display: str,
        empty_field_expected: str,
        invalid_cred_expected: str,
    ) -> str:
        # Missing field
        if username == "" or password == "":
            return empty_field_expected

        # Success (seed)
        if seed_u and seed_p and username == seed_u and password == seed_p:
            return seed_display or seed_u

        # Otherwise invalid
        return invalid_cred_expected

    def _validate_and_dedup_login_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        empty_field_expected: str = "Vui lòng điền vào trường này",
        invalid_cred_expected: str = "Tên đăng nhập hoặc mật khẩu không đúng!",
        auto_fix_expected: bool = True,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        - Drop rows missing required keys
        - Drop rows with Expected empty
        - Enforce business rules via infer_expected (optionally auto-fix)
        - Deduplicate by (Username, Password)
        - Warn if duplicates have conflicting Expected
        """
        warnings: List[str] = []
        seed_u, seed_p, seed_display = self._load_login_seed_account()

        required = ("Testcase", "Username", "Password", "Expected")

        seen: Dict[Tuple[str, str], Dict[str, Any]] = {}
        clean: List[Dict[str, Any]] = []

        dropped_missing = 0
        dropped_expected_empty = 0
        dropped_rule_mismatch = 0
        dropped_duplicate = 0
        fixed_expected = 0
        conflict_dup = 0

        for r in rows:
            if not isinstance(r, dict):
                dropped_missing += 1
                continue

            if not all(k in r for k in required):
                dropped_missing += 1
                continue

            rr = dict(r)
            rr["Testcase"] = self._norm_str(rr.get("Testcase"))
            rr["Username"] = self._norm_str(rr.get("Username"))
            rr["Password"] = self._norm_str(rr.get("Password"))
            rr["Expected"] = self._norm_str(rr.get("Expected"))
            rr.setdefault("_source", "AI")

            if rr["Expected"] == "":
                dropped_expected_empty += 1
                continue

            expected_should_be = self._infer_login_expected(
                rr["Username"],
                rr["Password"],
                seed_u,
                seed_p,
                seed_display,
                empty_field_expected,
                invalid_cred_expected,
            )

            if rr["Expected"] != expected_should_be:
                if auto_fix_expected:
                    rr["Expected"] = expected_should_be
                    fixed_expected += 1
                else:
                    dropped_rule_mismatch += 1
                    continue

            # Extra guard: invalid credential case must not have empty inputs
            if rr["Expected"] == invalid_cred_expected and (rr["Username"] == "" or rr["Password"] == ""):
                dropped_rule_mismatch += 1
                continue

            key = (rr["Username"], rr["Password"])
            if key in seen:
                prev = seen[key]
                if self._norm_str(prev.get("Expected")) != rr["Expected"]:
                    conflict_dup += 1
                dropped_duplicate += 1
                continue

            seen[key] = rr
            clean.append(rr)

        if dropped_missing:
            warnings.append(f"[login] Dropped {dropped_missing} rows: missing required fields {required}.")
        if dropped_expected_empty:
            warnings.append(f"[login] Dropped {dropped_expected_empty} rows: Expected is empty.")
        if dropped_rule_mismatch and not auto_fix_expected:
            warnings.append(f"[login] Dropped {dropped_rule_mismatch} rows: business-rule mismatch.")
        if fixed_expected and auto_fix_expected:
            warnings.append(f"[login] Auto-fixed Expected for {fixed_expected} rows to match business rules.")
        if dropped_duplicate:
            warnings.append(f"[login] Dropped {dropped_duplicate} duplicate rows by (Username, Password).")
        if conflict_dup:
            warnings.append(f"[login] Found {conflict_dup} duplicate conflicts: same (Username, Password) but different Expected.")

        return clean, warnings

    # =========================
    # Writer compatibility
    # =========================
    def _write_raw_evidence(self, feature: str, raw_text: str) -> Tuple[Optional[Path], Optional[Path], List[str]]:
        warnings: List[str] = []
        raw_path: Optional[Path] = None
        raw_text_path: Optional[Path] = None

        # preferred
        if hasattr(self.writer, "write_raw_text"):
            try:
                raw_text_path = getattr(self.writer, "write_raw_text")(feature, raw_text)
            except Exception as e:
                warnings.append(f"Write raw_text failed: {e}")

        # backward compatible (wrapper)
        if hasattr(self.writer, "write_raw"):
            try:
                raw_path = getattr(self.writer, "write_raw")(feature, raw_text)
            except Exception as e:
                warnings.append(f"Write raw wrapper failed: {e}")

        return raw_path, raw_text_path, warnings

    def _write_raw_json_clean(self, feature: str, payload: Dict[str, Any]) -> Tuple[Optional[Path], List[str]]:
        warnings: List[str] = []
        raw_json_path: Optional[Path] = None

        if hasattr(self.writer, "write_raw_json"):
            try:
                raw_json_path = getattr(self.writer, "write_raw_json")(feature, payload)
            except Exception as e:
                warnings.append(f"Write raw JSON clean failed: {e}")

        return raw_json_path, warnings

    # =========================
    # Main generate
    # =========================
    def generate(
        self,
        feature: str,
        prompt: str,
        *,
        system: Optional[str] = None,
        formats: Optional[Iterable[str]] = None,
        yaml_ext: str = "yaml",
        llm_kwargs: Optional[Dict[str, Any]] = None,
    ) -> GenerationResult:
        feature = (feature or "").strip().lower()
        llm_kwargs = llm_kwargs or {}

        # 1) Call AI
        try:
            raw_text = self.client.generate_text(prompt=prompt, system=system, **llm_kwargs)
        except Exception as e:
            return GenerationResult(
                ok=False,
                feature=feature,
                errors=[f"LLM call failed: {e}"],
            )

        # 2) Save raw evidence
        raw_path, raw_text_path, warn = self._write_raw_evidence(feature, raw_text)

        # 3) Parse JSON
        parse_result = self.parser.parse_json(raw_text)
        if not parse_result.ok or parse_result.data is None:
            return GenerationResult(
                ok=False,
                feature=feature,
                raw_path=raw_path,
                raw_text_path=raw_text_path,
                warnings=warn,
                errors=[parse_result.error or "Parse failed"],
                raw_text=raw_text,
                cleaned_text=parse_result.cleaned_text,
            )

        data = parse_result.data
        if not isinstance(data, dict) or not isinstance(data.get("items"), list):
            return GenerationResult(
                ok=False,
                feature=feature,
                raw_path=raw_path,
                raw_text_path=raw_text_path,
                warnings=warn,
                errors=["Root JSON must be an object with key 'items' (list)"],
                raw_text=raw_text,
                cleaned_text=parse_result.cleaned_text,
            )

        items = data["items"]

        # 4) Normalize rows (ensure dict + _source)
        normalized_rows: List[Dict[str, Any]] = []
        for it in items:
            if isinstance(it, dict):
                it.setdefault("_source", "AI")
                normalized_rows.append(it)

        # Enforce seed only for login
        if feature == "login":
            normalized_rows = self._enforce_login_success_case(normalized_rows)

            # 4.5) Business validate + dedup (NEW)
            normalized_rows, warn_login = self._validate_and_dedup_login_rows(
                normalized_rows,
                empty_field_expected="Vui lòng điền vào trường này",
                invalid_cred_expected="Tên đăng nhập hoặc mật khẩu không đúng!",
                auto_fix_expected=True,  # processed luôn đúng nghiệp vụ; raw vẫn giữ làm minh chứng
            )
            warn = warn + warn_login

        normalized_payload = {"items": normalized_rows}

        # 5) Validate schema
        v = self.validator.validate(feature, normalized_payload)
        if not isinstance(v, dict) or not v.get("ok", False):
            errs = v.get("errors", []) if isinstance(v, dict) else ["Validation failed"]
            return GenerationResult(
                ok=False,
                feature=feature,
                raw_path=raw_path,
                raw_text_path=raw_text_path,
                warnings=warn,
                errors=errs or ["Validation failed"],
                raw_text=raw_text,
                cleaned_text=parse_result.cleaned_text,
            )

        rows: List[Dict[str, Any]] = v.get("data", normalized_rows)

        # 6) Save raw JSON clean (items)
        raw_clean_path, warn2 = self._write_raw_json_clean(feature, {"items": rows})
        warn_all = warn + warn2
        if raw_clean_path is not None:
            raw_path = raw_clean_path

        # 7) Write processed formats
        try:
            processed_paths = self.writer.write_formats(
                feature=feature,
                rows=rows,
                formats=formats,
                yaml_ext=yaml_ext,
            )
        except Exception as e:
            return GenerationResult(
                ok=False,
                feature=feature,
                raw_path=raw_path,
                raw_text_path=raw_text_path,
                warnings=warn_all,
                errors=[f"Write processed failed: {e}"],
                rows=rows,
                raw_text=raw_text,
                cleaned_text=parse_result.cleaned_text,
            )

        return GenerationResult(
            ok=True,
            feature=feature,
            raw_path=raw_path,
            raw_text_path=raw_text_path,
            processed_paths=processed_paths,
            rows=rows,
            warnings=warn_all,
            errors=[],
            raw_text=raw_text,
            cleaned_text=parse_result.cleaned_text,
        )
