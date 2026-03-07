# testdata_generation/engine/validator.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set


@dataclass
class ValidationResult:
    ok: bool
    errors: List[str]
    warnings: List[str]
    data: Dict[str, Any]


class DataSchemaValidator:

    FEATURE_REQUIRED_KEYS: Dict[str, Set[str]] = {
        "login": {"Testcase", "Username", "Password", "Expected"},
        "register": {"Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"},
        "search": {"Testcase", "Keyword", "Expected"},
        "order": {"Testcase", "Product", "Quantity", "Expected"},
        "profile": {"Testcase", "Field", "Value", "Expected"},
    }

    FEATURE_ALLOWED_KEYS: Dict[str, Set[str]] = {
        "login": {"Testcase", "Username", "Password", "Expected"},
        "register": {"Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"},
        "search": {"Testcase", "Keyword", "Expected"},
        "order": {"Testcase", "Product", "Quantity", "Expected"},
        "profile": {"Testcase", "Field", "Value", "Expected"},
    }

    DEFAULT_REQUIRED_KEYS: Set[str] = {"Testcase", "Username", "Password", "Expected"}
    DEFAULT_ALLOWED_KEYS: Set[str] = {"Testcase", "Username", "Password", "Expected"}

    def validate(self, feature: str, data: Any) -> ValidationResult:
        errors: List[str] = []
        warnings: List[str] = []

        if not isinstance(data, dict):
            return ValidationResult(False, ["Root JSON must be an object"], [], {"items": []})

        if "items" not in data:
            return ValidationResult(False, ["Missing 'items' key"], [], {"items": []})

        items = data.get("items")
        if not isinstance(items, list):
            return ValidationResult(False, ["'items' must be a list"], [], {"items": []})

        required = self.FEATURE_REQUIRED_KEYS.get(feature, self.DEFAULT_REQUIRED_KEYS)
        allowed = self.FEATURE_ALLOWED_KEYS.get(feature, self.DEFAULT_ALLOWED_KEYS)

        cleaned_items: List[Dict[str, Any]] = []
        for idx, it in enumerate(items):
            if not isinstance(it, dict):
                warnings.append(f"Item[{idx}] is not an object -> dropped")
                continue

            missing = [k for k in required if k not in it]
            if missing:
                # warn (not hard fail) to avoid blocking framework
                warnings.append(f"Item[{idx}] missing keys: {missing}")

            # drop extra keys
            cleaned = {k: it.get(k, "") for k in it.keys() if k in allowed}
            # ensure required keys exist (fill empty)
            for k in required:
                cleaned.setdefault(k, "")

            cleaned_items.append(cleaned)

        ok = len(cleaned_items) > 0 and len(errors) == 0
        return ValidationResult(ok, errors, warnings, {"items": cleaned_items})