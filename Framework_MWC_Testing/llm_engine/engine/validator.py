# llm_engine/engine/validator.py
from __future__ import annotations

from typing import Any, Dict, List, Set


class DataSchemaValidator:
    """
    Feature-aware schema validator.

    Behavior:
    - Requires: required keys must be present.
    - Cleans: removes unexpected keys instead of failing (so LLM adding _source won't break register).
    - Optionally returns warnings about dropped keys.
    """

    # Required keys per feature
    FEATURE_REQUIRED_KEYS: Dict[str, Set[str]] = {
        # login keeps _source (your old pipeline)
        "login": {"Testcase", "Username", "Password", "Expected", "_source"},

        # register must NOT require _source
        "register": {"Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"},

        # add other features here when needed
        "search": {"Testcase", "Keyword", "Expected"},
        "order": {"Testcase", "Product", "Quantity", "Expected"},
        "profile": {"Testcase", "Field", "Value", "Expected"},
    }

    # Allowed keys per feature (anything else will be dropped)
    FEATURE_ALLOWED_KEYS: Dict[str, Set[str]] = {
        "login": {"Testcase", "Username", "Password", "Expected", "_source"},
        "register": {"Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"},
        "search": {"Testcase", "Keyword", "Expected"},
        "order": {"Testcase", "Product", "Quantity", "Expected"},
        "profile": {"Testcase", "Field", "Value", "Expected"},
    }

    # Fallback (backward compatibility if feature name is unknown)
    DEFAULT_REQUIRED_KEYS: Set[str] = {"Testcase", "Username", "Password", "Expected"}
    DEFAULT_ALLOWED_KEYS: Set[str] = {"Testcase", "Username", "Password", "Expected", "_source"}

    def _required_keys(self, feature: str) -> Set[str]:
        return self.FEATURE_REQUIRED_KEYS.get(feature, self.DEFAULT_REQUIRED_KEYS)

    def _allowed_keys(self, feature: str) -> Set[str]:
        return self.FEATURE_ALLOWED_KEYS.get(feature, self.DEFAULT_ALLOWED_KEYS)

    def validate(self, feature: str, data: Any) -> Dict[str, Any]:
        # Root validation
        if not isinstance(data, dict):
            return {"ok": False, "errors": ["Root JSON must be an object"], "warnings": [], "data": []}

        if "items" not in data:
            return {"ok": False, "errors": ["Missing 'items' key"], "warnings": [], "data": []}

        items = data["items"]
        if not isinstance(items, list):
            return {"ok": False, "errors": ["'items' must be a list"], "warnings": [], "data": []}

        required = self._required_keys(feature)
        allowed = self._allowed_keys(feature)

        errors: List[str] = []
        warnings: List[str] = []
        cleaned: List[Dict[str, Any]] = []

        if feature not in self.FEATURE_REQUIRED_KEYS:
            warnings.append(
                f"Unknown feature '{feature}'. Using DEFAULT_REQUIRED_KEYS={sorted(self.DEFAULT_REQUIRED_KEYS)}"
            )

        for i, row in enumerate(items):
            if not isinstance(row, dict):
                errors.append(f"Item {i} is not an object")
                continue

            row_keys = set(row.keys())
            missing = required - row_keys
            if missing:
                errors.append(f"Item {i} missing required keys: {sorted(missing)}")
                continue

            # Drop unexpected keys instead of failing
            extra = row_keys - allowed
            if extra:
                warnings.append(f"Item {i} dropped unexpected keys: {sorted(extra)}")

            # Keep only allowed keys (order not guaranteed in dict; writer handles columns)
            cleaned_row = {k: row.get(k, "") for k in allowed}
            # But ensure required keys are present (already checked)
            cleaned.append(cleaned_row)

        return {
            "ok": len(errors) == 0,
            "data": cleaned,
            "errors": errors,
            "warnings": warnings,
        }
