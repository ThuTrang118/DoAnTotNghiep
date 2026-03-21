from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class ValidationResult:
    ok: bool
    errors: List[str]
    warnings: List[str]
    data: Dict[str, Any]


class DataSchemaValidator:
    FEATURE_ALIASES: Dict[str, str] = {
        "profile": "profile_update",
    }

    FEATURE_COLUMNS: Dict[str, List[str]] = {
        "login": ["Testcase", "Username", "Password", "Expected"],
        "login_groups": ["GroupID", "BehaviorGroup", "UsernamePattern", "PasswordPattern", "Expected"],
        "register": ["Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"],
        "register_groups": [
            "GroupID",
            "BehaviorGroup",
            "UsernamePattern",
            "PhonePattern",
            "PasswordPattern",
            "ConfirmPasswordPattern",
            "Expected",
        ],
        "search": ["Testcase", "Keyword", "Expected"],
        "order": ["Testcase", "Product", "Quantity", "Expected"],
        "profile_update": ["Testcase", "Field", "Value", "Expected"],
        "product_review": ["Testcase", "Product", "Rating", "Comment", "Expected"],
    }

    TESTCASE_PREFIXES: Dict[str, str] = {
        "login": "LG",
        "register": "DK",
        "search": "TK",
        "order": "DH",
        "profile_update": "HS",
        "product_review": "DG",
    }

    GROUP_PREFIXES: Dict[str, str] = {
        "login_groups": "LGG",
        "register_groups": "RGG",
    }

    def _normalize_feature(self, feature: str) -> str:
        f = (feature or "").strip().lower()
        return self.FEATURE_ALIASES.get(f, f)

    def _as_str(self, value: Any) -> str:
        return "" if value is None else str(value)

    def _normalize_item(self, item: Dict[str, Any], columns: List[str]) -> Dict[str, str]:
        return {col: self._as_str(item.get(col, "")) for col in columns}

    def _build_testcase_id(self, feature: str, idx: int) -> str:
        prefix = self.TESTCASE_PREFIXES.get(feature, "TC")
        return f"{prefix}{idx:02d}"

    def _build_group_id(self, feature: str, idx: int) -> str:
        prefix = self.GROUP_PREFIXES.get(feature, "GRP")
        return f"{prefix}{idx:02d}"

    def _default_behavior_group(self, feature: str, idx: int) -> str:
        base = feature.replace("_groups", "").replace("_", " ").strip() or "feature"
        return f"{base.title()} group {idx}"

    def _is_groups_feature(self, feature: str) -> bool:
        return feature.endswith("_groups")

    def _id_column(self, feature: str) -> str:
        return "GroupID" if self._is_groups_feature(feature) else "Testcase"

    def _duplicate_key(self, feature: str, item: Dict[str, str]) -> tuple:
        id_col = self._id_column(feature)
        return tuple(v for k, v in item.items() if k != id_col)

    def _sanitize_items_preserve_count(
        self,
        feature: str,
        items: List[Dict[str, str]],
        warnings: List[str],
    ) -> List[Dict[str, str]]:
        normalized: List[Dict[str, str]] = []
        seen_rows = set()

        id_col = self._id_column(feature)

        for idx, item in enumerate(items, start=1):
            row = dict(item)

            # Chuẩn hóa ID nhưng không can thiệp nghiệp vụ/Expected
            if self._is_groups_feature(feature):
                gid = self._as_str(row.get("GroupID", "")).strip().upper()
                expected_gid = self._build_group_id(feature, idx)
                row["GroupID"] = gid if re.fullmatch(r"[A-Z]+\d+", gid) else expected_gid

                if "BehaviorGroup" in row and not row["BehaviorGroup"].strip():
                    row["BehaviorGroup"] = self._default_behavior_group(feature, idx)
            else:
                tid = self._as_str(row.get("Testcase", "")).strip().upper()
                expected_tid = self._build_testcase_id(feature, idx)
                row["Testcase"] = tid if re.fullmatch(r"[A-Z]+\d+", tid) else expected_tid

            dup_key = self._duplicate_key(feature, row)
            if dup_key in seen_rows:
                warnings.append(f"Item[{idx - 1}] duplicated row detected but kept")
            else:
                seen_rows.add(dup_key)

            normalized.append(row)

        # Đánh số lại tuần tự để đầu ra ổn định
        for idx, row in enumerate(normalized, start=1):
            row[id_col] = (
                self._build_group_id(feature, idx)
                if self._is_groups_feature(feature)
                else self._build_testcase_id(feature, idx)
            )

        return normalized

    def validate(self, feature: str, data: Any) -> ValidationResult:
        feature_norm = self._normalize_feature(feature)
        columns = self.FEATURE_COLUMNS.get(feature_norm, ["Testcase", "Expected"])

        if not isinstance(data, dict):
            return ValidationResult(
                ok=False,
                errors=["Root JSON must be an object"],
                warnings=[],
                data={"items": []},
            )

        if "items" not in data:
            return ValidationResult(
                ok=False,
                errors=["Missing 'items' key"],
                warnings=[],
                data={"items": []},
            )

        items = data.get("items")
        if not isinstance(items, list):
            return ValidationResult(
                ok=False,
                errors=["'items' must be a list"],
                warnings=[],
                data={"items": []},
            )

        warnings: List[str] = []
        cleaned_items: List[Dict[str, str]] = []

        for idx, raw_item in enumerate(items):
            if not isinstance(raw_item, dict):
                warnings.append(f"Item[{idx}] is not an object -> dropped")
                continue

            extra = [k for k in raw_item.keys() if k not in columns]
            if extra:
                warnings.append(f"Item[{idx}] extra keys dropped: {extra}")

            missing = [k for k in columns if k not in raw_item]
            if missing:
                warnings.append(f"Item[{idx}] missing keys filled with empty string: {missing}")

            cleaned_items.append(self._normalize_item(raw_item, columns))

        cleaned_items = self._sanitize_items_preserve_count(feature_norm, cleaned_items, warnings)

        errors: List[str] = []
        if not cleaned_items:
            errors.append("No valid items after validation")

        return ValidationResult(
            ok=(len(errors) == 0),
            errors=errors,
            warnings=warnings,
            data={"items": cleaned_items},
        )