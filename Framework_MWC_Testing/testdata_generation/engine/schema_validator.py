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

    LOGIN_SEED_USERNAME = "AnhDuong11"
    LOGIN_SEED_PASSWORD = "anhduong@123"
    LOGIN_EXPECTED_MISSING = "Vui lòng điền vào trường này"
    LOGIN_EXPECTED_WRONG = "Tên đăng nhập hoặc mật khẩu không đúng!"

    REGISTER_EXISTING_USERNAME = "AnhDuong11"
    REGISTER_EXPECTED_REQUIRED = "Vui lòng điền vào trường này."
    REGISTER_EXPECTED_PHONE = "Số điện thoại không đúng định dạng!"
    REGISTER_EXPECTED_PASSWORD = "Mật khẩu phải lớn hơn 8 ký tự và nhỏ hơn 20 ký tự!"
    REGISTER_EXPECTED_CONFIRM = "Mật khẩu không giống nhau"
    REGISTER_EXPECTED_DUPLICATE = "Tài khoản đã tồn tại trong hệ thống"

    def _normalize_feature(self, feature: str) -> str:
        f = (feature or "").strip().lower()
        return self.FEATURE_ALIASES.get(f, f)

    def _as_str(self, value: Any) -> str:
        return "" if value is None else str(value)

    def _normalize_item(self, item: Dict[str, Any], columns: List[str]) -> Dict[str, str]:
        cleaned: Dict[str, str] = {}
        for col in columns:
            cleaned[col] = self._as_str(item.get(col, ""))
        return cleaned

    # =====================================================
    # LOGIN
    # =====================================================
    def _normalize_login_expected(self, username: str, password: str, expected: str) -> str:
        if username == "" or password == "":
            return self.LOGIN_EXPECTED_MISSING
        if username == self.LOGIN_SEED_USERNAME and password == self.LOGIN_SEED_PASSWORD:
            return self.LOGIN_SEED_USERNAME
        return self.LOGIN_EXPECTED_WRONG

    def _sanitize_login_items_preserve_count(
        self,
        items: List[Dict[str, str]],
        warnings: List[str],
    ) -> List[Dict[str, str]]:
        normalized: List[Dict[str, str]] = []

        seen_exact = set()
        seen_input = set()

        for idx, item in enumerate(items):
            username = self._as_str(item.get("Username", ""))
            password = self._as_str(item.get("Password", ""))
            expected_old = self._as_str(item.get("Expected", ""))
            expected_new = self._normalize_login_expected(username, password, expected_old)

            if expected_old != expected_new:
                warnings.append(
                    f"Item[{idx}] login Expected adjusted from '{expected_old}' to '{expected_new}'"
                )

            key_exact = (username, password, expected_new)
            key_input = (username, password)

            if key_exact in seen_exact:
                warnings.append(f"Item[{idx}] duplicated login row detected but kept")
            else:
                seen_exact.add(key_exact)

            if key_input in seen_input:
                warnings.append(f"Item[{idx}] duplicated login input detected but kept")
            else:
                seen_input.add(key_input)

            normalized.append(
                {
                    "Testcase": self._as_str(item.get("Testcase", "")),
                    "Username": username,
                    "Password": password,
                    "Expected": expected_new,
                }
            )

        for idx, item in enumerate(normalized, start=1):
            item["Testcase"] = f"LG{idx:02d}"

        return normalized

    def _sanitize_login_groups_items_preserve_count(
        self,
        items: List[Dict[str, str]],
        warnings: List[str],
    ) -> List[Dict[str, str]]:
        normalized: List[Dict[str, str]] = []

        seen_exact = set()
        seen_input = set()

        for idx, item in enumerate(items):
            username = self._as_str(item.get("UsernamePattern", ""))
            password = self._as_str(item.get("PasswordPattern", ""))
            expected_old = self._as_str(item.get("Expected", ""))
            expected_new = self._normalize_login_expected(username, password, expected_old)

            if expected_old != expected_new:
                warnings.append(
                    f"Item[{idx}] login_groups Expected adjusted from '{expected_old}' to '{expected_new}'"
                )

            row = {
                "GroupID": self._as_str(item.get("GroupID", "")),
                "BehaviorGroup": self._as_str(item.get("BehaviorGroup", "")),
                "UsernamePattern": username,
                "PasswordPattern": password,
                "Expected": expected_new,
            }

            key_exact = (username, password, expected_new)
            key_input = (username, password)

            if key_exact in seen_exact:
                warnings.append(f"Item[{idx}] duplicated login_groups row detected but kept")
            else:
                seen_exact.add(key_exact)

            if key_input in seen_input:
                warnings.append(f"Item[{idx}] duplicated login_groups input detected but kept")
            else:
                seen_input.add(key_input)

            gid = self._as_str(row.get("GroupID", "")).strip().upper()
            row["GroupID"] = gid if re.fullmatch(r"LGG\d+", gid) else f"LGG{idx + 1:02d}"

            if not row["BehaviorGroup"].strip():
                row["BehaviorGroup"] = f"Login group {idx + 1}"

            normalized.append(row)

        return normalized

    def _validate_login_coverage(self, items: List[Dict[str, str]], warnings: List[str]) -> None:
        has_success = any(it["Expected"] == self.LOGIN_SEED_USERNAME for it in items)
        has_empty_username = any(it["Username"] == "" for it in items)
        has_empty_password = any(it["Password"] == "" for it in items)
        has_both_empty = any(it["Username"] == "" and it["Password"] == "" for it in items)
        has_wrong = any(it["Expected"] == self.LOGIN_EXPECTED_WRONG for it in items)
        has_spaces = any(it["Username"] == "   " or it["Password"] == "   " for it in items)
        has_trim = any(
            (it["Username"] != "" and it["Username"] != it["Username"].strip())
            or (it["Password"] != "" and it["Password"] != it["Password"].strip())
            for it in items
        )
        has_case = any(
            (it["Username"].lower() == self.LOGIN_SEED_USERNAME.lower() and it["Username"] != self.LOGIN_SEED_USERNAME)
            or (it["Password"].lower() == self.LOGIN_SEED_PASSWORD.lower() and it["Password"] != self.LOGIN_SEED_PASSWORD)
            for it in items
        )

        if not has_success:
            warnings.append("Login coverage warning: missing success case")
        if not has_empty_username:
            warnings.append("Login coverage warning: missing empty Username case")
        if not has_empty_password:
            warnings.append("Login coverage warning: missing empty Password case")
        if not has_both_empty:
            warnings.append("Login coverage warning: missing both-empty case")
        if not has_wrong:
            warnings.append("Login coverage warning: missing wrong-auth case")
        if not has_spaces:
            warnings.append("Login coverage warning: missing spaces-only case")
        if not has_trim:
            warnings.append("Login coverage warning: missing leading/trailing spaces case")
        if not has_case:
            warnings.append("Login coverage warning: missing case-sensitive case")

    # =====================================================
    # REGISTER
    # =====================================================
    def _is_phone_invalid(self, phone: str) -> bool:
        if phone == "":
            return False
        if len(phone) != 10:
            return True
        if not phone.startswith("0"):
            return True
        if not phone.isdigit():
            return True
        if " " in phone:
            return True
        return False

    def _is_password_length_invalid(self, password: str) -> bool:
        if password == "":
            return False
        return not (len(password) > 8 and len(password) < 20)

    def _normalize_register_expected(
        self,
        username: str,
        phone: str,
        password: str,
        confirm: str,
        expected: str,
    ) -> str:
        if username == "" or phone == "" or password == "" or confirm == "":
            return self.REGISTER_EXPECTED_REQUIRED
        if username == self.REGISTER_EXISTING_USERNAME:
            return self.REGISTER_EXPECTED_DUPLICATE
        if self._is_phone_invalid(phone):
            return self.REGISTER_EXPECTED_PHONE
        if self._is_password_length_invalid(password):
            return self.REGISTER_EXPECTED_PASSWORD
        if confirm != password:
            return self.REGISTER_EXPECTED_CONFIRM
        return username

    def _sanitize_register_items_preserve_count(
        self,
        items: List[Dict[str, str]],
        warnings: List[str],
    ) -> List[Dict[str, str]]:
        normalized: List[Dict[str, str]] = []

        seen_exact = set()
        seen_input = set()

        for idx, item in enumerate(items):
            username = self._as_str(item.get("Username", ""))
            phone = self._as_str(item.get("Phone", ""))
            password = self._as_str(item.get("Password", ""))
            confirm = self._as_str(item.get("ConfirmPassword", ""))
            expected_old = self._as_str(item.get("Expected", ""))
            expected_new = self._normalize_register_expected(username, phone, password, confirm, expected_old)

            if expected_old != expected_new:
                warnings.append(
                    f"Item[{idx}] register Expected adjusted from '{expected_old}' to '{expected_new}'"
                )

            key_exact = (username, phone, password, confirm, expected_new)
            key_input = (username, phone, password, confirm)

            if key_exact in seen_exact:
                warnings.append(f"Item[{idx}] duplicated register row detected but kept")
            else:
                seen_exact.add(key_exact)

            if key_input in seen_input:
                warnings.append(f"Item[{idx}] duplicated register input detected but kept")
            else:
                seen_input.add(key_input)

            normalized.append(
                {
                    "Testcase": self._as_str(item.get("Testcase", "")),
                    "Username": username,
                    "Phone": phone,
                    "Password": password,
                    "ConfirmPassword": confirm,
                    "Expected": expected_new,
                }
            )

        for idx, item in enumerate(normalized, start=1):
            item["Testcase"] = f"DK{idx:02d}"

        return normalized

    def _sanitize_register_groups_items_preserve_count(
        self,
        items: List[Dict[str, str]],
        warnings: List[str],
    ) -> List[Dict[str, str]]:
        normalized: List[Dict[str, str]] = []

        seen_exact = set()
        seen_input = set()

        for idx, item in enumerate(items):
            username = self._as_str(item.get("UsernamePattern", ""))
            phone = self._as_str(item.get("PhonePattern", ""))
            password = self._as_str(item.get("PasswordPattern", ""))
            confirm = self._as_str(item.get("ConfirmPasswordPattern", ""))
            expected_old = self._as_str(item.get("Expected", ""))
            expected_new = self._normalize_register_expected(username, phone, password, confirm, expected_old)

            if expected_old != expected_new:
                warnings.append(
                    f"Item[{idx}] register_groups Expected adjusted from '{expected_old}' to '{expected_new}'"
                )

            row = {
                "GroupID": self._as_str(item.get("GroupID", "")),
                "BehaviorGroup": self._as_str(item.get("BehaviorGroup", "")),
                "UsernamePattern": username,
                "PhonePattern": phone,
                "PasswordPattern": password,
                "ConfirmPasswordPattern": confirm,
                "Expected": expected_new,
            }

            key_exact = (username, phone, password, confirm, expected_new)
            key_input = (username, phone, password, confirm)

            if key_exact in seen_exact:
                warnings.append(f"Item[{idx}] duplicated register_groups row detected but kept")
            else:
                seen_exact.add(key_exact)

            if key_input in seen_input:
                warnings.append(f"Item[{idx}] duplicated register_groups input detected but kept")
            else:
                seen_input.add(key_input)

            gid = self._as_str(row.get("GroupID", "")).strip().upper()
            row["GroupID"] = gid if re.fullmatch(r"RGG\d+", gid) else f"RGG{idx + 1:02d}"

            if not row["BehaviorGroup"].strip():
                row["BehaviorGroup"] = f"Register group {idx + 1}"

            normalized.append(row)

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

        if feature_norm == "login":
            cleaned_items = self._sanitize_login_items_preserve_count(cleaned_items, warnings)
            self._validate_login_coverage(cleaned_items, warnings)

        elif feature_norm == "login_groups":
            cleaned_items = self._sanitize_login_groups_items_preserve_count(cleaned_items, warnings)

        elif feature_norm == "register":
            cleaned_items = self._sanitize_register_items_preserve_count(cleaned_items, warnings)

        elif feature_norm == "register_groups":
            cleaned_items = self._sanitize_register_groups_items_preserve_count(cleaned_items, warnings)

        else:
            for idx, item in enumerate(cleaned_items, start=1):
                if "Testcase" in item and not item["Testcase"].strip():
                    item["Testcase"] = f"TC{idx:02d}"

        errors: List[str] = []
        if not cleaned_items:
            errors.append("No valid items after validation")

        return ValidationResult(
            ok=(len(errors) == 0),
            errors=errors,
            warnings=warnings,
            data={"items": cleaned_items},
        )