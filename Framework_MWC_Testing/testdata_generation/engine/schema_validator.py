# testdata_generation/engine/schema_validator.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple


@dataclass
class ValidationResult:
    ok: bool
    errors: List[str]
    warnings: List[str]
    data: Dict[str, Any]


class DataSchemaValidator:
    """
    Validator theo từng chức năng.

    Nhiệm vụ:
    - Kiểm tra root JSON và key "items"
    - Giữ lại đúng các cột hợp lệ
    - Bổ sung cột thiếu bằng chuỗi rỗng
    - Cảnh báo nếu item thiếu cột / thừa cột / sai kiểu
    - Kiểm tra thêm logic nghiệp vụ cơ bản cho từng feature
    - Cảnh báo trường hợp trùng input
    - Cảnh báo thiếu các nhóm test quan trọng
    """

    FEATURE_ALIASES: Dict[str, str] = {
        "profile": "profile_update",  # hỗ trợ tương thích tên cũ
    }

    FEATURE_REQUIRED_KEYS: Dict[str, Set[str]] = {
        "login": {"Testcase", "Username", "Password", "Expected"},
        "register": {"Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"},
        "search": {"Testcase", "Keyword", "Expected"},
        "order": {"Testcase", "Product", "Quantity", "Expected"},
        "profile_update": {"Testcase", "Field", "Value", "Expected"},
        "product_review": {"Testcase", "Product", "Rating", "Comment", "Expected"},
    }

    FEATURE_ALLOWED_KEYS: Dict[str, Set[str]] = {
        "login": {"Testcase", "Username", "Password", "Expected"},
        "register": {"Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"},
        "search": {"Testcase", "Keyword", "Expected"},
        "order": {"Testcase", "Product", "Quantity", "Expected"},
        "profile_update": {"Testcase", "Field", "Value", "Expected"},
        "product_review": {"Testcase", "Product", "Rating", "Comment", "Expected"},
    }

    DEFAULT_REQUIRED_KEYS: Set[str] = {"Testcase", "Expected"}
    DEFAULT_ALLOWED_KEYS: Set[str] = {"Testcase", "Expected"}

    REGISTER_EXPECTED_SET: Set[str] = {
        "Vui lòng điền vào trường này.",
        "Số điện thoại không đúng định dạng!",
        "Mật khẩu phải lớn hơn 8 ký tự và nhỏ hơn 20 ký tự!",
        "mật khẩu không giống nhau",
        "tài khoản đã tồn tại trong hệ thống",
    }

    LOGIN_EXPECTED_SET: Set[str] = {
        "Vui lòng điền vào trường này",
        "Tên đăng nhập hoặc mật khẩu không đúng!",
    }

    def _normalize_feature(self, feature: str) -> str:
        f = (feature or "").strip().lower()
        return self.FEATURE_ALIASES.get(f, f)

    def _as_str(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    # ============================================================
    # LOGIN LOGIC
    # ============================================================
    def _validate_login_logic(self, items: List[Dict[str, Any]], warnings: List[str]) -> None:
        seen_inputs: Set[Tuple[str, str]] = set()

        seen_success = False
        seen_missing_username = False
        seen_missing_password = False
        seen_wrong_username_or_password = False

        for i, it in enumerate(items):
            username = self._as_str(it.get("Username", ""))
            password = self._as_str(it.get("Password", ""))
            expected = self._as_str(it.get("Expected", ""))

            key = (username, password)
            if key in seen_inputs:
                warnings.append(f"Item[{i}] duplicated input for login: Username + Password")
            seen_inputs.add(key)

            missing_username = username == ""
            missing_password = password == ""
            missing_any = missing_username or missing_password

            if missing_username:
                seen_missing_username = True
            if missing_password:
                seen_missing_password = True

            if expected == "Vui lòng điền vào trường này":
                if not missing_any:
                    warnings.append(
                        f"Item[{i}] login expected missing-field message but Username/Password are not empty"
                    )

            elif expected == "Tên đăng nhập hoặc mật khẩu không đúng!":
                if missing_any:
                    warnings.append(
                        f"Item[{i}] login expected wrong-auth message but some required field is empty"
                    )
                else:
                    seen_wrong_username_or_password = True

            else:
                # success case: expected được xem là username hiển thị / username thành công
                if missing_any:
                    warnings.append(
                        f"Item[{i}] login success-like expected but some required field is empty"
                    )
                seen_success = True

        # coverage warnings
        if items and not seen_success:
            warnings.append("Login coverage warning: missing success case")
        if items and not seen_missing_username:
            warnings.append("Login coverage warning: missing case with empty Username")
        if items and not seen_missing_password:
            warnings.append("Login coverage warning: missing case with empty Password")
        if items and not seen_wrong_username_or_password:
            warnings.append("Login coverage warning: missing wrong-auth case")

    # ============================================================
    # REGISTER LOGIC
    # ============================================================
    def _is_valid_phone_register(self, phone: str) -> bool:
        return bool(re.fullmatch(r"0\d{9}", phone))

    def _is_valid_password_register(self, password: str) -> bool:
        return 8 < len(password) < 20

    def _validate_register_logic(self, items: List[Dict[str, Any]], warnings: List[str]) -> None:
        seen_inputs: Set[Tuple[str, str, str, str]] = set()

        # coverage tracking
        seen_missing_username = False
        seen_missing_phone = False
        seen_missing_password = False
        seen_missing_confirm = False
        seen_missing_multi = False

        seen_phone_invalid = False
        seen_phone_invalid_letter = False
        seen_phone_invalid_special = False
        seen_phone_invalid_space = False
        seen_phone_invalid_length = False
        seen_phone_invalid_not_start_0 = False

        seen_password_invalid = False
        seen_password_len_7 = False
        seen_password_len_8 = False
        seen_password_len_20 = False
        seen_password_len_21 = False

        seen_confirm_mismatch = False
        seen_duplicate_username = False
        seen_success = False

        for i, it in enumerate(items):
            username = self._as_str(it.get("Username", ""))
            phone = self._as_str(it.get("Phone", ""))
            password = self._as_str(it.get("Password", ""))
            confirm = self._as_str(it.get("ConfirmPassword", ""))
            expected = self._as_str(it.get("Expected", ""))

            # duplicate input
            key = (username, phone, password, confirm)
            if key in seen_inputs:
                warnings.append(f"Item[{i}] duplicated input for register")
            seen_inputs.add(key)

            # basic field state
            missing_username = username == ""
            missing_phone = phone == ""
            missing_password = password == ""
            missing_confirm = confirm == ""
            missing_count = sum([missing_username, missing_phone, missing_password, missing_confirm])
            missing_any = missing_count > 0

            if missing_username:
                seen_missing_username = True
            if missing_phone:
                seen_missing_phone = True
            if missing_password:
                seen_missing_password = True
            if missing_confirm:
                seen_missing_confirm = True
            if missing_count >= 2:
                seen_missing_multi = True

            # phone analysis
            phone_valid = self._is_valid_phone_register(phone)

            if phone != "":
                if any(ch.isalpha() for ch in phone):
                    seen_phone_invalid = True
                    seen_phone_invalid_letter = True

                if any((not ch.isdigit()) and (not ch.isspace()) for ch in phone):
                    seen_phone_invalid = True
                    seen_phone_invalid_special = True

                if any(ch.isspace() for ch in phone):
                    seen_phone_invalid = True
                    seen_phone_invalid_space = True

                if phone.isdigit() and len(phone) != 10:
                    seen_phone_invalid = True
                    seen_phone_invalid_length = True

                if phone.isdigit() and len(phone) == 10 and not phone.startswith("0"):
                    seen_phone_invalid = True
                    seen_phone_invalid_not_start_0 = True

                if not phone_valid:
                    seen_phone_invalid = True

            # password analysis
            pw_len = len(password)
            pw_valid = self._is_valid_password_register(password)

            if password != "":
                if not pw_valid:
                    seen_password_invalid = True
                if pw_len == 7:
                    seen_password_len_7 = True
                if pw_len == 8:
                    seen_password_len_8 = True
                if pw_len == 20:
                    seen_password_len_20 = True
                if pw_len == 21:
                    seen_password_len_21 = True

            confirm_match = password == confirm

            # ------------------------------------------------
            # consistency checks giữa dữ liệu và expected
            # ------------------------------------------------
            if expected == "Vui lòng điền vào trường này.":
                if not missing_any:
                    warnings.append(
                        f"Item[{i}] register expected missing-field message but no required field is empty"
                    )

            elif expected == "Số điện thoại không đúng định dạng!":
                if missing_any:
                    warnings.append(
                        f"Item[{i}] register expected phone-format error but some required field is empty"
                    )
                if phone_valid:
                    warnings.append(
                        f"Item[{i}] register expected phone-format error but Phone is valid"
                    )
                else:
                    # đảm bảo đây là nhóm phone invalid
                    seen_phone_invalid = True

            elif expected == "Mật khẩu phải lớn hơn 8 ký tự và nhỏ hơn 20 ký tự!":
                if missing_any:
                    warnings.append(
                        f"Item[{i}] register expected password-length error but some required field is empty"
                    )
                if pw_valid:
                    warnings.append(
                        f"Item[{i}] register expected password-length error but Password length is valid"
                    )
                else:
                    seen_password_invalid = True

            elif expected == "mật khẩu không giống nhau":
                if missing_any:
                    warnings.append(
                        f"Item[{i}] register expected confirm-mismatch but some required field is empty"
                    )
                if confirm_match:
                    warnings.append(
                        f"Item[{i}] register expected confirm-mismatch but Password == ConfirmPassword"
                    )
                else:
                    seen_confirm_mismatch = True

            elif expected == "tài khoản đã tồn tại trong hệ thống":
                if missing_any:
                    warnings.append(
                        f"Item[{i}] register expected duplicate-username but some required field is empty"
                    )
                # không thể xác thực trùng thật nếu không có seed, nên chỉ đánh dấu coverage
                seen_duplicate_username = True

            else:
                # success case: Expected phải là chính Username
                if expected != username:
                    warnings.append(
                        f"Item[{i}] register success case expected must equal Username, got Expected='{expected}', Username='{username}'"
                    )

                if missing_any:
                    warnings.append(
                        f"Item[{i}] register success case but some required field is empty"
                    )

                if not phone_valid:
                    warnings.append(
                        f"Item[{i}] register success case but Phone is invalid"
                    )

                if not pw_valid:
                    warnings.append(
                        f"Item[{i}] register success case but Password length is invalid"
                    )

                if not confirm_match:
                    warnings.append(
                        f"Item[{i}] register success case but ConfirmPassword does not match Password"
                    )

                seen_success = True

        # ------------------------------------------------
        # coverage warnings
        # ------------------------------------------------
        if items and not seen_missing_username:
            warnings.append("Register coverage warning: missing case with empty Username")
        if items and not seen_missing_phone:
            warnings.append("Register coverage warning: missing case with empty Phone")
        if items and not seen_missing_password:
            warnings.append("Register coverage warning: missing case with empty Password")
        if items and not seen_missing_confirm:
            warnings.append("Register coverage warning: missing case with empty ConfirmPassword")
        if items and not seen_missing_multi:
            warnings.append("Register coverage warning: missing case with multiple empty required fields")

        if items and not seen_phone_invalid:
            warnings.append("Register coverage warning: missing phone invalid case")
        else:
            if items and not seen_phone_invalid_letter:
                warnings.append("Register coverage warning: missing phone invalid case with letters")
            if items and not seen_phone_invalid_special:
                warnings.append("Register coverage warning: missing phone invalid case with special characters")
            if items and not seen_phone_invalid_space:
                warnings.append("Register coverage warning: missing phone invalid case with whitespace")
            if items and not seen_phone_invalid_length:
                warnings.append("Register coverage warning: missing phone invalid case with wrong length")
            if items and not seen_phone_invalid_not_start_0:
                warnings.append("Register coverage warning: missing phone invalid case not starting with 0")

        if items and not seen_password_invalid:
            warnings.append("Register coverage warning: missing password invalid-length case")
        else:
            if items and not seen_password_len_7:
                warnings.append("Register coverage warning: missing password boundary case length 7")
            if items and not seen_password_len_8:
                warnings.append("Register coverage warning: missing password boundary case length 8")
            if items and not seen_password_len_20:
                warnings.append("Register coverage warning: missing password boundary case length 20")
            if items and not seen_password_len_21:
                warnings.append("Register coverage warning: missing password boundary case length 21")

        if items and not seen_confirm_mismatch:
            warnings.append("Register coverage warning: missing ConfirmPassword mismatch case")
        if items and not seen_success:
            warnings.append("Register coverage warning: missing success case")
        if items and not seen_duplicate_username:
            warnings.append("Register coverage warning: missing duplicate username case")

    # ============================================================
    # PUBLIC VALIDATE
    # ============================================================
    def validate(self, feature: str, data: Any) -> ValidationResult:
        feature_norm = self._normalize_feature(feature)

        errors: List[str] = []
        warnings: List[str] = []

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

        required = self.FEATURE_REQUIRED_KEYS.get(feature_norm, self.DEFAULT_REQUIRED_KEYS)
        allowed = self.FEATURE_ALLOWED_KEYS.get(feature_norm, self.DEFAULT_ALLOWED_KEYS)

        cleaned_items: List[Dict[str, Any]] = []

        for idx, it in enumerate(items):
            if not isinstance(it, dict):
                warnings.append(f"Item[{idx}] is not an object -> dropped")
                continue

            missing = [k for k in required if k not in it]
            if missing:
                warnings.append(f"Item[{idx}] missing keys: {missing}")

            extra = [k for k in it.keys() if k not in allowed]
            if extra:
                warnings.append(f"Item[{idx}] extra keys dropped: {extra}")

            cleaned: Dict[str, Any] = {}

            # Giữ đúng các cột hợp lệ, ép None -> ""
            for k in allowed:
                if k in it:
                    cleaned[k] = "" if it.get(k) is None else it.get(k)

            # Bổ sung cột bắt buộc còn thiếu
            for k in required:
                cleaned.setdefault(k, "")

            cleaned_items.append(cleaned)

        if not cleaned_items:
            errors.append("No valid items after validation")

        # ------------------------------------------------
        # feature-specific logic checks
        # ------------------------------------------------
        if cleaned_items:
            if feature_norm == "login":
                self._validate_login_logic(cleaned_items, warnings)
            elif feature_norm == "register":
                self._validate_register_logic(cleaned_items, warnings)

        ok = len(errors) == 0 and len(cleaned_items) > 0

        return ValidationResult(
            ok=ok,
            errors=errors,
            warnings=warnings,
            data={"items": cleaned_items},
        )