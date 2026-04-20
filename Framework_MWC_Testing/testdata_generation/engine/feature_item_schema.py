from __future__ import annotations

from typing import Dict, List


FEATURE_ITEM_FIELDS: Dict[str, List[str]] = {
    "login": ["Username", "Password"],
    "register": ["Username", "Phone", "Password", "ConfirmPassword"],
    "search": ["Keyword"],
    "order": ["FullName", "Phone", "Address", "Province", "District", "Ward"],
    "profile_update": ["Field", "Value"],
    "product_review": ["FullName", "Phone", "Email", "Title", "Rating", "Content"],
}

FEATURE_TESTCASE_PREFIX: Dict[str, str] = {
    "login": "LG",
    "register": "RG",
    "search": "SR",
    "order": "OR",
    "profile_update": "PF",
    "product_review": "RV",
}

FEATURE_OUTPUT_BASENAME: Dict[str, str] = {
    "login": "LoginData",
    "register": "RegisterData",
    "search": "SearchData",
    "order": "OrderData",
    "profile_update": "ProfileUpdateData",
    "product_review": "ProductReviewData",
}


def normalize_feature_name(feature: str) -> str:
    """
    Chuẩn hoá tên chức năng về feature key nội bộ.
    Hỗ trợ key kỹ thuật, alias tiếng Việt có/không dấu và một số biến thể dài.
    """
    feature = (feature or "").strip().lower()

    aliases = {
        # login
        "login": "login",
        "đăng nhập": "login",
        "dang nhap": "login",
        "đăng nhập tài khoản": "login",
        "dang nhap tai khoan": "login",

        # register
        "register": "register",
        "đăng ký": "register",
        "dang ky": "register",
        "đăng ký tài khoản": "register",
        "dang ky tai khoan": "register",
        "tạo tài khoản": "register",
        "tao tai khoan": "register",

        # search
        "search": "search",
        "tìm kiếm": "search",
        "tim kiem": "search",

        # order
        "order": "order",
        "đặt hàng": "order",
        "dat hang": "order",

        # profile update
        "profile_update": "profile_update",
        "profile update": "profile_update",
        "profile": "profile_update",
        "cập nhật thông tin": "profile_update",
        "cap nhat thong tin": "profile_update",
        "cập nhật thông tin cá nhân": "profile_update",
        "cap nhat thong tin ca nhan": "profile_update",

        # product review
        "product_review": "product_review",
        "product review": "product_review",
        "review": "product_review",
        "đánh giá sản phẩm": "product_review",
        "danh gia san pham": "product_review",
    }

    return aliases.get(feature, feature)


def get_feature_item_fields(feature: str) -> List[str]:
    feature_name = normalize_feature_name(feature)
    if feature_name not in FEATURE_ITEM_FIELDS:
        raise ValueError(f"Unsupported feature for item schema: '{feature}'")
    return FEATURE_ITEM_FIELDS[feature_name]


def get_feature_testcase_prefix(feature: str) -> str:
    feature_name = normalize_feature_name(feature)
    if feature_name not in FEATURE_TESTCASE_PREFIX:
        raise ValueError(f"Unsupported feature for testcase prefix: '{feature}'")
    return FEATURE_TESTCASE_PREFIX[feature_name]


def get_feature_output_basename(feature: str) -> str:
    feature_name = normalize_feature_name(feature)
    if feature_name not in FEATURE_OUTPUT_BASENAME:
        raise ValueError(f"Unsupported feature for output basename: '{feature}'")
    return FEATURE_OUTPUT_BASENAME[feature_name]


def get_feature_column_order(feature: str) -> List[str]:
    fields = get_feature_item_fields(feature)
    return ["Testcase", *fields, "Expected"]


def extract_inputs_from_testcase(item: dict) -> dict:
    if not isinstance(item, dict):
        raise ValueError(f"Each testcase must be a dict, got: {type(item).__name__}")

    inputs = item.get("inputs", {})
    if not isinstance(inputs, dict):
        raise ValueError("Invalid testcase: 'inputs' must be an object")

    return inputs


def build_default_testcase_id(feature: str, index: int) -> str:
    prefix = get_feature_testcase_prefix(feature)
    return f"{prefix}{index:02d}"
