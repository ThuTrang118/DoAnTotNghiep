from __future__ import annotations

from typing import Dict, List


FEATURE_ITEM_FIELDS: Dict[str, List[str]] = {
    "login": ["Username", "Password"],
    "register": ["Username", "Phone", "Password", "ConfirmPassword"],
    "search": ["Keyword"],
    "order": ["Product", "Quantity"],
    "profile_update": ["Field", "Value"],
    "product_review": ["Product", "Rating", "Comment"],
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
    feature = (feature or "").strip().lower()
    aliases = {
        "profile": "profile_update",
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


def build_item_fields_schema(feature: str, indent: str = "      ") -> str:
    fields = get_feature_item_fields(feature)
    return "\n".join([f'{indent}"{field}": "",' for field in fields])


def build_item_fields_rules(feature: str) -> str:
    fields = get_feature_item_fields(feature)
    return "\n".join([f"- {field}" for field in fields])


def build_item_fields_type_rules(feature: str) -> str:
    fields = get_feature_item_fields(feature)
    return "\n".join([f"- {field} là string" for field in fields])


def build_testcase_id_rule_text() -> str:
    lines = [
        "Testcase phải có định dạng:",
        "",
        "<PREFIX><NN>",
        "",
        "Trong đó:",
        "- PREFIX là viết tắt của chức năng",
        "- NN là số thứ tự 2 chữ số: 01, 02, 03...",
        "",
        "Mapping PREFIX theo feature:",
    ]

    for feature, prefix in FEATURE_TESTCASE_PREFIX.items():
        lines.append(f"- {feature} -> {prefix}")

    lines.extend(
        [
            "",
            "Ví dụ:",
            "- LG01, LG02, LG03",
            "- RG01, RG02",
            "- SR01",
        ]
    )

    return "\n".join(lines)


def get_feature_column_order(feature: str) -> List[str]:
    fields = get_feature_item_fields(feature)
    return ["Testcase", "Technique", "Objective", *fields, "Expected"]


def assign_testcase_ids(feature: str, items: List[dict]) -> List[dict]:
    prefix = get_feature_testcase_prefix(feature)

    for i, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Each item must be a dict, got: {type(item).__name__}")
        item["Testcase"] = f"{prefix}{i:02d}"

    return items