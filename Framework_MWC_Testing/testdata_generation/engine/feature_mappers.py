from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value)


def _group_id_to_login_testcase(group_id: str, index: int) -> str:
    gid = (group_id or "").strip().upper()
    if gid.startswith("LGG") and gid[3:].isdigit():
        return f"LG{int(gid[3:]):02d}"
    return f"LG{index:02d}"


def _group_id_to_register_testcase(group_id: str, index: int) -> str:
    gid = (group_id or "").strip().upper()
    if gid.startswith("RGG") and gid[3:].isdigit():
        return f"DK{int(gid[3:]):02d}"
    return f"DK{index:02d}"


def build_login_payload_from_groups(groups_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
    items: List[Dict[str, str]] = []
    warnings: List[str] = []

    for idx, row in enumerate(groups_rows, start=1):
        if not isinstance(row, dict):
            warnings.append(f"Skipped non-object login group at source index {idx}")
            continue

        items.append(
            {
                "Testcase": _group_id_to_login_testcase(_safe_str(row.get("GroupID", "")), idx),
                "Username": _safe_str(row.get("UsernamePattern", "")),
                "Password": _safe_str(row.get("PasswordPattern", "")),
                "Expected": _safe_str(row.get("Expected", "")),
            }
        )

    for idx, item in enumerate(items, start=1):
        item["Testcase"] = f"LG{idx:02d}"

    return {"items": items}, warnings


def build_register_payload_from_groups(groups_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
    items: List[Dict[str, str]] = []
    warnings: List[str] = []

    for idx, row in enumerate(groups_rows, start=1):
        if not isinstance(row, dict):
            warnings.append(f"Skipped non-object register group at source index {idx}")
            continue

        items.append(
            {
                "Testcase": _group_id_to_register_testcase(_safe_str(row.get("GroupID", "")), idx),
                "Username": _safe_str(row.get("UsernamePattern", "")),
                "Phone": _safe_str(row.get("PhonePattern", "")),
                "Password": _safe_str(row.get("PasswordPattern", "")),
                "ConfirmPassword": _safe_str(row.get("ConfirmPasswordPattern", "")),
                "Expected": _safe_str(row.get("Expected", "")),
            }
        )

    for idx, item in enumerate(items, start=1):
        item["Testcase"] = f"DK{idx:02d}"

    return {"items": items}, warnings


GROUP_TO_FINAL_MAPPERS = {
    "login": build_login_payload_from_groups,
    "register": build_register_payload_from_groups,
}