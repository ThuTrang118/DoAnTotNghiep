from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import List


def normalize_feature_name(feature: str) -> str:
    """
    Chuẩn hoá tên chức năng về key nội bộ.

    Không cần khai báo thủ công từng feature.
    Ví dụ:
    - "register" -> "register"
    - "đăng ký" -> "dang_ky"
    - "profile update" -> "profile_update"
    """
    text = str(feature or "").strip().lower()
    if not text:
        return ""

    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("đ", "d")

    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")

    return text


def _default_features_dir() -> Path:
    """
    Trỏ tới thư mục:
    testdata_generation/input/features/
    """
    return Path(__file__).resolve().parents[1] / "input" / "features"


def _resolve_feature_spec_path(
    feature: str,
    features_dir: str | Path | None = None,
) -> Path:
    """
    Tìm file mô tả nghiệp vụ của feature.

    Ưu tiên:
    1. input/features/<feature_key>.txt
    2. input/features/<tên gốc>.txt
    """
    base_dir = Path(features_dir).resolve() if features_dir else _default_features_dir()

    raw_name = str(feature or "").strip()
    feature_key = normalize_feature_name(raw_name)

    candidates: List[Path] = []

    if feature_key:
        candidates.append(base_dir / f"{feature_key}.txt")

    if raw_name:
        candidates.append(base_dir / f"{raw_name}.txt")

    # tránh trùng path
    unique_candidates: List[Path] = []
    seen = set()
    for path in candidates:
        key = str(path)
        if key not in seen:
            seen.add(key)
            unique_candidates.append(path)

    for path in unique_candidates:
        if path.exists() and path.is_file():
            return path

    tried = "\n- ".join(str(p) for p in unique_candidates)
    raise FileNotFoundError(
        "Không tìm thấy file mô tả nghiệp vụ cho feature "
        f"'{feature}'. Đã thử:\n- {tried}"
    )


def load_feature_spec_text(
    feature: str,
    features_dir: str | Path | None = None,
) -> str:
    """
    Đọc nội dung file input/features/<feature>.txt.
    """
    path = _resolve_feature_spec_path(feature, features_dir=features_dir)
    content = path.read_text(encoding="utf-8").strip()

    if not content:
        raise ValueError(f"File mô tả nghiệp vụ đang rỗng: {path}")

    return content


def extract_input_fields_from_spec_text(spec_text: str) -> List[str]:
    """
    Tự trích xuất danh sách field từ phần INPUT trong mô tả nghiệp vụ.

    Hỗ trợ dạng:

    INPUT: gồm n field
    1. FieldA: ...
    2. FieldB: ...
    3. FieldC: ...

    Kết quả:
    ["FieldA", "FieldB", "FieldC"]
    """
    if not isinstance(spec_text, str) or not spec_text.strip():
        raise ValueError("Feature specification is empty.")

    fields: List[str] = []
    in_input_block = False

    for raw_line in spec_text.splitlines():
        line = raw_line.strip()

        if not line:
            continue

        upper = line.upper()

        # Bắt đầu vùng INPUT
        if upper.startswith("INPUT") or "INPUT:" in upper:
            in_input_block = True
            continue

        if not in_input_block:
            continue

        # Nếu gặp đường phân cách thì bỏ qua
        if set(line) <= {"=", "-", "_"}:
            continue

        # Nếu đã vào INPUT mà gặp tiêu đề section mới thì dừng
        if re.match(r"^[A-ZÀ-Ỹ0-9 _()/.-]+:?$", line) and not re.match(r"^\d+\.", line):
            if ":" not in line or not re.match(r"^\d+\.", line):
                break

        # Bắt field dạng: 1. FieldName: ...
        match = re.match(r"^\s*\d+\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        if match:
            field_name = match.group(1).strip()
            if field_name and field_name not in fields:
                fields.append(field_name)

    if not fields:
        raise ValueError(
            "Không trích xuất được input field từ FEATURE SPECIFICATION. "
            "Mô tả cần có dạng:\n"
            "INPUT: gồm n field\n"
            "1. FieldA: ...\n"
            "2. FieldB: ..."
        )

    return fields


def get_feature_item_fields(
    feature: str,
    features_dir: str | Path | None = None,
) -> List[str]:
    """
    Lấy danh sách input field của feature bằng cách đọc trực tiếp
    từ file input/features/<feature>.txt.

    Đây là nguồn schema động thay cho danh sách hardcode.
    """
    spec_text = load_feature_spec_text(feature, features_dir=features_dir)
    return extract_input_fields_from_spec_text(spec_text)


def get_feature_column_order(
    feature: str,
    features_dir: str | Path | None = None,
) -> List[str]:
    """
    Thứ tự cột output cuối cùng:
    Testcase + input fields lấy từ FEATURE SPECIFICATION + Expected
    """
    fields = get_feature_item_fields(feature, features_dir=features_dir)
    return ["Testcase", *fields, "Expected"]


def get_feature_testcase_prefix(feature: str) -> str:
    """
    Sinh prefix testcase tự động từ tên feature.

    Ví dụ:
    - register -> RE
    - login -> LO
    - profile_update -> PU
    - forgot_password -> FP
    """
    feature_key = normalize_feature_name(feature)

    if not feature_key:
        return "TC"

    parts = [p for p in feature_key.split("_") if p]

    if len(parts) >= 2:
        prefix = "".join(part[0] for part in parts[:2]).upper()
    else:
        prefix = feature_key[:2].upper()

    return prefix or "TC"


def get_feature_output_basename(feature: str) -> str:
    """
    Sinh tên file output tự động từ tên feature.

    Ví dụ:
    - register -> RegisterData
    - forgot_password -> ForgotPasswordData
    """
    feature_key = normalize_feature_name(feature)

    if not feature_key:
        return "TestData"

    name = "".join(part.capitalize() for part in feature_key.split("_") if part)
    return f"{name}Data" if name else "TestData"


def extract_inputs_from_testcase(item: dict) -> dict:
    """
    Tương thích schema cũ nếu testcase còn dạng:
    {
      "inputs": {...}
    }

    Với schema mới dạng phẳng, hàm này vẫn trả về phần inputs nếu có.
    """
    if not isinstance(item, dict):
        raise ValueError(f"Each testcase must be a dict, got: {type(item).__name__}")

    inputs = item.get("inputs", {})
    if not isinstance(inputs, dict):
        raise ValueError("Invalid testcase: 'inputs' must be an object")

    return inputs


def build_default_testcase_id(feature: str, index: int) -> str:
    """
    Sinh mã testcase tự động.
    """
    prefix = get_feature_testcase_prefix(feature)
    return f"{prefix}{index:02d}"