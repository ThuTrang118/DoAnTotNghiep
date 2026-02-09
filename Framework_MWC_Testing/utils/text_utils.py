import unicodedata
import re


def normalize_vi(text: str) -> str:
    """
    Chuẩn hoá chuỗi tiếng Việt để so sánh:
    - Bỏ dấu unicode
    - Đưa về chữ thường
    - Gom nhiều khoảng trắng thành 1
    """
    if text is None:
        text = ""
    s = unicodedata.normalize("NFD", str(text))
    s = s.encode("ascii", "ignore").decode("utf-8").lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_region(text: str) -> str:
    """
    Chuẩn hoá tên Tỉnh/TP/Quận/Huyện/Phường/Xã để so khớp:
    - Dùng normalize_vi để bỏ dấu + đưa về chữ thường
    - Bỏ các tiền tố: TP, Tỉnh, Quận, Huyện, Phường, Xã (cả biến thể)
    - Chỉ giữ chữ cái + chữ số, gom nhiều space thành 1
    """
    if not text:
        return ""

    s = normalize_vi(text)

    # Bỏ các tiền tố hành chính phổ biến
    for w in ["tp.", "tp", "thanh pho", "tinh", "quan", "huyen", "phuong", "xa"]:
        s = re.sub(rf"\b{w}\b", " ", s)

    # Chỉ giữ chữ cái + số
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
