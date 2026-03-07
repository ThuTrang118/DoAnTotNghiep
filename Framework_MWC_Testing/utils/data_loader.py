import os
import csv
import json
import yaml
from typing import List, Dict, Any, Optional

import pandas as pd


def _read_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # chấp nhận cả list hoặc {items:[...]}
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    if isinstance(data, list):
        return data
    raise ValueError(f"JSON format not supported: {path}")


def _read_yaml(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    if isinstance(data, list):
        return data
    raise ValueError(f"YAML format not supported: {path}")


def _read_xlsx(path: str, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
    df = pd.read_excel(path, sheet_name=sheet_name or 0)
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def _load_app_config(base_dir: str) -> Dict[str, Any]:
    """Load config từ <root>/app_config.yaml."""
    cfg_path = os.path.join(base_dir, "app_config.yaml")
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(
            f"Không tìm thấy app_config.yaml tại: {cfg_path}. "
            f"Hãy đặt app_config.yaml ở thư mục root (cùng cấp data/, tests/, pages/...)."
        )
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_test_data(
    base_dir: str,
    feature: str,
    pytestconfig=None,
    sheet_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    base_dir: thư mục root project (FRAMEWORK_MWC_TESTING)
    feature: login/order/profile/search/register

    Đọc config từ: <root>/app_config.yaml
    Ưu tiên:
    1) app_config.yaml
    2) pytest option (nếu bạn có): --data-mode, --data-format
    """
    cfg = _load_app_config(base_dir)
    framework_cfg: Dict[str, Any] = (cfg.get("framework") or {}) if isinstance(cfg, dict) else {}

    data_mode = str(framework_cfg.get("data_mode", "manual")).strip().lower()
    default_format = str(framework_cfg.get("default_format", "csv")).strip().lower()

    # Override bằng pytest option (nếu có)
    if pytestconfig is not None:
        opt_mode = pytestconfig.getoption("--data-mode") if hasattr(pytestconfig, "getoption") else None
        opt_fmt = pytestconfig.getoption("--data-format") if hasattr(pytestconfig, "getoption") else None
        if opt_mode:
            data_mode = str(opt_mode).strip().lower()
        if opt_fmt:
            default_format = str(opt_fmt).strip().lower()

    if data_mode == "ai":
        ai_processed_dir = str(framework_cfg.get("ai_processed_dir", "data/ai_generated/processed"))
        root = os.path.join(base_dir, ai_processed_dir)
        # processed/<fmt>/<feature>.<fmt>
        path = os.path.join(root, default_format, f"{feature}.{default_format}")
    else:
        manual_dir = str(framework_cfg.get("manual_dir", "data/manual"))
        root = os.path.join(base_dir, manual_dir)
        mapping = {
            "login": f"LoginData.{default_format}",
            "order": f"OrderData.{default_format}",
            "profile": f"ProfileData.{default_format}",
            "search": f"SearchData.{default_format}",
            "register": f"RegisterData.{default_format}",
        }
        path = os.path.join(root, mapping.get(feature, f"{feature}.{default_format}"))

    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")

    if default_format == "csv":
        return _read_csv(path)
    if default_format == "json":
        return _read_json(path)
    if default_format in ("yaml", "yml"):
        return _read_yaml(path)
    if default_format in ("xlsx", "xls"):
        return _read_xlsx(path, sheet_name=sheet_name)

    raise ValueError(f"Unsupported format: {default_format}")