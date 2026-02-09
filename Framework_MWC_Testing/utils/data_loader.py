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


def load_test_data(
    base_dir: str,
    feature: str,
    pytestconfig=None,
    sheet_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    base_dir: thư mục root project (FRAMEWORK_MWC_TESTING)
    feature: login/order/profile/search/register
    Ưu tiên đọc theo framework_config.yaml, hoặc override bằng pytest option nếu bạn có.
    """
    import yaml as _yaml

    cfg_path = os.path.join(base_dir, "config", "framework_config.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = _yaml.safe_load(f)

    data_mode = cfg.get("data_mode", "manual")
    default_format = cfg.get("default_format", "csv").lower()

    # Nếu bạn đã có option pytest --data-mode, --data-format thì có thể tự map ở đây
    if pytestconfig is not None:
        opt_mode = pytestconfig.getoption("--data-mode") if hasattr(pytestconfig, "getoption") else None
        opt_fmt = pytestconfig.getoption("--data-format") if hasattr(pytestconfig, "getoption") else None
        if opt_mode:
            data_mode = opt_mode
        if opt_fmt:
            default_format = str(opt_fmt).lower()

    if data_mode == "ai":
        root = os.path.join(base_dir, cfg.get("ai_processed_dir", "data/ai_generated/processed"))
        # processed/<fmt>/<feature>.<fmt>
        path = os.path.join(root, default_format, f"{feature}.{default_format}")
    else:
        root = os.path.join(base_dir, cfg.get("manual_dir", "data/manual"))
        # manual/<FeatureData.*> (theo convention bạn đặt)
        # Map tên file thủ công
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
    if default_format == "yaml" or default_format == "yml":
        return _read_yaml(path)
    if default_format == "xlsx":
        return _read_xlsx(path, sheet_name=sheet_name)

    raise ValueError(f"Unsupported format: {default_format}")
