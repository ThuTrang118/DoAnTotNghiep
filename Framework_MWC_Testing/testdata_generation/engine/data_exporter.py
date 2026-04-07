from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


class DataExporter:
    """
    Exporter dùng chung cho pipeline mới.

    Hỗ trợ 2 nhóm output:
    - raw_dir: chỉ lưu evidence AI trả về
    - processed_dir: dữ liệu chuẩn hoá để framework test dùng

    Tương thích với:
    - generate_ai_data.py mới
    - generation_pipeline.py mới
    - code cũ đang dùng DataWriter
    """

    SUPPORTED_FORMATS = {"csv", "json", "xlsx", "xls", "xml", "yaml", "yml", "db"}

    FEATURE_ALIASES: Dict[str, str] = {
        "profile": "profile_update",
    }

    FEATURE_BASENAME: Dict[str, str] = {
        "login": "LoginData",
        "register": "RegisterData",
        "search": "SearchData",
        "order": "OrderData",
        "profile_update": "ProfileUpdateData",
        "product_review": "ProductReviewData",
    }

    FEATURE_COLUMN_ORDER: Dict[str, List[str]] = {
        "login": ["Testcase", "Username", "Password", "Expected"],
        "register": ["Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"],
        "search": ["Testcase", "Keyword", "Expected"],
        "order": ["Testcase", "Product", "Quantity", "Expected"],
        "profile_update": ["Testcase", "Field", "Value", "Expected"],
        "product_review": ["Testcase", "Product", "Rating", "Comment", "Expected"],
    }

    def __init__(
        self,
        output_dir: Optional[str | Path] = None,
        raw_dir: Optional[str | Path] = None,
        processed_dir: Optional[str | Path] = None,
    ) -> None:
        """
        Hỗ trợ 2 cách khởi tạo:

        1) Kiểu mới:
           DataExporter(output_dir="...")

           -> raw_dir = <output_dir>
           -> processed_dir = <output_dir>/processed

        2) Kiểu cũ:
           DataExporter(raw_dir="...", processed_dir="...")

        Nếu không truyền gì:
           raw_dir = testdata_generation/output
           processed_dir = data/ai_processed
        """
        project_root = Path(__file__).resolve().parents[2]

        if output_dir is not None:
            base = Path(output_dir).resolve()
            self.raw_dir = base
            self.processed_dir = base / "processed"
        else:
            self.raw_dir = (
                Path(raw_dir).resolve()
                if raw_dir is not None
                else (project_root / "testdata_generation" / "output")
            )
            self.processed_dir = (
                Path(processed_dir).resolve()
                if processed_dir is not None
                else (project_root / "data" / "ai_processed")
            )

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    # =========================================================
    # Helpers
    # =========================================================
    def _normalize_feature(self, feature: str) -> str:
        f = (feature or "").strip().lower()
        return self.FEATURE_ALIASES.get(f, f)

    def _processed_basename(self, feature: str) -> str:
        f = self._normalize_feature(feature)
        return self.FEATURE_BASENAME.get(f, f"{f.capitalize()}Data" if f else "TestData")

    def _get_feature_processed_dir(self, feature: str) -> Path:
        feature_name = self._normalize_feature(feature)
        feature_dir = self.processed_dir / feature_name
        feature_dir.mkdir(parents=True, exist_ok=True)
        return feature_dir

    def _flatten_item_for_processed(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Chuẩn hoá 1 item AI về dạng phẳng để export.

        Hỗ trợ cả 2 kiểu:
        1. Flat:
           {
             "Testcase": "LG01",
             "Username": "...",
             "Password": "...",
             "Expected": "..."
           }

        2. Nested Inputs:
           {
             "Testcase": "LG01",
             "Objective": "...",
             "Technique": "DT",
             "Inputs": {
               "Username": "...",
               "Password": "..."
             },
             "Expected": "..."
           }
        """
        flat: Dict[str, Any] = {}

        # giữ lại các field cấp ngoài cần thiết
        for key, value in row.items():
            if key == "Inputs":
                continue
            flat[key] = value

        inputs = row.get("Inputs")
        if isinstance(inputs, dict):
            for k, v in inputs.items():
                flat[k] = v

        return flat

    def _prepare_rows_for_processed(
        self,
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Chuẩn hoá danh sách item trước khi export:
        - flatten Inputs
        - bỏ các cột không cần cho framework test
        """
        cleaned: List[Dict[str, Any]] = []

        for row in rows:
            flat_row = self._flatten_item_for_processed(dict(row))
            flat_row.pop("Technique", None)
            flat_row.pop("Objective", None)
            flat_row.pop("Inputs", None)
            cleaned.append(flat_row)

        return cleaned

    def _get_headers(self, feature: str, rows: List[Dict[str, Any]]) -> List[str]:
        f = self._normalize_feature(feature)
        if f in self.FEATURE_COLUMN_ORDER:
            return self.FEATURE_COLUMN_ORDER[f]

        if not rows:
            return []

        return list(rows[0].keys())

    def _normalize_rows(
        self,
        rows: List[Dict[str, Any]],
        headers: List[str],
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            normalized_row: Dict[str, Any] = {}
            for h in headers:
                value = row.get(h, "")
                normalized_row[h] = "" if value is None else value
            normalized.append(normalized_row)
        return normalized

    # =========================================================
    # RAW (Evidence)
    # =========================================================
    def write_raw_text(self, feature: str, raw_text: str, suffix: str = "raw") -> Path:
        path = self.raw_dir / f"{feature}_{suffix}.txt"
        path.write_text(raw_text or "", encoding="utf-8")
        return path

    def write_raw_json(self, feature: str, data: Dict[str, Any], suffix: str = "raw") -> Path:
        path = self.raw_dir / f"{feature}_{suffix}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return path

    # =========================================================
    # PROCESSED
    # =========================================================
    def write_processed_json(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        feature_dir = self._get_feature_processed_dir(feature)
        path = feature_dir / f"{name}.json"

        rows = self._prepare_rows_for_processed(rows)
        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers) if headers else rows

        with path.open("w", encoding="utf-8") as f:
            json.dump({"items": rows_norm}, f, ensure_ascii=False, indent=2)

        return path

    def write_processed_csv(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        feature_dir = self._get_feature_processed_dir(feature)
        path = feature_dir / f"{name}.csv"

        rows = self._prepare_rows_for_processed(rows)
        if not rows:
            path.write_text("", encoding="utf-8")
            return path

        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers)

        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows_norm)

        return path

    def write_processed_xlsx(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        feature_dir = self._get_feature_processed_dir(feature)
        path = feature_dir / f"{name}.xlsx"

        rows = self._prepare_rows_for_processed(rows)
        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers)

        df = pd.DataFrame(rows_norm, columns=headers)
        sheet_name = name[:31] if name else "Sheet1"
        df.to_excel(path, index=False, sheet_name=sheet_name)

        return path

    def write_processed_xls(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        try:
            import xlwt  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "Cannot write .xls. Install in venv: pip install xlwt. "
                f"Original error: {e}"
            )

        name = self._processed_basename(feature)
        feature_dir = self._get_feature_processed_dir(feature)
        path = feature_dir / f"{name}.xls"

        rows = self._prepare_rows_for_processed(rows)
        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers)

        wb = xlwt.Workbook()
        ws = wb.add_sheet(name[:31] if name else "Sheet1")

        for col_idx, header in enumerate(headers):
            ws.write(0, col_idx, header)

        for row_idx, row in enumerate(rows_norm, start=1):
            for col_idx, header in enumerate(headers):
                value = row.get(header, "")
                ws.write(row_idx, col_idx, "" if value is None else str(value))

        wb.save(str(path))
        return path

    def write_processed_xml(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        feature_dir = self._get_feature_processed_dir(feature)
        path = feature_dir / f"{name}.xml"

        rows = self._prepare_rows_for_processed(rows)
        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers) if headers else rows

        def esc(s: str) -> str:
            return (
                s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;")
            )

        lines = ["<items>"]
        for row in rows_norm:
            lines.append("  <item>")
            for key in headers:
                value = row.get(key, "")
                lines.append(f"    <{key}>{esc('' if value is None else str(value))}</{key}>")
            lines.append("  </item>")
        lines.append("</items>")

        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def write_processed_yaml(self, feature: str, rows: List[Dict[str, Any]], ext: str) -> Path:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise RuntimeError(
                f"Cannot write YAML. Install: pip install pyyaml. Original error: {e}"
            )

        name = self._processed_basename(feature)
        feature_dir = self._get_feature_processed_dir(feature)
        path = feature_dir / f"{name}.{ext}"

        rows = self._prepare_rows_for_processed(rows)
        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers) if headers else rows

        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump({"items": rows_norm}, f, allow_unicode=True, sort_keys=False)

        return path

    def write_processed_db(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        feature_dir = self._get_feature_processed_dir(feature)
        path = feature_dir / f"{name}.db"
        table = self._normalize_feature(feature) or "testdata"

        rows = self._prepare_rows_for_processed(rows)

        conn = sqlite3.connect(path)
        try:
            cur = conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')

            if not rows:
                conn.commit()
                return path

            cols = self._get_headers(feature, rows)
            rows_norm = self._normalize_rows(rows, cols)

            col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
            cur.execute(f'CREATE TABLE "{table}" ({col_defs})')

            placeholders = ", ".join(["?"] * len(cols))
            col_names = ", ".join([f'"{c}"' for c in cols])
            sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

            values = []
            for row in rows_norm:
                values.append(["" if row.get(c) is None else str(row.get(c)) for c in cols])

            cur.executemany(sql, values)
            conn.commit()
            return path
        finally:
            conn.close()

    def write_formats(
        self,
        feature: str,
        rows: List[Dict[str, Any]],
        formats: Optional[Iterable[str]] = None,
        yaml_ext: str = "yaml",
    ) -> Dict[str, Path]:
        if formats is None:
            formats = sorted(self.SUPPORTED_FORMATS)

        out: Dict[str, Path] = {}

        for fmt in formats:
            f = (fmt or "").strip().lower()
            if f not in self.SUPPORTED_FORMATS:
                continue

            if f == "json":
                out["json"] = self.write_processed_json(feature, rows)
            elif f == "csv":
                out["csv"] = self.write_processed_csv(feature, rows)
            elif f == "xlsx":
                out["xlsx"] = self.write_processed_xlsx(feature, rows)
            elif f == "xls":
                out["xls"] = self.write_processed_xls(feature, rows)
            elif f == "xml":
                out["xml"] = self.write_processed_xml(feature, rows)
            elif f == "yaml":
                out["yaml"] = self.write_processed_yaml(feature, rows, ext=yaml_ext)
            elif f == "yml":
                out["yml"] = self.write_processed_yaml(feature, rows, ext="yml")
            elif f == "db":
                out["db"] = self.write_processed_db(feature, rows)

        return out

    # =========================================================
    # Compatibility methods for new pipeline
    # =========================================================
    def export(
        self,
        feature: str,
        rows: List[Dict[str, Any]],
        formats: Optional[Iterable[str]] = None,
    ) -> Dict[str, str]:
        paths = self.write_formats(feature=feature, rows=rows, formats=formats)
        return {k: str(v) for k, v in paths.items()}

    def export_feature_items(
        self,
        feature: str,
        items: List[Dict[str, Any]],
        formats: Optional[Iterable[str]] = None,
    ) -> List[str]:
        paths = self.write_formats(feature=feature, rows=items, formats=formats)
        return [str(v) for v in paths.values()]


class DataWriter(DataExporter):
    """
    Alias tương thích ngược với code cũ.
    Code cũ có thể vẫn import:
        from ...data_exporter import DataWriter
    """
    pass