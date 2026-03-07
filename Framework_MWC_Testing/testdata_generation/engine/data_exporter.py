from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


class DataWriter:
    """
    Writer tách bạch 2 nơi:
    - raw_dir: chỉ lưu evidence AI trả về (testdata_generation/output)
    - processed_dir: dữ liệu chuẩn hoá để framework test dùng (data/ai_generated)
    """

    SUPPORTED_FORMATS = {"csv", "json", "xlsx", "xls", "xml", "yaml", "yml", "db"}

    # Tên file processed (flat ở data/ai_generated)
    FEATURE_BASENAME: Dict[str, str] = {
        "login": "LoginData",
        "register": "RegisterData",
        "search": "SearchData",
        "order": "OrderData",
        "profile": "ProfileData",
    }

    # Thứ tự cột chuẩn theo từng feature
    FEATURE_COLUMN_ORDER: Dict[str, List[str]] = {
        "login": ["Testcase", "Username", "Password", "Expected"],
        "register": ["Testcase", "Username", "Phone", "Password", "ConfirmPassword", "Expected"],
        "search": ["Testcase", "Keyword", "Expected"],
        "order": ["Testcase", "Product", "Quantity", "Expected"],
        "profile": ["Testcase", "Field", "Value", "Expected"],
    }

    def __init__(self, raw_dir: Path, processed_dir: Path):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Helpers
    # -------------------------
    def _get_headers(self, feature: str, rows: List[Dict[str, Any]]) -> List[str]:
        f = (feature or "").strip().lower()
        if f in self.FEATURE_COLUMN_ORDER:
            return self.FEATURE_COLUMN_ORDER[f]
        return list(rows[0].keys()) if rows else []

    def _normalize_rows(self, rows: List[Dict[str, Any]], headers: List[str]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for r in rows:
            normalized.append({h: ("" if r.get(h) is None else r.get(h)) for h in headers})
        return normalized

    def _processed_basename(self, feature: str) -> str:
        f = (feature or "").strip().lower()
        return self.FEATURE_BASENAME.get(f, f"{f.capitalize()}Data" if f else "TestData")

    # -------- RAW (Evidence) --------
    def write_raw_text(self, feature: str, raw_text: str) -> Path:
        path = self.raw_dir / f"{feature}_raw.txt"
        path.write_text(raw_text or "", encoding="utf-8")
        return path

    def write_raw_json(self, feature: str, data: Dict[str, Any]) -> Path:
        path = self.raw_dir / f"{feature}_raw.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return path

    # -------- PROCESSED (for framework test) --------
    def write_processed_json(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        path = self.processed_dir / f"{name}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump({"items": rows}, f, ensure_ascii=False, indent=2)
        return path

    def write_processed_csv(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        path = self.processed_dir / f"{name}.csv"
        if not rows:
            path.write_text("", encoding="utf-8")
            return path

        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers)

        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            w.writerows(rows_norm)
        return path

    def write_processed_xlsx(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        path = self.processed_dir / f"{name}.xlsx"
        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers)

        df = pd.DataFrame(rows_norm, columns=headers)
        sheet = (name[:31] if name else "Sheet1")
        df.to_excel(path, index=False, sheet_name=sheet)
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
        path = self.processed_dir / f"{name}.xls"

        wb = xlwt.Workbook()
        ws = wb.add_sheet((name[:31] if name else "Sheet1"))

        headers = self._get_headers(feature, rows)
        rows_norm = self._normalize_rows(rows, headers)

        for c, h in enumerate(headers):
            ws.write(0, c, h)

        for r_idx, row in enumerate(rows_norm, start=1):
            for c, h in enumerate(headers):
                v = row.get(h, "")
                ws.write(r_idx, c, "" if v is None else str(v))

        wb.save(str(path))
        return path

    def write_processed_xml(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        path = self.processed_dir / f"{name}.xml"

        def esc(s: str) -> str:
            return (
                s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;")
            )

        lines = ["<items>"]
        for row in rows:
            lines.append("  <item>")
            for k, v in row.items():
                lines.append(f"    <{k}>{esc('' if v is None else str(v))}</{k}>")
            lines.append("  </item>")
        lines.append("</items>")

        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def write_processed_yaml(self, feature: str, rows: List[Dict[str, Any]], ext: str) -> Path:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Cannot write YAML. Install: pip install pyyaml. Original error: {e}")

        name = self._processed_basename(feature)
        path = self.processed_dir / f"{name}.{ext}"
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump({"items": rows}, f, allow_unicode=True, sort_keys=False)
        return path

    def write_processed_db(self, feature: str, rows: List[Dict[str, Any]]) -> Path:
        name = self._processed_basename(feature)
        path = self.processed_dir / f"{name}.db"
        table = (feature or "testdata").lower()

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
            for r in rows_norm:
                values.append(["" if r.get(c) is None else str(r.get(c)) for c in cols])

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