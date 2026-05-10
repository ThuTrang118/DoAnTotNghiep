import os
import pytest
import allure
from datetime import datetime
from pages.search_page import MWCSearchPage
from tests.test_search_ddt import MANUAL_ROOT_DIR
from utils.data_io import load_data
from utils.logger_utils import create_logger, log_data_source_from_pytest

logger = create_logger("SearchTest")

@pytest.fixture(scope="session", autouse=True)
def _auto_log_data_source(pytestconfig):
    log_data_source_from_pytest(logger, pytestconfig)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SHEET = "Search"

DATA_ROOT_DIR   = os.path.join(BASE_DIR, "data")
MANUAL_FEATURE_DIR = os.path.join(MANUAL_ROOT_DIR, "Search")
AI_DATA_DIR = os.path.join(DATA_ROOT_DIR, "ai_processed", "search")

DEFAULT_FILES = {
    "xlsx": "TestData.xlsx",
    "xls": "TestData.xls",
    "csv": "SearchData.csv",
    "json": "SearchData.json",
    "yaml": "SearchData.yaml",
    "yml": "SearchData.yml",
    "xml": "SearchData.xml",
    "db": "SearchData.db",
}

def get_test_data(pytestconfig):
    source = (pytestconfig.getoption("--data-source") or "manual").lower().strip()
    mode = (pytestconfig.getoption("--data-mode") or "excel").lower().strip()
    data_file = (pytestconfig.getoption("--data-file") or "").strip()

    if mode == "sqlite":
        mode = "db"

    db_table = (pytestconfig.getoption("--db-table") or "register").strip()
    xml_item_tag = (pytestconfig.getoption("--xml-item-tag") or "item").strip()

    if source == "manual":
        base_dir = MANUAL_ROOT_DIR if mode in ("excel", "xlsx", "xls") else MANUAL_FEATURE_DIR
    else:
        base_dir = AI_DATA_DIR

    file_name = DEFAULT_FILES.get(mode)
    if not file_name:
        raise ValueError("data-mode không hợp lệ")

    if data_file:
        full_path = data_file if os.path.isabs(data_file) else os.path.join(BASE_DIR, data_file)
    else:
        full_path = os.path.join(base_dir, file_name)

    if not os.path.exists(full_path):
        raise pytest.UsageError(f"Không tìm thấy file data:\n  {full_path}")

    if full_path.endswith((".xlsx", ".xls")):
        return load_data(full_path, sheet_name=SHEET, db_table=db_table, xml_item_tag=xml_item_tag)

    return load_data(full_path, db_table=db_table, xml_item_tag=xml_item_tag)

def pytest_generate_tests(metafunc):
    if {"tc", "keyword", "expected_raw"}.issubset(metafunc.fixturenames):
        data = get_test_data(metafunc.config)
        seen, params = set(), []
        for r in data:
            tc = str(r.get("testcase", "")).strip()
            if tc and tc not in seen:
                params.append(pytest.param(
                    r.get("testcase", ""),
                    r.get("keyword", ""),
                    r.get("expected", ""),
                    id=tc
                ))
                seen.add(tc)
        metafunc.parametrize("tc,keyword,expected_raw", params)

@allure.feature("Search")
@allure.story("Tìm kiếm sản phẩm - DDT")
def test_search_ddt(driver, result_writer, tc, keyword, expected_raw):
    logger.info(f"\n=== BẮT ĐẦU TESTCASE {tc} ===")
    logger.info(f"Input | Keyword='{keyword}' | Expected='{expected_raw}'")

    with allure.step("Mở trang và thực hiện tìm kiếm"):
        page = MWCSearchPage(driver)
        page.open()
        page.search(keyword)

    status, actual = "FAIL", ""
    try:
        with allure.step("Lấy sản phẩm đầu tiên và chuẩn hóa dữ liệu"):
            first_name = (page.get_first_result_text() or "").strip()
            actual = first_name if first_name else "Không tìm thấy sản phẩm"

            keyword_norm  = page.normalize_text(keyword)
            actual_norm   = page.normalize_text(first_name)
            expected_norm = page.normalize_text(expected_raw)

        with allure.step("Đánh giá kết quả theo rule hiện tại"):
            if not keyword:
                if "vui long nhap" in expected_norm or "trong" in expected_norm:
                    actual = "Từ khóa trống"
                    status = "PASS"
                else:
                    status = "FAIL"

            elif not first_name:
                status = "PASS" if "khong tim thay" in expected_norm else "FAIL"

            else:
                if keyword_norm in actual_norm:
                    status = "PASS"
                else:
                    logger.warning(f"Không tìm thấy sản phẩm nào chứa từ khóa '{keyword}'.")
                    actual = "Không tìm thấy sản phẩm"
                    status = "PASS" if "khong tim thay" in expected_norm else "FAIL"

    except Exception as e:
        actual = f"Lỗi khi chạy testcase: {e}"
        logger.error(actual)
        status = "FAIL"

    with allure.step("Ghi kết quả ra Excel"):
        result_writer.add_row(SHEET, {
            "Testcase": tc,
            "Keyword": keyword,
            "Expected": expected_raw,
            "Actual": actual,
            "Status": status,
            "Time": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        })

    if status == "FAIL":
        with allure.step("Đánh dấu testcase FAIL"):
            pytest.fail(f"Testcase {tc} thất bại.\nExpected: '{expected_raw}'\nActual: '{actual}'", pytrace=False)

    logger.info(f"Expected: {expected_raw}")
    logger.info(f"Actual:   {actual}")
    logger.info(f"Status:   {status}")
    logger.info(f"KẾT THÚC TESTCASE {tc}")
    logger.info("=" * 80 + "\n")
