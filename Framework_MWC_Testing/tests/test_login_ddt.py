import os
from datetime import datetime

import pytest
import allure

from pages.login_page import MWCLoginPage
from pages.profile_page import ProfilePage
from utils.data_io import load_data
from utils.logger_utils import create_logger, log_data_source_from_pytest

logger = create_logger("LoginTest")


# =========================
# AUTOUSE: log data source
# =========================
@pytest.fixture(scope="session", autouse=True)
def _auto_log_data_source(pytestconfig):
    log_data_source_from_pytest(logger, pytestconfig)


# =========================
# PATH CONFIG
# =========================
# tests/ -> project root
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

FEATURE_NAME = "login"
SHEET = "Login"

DATA_ROOT_DIR = os.path.join(BASE_DIR, "data")

# manual root (Excel shared here)
MANUAL_ROOT_DIR = os.path.join(DATA_ROOT_DIR, "manual")
# manual feature folder (non-excel per feature)
MANUAL_FEATURE_DIR = os.path.join(MANUAL_ROOT_DIR, "Login")

# AI processed
AI_PROCESSED_DIR = os.path.join(DATA_ROOT_DIR, "ai_generated")

# ---- Default file mapping ----
# Manual:
#   - xlsx/xls: shared file at data/manual/
#   - others : per-feature file at data/manual/Login/
DEFAULT_MANUAL_FILES = {
    "xlsx": "TestData.xlsx",
    "xls":  "TestData.xls",
    "csv":  "LoginData.csv",
    "json": "LoginData.json",
    "yaml": "LoginData.yaml",
    "yml":  "LoginData.yml",
    "xml":  "LoginData.xml",
    "db":   "LoginData.db",
    "sqlite": "LoginData.db",
}

# AI:
#   Convention: data/ai_generated/<feature>.<ext>
DEFAULT_AI_FILES = {
    "csv":  "LoginData.csv",
    "json": "LoginData.json",
    "xlsx": "LoginData.xlsx",   
    "xls":  "LoginData.xls",    
    "yaml": "LoginData.yaml",
    "yml":  "LoginData.yml",
    "xml":  "LoginData.xml",
    "db":   "LoginData.db",
    "sqlite": "LoginData.db",
    "excel": "LoginData.xlsx",
}


# =========================
# DATA PROVIDER
# =========================
def get_test_data(pytestconfig):
    """
    Resolve data path based on:
      --data-source: manual | ai
      --data-mode  : xlsx|xls|csv|json|yaml|yml|xml|db|sqlite|excel
      --data-file  : optional filename override (chỉ tên file, không cần path)

    Extra options (cho XML/DB):
      --db-table     : tên table trong sqlite (default: testdata)
      --xml-item-tag : tag item trong xml (default: item)
    """
    source = (pytestconfig.getoption("--data-source") or "manual").lower().strip()
    mode = (pytestconfig.getoption("--data-mode") or "xlsx").lower().strip()
    data_file = (pytestconfig.getoption("--data-file") or "").strip()

    # normalize some aliases
    if mode == "excel":
        mode = "xlsx"
    if mode == "sqlite":
        mode = "db"

    db_table = (pytestconfig.getoption("--db-table") or "testdata").strip()
    xml_item_tag = (pytestconfig.getoption("--xml-item-tag") or "item").strip()

    if source not in ("manual", "ai"):
        raise pytest.UsageError(f"--data-source không hợp lệ: {source}. Chỉ chấp nhận manual|ai")

    # =========================
    # Build base_dir + file_name
    # =========================
    if source == "manual":
        # Manual Excel uses shared files under data/manual/
        if mode in ("xlsx", "xls"):
            base_dir = MANUAL_ROOT_DIR
        else:
            base_dir = MANUAL_FEATURE_DIR

        file_name = os.path.basename(data_file) if data_file else DEFAULT_MANUAL_FILES.get(mode)

        if not file_name:
            raise pytest.UsageError(
                f"--data-mode không hợp lệ: {mode}. "
                f"Chỉ chấp nhận: {', '.join(sorted(DEFAULT_MANUAL_FILES.keys()))}"
            )

    else:
        # AI data in processed/<format>/
        base_dir = AI_PROCESSED_DIR

        file_name = os.path.basename(data_file) if data_file else DEFAULT_AI_FILES.get(mode)
        if not file_name:
            raise pytest.UsageError(
                f"--data-mode không hợp lệ: {mode}. "
                f"Chỉ chấp nhận: {', '.join(sorted(DEFAULT_AI_FILES.keys()))}"
            )

    full_path = os.path.join(base_dir, file_name)

    # =========================
    # Fail-fast if missing
    # =========================
    if not os.path.exists(full_path):
        # helpful hints for manual excel
        hints = []
        if source == "manual" and mode in ("xlsx", "xls"):
            hints.append(f"- Bạn đang chọn Excel dùng chung, cần có file: {os.path.join(MANUAL_ROOT_DIR, file_name)}")
            hints.append(f"- File phải có sheet: {SHEET}")

        raise pytest.UsageError(
            "Không tìm thấy file data:\n"
            f"  {full_path}\n\n"
            "Gợi ý kiểm tra:\n"
            + ("\n".join(hints) + "\n\n" if hints else "")
            + "Kiểm tra lại lựa chọn trong run_login.bat hoặc --data-file (nếu override)."
        )

    # =========================
    # Load by type
    # =========================
    if full_path.endswith((".xlsx", ".xls")):
        return load_data(
            full_path,
            sheet_name=SHEET,
            db_table=db_table,
            xml_item_tag=xml_item_tag
        )

    return load_data(
        full_path,
        db_table=db_table,
        xml_item_tag=xml_item_tag
    )


def _normalize_row(row: dict) -> dict:
    """
    Chuẩn hóa key để tránh mismatch do file csv/json/yaml/xml/db khác cột.
    Expect keys: testcase, username, password, expected
    """
    if not isinstance(row, dict):
        return {}

    lowered = {str(k).strip().lower(): v for k, v in row.items()}

    testcase = lowered.get("testcase") or lowered.get("tc") or lowered.get("id") or lowered.get("case")
    username = lowered.get("username") or lowered.get("user") or lowered.get("email")
    password = lowered.get("password") or lowered.get("pass") or lowered.get("pwd")
    expected = lowered.get("expected") or lowered.get("expect") or lowered.get("message") or lowered.get("error")

    return {
        "testcase": testcase,
        "username": username,
        "password": password,
        "expected": expected,
    }


# =========================
# DDT
# =========================
def pytest_generate_tests(metafunc):
    required = {"tc", "username", "password", "expected_raw"}
    if not required.issubset(metafunc.fixturenames):
        return

    data = get_test_data(metafunc.config)
    params = []
    seen = set()

    for raw in data:
        r = _normalize_row(raw)
        tc = str(r.get("testcase") or "").strip()
        if not tc:
            continue

        if tc in seen:
            continue

        params.append(
            pytest.param(
                r.get("testcase"),
                r.get("username"),
                r.get("password"),
                r.get("expected"),
                id=tc,
            )
        )
        seen.add(tc)

    if not params:
        raise pytest.UsageError(
            "DDT không sinh được testcase nào (params rỗng).\n"
            "Nguyên nhân thường gặp:\n"
            "  - File data không đúng cột (testcase/username/password/expected)\n"
            "  - load_data đọc ra rỗng\n"
            "  - Bạn chọn sai định dạng / sai thư mục"
        )

    metafunc.parametrize("tc,username,password,expected_raw", params)


# =========================
# TEST
# =========================
@allure.feature("Login")
@allure.story("Login DDT")
def test_login_ddt(driver, result_writer, request, tc, username, password, expected_raw):
    data_source = (request.config.getoption("--data-source") or "manual").lower()
    data_mode = (request.config.getoption("--data-mode") or "xlsx").lower()
    data_file = (request.config.getoption("--data-file") or "").strip()

    allure.dynamic.id(str(tc))
    allure.dynamic.title(str(tc))
    allure.dynamic.label("data_source", data_source)
    allure.dynamic.label("data_mode", data_mode)
    if data_file:
        allure.dynamic.label("data_file", os.path.basename(data_file))

    logger.info("")
    logger.info(f"=== BẮT ĐẦU TESTCASE {tc} ===")

    page = MWCLoginPage(driver)
    page.open()

    page.clear_input(page.USERNAME)
    page.clear_input(page.PASSWORD)

    page.login(username, password)

    status, actual = "FAIL", ""

    try:
        # 1) HTML5 validation
        html5_msgs = []
        for locator in [page.USERNAME, page.PASSWORD]:
            msg = page.get_validation_message(locator)
            if msg:
                html5_msgs.append(msg)

        if html5_msgs:
            actual = " | ".join(html5_msgs)
            if expected_raw and str(expected_raw).lower() in actual.lower():
                status = "PASS"
        else:
            # 2) alert/toast
            alert = page.get_alert_text()
            if alert:
                actual = alert
                if expected_raw and str(expected_raw).lower() in alert.lower():
                    status = "PASS"

        # 3) success
        if status == "FAIL" and page.at_home():
            profile = ProfilePage(driver)
            profile.open_profile()
            actual = profile.read_profile_username()
            if username and str(username).lower() in (actual or "").lower():
                status = "PASS"

        if not actual:
            actual = "Đăng nhập không thành công"

    except Exception as e:
        actual = str(e)

    logger.info(f"Expected: {expected_raw}")
    logger.info(f"Actual:   {actual}")
    logger.info(f"Status:   {status}")
    logger.info(f"KẾT THÚC TESTCASE {tc}")
    logger.info("================================================================================")
    logger.info("")

    allure.attach(str(expected_raw), name="Expected", attachment_type=allure.attachment_type.TEXT)
    allure.attach(str(actual), name="Actual", attachment_type=allure.attachment_type.TEXT)
    allure.attach(str(status), name="Status", attachment_type=allure.attachment_type.TEXT)

    result_writer.add_row(SHEET, {
        "Testcase": tc,
        "Username": username,
        "Password": password,
        "Expected": expected_raw,
        "Actual": actual,
        "Status": status,
        "Time": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "DataSource": data_source,
        "DataMode": data_mode,
        "DataFile": os.path.basename(data_file) if data_file else "",
    })

    if status == "FAIL":
        pytest.fail(f"{tc} FAILED | Expected: {expected_raw} | Actual: {actual}", pytrace=False)
