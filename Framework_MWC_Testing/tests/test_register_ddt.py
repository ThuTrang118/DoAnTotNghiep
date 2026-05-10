import os
import pytest
import allure
from datetime import datetime
from pages.register_page import MWCRegisterPage
from pages.profile_page import ProfilePage
from utils.data_io import load_data
from utils.logger_utils import create_logger, log_data_source_from_pytest

logger = create_logger("RegisterTest")

@pytest.fixture(scope="session", autouse=True)
def _auto_log_data_source(pytestconfig):
    log_data_source_from_pytest(logger, pytestconfig)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SHEET = "Register"

DATA_ROOT_DIR = os.path.join(BASE_DIR, "data")

MANUAL_ROOT_DIR = os.path.join(DATA_ROOT_DIR, "manual")
MANUAL_FEATURE_DIR = os.path.join(MANUAL_ROOT_DIR, "Register")

AI_DATA_DIR = os.path.join(DATA_ROOT_DIR, "ai_processed", "register")

DEFAULT_FILES = {
    "xlsx": "TestData.xlsx",
    "xls": "TestData.xls",
    "csv": "RegisterData.csv",
    "json": "RegisterData.json",
    "yaml": "RegisterData.yaml",
    "yml": "RegisterData.yml",
    "xml": "RegisterData.xml",
    "db": "RegisterData.db",
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
    if {"tc", "username", "phone", "password", "repass", "expected_raw"}.issubset(metafunc.fixturenames):
        data = get_test_data(metafunc.config)
        seen, params = set(), []
        for r in data:
            tc = str(r.get("testcase", "")).strip()
            if tc and tc not in seen:
                params.append(pytest.param(
                    r.get("testcase", ""),
                    r.get("username", ""),
                    r.get("phone", ""),
                    r.get("password", ""),
                    r.get("passwordconfirm", ""),
                    r.get("expected", ""),
                    id=tc
                ))
                seen.add(tc)
        metafunc.parametrize("tc,username,phone,password,repass,expected_raw", params)

@allure.feature("Register")
@allure.story("Đăng ký - DDT")
def test_register_ddt(driver, result_writer, tc, username, phone, password, repass, expected_raw):
    logger.info(f"\n=== BẮT ĐẦU TESTCASE {tc} ===")
    logger.info(f"Input | Username='{username}' | Phone='{phone}' | Password='***' | Expected='{expected_raw}'")

    with allure.step("Mở trang đăng ký"):
        page = MWCRegisterPage(driver)
        page.open()

    with allure.step("Nhập form đăng ký"):
        page.fill_form(username, phone, password, repass)

    with allure.step("Click nút đăng ký"):
        page.click_register()

    status, actual = "FAIL", ""
    try:
        with allure.step("Thu thập HTML5 validation messages"):
            html5_msgs = []
            for locator in [page.USERNAME, page.PHONE, page.PASSWORD, page.REPASS]:
                msg = page.get_validation_message(locator)
                if msg:
                    html5_msgs.append(msg)

        with allure.step("Xử lý kết quả (validation / alert / register success)"):
            if html5_msgs:
                actual = " | ".join(html5_msgs)
                if "vui lòng điền" in actual.lower() and "vui lòng điền" in (expected_raw or "").lower():
                    status = "PASS"

            elif not html5_msgs:
                alert_text = (page.get_alert_text() or "").strip().lower()
                if alert_text:
                    actual = alert_text
                    if (expected_raw or "").lower() in alert_text:
                        status = "PASS"

            if status == "FAIL" and page.at_home():
                profile = ProfilePage(driver)
                profile.open_profile()
                if profile.profile_username_present():
                    actual = profile.read_profile_username()
                    if username.lower() in (actual or "").lower():
                        status = "PASS"
                    else:
                        actual = f"Tên người dùng khác mong đợi: {actual}"
                else:
                    actual = "Không hiển thị tên người dùng trong hồ sơ."

            if status == "FAIL" and not actual:
                actual = "Đăng ký không thành công."

    except Exception as e:
        actual = f"Lỗi khi chạy testcase: {e}"
        logger.error(actual)

    with allure.step("Ghi kết quả ra Excel"):
        result_writer.add_row(SHEET, {
            "Testcase": tc,
            "Username": username,
            "Phone": phone,
            "Password": password,
            "PasswordConfirm": repass,
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
