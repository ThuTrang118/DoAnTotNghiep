import os
import pytest
import allure
from datetime import datetime
import unicodedata
from pages.login_page import MWCLoginPage
from pages.profile_update_page import MWCProfileUpdatePage
from tests.test_profile_update_ddt import MANUAL_ROOT_DIR
from utils.data_io import load_data
from utils.logger_utils import create_logger, log_data_source_from_pytest

logger = create_logger("ProfileUpdateTest")

@pytest.fixture(scope="session", autouse=True)
def _auto_log_data_source(pytestconfig):
    log_data_source_from_pytest(logger, pytestconfig)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SHEET = "Profile"

DATA_ROOT_DIR   = os.path.join(BASE_DIR, "data")
MANUAL_FEATURE_DIR = os.path.join(MANUAL_ROOT_DIR, "Profile")
AI_DATA_DIR = os.path.join(DATA_ROOT_DIR, "ai_processed", "profile")

DEFAULT_FILES = {
    "xlsx": "TestData.xlsx",
    "xls": "TestData.xls",
    "csv": "ProfileData.csv",
    "json": "ProfileData.json",
    "yaml": "ProfileData.yaml",
    "yml": "ProfileData.yml",
    "xml": "ProfileData.xml",
    "db": "ProfileData.db",
    "sqlite": "ProfileData.db",
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
    required = {
        "tc", "fullname", "email", "phone", "gender",
        "day", "month", "year",
        "province", "district", "ward", "address",
        "expected_raw"
    }
    if not required.issubset(metafunc.fixturenames):
        return

    data = get_test_data(metafunc.config)
    params = []
    seen = set()

    for r in data:
        tc = str(r.get("testcase", "")).strip()
        if not tc or tc in seen:
            continue

        params.append(pytest.param(
            tc,
            r.get("fullname", ""),
            r.get("email", ""),
            r.get("phone", ""),
            r.get("gender", ""),
            r.get("day", ""),
            r.get("month", ""),
            r.get("year", ""),
            r.get("province", ""),
            r.get("district", ""),
            r.get("ward", ""),
            r.get("address", ""),
            r.get("expected", ""),
            id=tc
        ))
        seen.add(tc)

    metafunc.parametrize(
        "tc,fullname,email,phone,gender,day,month,year,"
        "province,district,ward,address,expected_raw",
        params
    )

@allure.feature("Profile")
@allure.story("Cập nhật hồ sơ - DDT")
def test_profile_update(
    driver, result_writer,
    tc, fullname, email, phone, gender,
    day, month, year,
    province, district, ward, address,
    expected_raw
):
    logger.info("=" * 80)
    logger.info(f"BẮT ĐẦU TESTCASE {tc}")
    logger.info(f"Input → Email='{email}', Phone='{phone}', Expected='{expected_raw}'")

    with allure.step("Reset session (cookies/localStorage/sessionStorage)"):
        try:
            driver.delete_all_cookies()
            driver.execute_script("window.localStorage && window.localStorage.clear();")
            driver.execute_script("window.sessionStorage && window.sessionStorage.clear();")
        except Exception:
            pass

    with allure.step("Đăng nhập tài khoản mẫu"):
        login = MWCLoginPage(driver)
        login.open()
        login.login("Ánh Dương Phạm", "anhduong@123")
        assert login.at_home(), "Không đăng nhập được!"
        logger.info("Đăng nhập thành công.")

    with allure.step("Mở trang profile và nhập dữ liệu cập nhật"):
        page = MWCProfileUpdatePage(driver)
        page.open()
        page.fill_profile(
            fullname, email, phone,
            gender, day, month, year,
            province, district, ward, address
        )

    with allure.step("Click Lưu"):
        page.click_save()

    actual = ""
    status = "FAIL"

    def normalize(s):
        return unicodedata.normalize("NFD", (s or "").lower()) \
            .encode("ascii", "ignore").decode("utf-8")

    exp_norm = normalize(expected_raw)
    expect_success = ("thanh cong" in exp_norm) or ("success" in exp_norm)

    try:
        with allure.step("Thu thập kết quả (toast/alert/validation/persist)"):
            toast_msg = page.get_toast_message()
            if toast_msg:
                actual = toast_msg

            if not actual:
                alert_msg = page.get_alert_text()
                if alert_msg:
                    actual = alert_msg

            if not actual and not expect_success:
                invalid_msg = page.get_first_invalid_validation()
                if invalid_msg:
                    actual = invalid_msg

            if not actual and expect_success:
                page.open()
                persisted = {
                    "fullname": page.get_value(page.FULLNAME),
                    "email": page.get_value(page.EMAIL),
                    "phone": page.get_value(page.PHONE),
                    "address": page.get_value(page.ADDRESS),
                }

                def ok(inp, got):
                    if not inp:
                        return True
                    return normalize(inp) in normalize(got)

                if (
                    ok(fullname, persisted["fullname"]) and
                    ok(email, persisted["email"]) and
                    ok(phone, persisted["phone"]) and
                    ok(address, persisted["address"])
                ):
                    actual = "Cập nhập tài khoản thành công!"
                else:
                    actual = f"Dữ liệu không lưu sau reload: {persisted}"

            if not actual:
                actual = "Không thấy thông báo sau khi lưu."

            if exp_norm and (exp_norm in normalize(actual) or normalize(actual) in exp_norm):
                status = "PASS"

    except Exception as e:
        actual = f"Lỗi khi chạy testcase: {e}"

    with allure.step("Ghi kết quả ra Excel"):
        result_writer.add_row(SHEET, {
            "Testcase": tc,
            "FullName": fullname,
            "Email": email,
            "Phone": phone,
            "Gender": gender,
            "Day": day,
            "Month": month,
            "Year": year,
            "Province": province,
            "District": district,
            "Ward": ward,
            "Address": address,
            "Expected": expected_raw,
            "Actual": actual,
            "Status": status,
            "Time": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        })

    if status == "FAIL":
        with allure.step("Đánh dấu testcase FAIL"):
            pytest.fail(f"Testcase {tc} thất bại.\nExpected: '{expected_raw}'\nActual: '{actual}'", pytrace=False)

    logger.info(f"Expected: {expected_raw}")
    logger.info(f"Actual:   {actual}")
    logger.info(f"Status:   {status}")
    logger.info(f"KẾT THÚC TESTCASE {tc}")
    logger.info("=" * 80 + "\n")
