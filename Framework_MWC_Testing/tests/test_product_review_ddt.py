import os
import pytest
import allure
from datetime import datetime
from pages.product_review_page import MWCProductReviewPage
from utils.data_io import load_data
from utils.logger_utils import create_logger, log_data_source_from_pytest

logger = create_logger("ProductReviewTest")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SHEET = "Product_Review"

DATA_ROOT_DIR   = os.path.join(BASE_DIR, "data")
MANUAL_DATA_DIR = os.path.join(DATA_ROOT_DIR, "manual")
AI_DATA_DIR     = os.path.join(DATA_ROOT_DIR, "ai_generated", "processed")

DEFAULT_FILES = {
    "excel": "TestData.xlsx",
    "csv":   "ProductReviewData.csv",
    "json":  "ProductReviewData.json",
}

def get_test_data(pytestconfig):
    source = (pytestconfig.getoption("--data-source") or "manual").lower()
    mode   = (pytestconfig.getoption("--data-mode") or "excel").lower()
    data_file = (pytestconfig.getoption("--data-file") or "").strip()

    base_dir = MANUAL_DATA_DIR if source == "manual" else AI_DATA_DIR

    if data_file:
        file_name = os.path.basename(data_file)
        full_path = os.path.join(base_dir, file_name)
        return load_data(
            full_path,
            sheet_name=SHEET if file_name.endswith((".xlsx", ".xls")) else None
        )

    file_name = DEFAULT_FILES.get(mode)
    if not file_name:
        raise ValueError("data-mode không hợp lệ")

    file_path = os.path.join(base_dir, file_name)

    if mode == "excel":
        return load_data(file_path, sheet_name=SHEET)
    return load_data(file_path)

def pytest_generate_tests(metafunc):
    required = {"tc", "fullname", "phone", "email", "title", "content", "rating", "expected_raw"}
    if required.issubset(metafunc.fixturenames):
        data = get_test_data(metafunc.config)

        seen, params = set(), []
        for r in data:
            tc = str(r.get("testcase", "")).strip()
            if tc and tc not in seen:
                params.append(pytest.param(
                    r.get("testcase", ""),
                    r.get("fullname", ""),
                    r.get("phone", ""),
                    r.get("email", ""),
                    r.get("title", ""),
                    r.get("content", ""),
                    r.get("rating", ""),
                    r.get("expected", ""),
                    id=tc
                ))
                seen.add(tc)

        metafunc.parametrize("tc,fullname,phone,email,title,content,rating,expected_raw", params)

@allure.feature("Product Review")
@allure.story("Đánh giá sản phẩm - DDT")
def test_product_review_ddt(driver, result_writer, tc, fullname, phone, email, title, content, rating, expected_raw):
    logger.info(f"\n=== BẮT ĐẦU TESTCASE {tc} ===")

    page = MWCProductReviewPage(driver)

    status, actual = "FAIL", ""
    try:
        with allure.step("Login + search + mở tab Bình luận"):
            page.login_search_open_comment_tab()

        with allure.step("Nhập form đánh giá (fullname/phone/email/title/content)"):
            page.fill_form(fullname=fullname, phone=phone, email=email, title=title, content=content)

        with allure.step(f"Chọn số sao rating = {rating}"):
            page.select_rating(int(rating) if str(rating).strip() else 0)

        with allure.step("Click Gửi và lấy kết quả thực tế"):
            page.click_send()
            actual = page.get_actual_result()

        with allure.step("So sánh Expected vs Actual"):
            if (actual or "").strip().lower() == (expected_raw or "").strip().lower():
                status = "PASS"

    except Exception as e:
        actual = f"Lỗi khi chạy testcase: {e}"
        logger.error(actual)

    with allure.step("Ghi kết quả ra Excel"):
        result_writer.add_row(SHEET, {
            "Testcase": tc,
            "FullName": fullname,
            "Phone": phone,
            "Email": email,
            "Title": title,
            "Content": content,
            "Rating": rating,
            "Expected": expected_raw,
            "Actual": actual,
            "Status": status,
            "Time": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        })

    logger.info(f"Expected: {expected_raw}")
    logger.info(f"Actual:   {actual}")
    logger.info(f"Status:   {status}")
    logger.info(f"KẾT THÚC TESTCASE {tc}")
    logger.info("=" * 80 + "\n")

    if status == "FAIL":
        with allure.step("Đánh dấu testcase FAIL"):
            pytest.fail(
                f"Testcase {tc} thất bại.\nExpected: '{expected_raw}'\nActual: '{actual}'",
                pytrace=False
            )
