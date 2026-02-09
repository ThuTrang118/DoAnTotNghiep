import os
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pages.base_page import BasePage
from utils.logger_utils import create_logger

logger = create_logger("ProductReviewPage")


class MWCProductReviewPage(BasePage):
    LOGIN_URL = "https://mwc.com.vn/login"

    # Login
    LOGIN_USERNAME = (By.ID, "UserName")
    LOGIN_PASSWORD = (By.ID, "Password")
    BTN_LOGIN = (By.XPATH, "(//input[@value='Đăng nhập'])[1]")

    # Search (header)
    SEARCH_BOX = (By.XPATH, "(//input[@placeholder='Tìm kiếm'])[1]")

    # Open product
    FIRST_PRODUCT = (By.XPATH, "(//div[@class='product-grid-item'])[1]")

    # Comment tab
    TAB_COMMENT = (By.XPATH, "//button[@id='product-detail-review-tab']")

    # Review form fields
    FULLNAME = (By.XPATH, "(//input[@id='FullName'])")
    PHONE = (By.XPATH, "(//input[@id='Phone'])")
    EMAIL = (By.XPATH, "(//input[@id='Email'])")
    TITLE = (By.XPATH, "//input[@id='Title']")
    CONTENT = (By.XPATH, "//textarea[@id='Content']")

    # Rating: input range
    RATING = (By.ID, "Rating")

    # Send
    BTN_SEND = (By.XPATH, "(//button[contains(text(),'Gửi')])[1]")

    # Rule 1: error ids
    FULLNAME_ERR = (By.ID, "FullName-error")
    PHONE_ERR = (By.ID, "Phone-error")
    EMAIL_ERR = (By.ID, "Email-error")
    TITLE_ERR = (By.ID, "Title-error")
    CONTENT_ERR = (By.ID, "Content-error")

    # Rule 2: success
    SWAL_ACTIONS = (By.XPATH, "(//div[@class='swal2-actions'])[1]")
    SWAL_SUCCESS_TITLE = (By.XPATH, "(//h2[contains(text(),'Gửi bình luận thành công!')])[1]")

    def __init__(self, driver, timeout: int = 12):
        super().__init__(driver, timeout=timeout)

    # -------------------------
    # Steps: login/search/open
    # -------------------------
    def open_login(self):
        try:
            logger.info("Mở trang đăng nhập MWC...")
            super().open(self.LOGIN_URL)
            logger.info("Mở trang đăng nhập MWC thành công.")
        except Exception as e:
            self._log_and_raise("Không thể mở trang đăng nhập", e, "open_login")

    def login(self, username: str, password: str):
        try:
            logger.info("Bắt đầu thao tác đăng nhập...")

            logger.info(f"Nhập UserName: {username}")
            self.safe_type(self.LOGIN_USERNAME, username)

            logger.info("Nhập Password (đã ẩn).")
            self.safe_type(self.LOGIN_PASSWORD, password)

            logger.info("Click nút 'Đăng nhập'.")
            self.click_robust(self.BTN_LOGIN)

            logger.info("Đã click 'Đăng nhập'.")
        except Exception as e:
            self._log_and_raise("Đăng nhập thất bại", e, "login")

    def search_keyword(self, keyword: str):
        try:
            logger.info(f"Nhập từ khóa tìm kiếm: {keyword}")
            box = self.find(self.SEARCH_BOX)
            box.clear()
            box.send_keys(keyword)
            box.send_keys(Keys.ENTER)
        except Exception as e:
            self._log_and_raise("Tìm kiếm sản phẩm thất bại", e, "search_keyword")

    def open_first_product(self):
        try:
            logger.info("Chọn sản phẩm đầu tiên trong danh sách tìm kiếm.")
            self.click_robust(self.FIRST_PRODUCT)
            logger.info("Đã mở trang chi tiết sản phẩm.")

            # Tùy chọn: thu nhỏ zoom để giảm phải scroll nhiều lần
            try:
                self.set_zoom(80)
                logger.info("Đã set zoom trang = 80%.")
            except Exception:
                logger.warning("Không set được zoom (bỏ qua).")

        except Exception as e:
            self._log_and_raise("Không thể mở sản phẩm đầu tiên", e, "open_first_product")

    def open_comment_tab(self):
        """
        Scroll #1: Chỉ dùng để di chuyển xuống và click tab 'Bình luận'.
        Không scroll thêm trong hàm này để tránh “scroll loạn”.
        """
        try:
            logger.info("Scroll #1: Kéo xuống và mở tab 'Bình luận'.")

            # scroll + click an toàn vào tab (tự canh offset chống sticky che)
            self.click_covered_safe(self.TAB_COMMENT, offset_up=220, timeout=10, js_fallback=True)

            logger.info("Đã click tab 'Bình luận'. Chờ form hiển thị...")

            # Chờ form render xong (không scroll thêm)
            WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located(self.FULLNAME)
            )

            logger.info("Tab 'Bình luận' đã sẵn sàng.")
        except Exception as e:
            self._log_and_raise("Không thể mở tab 'Bình luận'", e, "open_comment_tab")

    def login_search_open_comment_tab(self):
        """Wrapper chạy đúng 5 bước (login->search->open product->open comment tab)."""
        self.open_login()
        self.login(username="Ánh Dương Phạm", password="anhduong@123")
        self.search_keyword("Giày Cao Gót MWC G299")
        self.open_first_product()
        self.open_comment_tab()

    # -------------------------
    # Fill form (không scroll lẻ từng field để tránh loạn)
    # -------------------------
    def _focus(self, locator):
        el = self.find(locator)
        try:
            self.driver.execute_script("arguments[0].focus();", el)
        except Exception:
            pass
        return el

    def fill_form(self, fullname: str, phone: str, email: str, title: str, content: str):
        try:
            logger.info("Bắt đầu nhập form bình luận (không scroll từng field)...")

            logger.info(f"Nhập Họ tên: {fullname}")
            self._focus(self.FULLNAME)
            self.safe_type(self.FULLNAME, fullname)

            logger.info(f"Nhập Số điện thoại: {phone}")
            self._focus(self.PHONE)
            self.safe_type(self.PHONE, phone)

            logger.info(f"Nhập Email: {email}")
            self._focus(self.EMAIL)
            self.safe_type(self.EMAIL, email)

            logger.info(f"Nhập Tiêu đề: {title}")
            self._focus(self.TITLE)
            self.safe_type(self.TITLE, title)

            preview = (content or "")
            preview = preview if len(preview) <= 120 else preview[:120] + "..."
            logger.info(f"Nhập Nội dung: {preview}")
            self._focus(self.CONTENT)
            self.safe_type(self.CONTENT, content)

            logger.info("Nhập form bình luận hoàn tất.")
        except Exception as e:
            self._log_and_raise("Nhập form bình luận thất bại", e, "fill_form")

    # -------------------------
    # Rating (id=Rating, type=range) 1..5
    # -------------------------
    def select_rating(self, rating: int):
        try:
            try:
                rating_int = int(str(rating).strip())
            except Exception:
                rating_int = 0

            logger.info(f"Chọn số sao đánh giá: {rating} (parse={rating_int})")
            if rating_int < 1 or rating_int > 5:
                logger.warning("Số sao không hợp lệ (phải từ 1 tới 5). Bỏ qua chọn sao.")
                return

            rating_el = self.find(self.RATING)

            self.driver.execute_script(
                """
                const el = arguments[0];
                const val = arguments[1];

                el.value = val;
                el.setAttribute('value', val);

                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
                """,
                rating_el,
                str(rating_int)
            )

            new_val = rating_el.get_attribute("value")
            logger.info(f"Đã set Rating. Giá trị hiện tại = {new_val}")
        except Exception as e:
            self._log_and_raise("Chọn sao đánh giá thất bại", e, "select_rating")

    # -------------------------
    # Click send
    # -------------------------
    def click_send(self):
        """
        Scroll #2: Kéo xuống nút 'Gửi' rồi click.
        Sau click: chờ phản ứng (error hoặc popup) để tránh bước Scroll #3 kéo lên quá sớm.
        """
        try:
            logger.info("Scroll #2: Kéo xuống nút 'Gửi' và click.")

            # 1) Scroll xuống khu vực nút Gửi (chủ động để đúng 1 lần scroll cho phase #2)
            self.scroll_to(self.BTN_SEND, offset_up=260, timeout=10)

            # 2) Đợi nút clickable rồi click (ưu tiên click thường)
            btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.BTN_SEND)
            )
            try:
                btn.click()
            except Exception:
                # fallback JS click nếu bị overlay/covered
                self.driver.execute_script("arguments[0].click();", btn)

            logger.info("Đã click 'Gửi'. Chờ phản ứng validate/popup...")

            # 3) Chờ ít nhất 1 tín hiệu xuất hiện: error validate hoặc popup success
            error_locators = [
                self.FULLNAME_ERR, self.PHONE_ERR, self.EMAIL_ERR, self.TITLE_ERR, self.CONTENT_ERR
            ]

            def _any_result_ready(driver):
                # error visible?
                for loc in error_locators:
                    try:
                        el = driver.find_element(*loc)
                        if el.is_displayed():
                            return True
                    except Exception:
                        pass
                # swal visible?
                try:
                    el = driver.find_element(*self.SWAL_ACTIONS)
                    if el.is_displayed():
                        return True
                except Exception:
                    pass
                try:
                    el = driver.find_element(*self.SWAL_SUCCESS_TITLE)
                    if el.is_displayed():
                        return True
                except Exception:
                    pass
                return False

            WebDriverWait(self.driver, 6).until(_any_result_ready)
            logger.info("Đã ghi nhận phản ứng sau khi click 'Gửi'.")

        except Exception as e:
            self._log_and_raise("Click nút 'Gửi' thất bại", e, "click_send")

    # -------------------------
    # Helpers
    # -------------------------
    def _is_visible(self, locator, timeout: int = 2) -> bool:
        try:
            WebDriverWait(self.driver, timeout).until(EC.visibility_of_element_located(locator))
            return True
        except Exception:
            return False

    # -------------------------
    # Verify result
    # -------------------------
    def get_actual_result(self) -> str:
        """
        Quy tắc kiểm tra:
        1) Nếu xuất hiện 1 trong id *-error -> "Vui lòng nhập!"
        2) Nếu xuất hiện swal2-actions hoặc h2 success -> "Gửi bình luận thành công!"
        3) Còn lại -> "Kết quả không hợp lệ"

        Scroll #3: Chỉ scroll để kiểm tra KHI CẦN (khi check nhanh chưa thấy gì),
        tránh kéo lên ngay làm “giật” viewport sau Scroll #2.
        """
        try:
            logger.info("Kiểm tra kết quả sau khi gửi (chưa scroll vội)...")

            error_locators = [
                self.FULLNAME_ERR,
                self.PHONE_ERR,
                self.EMAIL_ERR,
                self.TITLE_ERR,
                self.CONTENT_ERR,
            ]

            # 1) Check nhanh ngay tại viewport hiện tại
            for loc in error_locators:
                if self._is_visible(loc, timeout=1):
                    logger.info("Phát hiện lỗi validate => Vui lòng nhập!")
                    return "Vui lòng nhập!"

            if self._is_visible(self.SWAL_ACTIONS, timeout=2) or self._is_visible(self.SWAL_SUCCESS_TITLE, timeout=2):
                logger.info("Phát hiện popup thành công => Gửi bình luận thành công!")
                return "Gửi bình luận thành công!"

            # 2) Nếu chưa thấy, lúc này mới thực hiện Scroll #3
            logger.info("Scroll #3: Chưa thấy kết quả rõ ràng, kéo về khu vực form để kiểm tra lại...")
            self.scroll_to(self.FULLNAME, offset_up=220, timeout=8)

            for loc in error_locators:
                if self._is_visible(loc, timeout=2):
                    logger.info("Phát hiện lỗi validate (sau scroll) => Vui lòng nhập!")
                    return "Vui lòng nhập!"

            if self._is_visible(self.SWAL_ACTIONS, timeout=3) or self._is_visible(self.SWAL_SUCCESS_TITLE, timeout=3):
                logger.info("Phát hiện popup thành công (sau scroll) => Gửi bình luận thành công!")
                return "Gửi bình luận thành công!"

            logger.info("Không phát hiện lỗi validate hoặc popup thành công => Kết quả không hợp lệ.")
            return "Kết quả không hợp lệ"

        except Exception as e:
            self._log_and_raise("Lỗi khi kiểm tra kết quả sau khi gửi", e, "get_actual_result")
            return "Kết quả không hợp lệ"
