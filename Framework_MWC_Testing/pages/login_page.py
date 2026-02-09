from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from pages.base_page import BasePage
from utils.logger_utils import create_logger

logger = create_logger("LoginPage")


class MWCLoginPage(BasePage):
    """Trang Đăng nhập MWC."""

    URL = "https://mwc.com.vn/login"
    HOME_URL = "https://mwc.com.vn/"
    USERNAME = (By.XPATH, "(//input[@id='UserName'])[1]")
    PASSWORD = (By.XPATH, "(//input[@id='Password'])[1]")
    LOGIN_BTN = (By.CSS_SELECTOR, "input[value='Đăng nhập']")
    ALERT = (By.XPATH, "//div[contains(@class,'alert') or contains(text(),'mật khẩu không đúng')]")

    def __init__(self, driver, timeout: int = 12):
        # BasePage quản lý driver + WebDriverWait
        super().__init__(driver, timeout=timeout)

    def open(self):
        """Mở trang đăng nhập."""
        super().open(self.URL)
        logger.info("Mở trang đăng nhập MWC thành công.")

    def clear_input(self, locator):
        """Giữ lại để tương thích code cũ: clear input theo locator."""
        try:
            self.clear(locator)
            logger.info(f"Đã xóa dữ liệu cũ trong {locator}.")
        except Exception as e:
            logger.warning(f"Không thể xóa nội dung: {e}")

    def login(self, username, password):
        """Thực hiện đăng nhập (dọn sạch input trước khi nhập)."""
        logger.info("Bắt đầu thao tác đăng nhập...")

        self.safe_type(self.USERNAME, username)
        logger.info(f"Nhập Username: {username}")

        self.safe_type(self.PASSWORD, password)
        logger.info("Nhập Password (đã ẩn).")

        self.click(self.LOGIN_BTN)
        logger.info("Click nút 'Đăng nhập'.")

    def get_alert_text(self) -> str:
        """Lấy nội dung alert (nếu có)."""
        try:
            el = self.wait.until(EC.visibility_of_element_located(self.ALERT))
            return (el.text or "").strip()
        except Exception:
            return ""

    def at_home(self) -> bool:
        """Kiểm tra đã về trang chủ chưa."""
        return (self.driver.current_url or "").startswith(self.HOME_URL)

    def get_validation_message(self, locator) -> str:
        """Giữ API cũ, dùng logic chung của BasePage."""
        return super().get_validation_message(locator)
