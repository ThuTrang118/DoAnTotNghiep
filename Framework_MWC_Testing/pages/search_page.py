from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from pages.base_page import BasePage
from utils.logger_utils import create_logger
from utils.text_utils import normalize_vi

logger = create_logger("SearchPage")


class MWCSearchPage(BasePage):
    """Trang tìm kiếm sản phẩm trên website MWC."""

    URL = "https://mwc.com.vn/"
    SEARCH_BOX = (By.XPATH, "(//input[@placeholder='Tìm kiếm'])[1]")
    FIRST_RESULT = (By.XPATH, "(//div[@class='product-grid-item'])[1]")
    PRODUCT_TITLES = (
        By.CSS_SELECTOR,
        "a[class='product-grid-info pl-id-5370'] p[class='product-grid-title']",
    )

    def open(self):
        """Mở trang chủ MWC."""
        super().open(self.URL)
        logger.info("Đã mở trang chủ MWC.")

    def search(self, keyword: str):
        """Nhập từ khóa vào ô tìm kiếm và nhấn Enter."""
        box = self.find(self.SEARCH_BOX)
        box.clear()
        box.send_keys(keyword)
        box.submit()
        logger.info(f"Tìm kiếm với từ khóa: '{keyword}'")

    def get_first_result_text(self) -> str:
        """Lấy tên sản phẩm đầu tiên (nếu có)."""
        try:
            return self.find(self.FIRST_RESULT).text.strip()
        except Exception:
            logger.warning("Không tìm thấy sản phẩm đầu tiên.")
            return ""

    def get_all_titles(self):
        """Lấy danh sách tiêu đề sản phẩm hiển thị."""
        try:
            self.wait.until(EC.presence_of_all_elements_located(self.PRODUCT_TITLES))
            return [
                el.text.strip()
                for el in self.driver.find_elements(*self.PRODUCT_TITLES)
                if el.text.strip()
            ]
        except Exception:
            logger.warning("Không thể lấy danh sách tiêu đề sản phẩm.")
            return []

    def normalize_text(self, text: str) -> str:
        """Chuẩn hóa tiếng Việt (bỏ dấu, viết thường) để so sánh."""
        return normalize_vi(text)

    def check_keyword(self, keyword: str) -> tuple[bool, str]:
        """
        Kiểm tra từ khóa có xuất hiện trong các sản phẩm hiển thị không.
        Trả về (True, title_matched) hoặc (False, message).
        """
        keyword_norm = self.normalize_text(keyword)

        # Kiểm tra sản phẩm đầu tiên
        first = self.get_first_result_text()
        if keyword_norm in self.normalize_text(first):
            return True, first

        # Kiểm tra toàn bộ tiêu đề
        for title in self.get_all_titles():
            if keyword_norm in self.normalize_text(title):
                return True, title

        return False, "Không tìm thấy sản phẩm"
