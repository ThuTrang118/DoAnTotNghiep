import time
import unicodedata
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from pages.base_page import BasePage
from utils.logger_utils import create_logger

logger = create_logger("OrderPage")


class MWCOrderPage(BasePage):
    """
    Trang Đặt hàng (Buy Now) cho sản phẩm MWC.
    Quy trình:
      1. Mở trang chủ
      2. Tìm kiếm & mở chi tiết sản phẩm
      3. Chọn màu/size, Mua ngay
      4. Nhập thông tin khách hàng + chọn Tỉnh/Huyện/Xã
      5. Đặt hàng
      6. Đọc thông báo thành công / lỗi
    """

    URL = "https://mwc.com.vn/"

    # --- Tìm kiếm & mở sản phẩm ---
    SEARCH_BOX = (By.XPATH, "(//input[@placeholder='Tìm kiếm'])[1]")
    FIRST_PRODUCT = (By.XPATH, "(//div[@class='product-grid-info-top'])[1]")
    PRODUCT_TITLE = (By.XPATH, "//h1[contains(text(),'Giày Cao Gót MWC 4431- Giày Cao Gót Nữ Quai Mảnh C')]")

    # --- Màu & size ---
    COLOR_SILVER = (By.ID, "bac")
    COLOR_BLACK = (By.ID, "den")
    SIZE_IDS = ["35", "36", "37", "38", "39"]

    BTN_BUY_NOW = (By.ID, "btnBuyNow")

    # --- Giỏ hàng ---
    CART_PRODUCT_NAME = (By.XPATH, "//a[contains(text(),'Giày Cao Gót MWC 4431- Giày Cao Gót Nữ Quai Mảnh C')]")
    CART_PRODUCT_OPTIONS = (
        By.XPATH,
        "//div[@class='cart-item-body-item-product-options-name d-none d-lg-block']"
    )

    # --- Form thông tin khách hàng ---
    FULLNAME_BOX = (By.ID, "FullName")
    PHONE_BOX = (By.ID, "Phone")
    ADDRESS_BOX = (By.ID, "Address")

    # 3 dropdown địa chỉ – dùng ID cho chắc
    PROVINCE_SELECT = (By.ID, "provinceOptions")
    DISTRICT_SELECT = (By.ID, "districtSelect")
    WARD_SELECT = (By.ID, "wardSelect")

    BTN_ORDER = (By.ID, "btnDatHang")

    # --- Thông báo ---
    ALERT_ERROR = (By.ID, "swal2-html-container")
    SUCCESS_TEXT = (By.XPATH, "//h1[contains(text(),'Đặt hàng thành công!')]")

    # ======================================================
    # B1. Mở trang
    # ======================================================
    def open(self):
        super().open(self.URL)
        logger.info("Đã mở trang chủ MWC (ORDER).")

    def open_home(self):
        self.open()

    # ======================================================
    # B2–B3. Tìm kiếm & mở sản phẩm
    # ======================================================
    def search_product(self, keyword: str):
        box = self.find(self.SEARCH_BOX)
        box.clear()
        box.send_keys(keyword)
        box.submit()
        logger.info(f"Tìm kiếm sản phẩm với keyword='{keyword}'")
        time.sleep(2)

    def click_first_product(self):
        el = self.wait.until(EC.element_to_be_clickable(self.FIRST_PRODUCT))
        el.click()
        logger.info("Đã click sản phẩm đầu tiên trong kết quả tìm kiếm.")
        time.sleep(2)

    def verify_product_page(self) -> bool:
        try:
            title_el = self.wait.until(EC.visibility_of_element_located(self.PRODUCT_TITLE))
            ok = "Giày Cao Gót MWC 4431" in (title_el.text or "")
            logger.info(f"Kiểm tra trang sản phẩm: {ok} - title='{title_el.text}'")
            return ok
        except Exception:
            logger.warning("Không kiểm tra được tiêu đề trang sản phẩm.")
            return False

    # ======================================================
    # B4. Chọn màu & size, Mua ngay
    # ======================================================
    def select_color_and_size(self, color: str, size: str):
        try:
            if color:
                c = color.strip().lower()
                if c == "bạc":
                    self.driver.find_element(*self.COLOR_SILVER).click()
                    logger.info("Đã chọn màu: Bạc")
                elif c == "đen":
                    self.driver.find_element(*self.COLOR_BLACK).click()
                    logger.info("Đã chọn màu: Đen")
                else:
                    logger.warning(f"Màu '{color}' không được hỗ trợ.")
            time.sleep(0.3)

            if str(size) in self.SIZE_IDS:
                self.driver.find_element(By.ID, str(size)).click()
                logger.info(f"Đã chọn size: {size}")
            else:
                logger.warning(f"Size '{size}' không hợp lệ.")
        except Exception as e:
            logger.warning(f"Không chọn được màu/size: {e}")

    def click_buy_now(self):
        try:
            btn = self.wait.until(EC.element_to_be_clickable(self.BTN_BUY_NOW))
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            btn.click()
            logger.info("Đã click nút Mua ngay.")
        except ElementClickInterceptedException:
            # fallback JS click
            self.driver.execute_script("arguments[0].click();", btn)
            logger.info("Đã click Mua ngay bằng JS (fallback).")
        except Exception as e:
            logger.warning(f"Không thể click Mua ngay: {e}")

    def verify_cart_info(self):
        try:
            name = self.wait.until(
                EC.visibility_of_element_located(self.CART_PRODUCT_NAME)
            ).text.strip()
            options = self.wait.until(
                EC.visibility_of_element_located(self.CART_PRODUCT_OPTIONS)
            ).text.strip()
            logger.info(f"Giỏ hàng: {name} | {options}")
            return name, options
        except Exception as e:
            logger.warning(f"Không đọc được thông tin giỏ hàng: {e}")
            return "", ""

    # ======================================================
    # B5–B6. Nhập thông tin & chọn Tỉnh/Huyện/Xã
    # ======================================================
    @staticmethod
    def _normalize_region_text(text: str) -> str:
        """
        Chuẩn hóa tên Tỉnh/TP/Quận/Huyện/Phường/Xã để so khớp:
        - Bỏ dấu unicode
        - Viết thường
        - Bỏ các tiền tố: TP, Tỉnh, Quận, Huyện, Phường, Xã
        - Chỉ giữ chữ số, gom nhiều space thành 1
        """
        if not text:
            return ""

        # Bỏ dấu
        s = unicodedata.normalize("NFD", text)
        s = s.encode("ascii", "ignore").decode("utf-8").lower()

        # Bỏ tiền tố hành chính
        for w in ["tp.", "tp", "thanh pho", "tinh", "quan", "huyen", "phuong", "xa"]:
            s = s.replace(w, " ")

        # Chỉ giữ chữ số
        s = re.sub(r"[^a-z0-9]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @staticmethod
    def _should_skip_select(value: str) -> bool:
        """Nếu Excel ghi 'Chọn ...' thì bỏ qua không chọn dropdown đó (test negative)."""
        if not value:
            return True
        v = value.strip().lower()
        return v.startswith("chọn ") or v.startswith("chon ")

    def _wait_for_dropdown(self, locator: tuple):
        """Đợi dropdown có ít nhất 2 option (tránh case load chậm)."""
        def _has_options(driver):
            try:
                sel = Select(driver.find_element(*locator))
                return len(sel.options) > 1
            except Exception:
                return False

        self.wait.until(_has_options)

    def _select_option_approx(self, locator: tuple, value: str, field_name: str):
        """Chọn option trong <select> theo text đã normalize."""
        if not value:
            logger.warning(f"Bỏ qua chọn {field_name} vì value rỗng.")
            return

        try:
            sel = Select(self.driver.find_element(*locator))
            target = self._normalize_region_text(value)
            logger.info(f"[DEBUG] Chọn {field_name} - Excel='{value}' | norm='{target}'")

            for opt in sel.options:
                norm_opt = self._normalize_region_text(opt.text)
                logger.info(f"[DEBUG]   option='{opt.text}' | norm='{norm_opt}'")
                if (
                    target == norm_opt or
                    target in norm_opt or
                    norm_opt in target
                ):
                    opt.click()
                    logger.info(f"Đã chọn {field_name}: '{opt.text}'")
                    return

            logger.warning(f"Không tìm thấy {field_name} khớp với '{value}' (norm='{target}')")
        except Exception as e:
            logger.warning(f"Lỗi khi chọn {field_name}='{value}': {e}")

    def fill_customer_info(
        self,
        fullname: str,
        phone: str,
        address: str,
        province: str,
        district: str,
        ward: str,
    ):
        """Nhập thông tin người nhận + chọn Tỉnh/Huyện/Xã đúng thứ tự."""
        try:
            # Nhập text
            for locator, val, label in [
                (self.FULLNAME_BOX, fullname, "Họ tên"),
                (self.PHONE_BOX, phone, "SĐT"),
                (self.ADDRESS_BOX, address, "Địa chỉ"),
            ]:
                el = self.find(locator)
                el.clear()
                if val:
                    el.send_keys(val)
                    logger.info(f"Đã nhập {label}: '{val}'")
                else:
                    logger.warning(f"{label} bị để trống trong dữ liệu.")

            # 1) Tỉnh/TP
            if not self._should_skip_select(province):
                self._wait_for_dropdown(self.PROVINCE_SELECT)
                self._select_option_approx(self.PROVINCE_SELECT, province, "Tỉnh/TP")
                time.sleep(0.8)
            else:
                logger.info("Bỏ qua chọn Tỉnh/TP (dữ liệu test 'Chọn ...').")

            # 2) Quận/Huyện
            if not self._should_skip_select(district):
                self._wait_for_dropdown(self.DISTRICT_SELECT)
                self._select_option_approx(self.DISTRICT_SELECT, district, "Quận/Huyện")
                time.sleep(0.8)
            else:
                logger.info("Bỏ qua chọn Quận/Huyện (dữ liệu test 'Chọn ...').")

            # 3) Phường/Xã
            if not self._should_skip_select(ward):
                self._wait_for_dropdown(self.WARD_SELECT)
                self._select_option_approx(self.WARD_SELECT, ward, "Phường/Xã")
            else:
                logger.info("Bỏ qua chọn Phường/Xã (dữ liệu test 'Chọn ...').")

        except Exception as e:
            logger.warning(f"Không nhập được thông tin người nhận: {e}")

    # ======================================================
    # B7. Click Đặt hàng
    # ======================================================
    def click_order(self):
        try:
            btn = self.wait.until(EC.element_to_be_clickable(self.BTN_ORDER))
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            btn.click()
            logger.info("Đã click nút Đặt hàng.")
        except ElementClickInterceptedException:
            self.driver.execute_script("arguments[0].click();", btn)
            logger.info("Đã click Đặt hàng bằng JS (fallback).")
        except Exception as e:
            logger.warning(f"Không thể click Đặt hàng: {e}")

    # ======================================================
    # B8. Lấy thông báo kết quả
    # ======================================================
    def get_alert_message(self) -> str:
        try:
            el = self.wait.until(EC.visibility_of_element_located(self.ALERT_ERROR))
            msg = (el.text or "").strip()
            if msg:
                logger.info(f"Thông báo lỗi (Swal2): '{msg}'")
                return msg
        except Exception:
            logger.info("Không thấy Swal2 lỗi, thử lấy HTML5 validationMessage.")

        msg = self.driver.execute_script(
            "return document.querySelector(':invalid')?.validationMessage || '';"
        )
        msg = (msg or "").strip()
        if msg:
            logger.info(f"Thông báo lỗi (HTML5 validation): '{msg}'")
            return msg

        logger.info("Không có thông báo lỗi hiển thị.")
        return "Không có thông báo hiển thị."

    def get_success_message(self) -> str:
        try:
            self.wait.until(EC.url_contains("/cart/success"))
            if "/cart/success" in (self.driver.current_url or ""):
                el = self.wait.until(EC.visibility_of_element_located(self.SUCCESS_TEXT))
                msg = (el.text or "").strip()
                if "đặt hàng thành công" in msg.lower():
                    logger.info(f"Đặt hàng thành công: '{msg}'")
                    return msg
        except Exception:
            pass

        return ""