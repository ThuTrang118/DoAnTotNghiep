import time
import unicodedata
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from utils.logger_utils import create_logger
from pages.base_page import BasePage

logger = create_logger("ProfileUpdatePage")


class MWCProfileUpdatePage(BasePage):
    """Trang cập nhật thông tin cá nhân — MWC."""

    PROFILE_URL = "https://mwc.com.vn/profile"

    # --- Locators ---USERNAME = (By.ID, "UserName")
    FULLNAME = (By.ID, "FullName")
    EMAIL = (By.ID, "Email")
    PHONE = (By.ID, "Phone")

    GENDER_MALE = (By.XPATH, "(//div[@class='stardust-radio-button__outer-circle'])[1]")
    GENDER_FEMALE = (By.XPATH, "(//div[contains(@class,'stardust-radio-button')])[4]")
    GENDER_OTHER = (By.XPATH, "(//div[@class='stardust-radio-button__outer-circle'])[3]")

    DAY = (By.ID, "Day")
    MONTH = (By.ID, "Month")
    YEAR = (By.ID, "Year")

    PROVINCE = (By.ID, "provinceOptions")
    DISTRICT = (By.ID, "districtSelect")
    WARD = (By.ID, "wardSelect")
    ADDRESS = (By.ID, "Address")

    SAVE_BTN = (By.XPATH, "//button[contains(text(),'Lưu')]")
    ALERT = (By.XPATH, "//div[contains(@class,'alert') or contains(text(),'thành công')]")
    TOAST_SUCCESS = (By.CSS_SELECTOR, ".jq-toast-single.jq-icon-success")
    TOAST_ANY = (By.CSS_SELECTOR, ".jq-toast-single")

    def __init__(self, driver, timeout=15):
        super().__init__(driver, timeout)

    def open(self):
        super().open(self.PROFILE_URL)
        logger.info("Mở trang hồ sơ cá nhân.")

    # ---------------------- VALUE HELPERS ----------------------
    def get_value(self, locator, timeout=10) -> str:
        """Lấy value hiện tại của input/select (ổn định cho assert sau khi reload)."""
        try:
            el = WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located(locator))
            return (el.get_attribute("value") or "").strip()
        except Exception:
            return ""

    def get_first_invalid_validation(self) -> str:
        """Lấy validationMessage của field :invalid đầu tiên (deterministic hơn so với thử từng locator)."""
        try:
            el = self.driver.execute_script(
                "return document.querySelector('input:invalid, select:invalid, textarea:invalid');"
            )
            if not el:
                return ""
            msg = self.driver.execute_script("return arguments[0].validationMessage;", el) or ""
            msg = (msg or "").strip().lower()
            if msg:
                logger.info(f"HTML5 validation(:invalid): {msg}")

            if "@" in msg and ("bao gồm" in msg or "include" in msg):
                return "Vui lòng bao gồm '@' trong địa chỉ email."
            if "vui lòng điền" in msg or "please fill" in msg:
                return "Vui lòng điền vào trường này."
            if "email" in msg and ("hợp lệ" in msg or "valid" in msg):
                return "Vui lòng nhập địa chỉ email hợp lệ."
            if "số" in msg or "number" in msg or "digits" in msg:
                return "Vui lòng nhập số hợp lệ."

            return msg
        except Exception:
            return ""

    # ---------------------- HELPERS ----------------------
    @staticmethod
    def _norm_text(s: str) -> str:
        s = (s or "").strip().lower()
        s = " ".join(s.split())
        s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("utf-8")
        return s

    def _get_select_signature(self, locator) -> tuple:
        try:
            sel = Select(self.driver.find_element(*locator))
            return tuple(self._norm_text(o.text) for o in sel.options)
        except Exception:
            return tuple()

    def _wait_select_ready(self, locator, min_options=2, timeout=15):
        wait = WebDriverWait(self.driver, timeout)

        def _ready(d):
            try:
                el = d.find_element(*locator)
                if not el.is_enabled():
                    return False
                opts = Select(el).options
                return len(opts) >= min_options
            except (StaleElementReferenceException, Exception):
                return False

        wait.until(_ready)
        return self.driver.find_element(*locator)

    def _wait_select_refreshed(self, locator, old_sig: tuple, timeout=15):
        wait = WebDriverWait(self.driver, timeout)

        def _changed(d):
            try:
                new_sig = self._get_select_signature(locator)
                return len(new_sig) >= 2 and new_sig != old_sig
            except (StaleElementReferenceException, Exception):
                return False

        wait.until(_changed)

    def _select_by_text_fuzzy(self, locator, text, timeout=15):
        if not text:
            return False

        target = self._norm_text(text)

        el = self._wait_select_ready(locator, min_options=2, timeout=timeout)
        sel = Select(el)

        try:
            sel.select_by_visible_text(text)
            return True
        except Exception:
            pass

        options = sel.options
        for o in options:
            if self._norm_text(o.text) == target:
                o.click()
                return True

        for o in options:
            ot = self._norm_text(o.text)
            if target and (target in ot or ot in target):
                o.click()
                return True

        available = [o.text for o in options]
        logger.warning(f"Không tìm thấy option '{text}' trong {locator}. Options hiện có: {available}")
        return False

    # ---------------------- FORM XỬ LÝ ----------------------
    def clear_field(self, locator):
        try:
            el = self.wait.until(EC.presence_of_element_located(locator))
            el.clear()
        except Exception:
            logger.warning(f"Không thể xóa {locator}")

    def safe_type(self, locator, value):
        try:
            el = self.wait.until(EC.presence_of_element_located(locator))
            el.clear()
            if value:
                el.send_keys(value)
                logger.info(f"Nhập vào {locator}: {value}")
        except Exception:
            logger.warning(f"Không thể nhập {locator}")

    def fill_profile(self, fullname, email, phone,
                     gender, day, month, year,
                     province, district, ward, address):

        logger.info("Bắt đầu điền thông tin hồ sơ...")

        for loc in [self.FULLNAME, self.EMAIL, self.PHONE, self.ADDRESS]:
            self.clear_field(loc)

        self.safe_type(self.FULLNAME, fullname)
        self.safe_type(self.EMAIL, email)
        self.safe_type(self.PHONE, phone)

        try:
            g = (gender or "").strip().lower()
            if g == "nam":
                self.click(self.GENDER_MALE)
            elif g == "nữ":
                self.click(self.GENDER_FEMALE)
            elif g == "khác":
                self.click(self.GENDER_OTHER)
            logger.info(f"Đã chọn giới tính: {gender}")
        except Exception:
            logger.warning("Không thể chọn giới tính.")

        try:
            if day:
                Select(self.driver.find_element(*self.DAY)).select_by_value(str(day))
            if month:
                Select(self.driver.find_element(*self.MONTH)).select_by_value(str(month))
            if year:
                Select(self.driver.find_element(*self.YEAR)).select_by_value(str(year))
            logger.info(f"Chọn ngày sinh: {day}-{month}-{year}")
        except Exception:
            logger.warning("Không thể chọn ngày/tháng/năm sinh.")

        try:
            if province:
                old_district_sig = self._get_select_signature(self.DISTRICT)
                ok = self._select_by_text_fuzzy(self.PROVINCE, province, timeout=15)
                if ok:
                    logger.info(f"Đã chọn Tỉnh/TP: {province}")
                    try:
                        self._wait_select_refreshed(self.DISTRICT, old_district_sig, timeout=15)
                    except TimeoutException:
                        logger.warning("District không refresh kịp sau khi chọn Province (timeout).")

            if district:
                old_ward_sig = self._get_select_signature(self.WARD)
                ok = self._select_by_text_fuzzy(self.DISTRICT, district, timeout=15)
                if ok:
                    logger.info(f"Đã chọn Quận/Huyện: {district}")
                    try:
                        self._wait_select_refreshed(self.WARD, old_ward_sig, timeout=15)
                    except TimeoutException:
                        logger.warning("Ward không refresh kịp sau khi chọn District (timeout).")

            if ward:
                ok = self._select_by_text_fuzzy(self.WARD, ward, timeout=15)
                if ok:
                    logger.info(f"Đã chọn Phường/Xã: {ward}")

        except Exception as e:
            logger.warning(f"Không chọn được địa chỉ hành chính (bỏ qua). Lý do: {e}")

        self.safe_type(self.ADDRESS, address)

    # ---------------------- HÀNH ĐỘNG ----------------------
    def click_save(self):
        try:
            self.click_robust(self.SAVE_BTN, timeout=15, js_fallback=True)
            logger.info("Click nút Lưu thông tin.")
        except Exception as e:
            logger.error(f"Không thể click nút Lưu. Lỗi: {type(e).__name__}: {e}")


    def get_toast_message(self):
        """Bắt thông báo toast thành công (ưu tiên wait visibility để giảm miss)."""
        try:
            try:
                el = WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located(self.TOAST_SUCCESS))
                inner = (el.get_attribute("innerText") or "").strip()
                if inner:
                    logger.info(f"Toast success: {inner}")
                    return inner
            except TimeoutException:
                pass

            try:
                el = WebDriverWait(self.driver, 3).until(EC.visibility_of_element_located(self.TOAST_ANY))
                inner = (el.get_attribute("innerText") or "").strip()
                if inner:
                    low = inner.lower()
                    if "thành công" in low or "success" in low:
                        logger.info(f"Toast(any) success-like: {inner}")
                        return inner
            except TimeoutException:
                return ""
            return ""
        except Exception:
            return ""

    def get_alert_text(self):
        try:
            el = self.wait.until(EC.visibility_of_element_located(self.ALERT))
            msg = (el.text or "").strip()
            if msg:
                logger.info(f"Alert DOM: {msg}")
            return msg
        except Exception:
            return ""

    def get_html5_validation(self, locator):
        """Giữ lại để tương thích, nhưng test đã ưu tiên get_first_invalid_validation()."""
        try:
            el = self.driver.find_element(*locator)
            msg = self.driver.execute_script("return arguments[0].validationMessage;", el) or ""
            msg = msg.strip().lower()
            if msg:
                logger.info(f"HTML5 validation: {msg}")
            if "@" in msg and "bao gồm" in msg:
                return "Vui lòng bao gồm '@' trong địa chỉ email."
            elif "vui lòng điền" in msg or "please fill" in msg:
                return "Vui lòng điền vào trường này."
            elif "email" in msg:
                return "Vui lòng nhập địa chỉ email hợp lệ."
            elif "số" in msg or "number" in msg:
                return "Vui lòng nhập số hợp lệ."
            return msg
        except Exception:
            return ""
