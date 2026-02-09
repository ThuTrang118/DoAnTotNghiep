from selenium.webdriver.common.by import By
from pages.base_page import BasePage

class ProfilePage(BasePage):
    ACCOUNT_ICON = (By.CSS_SELECTOR,
        "div.no-padding.col-xs-12.hidden-sm.hidden-xs.col-md-1.right-cus.d-none.d-lg-block a.account-handle-icon"
    )
    PROFILE_USERNAME = (By.CSS_SELECTOR, "#UserName")

    def open_profile(self):
        self.click(self.ACCOUNT_ICON)
        self.find(self.PROFILE_USERNAME)

    def profile_username_present(self) -> bool:
        try:
            self.find(self.PROFILE_USERNAME)
            return True
        except Exception:
            return False

    def read_profile_username(self) -> str:
        el = self.find(self.PROFILE_USERNAME)
        return (el.get_attribute("value") or el.text or "").strip()
