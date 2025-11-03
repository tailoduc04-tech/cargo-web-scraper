import logging
from selenium.webdriver.support.ui import WebDriverWait
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class SeleniumScraper(BaseScraper):
    """
    Lớp cơ sở cho các scraper sử dụng Selenium WebDriver.
    """
    def __init__(self, driver, config):
        # Yêu cầu driver khi khởi tạo
        if driver is None:
            raise ValueError("SeleniumScraper yêu cầu một đối tượng driver.")
        
        super().__init__(config=config, driver=driver)
        
        # Khởi tạo WebDriverWait chung
        self.wait = WebDriverWait(self.driver, 30) # 30 giây là thời gian chờ mặc định
        logger.debug(f"[{self.__class__.__name__}] SeleniumScraper initialized.")

    def scrape(self, tracking_number: str):
        # Vẫn là abstract, các lớp con tự định nghĩa
        raise NotImplementedError
