import logging
from playwright.async_api import Page # Import từ async_api
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class PlaywrightScraper(BaseScraper):
    """
    Lớp cơ sở cho các scraper sử dụng Playwright Page (Async).
    """
    def __init__(self, page: Page, config: dict):
        """
        Khởi tạo scraper với config và một Playwright Page (Async).

        Args:
            page (Page): Đối tượng Playwright Page đã được khởi tạo.
            config (dict): Dictionary cấu hình cho scraper này.
        """
        if page is None:
            raise ValueError("PlaywrightScraper yêu cầu một đối tượng Page.")
        super().__init__(config=config, driver=None) 
        
        self.page = page        
        logger.debug(f"[{self.__class__.__name__}] PlaywrightScraper initialized.")

    async def scrape(self, tracking_number: str):
        raise NotImplementedError("Phương thức scrape() phải được triển khai trong lớp con của PlaywrightScraper.")