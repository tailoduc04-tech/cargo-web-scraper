import requests
import logging
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class ApiScraper(BaseScraper):
    # Lớp cơ sở cho các scraper sử dụng 'requests' để gọi API trực tiếp.
    def __init__(self, config):
        # Khởi tạo scraper API
        super().__init__(config=config, driver=None)
        self.session = requests.Session()
        # Thiết lập các headers chung cho API scraper
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        logger.debug(f"[{self.__class__.__name__}] Đã khởi tạo ApiScraper.")

    def close(self):
        # Đóng session requests để giải phóng tài nguyên
        if self.session:
            try:
                self.session.close()
                logger.debug(f"[{self.__class__.__name__}] Đã đóng session.")
            except Exception as e:
                logger.error(f"Lỗi khi đóng session: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def scrape(self, tracking_number: str):
        # Hàm abstract, các lớp con tự định nghĩa
        raise NotImplementedError
