import requests
import logging
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class ApiScraper(BaseScraper):
    """
    Lớp cơ sở cho các scraper sử dụng 'requests' để gọi API trực tiếp.
    """
    def __init__(self, config):
        # Gọi super() với driver=None
        super().__init__(config=config, driver=None) 
        
        self.session = requests.Session()
        # Thiết lập các headers chung mà hầu hết các API scraper đều dùng
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        logger.debug(f"[{self.__class__.__name__}] ApiScraper initialized.")

    def scrape(self, tracking_number: str):
        # Vẫn là abstract, các lớp con tự định nghĩa
        raise NotImplementedError
