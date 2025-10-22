from abc import ABC, abstractmethod
from typing import Tuple, Optional
from schemas import N8nTrackingInfo

class BaseScraper(ABC):
    """
    Abstract Base Class cho tất cả scraper
    """
    def __init__(self, driver, config):
        """
        Khởi tạo scraper với driver và cấu hình riêng.

        Args:
            driver: Đối tượng Selenium WebDriver.
            config (dict): Dictionary cấu hình cho scraper này (lấy từ config.py).
        """
        self.driver = driver
        self.config = config
        self.wait = None

    @abstractmethod
    @abstractmethod
    def scrape(self, tracking_number: str) -> Tuple[Optional[N8nTrackingInfo], Optional[str]]:
        """
        Phương thức scraping chính.
        Returns:
            tuple: (data, error_message)
                   - data (dict | N8nTrackingInfor | None): Dữ liệu tracking đã chuẩn hóa hoặc None nếu lỗi.
                   - error_message (str | None): Thông báo lỗi nếu có.
        """
        pass