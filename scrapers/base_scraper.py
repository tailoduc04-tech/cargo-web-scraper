from abc import ABC, abstractmethod

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
    def scrape(self, tracking_number):
        """
        Phương thức scraping chính.
        Mỗi scraper con PHẢI triển khai phương thức này.

        Args:
            tracking_number (str): Mã vận đơn cần scrape.

        Returns:
            tuple: Một tuple chứa (data, error_message).
                   - data (dict): Một dictionary chứa các DataFrame kết quả. 
                                  Ví dụ: {'main': df1, 'details': df2}
                   - error_message (str or None): Thông báo lỗi nếu có.
        """
        pass