import queue
import logging
import time
from driver_setup import create_driver

logger = logging.getLogger(__name__)

class DriverPool:
    def __init__(self, size=4):
        self.size = size
        self.drivers = queue.Queue(maxsize=size)
        
    def initialize(self):
        """Khởi tạo sẵn các driver"""
        logger.info(f"Đang khởi tạo Pool với {self.size} drivers...")
        for _ in range(self.size):
            try:
                driver = create_driver()
                self.drivers.put(driver)
            except Exception as e:
                logger.error(f"Lỗi khởi tạo driver ban đầu: {e}")
        logger.info("Driver Pool đã sẵn sàng!")

    def get_driver(self):
        """Lấy một driver từ pool. Nếu pool trống, sẽ block chờ đến khi có driver trả về."""
        driver = self.drivers.get() # Block cho đến khi có driver
        
        # Kiểm tra sức khỏe driver (Health Check)
        try:
            # Thử ping nhẹ vào browser xem còn sống không
            driver.title 
            return driver
        except Exception:
            logger.warning("Phát hiện Driver chết, đang tạo lại...")
            try:
                driver.quit()
            except: 
                pass
            return create_driver() # Tạo mới bù vào

    def return_driver(self, driver):
        """Trả driver về pool sau khi dùng xong"""
        try:
            # Dọn dẹp session để không bị lẫn lộn giữa các request
            driver.delete_all_cookies()
            # Mở trang trắng để nhẹ ram
            driver.get("about:blank")
        except Exception as e:
            logger.warning(f"Lỗi khi dọn dẹp driver: {e}. Sẽ tạo mới thay thế.")
            try:
                driver.quit()
            except: 
                pass
            driver = create_driver()
            
        self.drivers.put(driver)

    def shutdown(self):
        """Tắt toàn bộ driver khi tắt app"""
        logger.info("Đang đóng toàn bộ drivers...")
        while not self.drivers.empty():
            driver = self.drivers.get()
            try:
                if hasattr(driver, "service") and driver.service:
                    driver.service.stop()
                else:
                    driver.quit()
            except Exception:
                pass

# Khởi tạo một instance toàn cục (Singleton)
driver_pool = DriverPool(size=4)