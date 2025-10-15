import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback

from .base_scraper import BaseScraper

class CordeliaScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Cordelia Line và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một số theo dõi nhất định từ trang web của Cordelia Line
        và trả về ở định dạng JSON chuẩn hóa.
        """
        try:
            # URL đã chứa sẵn phần truy vấn, chúng ta chỉ cần nối thêm số B/L vào cuối
            url = f"{self.config['url']}{tracking_number}"
            self.driver.get(url)
            self.wait = WebDriverWait(self.driver, 30)

            # Chờ cho bảng kết quả được JavaScript tải xong
            # Trang web có một div với id="loader" sẽ biến mất khi tải xong
            self.wait.until(EC.invisibility_of_element_located((By.ID, "loader")))
            
            # Sau đó, đợi bảng dữ liệu chính xuất hiện
            result_table = self.wait.until(
                EC.visibility_of_element_located((By.ID, "checkShedTable"))
            )
            print("Cordelia: Trang kết quả đã được tải và bảng dữ liệu đã hiển thị.")

            # Trích xuất và chuẩn hóa dữ liệu sang định dạng JSON mong muốn
            normalized_data = self._extract_and_normalize_data(result_table)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            # Dự án yêu cầu trả về một dictionary, vì vậy chúng ta trả về kết quả đầu tiên (và duy nhất)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/cordelia_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Đã xảy ra Timeout. Đang lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' hoặc trang web không phản hồi."
        except Exception as e:
            print(f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, table):
        """
        Trích xuất và chuẩn hóa dữ liệu từ bảng kết quả theo mẫu JSON đã chỉ định.
        """
        try:
            # Tìm hàng dữ liệu đầu tiên trong phần thân của bảng
            row = table.find_element(By.CSS_SELECTOR, "tbody tr")
            cells = row.find_elements(By.TAG_NAME, "td")

            if len(cells) < 9:
                print("Cordelia: Hàng dữ liệu có ít ô hơn dự kiến.")
                return None

            # Ánh xạ dữ liệu từ các ô trong bảng vào các trường của mẫu JSON
            bl_number = cells[0].text.strip()
            pol = cells[1].text.strip()
            sob_date = cells[4].text.strip()
            pod = cells[5].text.strip()
            eta_fpod = cells[6].text.strip()
            current_status = cells[7].text.strip()
            
            # "Vị trí hiện tại" dường như chỉ ra tàu/cảng trung chuyển
            current_location = cells[8].text.strip()
            transit_port = None
            if "leg" in current_status.lower():
                transit_port = current_location

            # Tạo cấu trúc JSON
            shipment_data = {
                "BookingNo": None,  # Không có trên trang
                "BlNumber": bl_number,
                "BookingStatus": current_status,
                "Pol": pol,
                "Pod": pod,
                "Etd": None,  # Thời gian khởi hành dự kiến không được cung cấp
                "Atd": sob_date,
                "Eta": eta_fpod,
                "Ata": None,  # Thời gian đến thực tế không được cung cấp
                "EtdTransit": None, # Không có
                "AtdTrasit": None, # Không có
                "TransitPort": transit_port,
                "EtaTransit": None, # Không có
                "AtaTrasit": None # Không có
            }
            
            return shipment_data

        except Exception as e:
            print(f"    Cảnh báo: Không thể phân tích bảng kết quả của Cordelia: {e}")
            traceback.print_exc()
            return None

