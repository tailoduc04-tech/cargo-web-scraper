import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper

class SealeadScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web SeaLead và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'Month DD, YYYY' (ví dụ: August 7, 2025) sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Định dạng của SeaLead là '%B %d, %Y'
            dt_obj = datetime.strptime(date_str, '%B %d, %Y')
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            print(f"[SeaLead Scraper] Cảnh báo: Không thể phân tích định dạng ngày: {date_str}")
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho SeaLead.
        """
        print(f"[SeaLead Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # --- 1. Nhập mã B/L và tìm kiếm ---
            print("[SeaLead Scraper] -> Đang điền thông tin tìm kiếm...")
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "bl_number")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            track_button = self.driver.find_element(By.CSS_SELECTOR, ".track-form-container button")
            self.driver.execute_script("arguments[0].click();", track_button)

            # --- 2. Chờ trang kết quả và trích xuất ---
            print("[SeaLead Scraper] -> Đang chờ trang kết quả tải...")
            self.wait.until(
                EC.visibility_of_element_located((By.XPATH, f"//h4[contains(text(), 'Bill of lading number:')]"))
            )
            print("[SeaLead Scraper] -> Trang kết quả đã tải. Bắt đầu trích xuất.")
            
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[SeaLead Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/sealead_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception:
                pass
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ trang kết quả và ánh xạ vào template JSON.
        """
        try:
            # --- 1. Trích xuất thông tin cơ bản ---
            bl_number = self.driver.find_element(By.XPATH, "//h4[contains(text(), 'Bill of lading number')]").text.replace("Bill of lading number:", "").strip()
            
            info_table = self.driver.find_element(By.CSS_SELECTOR, "table.route-table-bill")
            pol = info_table.find_element(By.XPATH, ".//th[contains(text(), 'Port of Loading')]/following-sibling::td").text.strip()
            pod = info_table.find_element(By.XPATH, ".//th[contains(text(), 'Port of Discharge')]/following-sibling::td").text.strip()

            # --- 2. Trích xuất lịch trình và thông tin trung chuyển ---
            etd, eta = None, None
            transit_port, etd_transit, eta_transit = None, None, None
            
            schedule_tables = self.driver.find_elements(By.CSS_SELECTOR, "table.route-table")
            
            if len(schedule_tables) > 0:
                main_schedule_rows = schedule_tables[0].find_elements(By.CSS_SELECTOR, "tbody tr")
                if main_schedule_rows:
                    # Chặng đầu tiên luôn là chặng khởi hành
                    first_leg_cells = main_schedule_rows[0].find_elements(By.TAG_NAME, "td")
                    etd = first_leg_cells[5].text.strip() if len(first_leg_cells) > 5 else None

                    # Chặng cuối cùng là chặng đến
                    last_leg_cells = main_schedule_rows[-1].find_elements(By.TAG_NAME, "td")
                    eta = last_leg_cells[7].text.strip() if len(last_leg_cells) > 7 else None

                    # Nếu có nhiều hơn một chặng, tức là có trung chuyển
                    if len(main_schedule_rows) > 1:
                        # Cảng trung chuyển là điểm đến của chặng đầu tiên
                        transit_port = first_leg_cells[6].text.strip() if len(first_leg_cells) > 6 else None
                        # ETA tại cảng trung chuyển
                        eta_transit = first_leg_cells[7].text.strip() if len(first_leg_cells) > 7 else None
                        
                        # ETD từ cảng trung chuyển là thời gian đi của chặng thứ hai
                        second_leg_cells = main_schedule_rows[1].find_elements(By.TAG_NAME, "td")
                        etd_transit = second_leg_cells[5].text.strip() if len(second_leg_cells) > 5 else None

            # --- 3. Trích xuất thời gian đến thực tế (Ata) từ chi tiết container ---
            ata = None
            if len(schedule_tables) > 1:
                container_details_table = schedule_tables[1]
                try:
                    first_container_row = container_details_table.find_element(By.CSS_SELECTOR, "tbody tr")
                    container_cells = first_container_row.find_elements(By.TAG_NAME, "td")
                    ata = container_cells[4].text.strip() if len(container_cells) > 4 else None
                except NoSuchElementException:
                    print("[SeaLead Scraper] Cảnh báo: Không tìm thấy chi tiết container để lấy Ata.")

            # --- 4. Xây dựng đối tượng JSON ---
            shipment_data = {
                "BookingNo": bl_number,
                "BlNumber": bl_number,
                "BookingStatus": None,
                "Pol": pol,
                "Pod": pod,
                "Etd": self._format_date(etd),
                "Atd": None, # Không có thông tin
                "Eta": self._format_date(eta),
                "Ata": self._format_date(ata),
                "TransitPort": transit_port,
                "EtdTransit": self._format_date(etd_transit),
                "AtdTrasit": None, # Không có thông tin
                "EtaTransit": self._format_date(eta_transit),
                "AtaTrasit": None, # Không có thông tin
            }
            return shipment_data

        except Exception as e:
            print(f"[SeaLead Scraper] -> Lỗi trong quá trình trích xuất: {e}")
            traceback.print_exc()
            return None