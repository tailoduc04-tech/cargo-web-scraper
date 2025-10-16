import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper

class OslScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Oceanic Star Line (OSL)
    và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'Weekday, DD-Mon-YYYY' (ví dụ: Sunday, 20-Jul-2025)
        sang định dạng 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            date_part = date_str.split(", ")[1]
            dt_obj = datetime.strptime(date_part, '%d-%b-%Y')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            print(f"    [OCL Scraper] Cảnh báo: Không thể phân tích định dạng ngày: {date_str}")
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scraping chính cho Oceanic Star Line.
        """
        print(f"[OCL Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            print("[OCL Scraper] -> Điền thông tin tìm kiếm...")
            bl_input = self.wait.until(EC.element_to_be_clickable((By.ID, "bl_no")))
            bl_input.clear()
            bl_input.send_keys(tracking_number)

            search_button = self.driver.find_element(By.ID, "search_btn")
            self.driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
            time.sleep(0.5)
            search_button.click()

            print("[OCL Scraper] -> Chờ bảng kết quả tải...")
            self.wait.until(EC.visibility_of_element_located((By.ID, "listing-table")))
            print("[OCL Scraper] -> Bảng kết quả đã tải. Bắt đầu trích xuất.")

            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[OCL Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_all_events(self):
        """
        Trích xuất toàn bộ lịch sử di chuyển từ bảng kết quả.
        """
        events = []
        rows = self.driver.find_elements(By.CSS_SELECTOR, ".table-body tr")
        
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 5:
                continue
            
            event_data = {
                "bl_number": cells[1].text.strip(),
                "date": cells[2].text.strip(),
                "description": cells[3].text.strip().upper(),
                "location": cells[4].text.strip()
            }
            events.append(event_data)
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """
        Tìm một sự kiện cụ thể trong danh sách, có thể lọc theo cảng.
        """
        for event in events:
            desc_match = description_keyword.upper() in event.get("description", "")
            
            loc_match = True # Mặc định là khớp nếu không cần kiểm tra cảng
            if location_keyword:
                loc_match = location_keyword.upper() in event.get("location", "").upper()

            if desc_match and loc_match:
                return event
        return {}

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu, áp dụng logic tìm kiếm sự kiện nâng cao.
        """
        try:
            all_events = self._extract_all_events()
            if not all_events:
                return None

            # --- Xác định các thông tin cơ bản ---
            bl_number = all_events[0].get("bl_number") or tracking_number
            booking_status = all_events[0].get("description") # Sự kiện mới nhất

            # --- Tìm sự kiện đi và đến chính ---
            departure_event = self._find_event(all_events, "LOAD FULL")
            arrival_event = self._find_event(all_events, "DISCHARGE FULL")

            pol = departure_event.get("location")
            pod = arrival_event.get("location")
            
            # --- Tìm kiếm cảng và ngày trung chuyển ---
            transit_ports = []
            ts_discharge_events = []
            ts_load_events = []

            for event in all_events:
                port = event.get('location')
                desc = event.get('description', '')
                
                # Nếu sự kiện dỡ/tải hàng không diễn ra tại POL hoặc POD, đó là cảng trung chuyển
                is_transit_event = "DISCHARGE" in desc or "LOAD" in desc
                is_at_pol_or_pod = (port and pol and port in pol) or \
                                   (port and pod and port in pod)

                if is_transit_event and not is_at_pol_or_pod:
                    if port and port not in transit_ports:
                        transit_ports.append(port)
                    if "DISCHARGE" in desc:
                        ts_discharge_events.append(event)
                    elif "LOAD" in desc:
                        ts_load_events.append(event)

            # --- Xây dựng đối tượng JSON cuối cùng ---
            shipment_data = {
                "BookingNo": tracking_number,
                "BlNumber": bl_number,
                "BookingStatus": booking_status,
                "Pol": pol,
                "Pod": pod,
                "Etd": None,
                "Atd": self._format_date(departure_event.get("date")),
                "Eta": None,
                "Ata": self._format_date(arrival_event.get("date")),
                "TransitPort": ", ".join(transit_ports) if transit_ports else None,
                "EtdTransit": None,
                "AtdTrasit": self._format_date(ts_load_events[0].get('date')) if ts_load_events else None,
                "EtaTransit": None,
                "AtaTrasit": self._format_date(ts_discharge_events[-1].get('date')) if ts_discharge_events else None,
            }
            return shipment_data
        except Exception as e:
            print(f"    [OCL Scraper] Lỗi trong quá trình trích xuất: {e}")
            traceback.print_exc()
            return None