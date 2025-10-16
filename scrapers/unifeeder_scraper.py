import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
from .base_scraper import BaseScraper

class UnifeederScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Unifeeder và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD-Mon-YY HH:MM AM/PM' sang 'DD/MM/YYYY'.
        Ví dụ: '29-Oct-25 07:00 AM' -> '29/10/2025'
        """
        if not date_str:
            return None
        try:
            # Loại bỏ phần '(Projected)' nếu có
            clean_date_str = date_str.replace("(Projected)", "").strip()
            # Phân tích chuỗi ngày tháng
            dt_obj = datetime.strptime(clean_date_str, '%d-%b-%y %I:%M %p')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            # Trả về chuỗi gốc nếu không phân tích được
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính, truy cập URL trực tiếp và trả về JSON.
        """
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            # Chờ cho đến khi chi tiết booking được tải
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.booking-details"))
            )

            # Trích xuất và chuẩn hóa dữ liệu theo template mới
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                return None, f"Could not extract normalized data for '{tracking_number}'."

            # Trả về một dictionary duy nhất theo yêu cầu
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/unifeeder_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception:
                pass
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ trang và ánh xạ vào template JSON.
        """
        try:
            # 1. Trích xuất POL và POD
            route_container = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.route-display")))
            route_spans = route_container.find_elements(By.TAG_NAME, "span")
            pol = route_spans[0].text.strip() if len(route_spans) > 0 else None
            pod = route_spans[1].text.strip() if len(route_spans) > 1 else None

            # 2. Trích xuất tất cả các sự kiện
            events = self._extract_events()
            
            # 3. Tìm các sự kiện quan trọng để xác định ngày tháng và cảng trung chuyển
            departure_event = self._find_event(events, "LOAD FULL", pol, event_type="ngay_thuc_te")
            arrival_event = self._find_event(events, "DISCHARGE FULL", pod)
            
            transit_ports = []
            ts_discharge_events = []
            ts_load_events = []

            for event in events:
                desc = event.get('description', '').upper()
                if "T/S" in desc:
                    port = event.get('location')
                    if port and port not in transit_ports:
                        transit_ports.append(port)
                    if "DISCHARGE" in desc:
                        ts_discharge_events.append(event)
                    elif "LOAD" in desc:
                        ts_load_events.append(event)
            
            # 4. Tạo đối tượng JSON và điền dữ liệu
            shipment_data = {
                "BookingNo": tracking_number,
                "BlNumber": tracking_number,
                "BookingStatus": None, # Không có thông tin
                "Pol": pol,
                "Pod": pod,
                "Etd": None, # Không có thông tin
                "Atd": self._format_date(departure_event.get('date')) if departure_event else None,
                "Eta": self._format_date(arrival_event.get('date')) if arrival_event and arrival_event.get('type') == 'ngay_du_kien' else None,
                "Ata": self._format_date(arrival_event.get('date')) if arrival_event and arrival_event.get('type') == 'ngay_thuc_te' else None,
                "TransitPort": ", ".join(transit_ports) if transit_ports else None,
                "EtdTransit": None,
                "AtdTransit": self._format_date(ts_load_events[-1].get('date')) if ts_load_events else None, 
                "EtaTransit": None,
                "AtaTransit": self._format_date(ts_discharge_events[0].get('date')) if ts_discharge_events else None,
            }
            
            return shipment_data

        except Exception as e:
            traceback.print_exc()
            return None

    def _extract_events(self):
        """
        Trích xuất toàn bộ lịch sử từ mục "Tracking".
        """
        events = []
        event_rows = self.driver.find_elements(By.CSS_SELECTOR, "div.row-item")
        
        for row in event_rows:
            # Bỏ qua hàng tiêu đề
            if row.find_elements(By.CSS_SELECTOR, ".table-title"):
                continue

            try:
                cells = row.find_elements(By.CSS_SELECTOR, ".list-box > div")
                if len(cells) < 3: continue

                date_text = cells[0].text.strip()
                description = cells[1].text.strip()
                location = cells[2].text.strip()
                
                event_type = "ngay_du_kien" if "(Projected)" in date_text else "ngay_thuc_te"
                
                events.append({
                    "date": date_text,
                    "type": event_type,
                    "description": description,
                    "location": location
                })
            except NoSuchElementException:
                continue
        return events

    def _find_event(self, events, description_keyword, location_keyword, event_type=None):
        """
        Tìm một sự kiện cụ thể trong danh sách, có thể lọc theo loại (dự kiến/thực tế).
        """
        if not location_keyword: return {}
        
        # Duyệt ngược để tìm sự kiện gần nhất
        for event in reversed(events):
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = location_keyword.lower() in (event.get("location") or "").lower()
            
            type_match = True
            if event_type:
                type_match = (event.get("type") == event_type)

            if desc_match and loc_match and type_match:
                return event
        
        return {}