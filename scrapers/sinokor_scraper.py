import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
import time
import traceback

from .base_scraper import BaseScraper

class SinokorScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Sinokor và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã B/L trên trang Sinokor.
        URL được xây dựng động dựa trên mã tracking.
        """
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            # Chờ cho panel schedule xuất hiện để chắc chắn trang đã tải
            self.wait.until(EC.visibility_of_element_located((By.ID, "divSchedule")))
            
            # Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data()
            
            if not normalized_data:
                return None, f"Could not extract normalized data for '{tracking_number}'."

            results_df = pd.DataFrame(normalized_data)
            
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/sinokor_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'. Trang web có thể đang chậm hoặc mã không hợp lệ."
        except Exception as e:
            print(f"An unexpected error occurred for '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self):
        """
        Hàm chính để trích xuất, xử lý và chuẩn hóa dữ liệu từ trang chi tiết.
        """
        schedule_panel = self.wait.until(EC.presence_of_element_located((By.ID, "divSchedule")))

        etd_str = self._get_text_from_element(schedule_panel, By.CSS_SELECTOR, "li.col-sm-8 .col-sm-6:nth-child(1)")
        eta_str = self._get_text_from_element(schedule_panel, By.CSS_SELECTOR, "li.col-sm-8 .col-sm-6:nth-child(2)")
        
        pol_text, etd = split_location_and_datetime(etd_str)
        pod_text, eta = split_location_and_datetime(eta_str)

        cargo_tracking_panel_selector = "#wrapper > div > div > div:nth-child(5).panel.hpanel"
        cargo_tracking_panel = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, cargo_tracking_panel_selector))
        )
        
        try:
            toggle_button = cargo_tracking_panel.find_element(By.ID, "tglDetailInfo")
            if 'fa-chevron-down' in toggle_button.get_attribute('class'):
                 self.driver.execute_script("arguments[0].click();", toggle_button)
                 self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, f"{cargo_tracking_panel_selector} #divDetailInfo div.splitTable")))
            print("Successfully expanded cargo tracking details.")
        except Exception as e:
            print(f"Could not expand cargo tracking details or it was already open: {e}")

        history_events = self._extract_history_events(cargo_tracking_panel)
        
        actual_departure = self._find_event(history_events, "Departure", pol_text)
        actual_arrival = self._find_event(history_events, "Arrival", pod_text)
        
        transit_ports = []
        for event in history_events:
            location = event.get('location', '')
            description = event.get('description', '').lower()
            if ("vessel" in description or "departure" in description or "arrival" in description) and \
               pol_text and pol_text.lower() not in location.lower() and \
               pod_text and pod_text.lower() not in location.lower():
                if location not in transit_ports:
                    transit_ports.append(location)

        shipment_data = {
            "POL": pol_text,
            "POD": pod_text,
            "transit_port": ", ".join(transit_ports) if transit_ports else None,
            "ngay_tau_di": {
                "ngay_du_kien": etd.replace('\n', ' ').strip() if etd else None,
                "ngay_thuc_te": actual_departure.get('date') if actual_departure else None
            },
            "ngay_tau_den": {
                "ngay_du_kien": eta.replace('\n', ' ').strip() if eta else None,
                "ngay_thuc_te": actual_arrival.get('date') if actual_arrival else None
            },
            "lich_su": history_events
        }
        
        return [shipment_data]

    def _extract_history_events(self, cargo_tracking_panel):
        """
        [ĐÃ CẬP NHẬT] Trích xuất tất cả sự kiện từ bảng lịch sử chi tiết 
        bằng cách duyệt qua từng hàng và xác định nhóm sự kiện.
        """
        events = []
        try:
            detail_table_body = cargo_tracking_panel.find_element(By.CSS_SELECTOR, "#divDetailInfo .splitTable table tbody")
            rows = detail_table_body.find_elements(By.TAG_NAME, "tr")

            current_event_group = ""
            is_container_event = False

            for row in rows:
                # Kiểm tra nếu hàng là tiêu đề của một nhóm sự kiện mới (ví dụ: Pickup (5/5))
                header_th = row.find_elements(By.CSS_SELECTOR, "th.firstTh")
                if header_th:
                    current_event_group = header_th[0].text.strip()
                    # Xác định đây là sự kiện của container (Pickup/Return) hay của tàu (Departure/Arrival)
                    is_container_event = "pickup" in current_event_group.lower() or "return" in current_event_group.lower()
                    continue

                # Bỏ qua các hàng tiêu đề cột (ví dụ: [CNTR No., Location, Date & Time])
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    continue

                date_text = ""
                location = ""
                description = ""

                # Xử lý hàng dữ liệu dựa trên loại nhóm sự kiện đã xác định
                if is_container_event:
                    # Cấu trúc: [CNTR No., Location, Date & Time]
                    if len(cells) == 3:
                        cntr_no = cells[0].text.strip()
                        location = cells[1].text.strip()
                        date_text = cells[2].text.strip()
                        description = f"{current_event_group}: {cntr_no}"
                else:  # Sự kiện của tàu (Departure/Arrival)
                    # Cấu trúc: [Vessel / Voyage, Location, Date & Time]
                    if len(cells) == 3:
                        vessel_voyage = cells[0].text.strip()
                        location = cells[1].text.strip()
                        date_text = cells[2].text.strip()
                        description = f"{current_event_group}: {vessel_voyage}"
                
                # Chỉ thêm sự kiện nếu có dữ liệu ngày tháng
                if date_text:
                    events.append({
                        "description": description,
                        "location": location,
                        "date": self._format_date(date_text),
                        "type": self._get_date_type(date_text)
                    })
        except NoSuchElementException:
            print("Could not find cargo tracking detail table inside the specified panel.")
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        if not location_keyword: return None
        for event in events:
            # Tìm kiếm keyword trong description đã được chuẩn hóa (ví dụ: "Departure: ...")
            desc_match = description_keyword.lower() in event.get("description", "").lower().split(':')[0]
            loc_match = location_keyword.lower() in event.get("location", "").lower()
            if desc_match and loc_match:
                return event
        return None

    def _get_text_from_element(self, parent, by, value):
        try:
            return parent.find_element(by, value).text.strip()
        except NoSuchElementException:
            return None
            
    def _format_date(self, date_str):
        return date_str.replace('\n', ' ')

    def _get_date_type(self, date_str):
        try:
            date_part = date_str.split('\n')[0]
            for day in ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]:
                date_part = date_part.replace(day, "").strip()
            event_date = datetime.strptime(date_part, '%Y-%m-%d')
            if event_date.date() <= datetime.now().date():
                return "ngay_thuc_te"
            else:
                return "ngay_du_kien"
        except (ValueError, IndexError):
            return "ngay_du_kien"
        
def split_location_and_datetime(input_string):
    """
    Sử dụng regex để tìm chuỗi ngày giờ (YYYY-MM-DD HH:MM),
    sau đó chia chuỗi gốc thành hai phần: phần địa điểm và phần ngày giờ.

    Args:
        input_string (str): Chuỗi văn bản cần phân tích.

    Returns:
        tuple: Một tuple chứa hai chuỗi (location, datetime_str).
               Trả về (original_string, None) nếu không tìm thấy ngày giờ.
    """
    if not input_string:
        return None, None

    # Biểu thức chính quy để tìm chính xác định dạng YYYY-MM-DD HH:MM
    pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}'

    match = re.search(pattern, input_string)

    if match:
        # Lấy vị trí bắt đầu của chuỗi ngày giờ
        split_index = match.start()

        # Phần 1: từ đầu chuỗi đến trước ngày giờ
        location_part = input_string[:split_index].strip()
        
        # Phần 2: chính là chuỗi ngày giờ
        datetime_part = match.group(0)
        
        return location_part, datetime_part
    else:
        # Nếu không tìm thấy, trả về chuỗi gốc và None
        return input_string.strip(), None