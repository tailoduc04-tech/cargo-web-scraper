import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
import traceback

from .base_scraper import BaseScraper

class HeungALineScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Heung-A Line và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu, dựa trên cấu trúc tương tự Sinokor.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD ...' sang 'DD/MM/YYYY'.
        """
        if not date_str:
            return None
        try:
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            return date_str

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã B/L trên trang Heung-A Line.
        """
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            self.wait.until(EC.visibility_of_element_located((By.ID, "divSchedule")))
            
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                return None, f"Could not extract normalized data for '{tracking_number}'."

            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/heungaline_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'. Trang web có thể đang chậm hoặc mã không hợp lệ."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Hàm chính để trích xuất, xử lý và chuẩn hóa dữ liệu từ trang chi tiết.
        """
        # 1. Trích xuất thông tin chung
        bl_no = self._get_text_from_element(By.XPATH, "//label[contains(text(), 'B/L No.')]/../../div[contains(@class, 'font-bold')]/span")
        booking_status = self._get_text_from_element(By.XPATH, "//label[contains(text(), 'B/K Status')]/../../div[contains(@class, 'font-bold')]/span")
        
        schedule_panel = self.wait.until(EC.presence_of_element_located((By.ID, "divSchedule")))
        etd_str = self._get_text_from_element(By.CSS_SELECTOR, "li.col-sm-8 .col-sm-6:nth-child(1)", parent=schedule_panel)
        eta_str = self._get_text_from_element(By.CSS_SELECTOR, "li.col-sm-8 .col-sm-6:nth-child(2)", parent=schedule_panel)
        
        pol, etd = split_location_and_datetime(etd_str)
        pod, eta = split_location_and_datetime(eta_str)

        # 2. Mở rộng bảng chi tiết Cargo Tracking
        # Sử dụng một selector ổn định hơn để tìm panel
        cargo_tracking_panel = self.wait.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Cargo Tracking')]/ancestor::div[contains(@class, 'panel')]")))
        try:
            toggle_button = cargo_tracking_panel.find_element(By.ID, "tglDetailInfo")
            if 'fa-chevron-down' in toggle_button.get_attribute('class'):
                 self.driver.execute_script("arguments[0].click();", toggle_button)
                 self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#divDetailInfo div.splitTable")))
        except Exception:
            pass

        # 3. Trích xuất lịch sử và tìm các ngày thực tế
        history_events = self._extract_history_events(cargo_tracking_panel)
        actual_departure = self._find_event(history_events, "Departure", pol)
        actual_arrival = self._find_event(history_events, "Arrival", pod)
        
        # 4. Xác định cảng trung chuyển
        transit_ports = []
        ata_transit_event = None
        atd_transit_event = None
        for event in history_events:
            location = event.get('location', '')
            description = event.get('description', '').lower()
            if ("arrival" in description or "departure" in description):
                is_pol = pol and pol.lower() in location.lower()
                is_pod = pod and pod.lower() in location.lower()
                if not is_pol and not is_pod and location not in transit_ports:
                    transit_ports.append(location)

        if transit_ports:
            ata_transit_event = self._find_event(history_events, "Arrival", transit_ports[0])
            atd_transit_event = self._find_event(history_events, "Departure", transit_ports[-1])

        # 5. Xây dựng đối tượng JSON
        shipment_data = {
            "BookingNo": tracking_number,
            "BlNumber": bl_no,
            "BookingStatus": booking_status,
            "Pol": pol,
            "Pod": pod,
            "Etd": self._format_date(etd),
            "Atd": self._format_date(actual_departure.get('date')) if actual_departure else None,
            "Eta": self._format_date(eta),
            "Ata": self._format_date(actual_arrival.get('date')) if actual_arrival else None,
            "TransitPort": ", ".join(transit_ports) if transit_ports else None,
            "EtdTransit": None,
            "AtdTrasit": self._format_date(atd_transit_event.get('date')) if atd_transit_event else None,
            "EtaTransit": None,
            "AtaTrasit": self._format_date(ata_transit_event.get('date')) if ata_transit_event else None
        }
        
        return shipment_data

    def _extract_history_events(self, cargo_tracking_panel):
        events = []
        try:
            detail_table_body = cargo_tracking_panel.find_element(By.CSS_SELECTOR, "#divDetailInfo .splitTable table tbody")
            rows = detail_table_body.find_elements(By.TAG_NAME, "tr")
            current_event_group = ""
            is_container_event = False
            for row in rows:
                header_th = row.find_elements(By.CSS_SELECTOR, "th.firstTh")
                if header_th:
                    current_event_group = header_th[0].text.strip()
                    is_container_event = "pickup" in current_event_group.lower() or "return" in current_event_group.lower()
                    continue
                
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells or len(cells) < 3: continue

                date_text, location, description = "", "", ""
                if is_container_event:
                    cntr_no, location, date_text = [c.text.strip() for c in cells]
                    description = f"{current_event_group}: {cntr_no}"
                else:
                    vessel_voyage, location, date_text = [c.text.strip() for c in cells]
                    description = f"{current_event_group}: {vessel_voyage}"
                
                if date_text:
                    events.append({"description": description, "location": location, "date": date_text})
        except NoSuchElementException:
            pass
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        if not location_keyword: return None
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower().split(':')[0]
            loc_match = location_keyword.lower() in event.get("location", "").lower()
            if desc_match and loc_match:
                return event
        return None

    def _get_text_from_element(self, by, value, parent=None):
        try:
            source = parent or self.driver
            return source.find_element(by, value).text.strip()
        except NoSuchElementException:
            return None

def split_location_and_datetime(input_string):
    if not input_string: return None, None
    pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}'
    match = re.search(pattern, input_string)
    if match:
        split_index = match.start()
        location_part = input_string[:split_index].strip()
        datetime_part = match.group(0)
        return location_part, datetime_part
    else:
        return input_string.strip(), None