import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import traceback
import re

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

class InterasiaScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Interasia và chuẩn hóa kết quả
    theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """Chuyển đổi chuỗi ngày từ 'YYYY/MM/DD HH:MM:SS' sang 'DD/MM/YYYY'."""
        if not date_str:
            return None
        try:
            # Lấy phần ngày tháng năm, bỏ qua phần giờ
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y/%m/%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho Interasia.
        """
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 20)

            # --- 1. Thực hiện tìm kiếm ---
            search_input = self.wait.until(EC.presence_of_element_located((By.NAME, "query")))
            search_input.clear()
            search_input.send_keys(tracking_number)
            self.driver.find_element(By.CSS_SELECTOR, "#containerSumbit").click()
            
            # --- 2. Lấy link chi tiết B/L và truy cập ---
            try:
                # Chờ cho đến khi bảng kết quả ban đầu xuất hiện
                detail_link_element = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".m-table-group tbody tr:first-child td:first-child a"))
                )
                detail_link = detail_link_element.get_attribute('href')
            except TimeoutException:
                 return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên trang kết quả chính."

            # --- 3. Scrape trang chi tiết và chuẩn hóa ---
            normalized_data = self._scrape_and_normalize_details(detail_link, tracking_number)
            
            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            # --- 4. Trả về kết quả ---
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/interasia_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception:
                pass
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _scrape_and_normalize_details(self, detail_url, tracking_number):
        """
        Scrape trang chi tiết B/L, trích xuất và chuẩn hóa dữ liệu theo template JSON.
        """
        try:
            self.driver.get(detail_url)
            main_group = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "main-group")))
            
            # 1. Trích xuất thông tin tóm tắt chung
            summary_table = main_group.find_element(By.CSS_SELECTOR, ".m-table-group")
            cells = summary_table.find_elements(By.CSS_SELECTOR, "tbody tr td")
            
            pol = cells[0].text.strip() if len(cells) > 0 else None
            pod = cells[1].text.strip() if len(cells) > 1 else None
            etd = cells[2].text.strip() if len(cells) > 2 else None
            eta = cells[3].text.strip() if len(cells) > 3 else None

            # 2. Lặp qua từng container để tổng hợp tất cả sự kiện
            all_events = []
            container_blocks = main_group.find_elements(By.XPATH, "./div[.//p[contains(text(), 'Container No')]]")
            for block in container_blocks:
                events = self._extract_events_from_container(block)
                all_events.extend(events)

            # 3. Tìm các sự kiện quan trọng từ danh sách đã tổng hợp
            actual_departure = self._find_event(all_events, "LOADED ON BOARD VESSEL", pol)
            actual_arrival = self._find_event(all_events, "DISCHARGED FROM VESSEL", pod)
            
            transit_ports = []
            transit_arrival_events = []
            transit_departure_events = []

            for event in all_events:
                desc = event.get('description', '').lower()
                port = event.get('location')
                if "transhipment" in desc and port:
                    if port not in transit_ports:
                        transit_ports.append(port)
                    # Giả định "DISCHARGED" tại cảng lạ là sự kiện đến cảng trung chuyển
                    if "discharged" in desc:
                        transit_arrival_events.append(event)
                    # Giả định "LOADED" tại cảng lạ là sự kiện rời cảng trung chuyển
                    if "loaded" in desc:
                         transit_departure_events.append(event)

            # 4. Xây dựng đối tượng JSON chuẩn hóa
            #shipment_data = {
            #    "BookingNo": tracking_number,
            #    "BlNumber": tracking_number,
            #    "BookingStatus": None, # Không có thông tin
            #    "Pol": pol,
            #    "Pod": pod,
            #    "Etd": self._format_date(etd),
            #    "Atd": self._format_date(actual_departure.get('date')),
            #    "Eta": self._format_date(eta),
            #    "Ata": self._format_date(actual_arrival.get('date')),
            #    "TransitPort": ", ".join(transit_ports) if transit_ports else None,
            #    "EtdTransit": None, # Không có thông tin
            #    "AtdTrasit": self._format_date(transit_departure_events[-1].get('date')) if transit_departure_events else None,
            #    "EtaTransit": None, # Không có thông tin
            #    "AtaTrasit": self._format_date(transit_arrival_events[0].get('date')) if transit_arrival_events else None,
            #}
            
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= tracking_number,
                BookingStatus= None,
                Pol= pol,
                Pod= pod,
                Etd= self._format_date(etd),
                Atd= self._format_date(actual_departure.get('date')),
                Eta= self._format_date(eta),
                Ata= self._format_date(actual_arrival.get('date')),
                TransitPort= ", ".join(transit_ports) if transit_ports else None,
                EtdTransit= None,
                AtdTransit= self._format_date(transit_departure_events[-1].get('date')) if transit_departure_events else None,
                EtaTransit= None,
                AtaTransit= self._format_date(transit_arrival_events[0].get('date')) if transit_arrival_events else None
            )
            return shipment_data

        except Exception as e:
            traceback.print_exc()
            return None

    def _extract_events_from_container(self, container_block):
        """Trích xuất tất cả sự kiện từ một khối container được cung cấp."""
        events = []
        try:
            event_table = container_block.find_element(By.CLASS_NAME, "m-table-group")
            rows = event_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 4:
                    events.append({
                        "date": cells[0].text.strip(),
                        "description": cells[3].text.strip(),
                        "location": cells[2].text.strip().replace('\n', ' ')
                    })
        except NoSuchElementException:
            pass # Bỏ qua nếu không tìm thấy bảng sự kiện
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        """
        Tìm một sự kiện cụ thể trong danh sách các sự kiện.
        Trả về sự kiện đầu tiên khớp hoặc dictionary rỗng.
        """
        if not location_keyword:
            return {}
        
        # Chuẩn hóa location_keyword, ví dụ: "IDJKT(JAKARTA)" -> "jakarta"
        match = re.search(r'\((.*?)\)', location_keyword)
        normalized_loc_keyword = match.group(1).strip().lower() if match else location_keyword.lower()

        for event in events:
            event_location = event.get("location", "").lower()
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = normalized_loc_keyword in event_location

            if desc_match and loc_match:
                return event
        return {}