import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

class PilScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web PIL (Pacific International Lines)
    và chuẩn hóa kết quả đầu ra theo định dạng JSON yêu cầu.
    """

    def _format_pil_date(self, date_str):
        """
        Hàm trợ giúp để chuyển đổi chuỗi ngày tháng từ 'DD-Mon-YYYY' sang 'DD/MM/YYYY'.
        """
        if not date_str:
            return None
        try:
            # Tách phần ngày ra khỏi chuỗi có cả thời gian
            date_part = date_str.split(" ")[0]
            # Chuyển đổi từ định dạng 'DD-Mon-YYYY'
            dt_obj = datetime.strptime(date_part, '%d-%b-%Y')
            # Format lại thành 'DD/MM/YYYY'
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            print(f"[LOG] Không thể phân tích định dạng ngày: {date_str}")
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã B/L hoặc Booking từ trang web PIL.
        """
        print(f"\n--- [PIL Scraper] Bắt đầu scrape cho mã: {tracking_number} ---")
        try:
            # Thay thế placeholder bằng tracking_number thực tế
            url = self.config['url'].replace('<BL_NUMBER>', tracking_number)
            print(f"[LOG] Đang truy cập URL: {url}")
            self.driver.get(url)
            self.wait = WebDriverWait(self.driver, 45)

            print("[LOG] Chờ trang kết quả tải...")
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".results-wrapper .mypil-table")))
            print("[LOG] Trang kết quả đã tải thành công.")
            time.sleep(2) # Chờ để đảm bảo tất cả các element được render ổn định

            # --- 1. Trích xuất thông tin tóm tắt ---
            print("[LOG] Bắt đầu trích xuất thông tin tóm tắt...")
            summary_data = self._extract_summary_data()
            pol = summary_data.get("POL")
            pod = summary_data.get("POD")
            print(f"[LOG] Thông tin tóm tắt: POL={pol}, POD={pod}, ETD={summary_data.get('ETD')}, ETA={summary_data.get('ETA')}")

            # --- 2. Mở rộng tất cả chi tiết container ---
            self._expand_all_container_details()

            # --- 3. Thu thập lịch sử từ tất cả các container ---
            print("[LOG] Bắt đầu thu thập lịch sử chi tiết của các container...")
            all_events = self._gather_all_container_events()
            print(f"[LOG] Tổng cộng đã thu thập được {len(all_events)} sự kiện.")

            # --- 4. Xác định ngày thực tế và cảng trung chuyển từ lịch sử ---
            print("[LOG] Bắt đầu xác định các ngày quan trọng và cảng trung chuyển...")
            actual_departure_event = self._find_event(all_events, "Vessel Loading", pol)
            actual_arrival_event = self._find_event(all_events, "Vessel Discharge", pod)
            
            transit_ports = []
            for event in all_events:
                location = event.get('location', '')
                description = event.get('description', '').lower()
                if "discharge" in description and location and pol not in location and pod not in location:
                    if location not in transit_ports:
                        transit_ports.append(location)
            
            print(f"[LOG] Cảng trung chuyển được xác định: {transit_ports}")
            
            # Tìm sự kiện tại cảng trung chuyển
            ata_transit_event = None
            atd_transit_event = None
            if transit_ports:
                # Lấy ngày đến thực tế tại cảng trung chuyển đầu tiên
                ata_transit_event = self._find_event(all_events, "Vessel Discharge", transit_ports[0])
                # Lấy ngày đi thực tế từ cảng trung chuyển cuối cùng
                atd_transit_event = self._find_event(all_events, "Vessel Loading", transit_ports[-1])


            # --- 5. Chuẩn hóa dữ liệu theo định dạng JSON yêu cầu ---
            print("[LOG] Bắt đầu chuẩn hóa dữ liệu đầu ra...")
            #normalized_data = {
            #    "BookingNo": tracking_number,
            #    "BlNumber": tracking_number,
            #    "BookingStatus": None, # Không có thông tin này trên trang
            #    "Pol": pol,
            #    "Pod": pod,
            #    "Etd": self._format_pil_date(summary_data.get("ETD")),
            #    "Atd": self._format_pil_date(actual_departure_event.get("date")) if actual_departure_event else None,
            #    "Eta": self._format_pil_date(summary_data.get("ETA")),
            #    "Ata": self._format_pil_date(actual_arrival_event.get("date")) if actual_arrival_event else None,
            #    "TransitPort": ", ".join(transit_ports) if transit_ports else None,
            #    "EtdTransit": None, # Không có thông tin
            #    "AtdTrasit": self._format_pil_date(atd_transit_event.get("date")) if atd_transit_event else None,
            #    "EtaTransit": None, # Không có thông tin
            #    "AtaTrasit": self._format_pil_date(ata_transit_event.get("date")) if ata_transit_event else None,
            #}
            
            normalized_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= tracking_number,
                BookingStatus= None,
                Pol= pol,
                Pod= pod,
                Etd= self._format_pil_date(summary_data.get("ETD")),
                Atd= self._format_pil_date(actual_departure_event.get("date")) if actual_departure_event else None,
                Eta= self._format_pil_date(summary_data.get("ETA")),
                Ata= self._format_pil_date(actual_arrival_event.get("date")) if actual_arrival_event else None,
                TransitPort= ", ".join(transit_ports) if transit_ports else None,
                EtdTransit= None,
                AtdTransit= self._format_pil_date(atd_transit_event.get("date")) if atd_transit_event else None,
                EtaTransit= None,
                AtaTransit= self._format_pil_date(ata_transit_event.get("date")) if ata_transit_event else None,
            )
            print("[LOG] Đã chuẩn hóa dữ liệu thành công.")

            return normalized_data, None

        except TimeoutException:
            print(f"[LỖI] TimeoutException xảy ra cho mã '{tracking_number}'.")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/pil_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"[LOG] Đã lưu ảnh chụp màn hình lỗi vào {screenshot_path}")
            except Exception as ss_e:
                print(f"[LỖI] Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            print(f"[LỖI] Lỗi không mong muốn xảy ra cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_summary_data(self):
        """Trích xuất dữ liệu từ bảng tóm tắt chính."""
        summary_table = self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[@class='mypil-table']/table/tbody")))
        data = {}
        try:
            # Lấy POL
            location_text = summary_table.find_element(By.CSS_SELECTOR, "td.location").text
            lines = [line.strip() for line in location_text.split('\n') if line.strip()]
            data['POL'] = lines[1].split(',')[0] if len(lines) > 1 else None

            # Lấy POD và ETA
            next_location_text = summary_table.find_element(By.CSS_SELECTOR, "td.next-location").text
            lines = [line.strip() for line in next_location_text.split('\n') if line.strip()]
            data['POD'] = lines[0].split(',')[0] if len(lines) > 0 else None
            data['ETA'] = lines[1] if len(lines) > 1 else None

            # Lấy ETD
            arrival_delivery_text = summary_table.find_element(By.CSS_SELECTOR, "td.arrival-delivery").text
            lines = [line.strip() for line in arrival_delivery_text.split('\n') if line.strip()]
            data['ETD'] = lines[1] if len(lines) > 1 else None
        except (NoSuchElementException, IndexError) as e:
            print(f"[LỖI] Không thể trích xuất dữ liệu tóm tắt: {e}")
        return data

    def _expand_all_container_details(self):
        """Tìm và nhấp vào tất cả các nút 'Trace' để hiển thị lịch sử chi tiết."""
        print("[LOG] Bắt đầu mở rộng tất cả chi tiết container...")
        main_container_tbodys = self.driver.find_elements(By.XPATH, "//tbody[not(contains(@class, 'sub-info-table')) and .//b[@class='cont-numb']]")
        print(f"[LOG] Tìm thấy {len(main_container_tbodys)} khối thông tin container.")
        
        for i, main_tbody in enumerate(main_container_tbodys):
            try:
                button = main_tbody.find_element(By.CSS_SELECTOR, "a.trackinfo")
                self.driver.execute_script("arguments[0].click();", button)
                print(f"[LOG] Đã nhấp vào nút Trace #{i+1}.")
                # Chờ cho bảng lịch sử bên trong (là tbody ngay sau tbody hiện tại) xuất hiện
                history_tbody = main_tbody.find_element(By.XPATH, "./following-sibling::tbody[1]")
                WebDriverWait(self.driver, 15).until(
                    lambda d: 'hidden' not in history_tbody.get_attribute('class')
                )
            except Exception as e:
                print(f"[LỖI] Không thể nhấp và chờ nút trace #{i+1}: {e}")
        print("[LOG] Đã mở rộng tất cả các chi tiết container.")


    def _gather_all_container_events(self):
        """Thu thập sự kiện từ tất cả các container đã được mở rộng."""
        all_events = []
        main_container_tbodys = self.driver.find_elements(By.XPATH, "//tbody[not(contains(@class, 'sub-info-table')) and .//b[@class='cont-numb']]")
        
        for main_tbody in main_container_tbodys:
            try:
                container_no = main_tbody.find_element(By.CLASS_NAME, "cont-numb").text
                print(f"[LOG] Đang xử lý container: {container_no}")
                # Bảng lịch sử là tbody ngay sau tbody chính
                history_tbody = main_tbody.find_element(By.XPATH, "./following-sibling::tbody[1]")
                events = self._extract_container_events(history_tbody, container_no)
                print(f"[LOG]   -> Tìm thấy {len(events)} sự kiện cho container {container_no}.")
                all_events.extend(events)
            except NoSuchElementException:
                print(f"[CẢNH BÁO] Bỏ qua một tbody không có cấu trúc container mong đợi.")
                continue
        return all_events


    def _extract_container_events(self, history_tbody, container_no):
        """Trích xuất lịch sử sự kiện cho một container cụ thể từ tbody của nó."""
        events = []
        try:
            rows = history_tbody.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                if "text-fw-bold" in row.get_attribute("class"):
                    continue
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 6:
                    event_date_time = cells[3].text.strip().split(" ")[0]
                    event_name = cells[4].text.strip()
                    event_location = cells[5].text.strip().split(',')[0]
                    events.append({
                        "container_no": container_no,
                        "date": event_date_time,
                        "description": event_name,
                        "location": event_location
                    })
        except NoSuchElementException:
            print(f"[LỖI] Không tìm thấy bảng sự kiện cho container {container_no}.")
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """Tìm một sự kiện cụ thể trong danh sách các sự kiện."""
        if not events or not location_keyword:
            return {}
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            event_location = event.get("location") or ""
            loc_match = location_keyword.lower() in event_location.lower()

            if desc_match and loc_match:
                print(f"[LOG] Tìm thấy sự kiện '{description_keyword}' tại '{location_keyword}': {event}")
                return event
        print(f"[LOG] Không tìm thấy sự kiện '{description_keyword}' tại '{location_keyword}'.")
        return {}