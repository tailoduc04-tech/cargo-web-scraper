import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper

class PilScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web PIL (Pacific International Lines)
    và chuẩn hóa kết quả đầu ra theo định dạng JSON yêu cầu.
    """

    def _parse_date(self, date_str):
        """Hàm trợ giúp để chuyển đổi chuỗi ngày tháng thành đối tượng datetime."""
        if not date_str:
            return None
        try:
            # Thử định dạng 'DD-Mon-YYYY', ví dụ: '03-Sep-2025'
            return datetime.strptime(date_str, '%d-%b-%Y')
        except ValueError:
            # Thử định dạng khác có cả thời gian
            try:
                return datetime.strptime(date_str, '%d-%b-%Y %H:%M:%S')
            except ValueError:
                print(f"[LOG] Không thể phân tích định dạng ngày: {date_str}")
                return None

    def scrape(self, tracking_number):
        print(f"\n--- [PIL Scraper] Bắt đầu scrape cho mã: {tracking_number} ---")
        try:
            url = self.config['url'].replace('<BL_NUMBER>', tracking_number)
            print(f"[LOG] Đang truy cập URL: {url}")
            self.driver.get(url)
            self.wait = WebDriverWait(self.driver, 30)

            print("[LOG] Chờ trang kết quả tải...")
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".results-wrapper .mypil-table")))
            print("[LOG] Trang kết quả đã tải thành công.")
            time.sleep(2)

            # --- 1. Trích xuất thông tin chung ---
            print("[LOG] Bắt đầu trích xuất thông tin tóm tắt...")
            summary_data = self._extract_summary_data()
            pol = summary_data.get("POL")
            pod = summary_data.get("POD")
            print(f"[LOG] Thông tin tóm tắt: POL={pol}, POD={pod}, ETD={summary_data.get('ETD')}, ETA={summary_data.get('ETA')}")
            
            # --- 2. Mở rộng tất cả chi tiết container ---
            main_container_tbodys = self.driver.find_elements(By.XPATH, "//tbody[not(contains(@class, 'sub-info-table')) and .//b[@class='cont-numb']]")
            print(f"[LOG] Tìm thấy {len(main_container_tbodys)} khối thông tin container.")
            
            for i, main_tbody in enumerate(main_container_tbodys):
                try:
                    button = main_tbody.find_element(By.CSS_SELECTOR, "a.trackinfo")
                    self.driver.execute_script("arguments[0].click();", button)
                    print(f"[LOG] Đã nhấp vào nút Trace #{i+1}.")
                    # Chờ cho bảng lịch sử bên trong xuất hiện
                    history_tbody = main_tbody.find_element(By.XPATH, "./following-sibling::tbody[1]")
                    WebDriverWait(self.driver, 10).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, "tbody.sub-info-table"))
                    )
                except Exception as e:
                    print(f"[LỖI] Không thể nhấp và chờ nút trace: {e}")

            # --- 3. Thu thập lịch sử từ tất cả các container ---
            print("[LOG] Bắt đầu thu thập lịch sử chi tiết của các container...")
            all_events = []
            
            # Lấy lại danh sách container sau khi đã mở rộng tất cả
            main_container_tbodys = self.driver.find_elements(By.XPATH, "//tbody[not(contains(@class, 'sub-info-table')) and .//b[@class='cont-numb']]")
            
            for main_tbody in main_container_tbodys:
                try:
                    container_no = main_tbody.find_element(By.CLASS_NAME, "cont-numb").text
                    print(f"[LOG] Đang xử lý container: {container_no}")
                    history_tbody = main_tbody.find_element(By.XPATH, "./following-sibling::tbody[1]")
                    assert history_tbody is not None, "history_tbody is None"
                    events = self._extract_container_events(history_tbody, container_no)
                    print(f"[LOG]   -> Tìm thấy {len(events)} sự kiện cho container {container_no}.")
                    all_events.extend(events)
                except NoSuchElementException:
                    print(f"[CẢNH BÁO] Bỏ qua một tbody không có cấu trúc container mong đợi.")
                    continue
            
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

            # --- 5. Chuẩn hóa dữ liệu theo định dạng yêu cầu ---
            print("[LOG] Bắt đầu chuẩn hóa dữ liệu đầu ra...")
            departure_date_obj = self._parse_date(summary_data.get("ETD"))
            arrival_date_obj = self._parse_date(summary_data.get("ETA"))
            now = datetime.now()

            ngay_di_du_kien = None
            ngay_di_thuc_te = actual_departure_event.get('date') if actual_departure_event else None

            if departure_date_obj:
                if departure_date_obj < now and not ngay_di_thuc_te:
                    ngay_di_thuc_te = summary_data.get("ETD")
                elif not ngay_di_thuc_te:
                    ngay_di_du_kien = summary_data.get("ETD")

            ngay_den_du_kien = None
            ngay_den_thuc_te = actual_arrival_event.get('date') if actual_arrival_event else None

            if arrival_date_obj:
                if arrival_date_obj < now and not ngay_den_thuc_te:
                    ngay_den_thuc_te = summary_data.get("ETA")
                elif not ngay_den_thuc_te:
                    ngay_den_du_kien = summary_data.get("ETA")

            normalized_data = {
                "POL": pol,
                "POD": pod,
                "transit_port": ", ".join(transit_ports) if transit_ports else None,
                "ngay_tau_di": {
                    "ngay_du_kien": ngay_di_du_kien,
                    "ngay_thuc_te": ngay_di_thuc_te
                },
                "ngay_tau_den": {
                    "ngay_du_kien": ngay_den_du_kien,
                    "ngay_thuc_te": ngay_den_thuc_te
                },
                "lich_su": all_events
            }
            print("[LOG] Đã chuẩn hóa dữ liệu thành công.")
            
            results_df = pd.DataFrame([normalized_data])
            results = {"tracking_info": results_df}
            
            print(f"--- [PIL Scraper] Hoàn thành scrape cho mã: {tracking_number} ---")
            return results, None

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
            location_text = summary_table.find_element(By.CSS_SELECTOR, "td.location").text
            lines = [line.strip() for line in location_text.split('\n') if line.strip()]
            data['POL'] = lines[1].split(',')[0] if len(lines) > 1 else None

            next_location_text = summary_table.find_element(By.CSS_SELECTOR, "td.next-location").text
            lines = [line.strip() for line in next_location_text.split('\n') if line.strip()]
            data['POD'] = lines[0].split(',')[0] if len(lines) > 0 else None
            data['ETA'] = lines[1] if len(lines) > 1 else None

            arrival_delivery_text = summary_table.find_element(By.CSS_SELECTOR, "td.arrival-delivery").text
            lines = [line.strip() for line in arrival_delivery_text.split('\n') if line.strip()]
            data['ETD'] = lines[1] if len(lines) > 1 else None
        except (NoSuchElementException, IndexError) as e:
            print(f"[LỖI] Không thể trích xuất dữ liệu tóm tắt: {e}")
        return data

    def _extract_container_events(self, history_tbody, container_no):
        """Trích xuất lịch sử sự kiện cho một container cụ thể."""
        events = []
        try:
            # Sửa selector: Tìm tất cả các thẻ <tr> bên trong history_tbody
            rows = history_tbody.find_elements(By.TAG_NAME, "tr")
            
            for row in rows:
                # Bỏ qua dòng tiêu đề, dòng này thường có class 'text-fw-bold'
                if "text-fw-bold" in row.get_attribute("class"):
                    continue
                    
                cells = row.find_elements(By.TAG_NAME, "td")
                # Dòng dữ liệu sự kiện có 6 cột, cột đầu tiên trống
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