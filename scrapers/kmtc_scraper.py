import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import traceback
import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

class KmtcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web eKMTC,
    và chuẩn hóa kết quả đầu ra theo template JSON yêu cầu.
    """

    def _format_kmtc_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY.MM.DD HH:mm' hoặc 'YYYY.MM.DD' sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Tách phần ngày ra khỏi chuỗi có cả thời gian
            date_part = date_str.split(' ')[0]
            # Chuyển đổi từ định dạng 'YYYY.MM.DD'
            dt_obj = datetime.strptime(date_part, '%Y.%m.%d')
            # Format lại thành 'DD/MM/YYYY'
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            print(f"[LOG] Không thể phân tích định dạng ngày: {date_str}")
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        print(f"[KMTC Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            print("[KMTC Scraper] 1. Điều hướng đến URL...")
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            print("[KMTC Scraper] 2. Điền thông tin vào form tìm kiếm...")
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "blNo")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.button.blue.sh")))
            self.driver.execute_script("arguments[0].click();", search_button)
            print("[KMTC Scraper] -> Đã nhấn nút tìm kiếm.")
            
            print("[KMTC Scraper] 3. Chờ trang kết quả tải...")
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.location_detail_box")))
            print(f"[KMTC Scraper] -> Trang kết quả cho '{tracking_number}' đã tải xong.")
            time.sleep(1) 

            print("[KMTC Scraper] 4. Bắt đầu trích xuất và chuẩn hóa dữ liệu...")
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                print(f"[KMTC Scraper] Lỗi: Không thể trích xuất dữ liệu cho '{tracking_number}'.")
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[KMTC Scraper] 5. Trả về kết quả thành công.")
            return normalized_data, None

        except TimeoutException:
            print(f"[KMTC Scraper] Lỗi: TimeoutException xảy ra cho mã '{tracking_number}'.")
            try:
                if self.driver.find_element(By.ID, "e-alert-message").is_displayed():
                    print(f"[KMTC Scraper] -> Phát hiện thông báo 'No Data'.")
                    return None, f"Không tìm thấy dữ liệu (No Data) cho mã '{tracking_number}' trên trang eKMTC."
            except NoSuchElementException:
                print(f"[KMTC Scraper] -> Không tìm thấy kết quả, có thể mã không hợp lệ hoặc trang web chậm.")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = f"output/kmtc_timeout_{tracking_number}_{timestamp}.png"
                try:
                    self.driver.save_screenshot(screenshot_path)
                    print(f"  -> Đã lưu ảnh chụp màn hình vào {screenshot_path}")
                except Exception as ss_e:
                    print(f"  -> Lỗi: Không thể lưu ảnh chụp màn hình: {ss_e}")
                return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            print(f"[KMTC Scraper] Lỗi: Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ trang kết quả của eKMTC sang template JSON.
        """
        print("[KMTC Scraper] --- Bắt đầu _extract_and_normalize_data ---")
        try:
            # --- 1. Trích xuất thông tin chung từ bảng tóm tắt ---
            summary_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl_col tbody")))
            
            bl_no_cell = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:first-child")
            bl_number = bl_no_cell.text.split('\n')[0].strip()
            booking_status = bl_no_cell.find_element(By.TAG_NAME, "span").text.strip()
            booking_no = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:nth-child(2)").text.strip()
            
            pol_raw = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:nth-child(6)").text.strip()
            pod_raw = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:nth-child(7)").text.strip()

            pol = pol_raw.split('\n')[0]
            etd = pol_raw.split('\n')[1] if '\n' in pol_raw else None
            pod = pod_raw.split('(')[0].strip()
            eta = pod_raw.split('\n')[1] if '\n' in pod_raw else None
            
            print(f"[KMTC Scraper] -> Thông tin tóm tắt: POL='{pol}', POD='{pod}', ETD='{etd}', ETA='{eta}'")

            # --- 2. Trích xuất lịch sử từ biểu đồ tiến trình ---
            # Giả định dữ liệu từ container đầu tiên là đại diện cho lô hàng
            container_links = self.driver.find_elements(By.CSS_SELECTOR, ".cntrNo_area")
            if container_links and container_links[0].tag_name == 'a':
                container_no = container_links[0].text
                print(f"  -> Container '{container_no}' là một link, thực hiện click để cập nhật timeline...")
                self.driver.execute_script("arguments[0].click();", container_links[0])
                WebDriverWait(self.driver, 10).until(
                    EC.text_to_be_present_in_element((By.CSS_SELECTOR, ".location_detail_header .ship_num"), container_no)
                )
                time.sleep(0.5)

            events = self._extract_events_from_timeline()
            
            # --- 3. Tìm các ngày thực tế và cảng trung chuyển từ lịch sử ---
            departure_event = self._find_event(events, "Loading", pol)
            arrival_event = self._find_event(events, "Discharging", pod)
            
            transhipment_event = self._find_event(events, "Transhipment")
            transit_port = transhipment_event.get('location') if transhipment_event else None
            ata_transit = transhipment_event.get('date') if transhipment_event else None
            # ATD tại cảng transit khó xác định vì không có sự kiện tương ứng rõ ràng
            atd_transit = None

            # --- 4. Xây dựng đối tượng JSON cuối cùng ---
            #normalized_data = {
            #    "BookingNo": booking_no or tracking_number,
            #    "BlNumber": bl_number or tracking_number,
            #    "BookingStatus": booking_status,
            #    "Pol": pol,
            #    "Pod": pod,
            #    "Etd": self._format_kmtc_date(etd),
            #    "Atd": self._format_kmtc_date(departure_event.get('date')),
            #    "Eta": self._format_kmtc_date(eta),
            #    "Ata": self._format_kmtc_date(arrival_event.get('date')),
            #    "TransitPort": transit_port,
            #    "EtdTransit": None,
            #    "AtdTrasit": self._format_kmtc_date(atd_transit),
            #    "EtaTransit": None,
            #    "AtaTrasit": self._format_kmtc_date(ata_transit),
            #}
            
            normalized_data = N8nTrackingInfo(
                BookingNo= booking_no or tracking_number,
                BlNumber= bl_number or tracking_number,
                BookingStatus= booking_status,
                Pol= pol,
                Pod= pod,
                Etd= self._format_kmtc_date(etd),
                Atd= self._format_kmtc_date(departure_event.get('date')),
                Eta= self._format_kmtc_date(eta),
                Ata= self._format_kmtc_date(arrival_event.get('date')),
                TransitPort= transit_port,
                EtdTransit= None,
                AtdTransit= self._format_kmtc_date(atd_transit),
                EtaTransit= None,
                AtaTransit= self._format_kmtc_date(ata_transit)
            )
            
            print("[KMTC Scraper] --- Hoàn tất, đã chuẩn hóa dữ liệu ---")
            return normalized_data

        except Exception as e:
            print(f"[KMTC Scraper] Lỗi trong quá trình trích xuất: {e}")
            traceback.print_exc()
            return None


    def _extract_events_from_timeline(self):
        """
        Trích xuất tất cả các sự kiện từ biểu đồ tiến trình 'Current Location'.
        (Giữ nguyên logic từ code cũ vì nó đã hoạt động tốt)
        """
        events = []
        try:
            timeline = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.location_detail")))
            all_event_items = timeline.find_elements(By.TAG_NAME, "li")
            
            for item in all_event_items:
                item_class = item.get_attribute("class")
                if "inactive" in item_class or not item.is_displayed():
                    continue
                
                sub_event = item.find_element(By.CSS_SELECTOR, ".ts_scroll div")
                p_tags = sub_event.find_elements(By.TAG_NAME, "p")
                if len(p_tags) < 2: continue

                description = p_tags[0].text.replace('\n', ' ').strip()
                datetime_raw = p_tags[1].text.replace('\n', ' ').strip()
                main_event_text = item.find_element(By.CSS_SELECTOR, ".txt").text.lower()
                location = None

                if 'on board' in main_event_text:
                    location = self.driver.find_element(By.CSS_SELECTOR, "table.tbl_col tbody tr:first-child td:nth-child(6)").text.split('\n')[0].strip()
                elif 'discharging' in main_event_text:
                    location = self.driver.find_element(By.CSS_SELECTOR, "table.tbl_col tbody tr:first-child td:nth-child(7)").text.split('(')[0].strip()
                elif '(transhipped)' in main_event_text:
                    description = "Transhipment"
                    # Location được lấy từ chính text của event
                    location_text = p_tags[0].text
                    location = location_text.replace('T/S', '').replace('\n', ' ').strip()

                events.append({
                    "date": datetime_raw,
                    "description": description,
                    "location": location
                })
        except (NoSuchElementException, IndexError) as e:
             print(f"  -> Lỗi khi xử lý timeline event: {e}")
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """
        Tìm một sự kiện cụ thể trong danh sách.
        (Giữ nguyên logic từ code cũ)
        """
        if not events: return {}
        
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            
            if location_keyword:
                loc_match = location_keyword.lower() in (event.get("location") or "").lower()
                if desc_match and loc_match:
                    return event
            elif desc_match:
                return event
        return {}