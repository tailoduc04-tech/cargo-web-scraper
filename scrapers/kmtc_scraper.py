import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import traceback
import time

from .base_scraper import BaseScraper

class KmtcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web eKMTC,
    tập trung vào việc trích xuất dữ liệu từ bảng tóm tắt và biểu đồ tiến trình.
    """

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
            normalized_data = self._extract_and_normalize_data()

            if not normalized_data:
                print(f"[KMTC Scraper] Lỗi: Không thể trích xuất dữ liệu cho '{tracking_number}'.")
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[KMTC Scraper] 5. Đóng gói và trả về kết quả thành công.")
            results_df = pd.DataFrame(normalized_data)
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            print(f"[KMTC Scraper] Lỗi: TimeoutException xảy ra cho mã '{tracking_number}'.")
            try:
                # Kiểm tra xem có thông báo lỗi "No Data" hay không
                if self.driver.find_element(By.ID, "e-alert-message").is_displayed():
                    print(f"[KMTC Scraper] -> Phát hiện thông báo 'No Data'.")
                    return None, f"Không tìm thấy dữ liệu (No Data) cho mã '{tracking_number}' trên trang eKMTC."
            except NoSuchElementException:
                # Nếu không có thông báo lỗi, đây là timeout thật
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

    def _extract_and_normalize_data(self):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ trang kết quả của eKMTC.
        """
        print("[KMTC Scraper] --- Bắt đầu _extract_and_normalize_data ---")
        # 1. Trích xuất thông tin chung từ bảng tóm tắt
        summary_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl_col")))
        pol_raw = summary_table.find_element(By.CSS_SELECTOR, "tbody tr:first-child td:nth-child(6)").text
        pod_raw = summary_table.find_element(By.CSS_SELECTOR, "tbody tr:first-child td:nth-child(7)").text
        
        pol = pol_raw.split('\n')[0].strip()
        pod = pod_raw.split('(')[0].strip()
        print(f"[KMTC Scraper] -> Đã trích xuất POL: '{pol}', POD: '{pod}'")

        # 2. Lấy danh sách các container
        container_links = self.driver.find_elements(By.CSS_SELECTOR, ".cntrNo_area")
        print(f"[KMTC Scraper] -> Tìm thấy {len(container_links)} container trong bảng tóm tắt.")
        all_shipments = []

        for i in range(len(container_links)):
            # Phải tìm lại element mỗi lần lặp để tránh StaleElementReferenceException
            current_container_link = self.driver.find_elements(By.CSS_SELECTOR, ".cntrNo_area")[i]
            container_no = current_container_link.text
            print(f"\n[KMTC Scraper] -> Bắt đầu xử lý container #{i+1}: '{container_no}'")
            
            if current_container_link.tag_name == 'a':
                print(f"  -> Container '{container_no}' là một link, thực hiện click để cập nhật timeline...")
                self.driver.execute_script("arguments[0].click();", current_container_link)
                WebDriverWait(self.driver, 10).until(
                    EC.text_to_be_present_in_element((By.CSS_SELECTOR, ".location_detail_header .ship_num"), container_no)
                )
                print(f"  -> Timeline đã được cập nhật cho '{container_no}'.")
                time.sleep(0.5)
            else:
                 print(f"  -> Container '{container_no}' không phải link, timeline mặc định đã đúng.")

            # 3. Trích xuất lịch sử từ biểu đồ tiến trình
            events = self._extract_events_from_timeline()
            
            # 4. Tìm các ngày và thông tin quan trọng
            print(f"  -> Bắt đầu tìm kiếm các sự kiện quan trọng trong {len(events)} event đã trích xuất.")
            departure_event = self._find_event(events, "Loading", pol)
            arrival_event = self._find_event(events, "Discharging", pod)
            transhipment_event = self._find_event(events, "Transhipment")
            transit_port = transhipment_event.get('location') if transhipment_event else None

            # 5. Xây dựng đối tượng JSON chuẩn hóa
            shipment_data = {
                "container_no": container_no,
                "POL": pol,
                "POD": pod,
                "transit_port": transit_port,
                "ngay_tau_di": {
                    "ngay_du_kien": None,
                    "ngay_thuc_te": departure_event.get('date')
                },
                "ngay_tau_den": {
                    "ngay_du_kien": None,
                    "ngay_thuc_te": arrival_event.get('date')
                },
                "lich_su": events
            }
            all_shipments.append(shipment_data)
            print(f"[KMTC Scraper] -> Hoàn tất xử lý container '{container_no}'.")
            
        return all_shipments

    def _extract_events_from_timeline(self):
        """
        Trích xuất tất cả các sự kiện từ biểu đồ tiến trình 'Current Location'.
        """
        print("[KMTC Scraper] --- Bắt đầu _extract_events_from_timeline ---")
        events = []
        timeline = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.location_detail")))
        all_event_items = timeline.find_elements(By.TAG_NAME, "li")
        print(f"  -> Tìm thấy tổng cộng {len(all_event_items)} mục <li> trong timeline.")
        
        for index, item in enumerate(all_event_items):
            try:
                item_class = item.get_attribute("class")
                if "inactive" in item_class or not item.is_displayed():
                    # print(f"  -> Bỏ qua mục #{index+1} vì inactive hoặc không hiển thị.")
                    continue
                
                print(f"  -> Đang xử lý mục event #{index+1}...")
                sub_event = item.find_element(By.CSS_SELECTOR, ".ts_scroll div")
                print(f"  -> Lấy được sub event")
                p_tags = sub_event.find_elements(By.TAG_NAME, "p")
                if len(p_tags) < 2:
                    continue

                description = p_tags[0].text.replace('\n', ' ').strip()
                print(f"  -> Lấy được description")
                datetime_raw = p_tags[1].text.replace('\n', ' ').strip()
                print(f"  -> Lấy được datetime")
                
                main_event_text = item.find_element(By.CSS_SELECTOR, ".txt").text.lower()
                print(f"  -> Lấy được main_event_text")
                location = None

                if 'on board' in main_event_text:
                    location = self.driver.find_element(By.CSS_SELECTOR, "table.tbl_col tbody tr:first-child td:nth-child(6)").text.split('\n')[0].strip()
                elif 'discharging' in main_event_text:
                    location = self.driver.find_element(By.CSS_SELECTOR, "table.tbl_col tbody tr:first-child td:nth-child(7)").text.split('(')[0].strip()
                elif '(transhipped)' in main_event_text:
                    description = "Transhipment"
                    location = sub_event.find_element(By.TAG_NAME, "p:first-child").text.replace('T/S', '').replace('\n', ' ').strip()

                event_data = {
                    "date": datetime_raw,
                    "type": "ngay_thuc_te",
                    "description": description,
                    "location": location
                }
                events.append(event_data)
                print(f"    -> Đã trích xuất: {event_data}")

            except (NoSuchElementException, IndexError):
                print(f"  -> Lỗi: Không thể xử lý mục event #{index+1} do cấu trúc HTML khác biệt.")
                continue
        print(f"[KMTC Scraper] --- Hoàn tất _extract_events_from_timeline, trích xuất được {len(events)} events ---")
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """Tìm một sự kiện cụ thể trong danh sách."""
        # print(f"  -> Tìm kiếm event với từ khóa '{description_keyword}' và địa điểm '{location_keyword}'...")
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            
            if location_keyword:
                loc_match = location_keyword.lower() in (event.get("location") or "").lower()
                if desc_match and loc_match:
                    # print(f"    -> Tìm thấy event khớp: {event}")
                    return event
            elif desc_match:
                # print(f"    -> Tìm thấy event khớp: {event}")
                return event
        # print("    -> Không tìm thấy event nào khớp.")
        return {}