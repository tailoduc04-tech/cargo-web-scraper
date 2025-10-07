import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper

class GoldstarScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Gold Star Line và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # 1. Xử lý cookie nếu có (ĐÃ SỬA LỖI)
            try:
                # Chờ cho nút cookie có thể được nhấp vào trong tối đa 10 giây
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "rcc-confirm-button"))
                )
                # Sử dụng JavaScript click để tăng độ tin cậy
                self.driver.execute_script("arguments[0].click();", cookie_button)
                print("Đã chấp nhận cookies.")
            except Exception as e:
                # Bỏ qua mọi lỗi (Timeout, StaleElement, etc.) liên quan đến cookie
                print(f"Không thể xử lý banner cookie hoặc banner không xuất hiện: {e}")

            # 2. Nhập mã và tìm kiếm
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "containerid")))
            # Sử dụng JavaScript để điền vào ô input
            self.driver.execute_script("arguments[0].value = arguments[1];", search_input, tracking_number)
            time.sleep(1)
            
            # Sử dụng JavaScript click để tránh các vấn đề về element overlapping
            search_button = self.wait.until(EC.element_to_be_clickable((By.ID, "submitDetails")))
            self.driver.execute_script("arguments[0].click();", search_button)
            print(f"Đang tìm kiếm cho mã: {tracking_number}")

            # 3. Đợi kết quả tải và mở rộng tất cả chi tiết container
            self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "trackShipmentResultCard")))
            print("Trang kết quả đã tải.")
            time.sleep(2) # Đợi một chút để đảm bảo tất cả các element sẵn sàng

            # Nhấn vào tất cả các nút mũi tên để mở rộng chi tiết
            expand_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".accordion-item .arrowButton")
            print(f"Tìm thấy {len(expand_buttons)} container để mở rộng.")
            for button in expand_buttons:
                try:
                    # Cuộn tới nút và click bằng JS để đảm bảo độ tin cậy
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(0.5)
                    self.driver.execute_script("arguments[0].click();", button)
                    print("Đã nhấn nút mở rộng chi tiết container.")
                    time.sleep(1) # Đợi accordion mở ra
                except Exception as e:
                    print(f"Không thể nhấn nút mở rộng container: {e}")

            # 4. Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data()
            
            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            results_df = pd.DataFrame(normalized_data)
            results = {"tracking_info": results_df}
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/goldstar_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout xảy ra. Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            print(f"Một lỗi không mong muốn đã xảy ra với mã '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Một lỗi đã xảy ra khi xử lý mã '{tracking_number}': {e}"

    def _extract_and_normalize_data(self):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ trang kết quả.
        """
        try:
            # 1. Trích xuất thông tin tóm tắt chung
            summary_card = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".trackShipmentResultRowInner")))
            pol = summary_card.find_element(By.XPATH, ".//div[label='Port of Loading (POL)']/h3").text
            pod = summary_card.find_element(By.XPATH, ".//div[label='Port of Discharge (POD)']/h3").text
            sailing_date = summary_card.find_element(By.XPATH, ".//div[label='Sailing Date']/h3").text
            eta = summary_card.find_element(By.XPATH, ".//div[label='ETD']/h3").text 

            all_shipments = []
            
            # 2. Lặp qua từng khối container đã được mở rộng
            container_items = self.driver.find_elements(By.CSS_SELECTOR, ".accordion-item")

            for item in container_items:
                # Trích xuất lịch sử cho từng container
                events = self._extract_events_from_container(item)

                # Tìm các ngày quan trọng từ lịch sử
                actual_departure = self._find_event(events, "Container was loaded at Port of Loading", pol)
                actual_arrival = self._find_event(events, "Container was discharged at Port of Destination", pod)
                
                # Suy luận cảng trung chuyển
                transit_ports = []
                for event in events:
                    location = event.get('location', '')
                    description = event.get('description', '').lower()
                    if location and pol not in location and pod not in location:
                        if "discharged" in description or "loaded" in description:
                            if location not in transit_ports:
                                transit_ports.append(location)

                # 3. Xây dựng đối tượng JSON chuẩn hóa
                shipment_data = {
                    "POL": pol,
                    "POD": pod,
                    "transit_port": ", ".join(transit_ports) if transit_ports else None,
                    "ngay_tau_di": {
                        "ngay_du_kien": sailing_date,
                        "ngay_thuc_te": actual_departure.get('date') if actual_departure else None
                    },
                    "ngay_tau_den": {
                        "ngay_du_kien": eta,
                        "ngay_thuc_te": actual_arrival.get('date') if actual_arrival else None
                    },
                    "lich_su": events
                }
                all_shipments.append(shipment_data)
            
            return all_shipments
        except Exception as e:
            print(f"Lỗi khi trích xuất và chuẩn hóa dữ liệu: {e}")
            traceback.print_exc()
            return []

    def _extract_events_from_container(self, container_item):
        """Trích xuất tất cả các sự kiện từ một khối container."""
        events = []
        try:
            history_rows = container_item.find_elements(By.CSS_SELECTOR, ".accordion-body .grid-container")
            for row in history_rows:
                try:
                    description = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][2]").text.replace('Last Activity\n', '').strip()
                    location = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][3]").text.replace('Location\n', '').strip()
                    date = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][4]").text.replace('Date\n', '').strip()

                    events.append({
                        "date": date,
                        "type": "ngay_thuc_te",
                        "description": description,
                        "location": location
                    })
                except NoSuchElementException:
                    continue # Bỏ qua nếu một dòng không có đủ thông tin
        except NoSuchElementException:
            print("Không tìm thấy bảng lịch sử cho một container.")
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        """Tìm một sự kiện cụ thể trong danh sách các sự kiện."""
        if not location_keyword:
            return {}
        
        normalized_loc_keyword = location_keyword.lower()

        for event in events:
            event_location = event.get("location", "").lower()
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            
            if desc_match and normalized_loc_keyword in event_location:
                return event
        return {}