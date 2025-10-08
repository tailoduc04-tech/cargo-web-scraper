import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import traceback

from .base_scraper import BaseScraper

class InterasiaScraper(BaseScraper):
    """Triển khai logic scraping cụ thể cho trang Interasia và chuẩn hóa kết quả."""
    
    def scrape(self, tracking_number):
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 20)

            # --- Logic tìm kiếm chính ---
            search_input = self.wait.until(EC.presence_of_element_located((By.NAME, "query")))
            search_input.clear()
            search_input.send_keys(tracking_number)
            self.driver.find_element(By.CSS_SELECTOR, "#containerSumbit").click()
            
            # Chờ cho đến khi bảng kết quả ban đầu xuất hiện
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "m-table-group")))
            
            # --- Lấy link chi tiết B/L và truy cập ---
            # Giả định chỉ có một kết quả và lấy link chi tiết từ dòng đầu tiên
            try:
                detail_link = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".m-table-group tbody tr:first-child td:first-child a"))
                ).get_attribute('href')
            except TimeoutException:
                 return None, f"No data found for '{tracking_number}' on the main page."

            # --- Scrape trang chi tiết ---
            normalized_data = self._scrape_and_normalize_details(detail_link)
            
            if not normalized_data:
                return None, f"Could not extract normalized data for '{tracking_number}'."

            # --- Trả về theo định dạng chuẩn ---
            results_df = pd.DataFrame(normalized_data)
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/interasia_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            print(f"An unexpected error occurred for '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Không tìm thấy kết quả cho '{tracking_number}': {e}"

    def _scrape_and_normalize_details(self, detail_url):
        """
        Scrape trang chi tiết B/L, trích xuất và chuẩn hóa dữ liệu.
        """
        try:
            print(f"  Scraping B/L detail from: {detail_url}")
            self.driver.get(detail_url)
            main_group = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "main-group")))
            
            # 1. Trích xuất thông tin tóm tắt chung
            summary_table = main_group.find_element(By.CSS_SELECTOR, ".m-table-group")
            cells = summary_table.find_elements(By.CSS_SELECTOR, "tbody tr td")
            
            pol = cells[0].text.strip() if len(cells) > 0 else None
            pod = cells[1].text.strip() if len(cells) > 1 else None
            est_departure_date = cells[2].text.strip() if len(cells) > 2 else None
            est_arrival_date = cells[3].text.strip() if len(cells) > 3 else None

            # 2. Lặp qua từng container và xử lý
            all_shipments = []
            container_blocks = main_group.find_elements(By.XPATH, "./div[.//p[contains(text(), 'Container No')]]")

            for block in container_blocks:
                # Trích xuất lịch sử cho container này
                events = self._extract_events_from_container(block)

                # Tìm các thông tin quan trọng từ lịch sử
                actual_departure_event = self._find_event(events, "LADEN CONTAINER LOADED ON BOARD VESSEL", pol)
                
                transit_ports = []
                for event in events:
                    if "transhipment" in event.get('description', '').lower():
                        port = event.get('location')
                        if port and port not in transit_ports:
                            transit_ports.append(port)

                # Xây dựng đối tượng JSON chuẩn hóa
                shipment_data = {
                    "POL": pol,
                    "POD": pod,
                    "transit_port": ", ".join(transit_ports) if transit_ports else None,
                    "ngay_tau_di": {
                        "ngay_du_kien": est_departure_date,
                        "ngay_thuc_te": actual_departure_event.get('date') if actual_departure_event else None
                    },
                    "ngay_tau_den": {
                        "ngay_du_kien": est_arrival_date,
                        "ngay_thuc_te": None  # Logic tìm ngày thực tế sẽ cần thêm nếu có
                    },
                    "lich_su": events
                }
                all_shipments.append(shipment_data)
                
            return all_shipments

        except Exception as e:
            print(f"    Warning: Failed to scrape or normalize B/L detail page {detail_url}: {e}")
            traceback.print_exc()
            return []

    def _extract_events_from_container(self, container_block):
        """Trích xuất tất cả sự kiện từ một khối container."""
        events = []
        try:
            event_table = container_block.find_element(By.CLASS_NAME, "m-table-group")
            rows = event_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in rows:
                cells = [cell.text.strip().replace('\n', ' ') for cell in row.find_elements(By.TAG_NAME, "td")]
                if len(cells) >= 4:
                    events.append({
                        "date": cells[0],
                        "type": "ngay_thuc_te", # Interasia dường như chỉ hiển thị ngày thực tế trong lịch sử
                        "description": cells[3],
                        "location": cells[2]
                    })
        except NoSuchElementException:
            print("Could not find event table for a container.")
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        """Tìm một sự kiện cụ thể trong danh sách các sự kiện."""
        if not location_keyword:
            return {}
        
        # Chuẩn hóa location_keyword (ví dụ: từ "IDJKT(JAKARTA)" thành "jakarta")
        normalized_loc_keyword = location_keyword.split('(')[-1].replace(')', '').strip().lower()

        for event in events:
            # Chuẩn hóa event location
            event_location = event.get("location") or ""
            normalized_event_loc = event_location.split('(')[-1].replace(')', '').strip().lower()

            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = normalized_loc_keyword in normalized_event_loc

            if desc_match and loc_match:
                return event
        return {}