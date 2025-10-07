import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper

class HmmScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web HMM,
    chuẩn hóa kết quả theo định dạng yêu cầu.
    """

    def scrape(self, tracking_number):
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # 1. Tìm ô nhập liệu và nút tìm kiếm
            # Trang HMM có nhiều ô input, ta chọn ô đầu tiên cho B/L No.
            search_input = self.wait.until(
                EC.presence_of_element_located((By.NAME, "srchBlNo1"))
            )
            search_input.clear()
            search_input.send_keys(tracking_number)

            # Nút "Retrieve" có hàm onclick="search()"
            retrieve_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn') and @onclick='search()']"))
            )
            retrieve_button.click()
            print(f"Searching for B/L Number: {tracking_number}")

            # 2. Đợi kết quả tải và trích xuất dữ liệu
            self.wait.until(
                EC.visibility_of_element_located((By.ID, "trackingInfomationDateResultTable"))
            )
            print("Results page loaded.")

            normalized_data = self._extract_and_normalize_data()

            if not normalized_data:
                return None, f"Could not extract normalized data for '{tracking_number}'."

            # Đóng gói dữ liệu đã chuẩn hóa vào DataFrame
            results_df = pd.DataFrame(normalized_data)
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/hmm_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            print(f"An unexpected error occurred for '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"

    def _extract_and_normalize_data(self):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ trang kết quả.
        """
        # 1. Trích xuất thông tin chung (POL, POD, Ngày đi/đến)
        summary_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#cntrChangeArea table")))
        
        # Lấy POL và POD
        location_row = summary_table.find_element(By.XPATH, ".//tr[th/div[text()='Location']]")
        cells = location_row.find_elements(By.TAG_NAME, "td")
        pol = cells[1].text.strip() # Cột Loading Port
        pod = cells[3].text.strip() # Cột Discharging Port
        
        # Lấy ngày tàu đi và đến
        departure_row = summary_table.find_element(By.XPATH, ".//tr[th/div[text()='Departure']]")
        dep_cells = departure_row.find_elements(By.TAG_NAME, "td")
        actual_departure_date = dep_cells[0].text.strip() if 'red' in dep_cells[0].find_element(By.TAG_NAME, 'div').get_attribute('class') else None
        estimated_departure_date = dep_cells[1].text.strip() if 'blue' in dep_cells[1].find_element(By.TAG_NAME, 'div').get_attribute('class') else None


        arrival_row = summary_table.find_element(By.XPATH, ".//tr[th/div[contains(text(), 'Arrival')]]")
        arr_cells = arrival_row.find_elements(By.TAG_NAME, "td")
        estimated_arrival_date = arr_cells[1].text.strip() if 'blue' in arr_cells[1].find_element(By.TAG_NAME, 'div').get_attribute('class') else None
        actual_arrival_date = arr_cells[1].text.strip() if 'red' in arr_cells[1].find_element(By.TAG_NAME, 'div').get_attribute('class') else None # Cập nhật nếu có
        

        # 2. Lấy danh sách các container và xử lý từng container
        all_shipments = []
        container_table = self.wait.until(EC.presence_of_element_located((By.ID, "containerStatus")))
        container_rows = container_table.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        # Lấy danh sách các element container để tránh lỗi StaleElementReference
        container_selectors = [f"//div[@id='cntrId{i+1}']" for i in range(len(container_rows))]

        for selector in container_selectors:
            # Click vào từng container để tải lịch sử chi tiết của nó
            try:
                container_link = self.wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                container_link.click()
                # Đợi một chút để AJAX cập nhật bảng lịch sử
                time.sleep(2) 
            except Exception as e:
                print(f"Could not click on container selector {selector}. It might be the only one and already selected. Error: {e}")

            # 3. Trích xuất lịch sử chi tiết sau khi click
            events = self._extract_shipment_progress()
            
            # 4. Xác định cảng trung chuyển từ lịch sử
            transit_ports = []
            for event in events:
                location = event.get('location', '')
                desc = event.get('description', '').lower()
                is_transit_event = 'unload' in desc or 'discharge' in desc or 'load' in desc
                
                if is_transit_event and location not in [pol, pod] and location not in transit_ports:
                    transit_ports.append(location)

            # 5. Xây dựng đối tượng JSON chuẩn hóa
            shipment_data = {
                "POL": pol,
                "POD": pod,
                "transit_port": ", ".join(transit_ports) if transit_ports else None,
                "ngay_tau_di": {
                    "ngay_du_kien": estimated_departure_date,
                    "ngay_thuc_te": actual_departure_date
                },
                "ngay_tau_den": {
                    "ngay_du_kien": estimated_arrival_date,
                    "ngay_thuc_te": actual_arrival_date
                },
                "lich_su": events
            }
            all_shipments.append(shipment_data)
            
        return all_shipments

    def _extract_shipment_progress(self):
        """Trích xuất lịch sử từ bảng "Shipment Progress"."""
        events = []
        try:
            progress_table = self.wait.until(
                EC.presence_of_element_located((By.ID, "shipmentProgress"))
            )
            rows = progress_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 4:
                    date = cells[0].text.strip()
                    time = cells[1].text.strip()
                    
                    event_type = "ngay_thuc_te"
                    if 'past' not in cells[0].find_element(By.TAG_NAME, 'div').get_attribute('class'):
                         event_type = "ngay_du_kien"

                    events.append({
                        "date": f"{date} {time}",
                        "type": event_type,
                        "description": cells[3].text.strip(),
                        "location": cells[2].text.strip()
                    })
        except (NoSuchElementException, TimeoutException):
            print("Could not find or process shipment progress table.")
        return events