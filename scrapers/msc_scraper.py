import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper

class MscScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web MSC,
    tập trung vào tìm kiếm theo Booking Number và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 45)

            # 1. Xử lý cookie
            try:
                cookie_button = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                cookie_button.click()
                print("Accepted cookies.")
            except TimeoutException:
                print("Cookie banner not found or already accepted.")

            # 2. Chuyển sang tìm kiếm bằng Booking Number
            booking_radio_button = self.wait.until(
                EC.presence_of_element_located((By.ID, "bookingradio"))
            )
            self.driver.execute_script("arguments[0].click();", booking_radio_button)
            print("Switched to Booking Number search.")
            time.sleep(0.5)

            # 3. Nhập Booking Number và tìm kiếm
            search_input = self.wait.until(
                EC.presence_of_element_located((By.ID, "trackingNumber"))
            )
            search_input.clear()
            search_input.send_keys(tracking_number)
            
            search_form = self.driver.find_element(By.CSS_SELECTOR, "form.js-form")
            search_form.submit()
            print(f"Searching for Booking Number: {tracking_number}")

            # 4. Đợi kết quả và mở rộng chi tiết
            self.wait.until(
                EC.visibility_of_element_located((By.CLASS_NAME, "msc-flow-tracking__result"))
            )
            print("Results page loaded.")
            time.sleep(2)

            more_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__more-button")
            for button in more_buttons:
                try:
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                except Exception as e:
                    print(f"Could not click container expand button: {e}")
            
            print("All container details expanded.")

            # 5. Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data()
            
            # Đóng gói dữ liệu đã chuẩn hóa vào DataFrame
            # Mặc dù app.py sẽ chuyển nó thành JSON, việc sử dụng DataFrame
            # giúp duy trì cấu trúc nhất quán với các scraper khác.
            results_df = pd.DataFrame(normalized_data)
            
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/msc_timeout_{tracking_number}_{timestamp}.png"
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

    def _extract_and_normalize_data(self):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu thành định dạng JSON mong muốn.
        """
        # Trích xuất thông tin tóm tắt chung
        details_section = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".msc-flow-tracking__details ul")))
        
        pol = self._get_detail_value(details_section, "Port of Load")
        pod = self._get_detail_value(details_section, "Port of Discharge")
        
        # Xử lý trường hợp có nhiều cảng trung chuyển
        transhipment_elements = details_section.find_elements(By.XPATH, ".//li[contains(., 'Transhipment')]/span[contains(@class, 'details-value')]")
        transit_ports = [elem.text for elem in transhipment_elements if elem.text]

        # Lặp qua từng container để lấy lịch sử chi tiết
        normalized_results = []
        containers = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__container")

        for container in containers:
            events = self._extract_container_events(container)
            
            # Tìm các ngày quan trọng từ lịch sử container
            departure_date = self._find_event_date(events, "Export Loaded on Vessel", pol)
            arrival_date_estimated = self._find_event_date(events, "Estimated Time of Arrival", pod)
            
            # Xây dựng đối tượng JSON cho mỗi container
            container_info = {
                "POL": pol,
                "POD": pod,
                "transit_port": ", ".join(transit_ports) if transit_ports else None,
                "ngay_tau_di": {
                    "ngay_du_kien": None, # MSC không hiển thị ngày đi dự kiến rõ ràng
                    "ngay_thuc_te": departure_date
                },
                "ngay_tau_den": {
                    "ngay_du_kien": arrival_date_estimated,
                    "ngay_thuc_te": None # Cần logic để xác định ngày đến thực tế nếu có
                },
                "lich_su": events
            }
            normalized_results.append(container_info)
            
        return normalized_results

    def _get_detail_value(self, section, heading_text):
        """Lấy giá trị từ một mục trong phần details."""
        try:
            return section.find_element(By.XPATH, f".//li[contains(., '{heading_text}')]/span[contains(@class, 'details-value')]").text
        except NoSuchElementException:
            return None

    def _extract_container_events(self, container_element):
        """Trích xuất lịch sử di chuyển cho một container cụ thể."""
        events = []
        steps = container_element.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__step")
        
        for step in steps:
            try:
                date = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--two .data-value").text
                description = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--four .data-value").text
                
                event_type = "ngay_du_kien" if "estimated" in description.lower() or "intended" in description.lower() else "ngay_thuc_te"

                events.append({
                    "date": date,
                    "type": event_type,
                    "description": description,
                    "location": step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--three .data-value").text
                })
            except NoSuchElementException:
                continue
        return events

    def _find_event_date(self, events, description_keyword, location_keyword=None):
        """Tìm ngày của một sự kiện cụ thể trong danh sách."""
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = True
            if location_keyword:
                loc_match = location_keyword.lower() in event.get("location", "").lower()

            if desc_match and loc_match:
                return event.get("date")
        return None