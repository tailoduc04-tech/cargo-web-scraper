import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper

class TailwindScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web Tailwind Shipping,
    xử lý việc mở tab mới và lấy dữ liệu chi tiết.
    """

    def scrape(self, tracking_number):
        original_window = self.driver.current_window_handle
        
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # 1. Xử lý cookie
            try:
                cookie_button = self.wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                self.driver.execute_script("arguments[0].click();", cookie_button)
                self.wait.until(EC.invisibility_of_element_located((By.ID, "onetrust-banner-sdk")))
                print("Đã chấp nhận cookies.")
            except TimeoutException:
                print("Không tìm thấy banner cookie hoặc đã được chấp nhận.")

            # 2. Nhập liệu và tìm kiếm
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "booking-number")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.search-icon")))
            self.driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].click();", search_button)
            print(f"Đang tìm kiếm Booking Number: {tracking_number}")

            # 3. Chờ và chuyển sang tab mới
            self.wait.until(EC.number_of_windows_to_be(2))
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    self.driver.switch_to.window(window_handle)
                    break
            print("Đã chuyển sang tab kết quả.")

            # 4. Đợi trang kết quả tải và trích xuất dữ liệu
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.stepwizard")))
            print("Đã tải trang kết quả.")
            time.sleep(2)

            normalized_data = self._extract_and_normalize_data()

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            results_df = pd.DataFrame(normalized_data)
            results = {"tracking_info": results_df}
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/tailwind_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout xảy ra. Đang lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'. Trang web có thể đang chậm hoặc mã không hợp lệ."
        except Exception as e:
            print(f"Một lỗi không mong muốn đã xảy ra cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Một lỗi không mong muốn đã xảy ra cho '{tracking_number}': {e}"
        finally:
            # Đóng các tab không cần thiết và quay về tab gốc
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    self.driver.switch_to.window(window_handle)
                    self.driver.close()
            self.driver.switch_to.window(original_window)
            print("Đã dọn dẹp và quay về tab gốc.")

    def _extract_and_normalize_data(self):
        pol_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:first-child .txt_port_name")))
        pod_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:last-child .txt_port_name")))
        
        pol = self._get_tooltip_text(pol_element)
        pod = self._get_tooltip_text(pod_element)

        transit_elements = self.driver.find_elements(By.CSS_SELECTOR, ".stepwizard-step:not(:first-child):not(:last-child) .txt_port_name")
        transit_ports = [self._get_tooltip_text(el) for el in transit_elements]

        etd_element = self.driver.find_element(By.CSS_SELECTOR, ".stepwizard-step:first-child .tracker_icon")
        estimated_departure_date_raw = self._get_tooltip_text(etd_element) # "ETD: 01/10/2025 11:18 am"
        estimated_departure_date = estimated_departure_date_raw.replace("ETD:", "").strip()

        eta_element = self.driver.find_element(By.CSS_SELECTOR, ".stepwizard-step:last-child .tracker_icon")
        estimated_arrival_date_raw = self._get_tooltip_text(eta_element) # "ETD: 01/10/2025 11:18 am"
        estimated_arrival_date = estimated_arrival_date_raw.replace("ETA:", "").strip()

        normalized_results = []
        container_rows = self.driver.find_elements(By.CSS_SELECTOR, "#datatablebytrack tbody tr:not(.mailcontent)")
        
        for index, row in enumerate(container_rows):
            try:
                view_details_button = row.find_element(By.CSS_SELECTOR, "button.view_details")
                self.driver.execute_script("arguments[0].click();", view_details_button)
                
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container .timeline-small")))
                
                events = self._extract_events_from_popup()
                
                departure_event = self._find_event(events, "LOAD FULL", pol)
                actual_departure_date = departure_event.get("date") if departure_event else None

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
                        "ngay_thuc_te": None 
                    },
                    "lich_su": events
                }
                normalized_results.append(shipment_data)
                
                close_button = self.driver.find_element(By.CSS_SELECTOR, ".fancybox-container .fancybox-close-small")
                close_button.click()
                self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container")))

            except Exception as e:
                print(f"Lỗi khi xử lý container thứ {index + 1}: {e}")
                traceback.print_exc()
                try:
                    if self.driver.find_element(By.CSS_SELECTOR, ".fancybox-container").is_displayed():
                        self.driver.find_element(By.CSS_SELECTOR, ".fancybox-container .fancybox-close-small").click()
                except:
                    pass
                continue
                
        return normalized_results

    def _get_tooltip_text(self, element):
        try:
            return element.get_attribute('data-original-title')
        except:
            return element.text

    def _extract_events_from_popup(self):
        events = []
        movement_details = self.driver.find_elements(By.CSS_SELECTOR, ".fancybox-container .media")
        
        for detail in movement_details:
            try:
                description = detail.find_element(By.CSS_SELECTOR, ".movement_title").text
                date = detail.find_element(By.CSS_SELECTOR, ".date_track").text
                location = detail.find_element(By.XPATH, ".//label[contains(text(), 'Activity Location:')]/following-sibling::span").text
                
                events.append({
                    "date": date,
                    "type": "ngay_thuc_te",
                    "description": description,
                    "location": location
                })
            except NoSuchElementException:
                continue
        return events
        
    def _find_event(self, events, description_keyword, location_keyword):
        if not location_keyword:
            return {}
        
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = location_keyword.lower() in event.get("location", "").lower()

            if desc_match and loc_match:
                return event
        return {}