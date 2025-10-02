# cargo-web-scraper/scrapers/msc_scraper.py

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
    tập trung vào tìm kiếm theo Booking Number.
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

            # 2. Chuyển sang tìm kiếm bằng Booking Number (SỬA LỖI TẠI ĐÂY)
            booking_radio_button = self.wait.until(
                EC.presence_of_element_located((By.ID, "bookingradio"))
            )
            self.driver.execute_script("arguments[0].click();", booking_radio_button)
            print("Switched to Booking Number search.")
            time.sleep(0.5) # Thêm một khoảng chờ ngắn để đảm bảo giao diện cập nhật

            # 3. Nhập Booking Number và tìm kiếm
            search_input = self.wait.until(
                EC.presence_of_element_located((By.ID, "trackingNumber"))
            )
            search_input.clear()
            search_input.send_keys(tracking_number)
            
            search_form = self.driver.find_element(By.CSS_SELECTOR, "form.js-form")
            search_form.submit()
            print(f"Searching for Booking Number: {tracking_number}")

            # 4. Đợi kết quả và mở rộng tất cả các chi tiết
            self.wait.until(
                EC.visibility_of_element_located((By.CLASS_NAME, "msc-flow-tracking__result"))
            )
            print("Results page loaded.")
            time.sleep(2) 

            # Mở rộng tất cả các container
            more_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__more-button")
            print(f"Found {len(more_buttons)} container(s) to expand.")
            for button in more_buttons:
                try:
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                except Exception as e:
                    print(f"Could not click container expand button: {e}")

            # Mở rộng tất cả các cảng trung gian
            # Sử dụng XPath để tìm chính xác nút có chữ "Show all"
            show_all_xpath = "//button[contains(@class, 'msc-cta-icon--show') and .//span[contains(text(), 'Show all')]]"
            show_all_buttons = self.driver.find_elements(By.XPATH, show_all_xpath)
            print(f"Found {len(show_all_buttons)} 'Show all' buttons to expand.")
            for button in show_all_buttons:
                try:
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                except Exception as e:
                    print(f"Could not click 'Show all' button: {e}")
            
            print("All details expanded.")

            # 5. Trích xuất dữ liệu
            summary_data = self._extract_summary_data()
            summary_df = pd.DataFrame([summary_data])

            history_events = self._extract_history_data()
            history_df = pd.DataFrame(history_events)

            results = {
                "summary": summary_df,
                "history": history_df
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

    def _extract_summary_data(self):
        summary_data = {}
        details_section = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".msc-flow-tracking__details ul")))
        
        items = details_section.find_elements(By.TAG_NAME, "li")
        for item in items:
            try:
                heading_raw = item.find_element(By.CLASS_NAME, "msc-flow-tracking__details-heading").text
                heading = heading_raw.strip().strip(':')
                
                # Xử lý trường hợp có nhiều value cho một heading (ví dụ: Transhipment)
                values = item.find_elements(By.CLASS_NAME, "msc-flow-tracking__details-value")
                value_text = ', '.join([v.text for v in values if v.text])

                summary_data[heading] = value_text
            except NoSuchElementException:
                continue
        
        try:
            subtitle_info = self.driver.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__subtitle-info").text
            summary_data['Number of Containers'] = subtitle_info
        except NoSuchElementException:
             summary_data['Number of Containers'] = None

        return summary_data

    def _extract_history_data(self):
        all_events = []
        containers = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__container")

        for container in containers:
            try:
                container_no = container.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--one .data-value").text
                container_type = container.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--two .data-value").text
            except NoSuchElementException:
                continue

            steps = container.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__step")
            for step in steps:
                event_data = {
                    'Container No': container_no,
                    'Container Type': container_type,
                }
                try:
                    event_data['Date'] = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--two .data-value").text
                except NoSuchElementException: event_data['Date'] = None
                try:
                    event_data['Location'] = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--three .data-value").text
                except NoSuchElementException: event_data['Location'] = None
                try:
                    event_data['Description'] = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--four .data-value").text
                except NoSuchElementException: event_data['Description'] = None
                try:
                    # Lấy cả text chính và text trong tooltip nếu có
                    vessel_voyage_element = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--five .data-value span")
                    event_data['Vessel/Voyage'] = vessel_voyage_element.text
                except NoSuchElementException: 
                    event_data['Vessel/Voyage'] = None
                try:
                    facility_element = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--six .data-value span")
                    event_data['Facility'] = facility_element.text
                except NoSuchElementException: 
                    event_data['Facility'] = None
                
                all_events.append(event_data)

        return all_events