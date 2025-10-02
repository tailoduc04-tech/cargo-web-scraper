# cargo-web-scraper/scrapers/cma_cgm_scraper.py

import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time

from .base_scraper import BaseScraper

class CmaCgmScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang CMA CGM.
    """

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã tracking (Container hoặc Booking).
        """
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30) 

            # --- Nhập thông tin và tìm kiếm ---
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "Reference")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.wait.until(EC.element_to_be_clickable((By.ID, "btnTracking")))
            search_button.click()
            self.wait.until(EC.visibility_of_element_located((By.ID, "top-details-0")))
            time.sleep(1)

            # --- Click để hiển thị các di chuyển cũ hơn ---
            try:
                # Vòng lặp để click vào tất cả các nút "Display Previous Moves"
                while True:
                    display_moves_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a.k-svg-i-caret-alt-right")
                    if display_moves_buttons:
                        self.driver.execute_script("arguments[0].click();", display_moves_buttons[0])
                        time.sleep(1)
                    else:
                        break
            except Exception:
                pass


            # --- Trích xuất dữ liệu tóm tắt ---
            summary_data = self._extract_summary_data()
            summary_df = pd.DataFrame([summary_data])

            # --- Trích xuất lịch sử di chuyển ---
            history_events = self._extract_history_data(summary_data.get("Container No"))
            history_df = pd.DataFrame(history_events)

            results = {
                "summary": summary_df,
                "history": history_df
            }
            return results, None

        except TimeoutException:
            try:
                # Thêm timestamp vào tên file screenshot
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = f"output/cmacgm_timeout_{tracking_number}_{timestamp}.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Timeout waiting for results for '{tracking_number}'. The website might be slow or the number is invalid."
        except Exception as e:
            return None, f"An unexpected error occurred for '{tracking_number}': {e}"

    def _extract_summary_data(self):
        """Trích xuất dữ liệu từ phần tóm tắt đầu trang."""
        header = self.wait.until(EC.presence_of_element_located((By.ID, "top-details-0")))
        timeline = self.driver.find_element(By.CSS_SELECTOR, ".timeline-wrapper")
        details = self.driver.find_element(By.CSS_SELECTOR, ".details-container")

        data = {}
        try:
            data['Container No'] = header.find_element(By.CSS_SELECTOR, "ul.resume-filter li:nth-child(1) strong").text
        except NoSuchElementException:
            data['Container No'] = None

        try:
            container_type_raw = header.find_element(By.CSS_SELECTOR, "li.ico-container").text
            data['Container Type'] = container_type_raw.replace('\n', ' ')
        except NoSuchElementException:
            data['Container Type'] = None

        try:
            data['Status'] = header.find_element(By.CSS_SELECTOR, "span.capsule.primary").text
        except NoSuchElementException:
            data['Status'] = None
            
        try:
            data['Booking Reference'] = details.find_element(By.CSS_SELECTOR, ".value-Info").text
        except NoSuchElementException:
            data['Booking Reference'] = None

        try:
            data['POL'] = timeline.find_element(By.CSS_SELECTOR, "li.timeline--item.step.dotted.import strong").text
        except NoSuchElementException:
            data['POL'] = None

        try:
            data['POD'] = timeline.find_element(By.CSS_SELECTOR, "li.timeline--item.arrival strong").text
        except NoSuchElementException:
            data['POD'] = None

        try:
            eta_date = timeline.find_element(By.CSS_SELECTOR, ".timeline--item-eta p span:nth-child(1)").text
            eta_time = timeline.find_element(By.CSS_SELECTOR, ".timeline--item-eta p span:nth-child(2)").text
            data['ETA'] = f"{eta_date} | {eta_time}"
        except NoSuchElementException:
            data['ETA'] = None

        try:
            data['Days Remaining'] = timeline.find_element(By.CSS_SELECTOR, ".timeline--item-eta p.remaining").text
        except NoSuchElementException:
            data['Days Remaining'] = None

        return data


    def _extract_history_data(self, container_no):
        """Trích xuất dữ liệu từ bảng lịch sử di chuyển."""
        history_events = []
        rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridTrackingDetails > .k-grid-container tr.k-master-row")

        for row in rows:
            event = {"Container No": container_no}
            try:
                event['Event Date'] = row.find_element(By.CSS_SELECTOR, ".date .calendar").text
            except NoSuchElementException:
                event['Event Date'] = None
            try:
                event['Event Time'] = row.find_element(By.CSS_SELECTOR, ".date .time").text
            except NoSuchElementException:
                event['Event Time'] = None
            try:
                event['Event Description'] = row.find_element(By.CSS_SELECTOR, "td:nth-child(4) .capsule").text
            except NoSuchElementException:
                event['Event Description'] = None
            try:
                event['Location'] = row.find_element(By.CSS_SELECTOR, "td.location span").text
            except NoSuchElementException:
                event['Location'] = None
            try:
                vessel_voyage = row.find_element(By.CSS_SELECTOR, ".vesselVoyage").text.split('(')
                event['Vessel Name'] = vessel_voyage[0].strip()
                event['Voyage'] = vessel_voyage[1].replace(')', '').strip() if len(vessel_voyage) > 1 else None
            except (NoSuchElementException, IndexError):
                event['Vessel Name'] = None
                event['Voyage'] = None

            history_events.append(event)
        
        return history_events