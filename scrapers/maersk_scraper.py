import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback
from .base_scraper import BaseScraper

class MaerskScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Maersk.
    Sử dụng phương pháp truy cập URL trực tiếp để lấy dữ liệu.
    """

    def scrape(self, tracking_number):
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            print(f"Accessing direct URL: {direct_url}")
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            try:
                # Đợi và nhấn nút chấp nhận cookie nếu có
                allow_all_button = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'coi-banner__accept') and contains(., 'Allow all')]"))
                )
                self.driver.execute_script("arguments[0].click();", allow_all_button)
                # Đợi cho lớp phủ cookie biến mất
                self.wait.until(EC.invisibility_of_element_located((By.ID, "coiOverlay")))
            except TimeoutException:
                print("Cookie banner not found or already accepted.")
            
            try:
                # Đợi cho đến khi phần tóm tắt chính của lô hàng hiển thị
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))

                summary_data = self._extract_summary_data()
                summary_df = pd.DataFrame([summary_data])

                container_histories = self._extract_history_data()
                history_df = pd.DataFrame(container_histories)
                
                if history_df.empty:
                    print("Warning: History data is empty. The scraper might have failed to extract container details.")

                results = {
                    "summary": summary_df,
                    "history": history_df
                }
                return results, None

            except TimeoutException:
                try:
                    error_script = "return document.querySelector('mc-input[data-test=\"track-input\"]').shadowRoot.querySelector('.mds-helper-text--negative').textContent"
                    error_message = self.driver.execute_script(error_script)
                    if "Incorrect format" in error_message:
                        return None, f"Invalid tracking number format for '{tracking_number}': {error_message.strip()}"
                except Exception:
                    pass
                raise TimeoutException("Results page did not load.")

        except TimeoutException:
            try:
                # Thêm timestamp vào tên file screenshot
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = f"output/maersk_timeout_{tracking_number}_{timestamp}.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Timeout waiting for results for '{tracking_number}'. Website might be slow."
        except Exception as e:
            print(f"An unexpected error occurred for '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"An unexpected error occurred for '{tracking_number}': {e}"

    def _extract_summary_data(self):
        summary_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))
        data = {}
        try:
            data['Bill of Lading'] = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='transport-doc-value']").text
        except NoSuchElementException: data['Bill of Lading'] = None
        try:
            data['From'] = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-from-value']").text
        except NoSuchElementException: data['From'] = None
        try:
            data['To'] = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-to-value']").text
        except NoSuchElementException: data['To'] = None
        return data

    # cargo-web-scraper/scrapers/maersk_scraper.py

    def _extract_history_data(self):
        all_events = []
        containers = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.container--ocean")))
        #print(f"Found {len(containers)} containers on the page.")

        for idx, container in enumerate(containers):
            container_no = 'unknown' 
            try:
                # Trích xuất số container
                try:
                    container_details_element = container.find_element(By.CSS_SELECTOR, "mc-text-and-icon[data-test='container-details']")
                    container_no = container_details_element.find_element(By.CSS_SELECTOR, "span.mds-text--medium-bold").text.strip()
                except NoSuchElementException:
                    #print(f"Info: Could not find container number for container at index {idx}. Skipping.")
                    continue 

                # --- LOGIC ĐỢI VÀ CLICK ---
                toggle_buttons = container.find_elements(By.CSS_SELECTOR, "mc-button[data-test='container-toggle-details']")
                if toggle_buttons:
                    toggle_button_host = toggle_buttons[0]
                    
                    # Chỉ nhấn nếu nó đang ở trạng thái đóng
                    if toggle_button_host.get_attribute("aria-expanded") == 'false':
                        #print(f"Info: Expanding details for container '{container_no}' (index {idx}).")
                        
                        # Sử dụng JavaScript để click trực tiếp vào nút bên trong shadow DOM
                        button_to_click = self.driver.execute_script("return arguments[0].shadowRoot.querySelector('button')", toggle_button_host)
                        self.driver.execute_script("arguments[0].click();", button_to_click)
                        
                        # Đợi một cách tường minh cho đến khi thuộc tính aria-expanded chuyển thành 'true'
                        WebDriverWait(self.driver, 5).until(
                            lambda d: toggle_button_host.get_attribute("aria-expanded") == 'true'
                        )
                        # Thêm một khoảng chờ ngắn để đảm bảo animation và DOM được render xong
                        time.sleep(0.5)

                # ----------------------------------------------------

                # Trích xuất dữ liệu
                plan_lists = container.find_elements(By.CSS_SELECTOR, "ul.transport-plan__list")
                if not plan_lists:
                    continue

                plan_list = plan_lists[0]
                events = plan_list.find_elements(By.CSS_SELECTOR, "li.transport-plan__list__item")
                
                for event in events:
                    event_data = {'Container No': container_no}
                    try:
                        location_div = event.find_element(By.CSS_SELECTOR, "div.location")
                        event_data['Location'] = location_div.text.replace('\n', ', ')
                    except NoSuchElementException: 
                        event_data['Location'] = None

                    milestone_div = event.find_element(By.CSS_SELECTOR, "div.milestone")
                    milestone_lines = milestone_div.text.split('\n')

                    full_event_text = milestone_lines[0] if len(milestone_lines) > 0 else None
                    event_data['Date'] = milestone_lines[1] if len(milestone_lines) > 1 else None
                    
                    event_data['Event'] = full_event_text
                    event_data['Vessel'] = None
                    event_data['Voyage'] = None

                    if full_event_text and '(' in full_event_text:
                        event_parts = full_event_text.split('(', 1)
                        event_data['Event'] = event_parts[0].strip()
                        
                        vessel_voyage_str = event_parts[1].strip(' )')
                        if '/' in vessel_voyage_str:
                            vessel_parts = vessel_voyage_str.split('/')
                            event_data['Vessel'] = vessel_parts[0].strip()
                            event_data['Voyage'] = vessel_parts[1].strip()
                        else:
                            event_data['Vessel'] = vessel_voyage_str
                    
                    all_events.append(event_data)
                    
            except Exception as e:
                print(f"An unexpected error occurred while processing container '{container_no}'. Error: {e}")
                traceback.print_exc()
                continue
                
        return all_events