# cargo-web-scraper/scrapers/maersk_scraper.py
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from .base_scraper import BaseScraper

class MaerskScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Maersk.
    Trang này sử dụng Shadow DOM, nên cần dùng JavaScript để tương tác với form.
    """

    def scrape(self, tracking_number):
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # --- Xử lý Cookie Banner ---
            try:
                # Chờ và click nút "Allow all" trên banner cookie
                allow_all_button = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.coi-banner__accept"))
                )
                allow_all_button.click()
                time.sleep(1) # Chờ cho banner biến mất
            except TimeoutException:
                print("Cookie banner not found or already accepted.")

            # --- Nhập thông tin và tìm kiếm (sử dụng JavaScript) ---
            # Do form nằm trong Shadow DOM, chúng ta cần dùng JS để truy cập
            
            # Tìm đến phần tử mc-input và gán giá trị cho nó
            input_element_script = "return document.querySelector('mc-input[data-test=\"track-input\"]').shadowRoot.querySelector('input')"
            search_input = self.driver.execute_script(input_element_script)
            self.driver.execute_script("arguments[0].value = arguments[1];", search_input, tracking_number)
            
            # Click nút Track
            button_element_script = "return document.querySelector('mc-button[data-test=\"track-button\"]').shadowRoot.querySelector('button')"
            search_button = self.driver.execute_script(button_element_script)
            search_button.click()

            # --- Chờ kết quả và trích xuất ---
            # Chờ cho phần tóm tắt lô hàng xuất hiện
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))

            summary_data = self._extract_summary_data()
            summary_df = pd.DataFrame([summary_data])

            container_histories = self._extract_history_data()
            history_df = pd.DataFrame(container_histories)
            
            results = {
                "summary": summary_df,
                "history": history_df
            }
            return results, None

        except TimeoutException:
            try:
                screenshot_path = f"output/maersk_timeout_{tracking_number}.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Timeout waiting for results for '{tracking_number}'. The website might be slow or the number is invalid."
        except Exception as e:
            return None, f"An unexpected error occurred for '{tracking_number}': {e}"

    def _extract_summary_data(self):
        """Trích xuất dữ liệu tóm tắt của lô hàng."""
        summary_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))
        
        data = {}
        try:
            data['Bill of Lading'] = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='transport-doc-value']").text
        except NoSuchElementException:
            data['Bill of Lading'] = None
        try:
            data['From'] = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-from-value']").text
        except NoSuchElementException:
            data['From'] = None
        try:
            data['To'] = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-to-value']").text
        except NoSuchElementException:
            data['To'] = None
            
        return data

    def _extract_history_data(self):
        """Trích xuất lịch sử di chuyển của tất cả các container."""
        all_events = []
        
        # Chờ cho tất cả các container được tải
        containers = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.container--ocean")))

        for container in containers:
            try:
                # Lấy thông tin container
                header = container.find_element(By.CSS_SELECTOR, "header[data-test^='container-header-']")
                container_details = header.find_element(By.CSS_SELECTOR, "mc-text-and-icon[data-test='container-details']").text.split('|')
                container_no = container_details[0].strip()
                
                # Click để mở rộng chi tiết nếu nó đang đóng
                toggle_button = header.find_element(By.CSS_SELECTOR, "mc-button.container__toggle")
                if toggle_button.get_attribute("aria-expanded") == 'false':
                    self.driver.execute_script("arguments[0].click();", toggle_button)
                    # Chờ cho bảng lịch sử xuất hiện sau khi click
                    self.wait.until(EC.visibility_of(container.find_element(By.CSS_SELECTOR, "div.transport-plan")))

                # Trích xuất các sự kiện
                plan_list = container.find_element(By.CSS_SELECTOR, "ul.transport-plan__list")
                events = plan_list.find_elements(By.CSS_SELECTOR, "li.transport-plan__list__item")
                
                for event in events:
                    event_data = {'Container No': container_no}
                    try:
                        location_div = event.find_element(By.CSS_SELECTOR, "div.location")
                        event_data['Location'] = location_div.text.replace('\n', ', ')
                    except NoSuchElementException:
                        event_data['Location'] = None

                    milestone_div = event.find_element(By.CSS_SELECTOR, "div.milestone")
                    milestone_text = milestone_div.text.split('\n')
                    
                    event_data['Event'] = milestone_text[0] if len(milestone_text) > 0 else None
                    event_data['Date'] = milestone_text[1] if len(milestone_text) > 1 else None
                    
                    # Tách Vessel và Voyage nếu có
                    vessel_voyage_str = ""
                    try:
                       vessel_voyage_str = milestone_div.find_element(By.XPATH, "./span[contains(text(), '/')]").text
                       parts = vessel_voyage_str.strip(' ()').split('/')
                       event_data['Vessel'] = parts[0].strip() if len(parts) > 0 else None
                       event_data['Voyage'] = parts[1].strip() if len(parts) > 1 else None
                    except NoSuchElementException:
                        event_data['Vessel'] = None
                        event_data['Voyage'] = None

                    all_events.append(event_data)
                    
            except Exception as e:
                print(f"Could not process a container. Error: {e}")
                continue
                
        return all_events