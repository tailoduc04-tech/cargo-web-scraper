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
    Sử dụng phương pháp truy cập URL trực tiếp và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            print(f"Accessing direct URL: {direct_url}")
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            try:
                allow_all_button = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'coi-banner__accept') and contains(., 'Allow all')]"))
                )
                self.driver.execute_script("arguments[0].click();", allow_all_button)
                self.wait.until(EC.invisibility_of_element_located((By.ID, "coiOverlay")))
            except TimeoutException:
                print("Cookie banner not found or already accepted.")
            
            try:
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))

                normalized_data = self._extract_and_normalize_data()
                
                results_df = pd.DataFrame(normalized_data)
                
                results = {
                    "tracking_info": results_df
                }
                return results, None

            except TimeoutException:
                try:
                    error_script = "return document.querySelector('mc-input[data-test=\"track-input\"]').shadowRoot.querySelector('.mds-helper-text--negative').textContent"
                    error_message = self.driver.execute_script(error_script)
                    if "Incorrect format" in error_message:
                        return None, f"Không tìm thấy kết quả cho '{tracking_number}': {error_message.strip()}"
                except Exception:
                    pass
                raise TimeoutException("Results page did not load.")

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/maersk_timeout_{tracking_number}_{timestamp}.png"
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
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ trang chi tiết của Maersk.
        """
        summary_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))
        pol = self._get_summary_value(summary_element, "track-from-value")
        pod = self._get_summary_value(summary_element, "track-to-value")

        all_shipments = []
        
        containers = self.driver.find_elements(By.CSS_SELECTOR, "div.container--ocean")
        
        for container in containers:
            try:
                toggle_button_host = container.find_element(By.CSS_SELECTOR, "mc-button[data-test='container-toggle-details']")
                if toggle_button_host.get_attribute("aria-expanded") == 'false':
                    button_to_click = self.driver.execute_script("return arguments[0].shadowRoot.querySelector('button')", toggle_button_host)
                    self.driver.execute_script("arguments[0].click();", button_to_click)
                    WebDriverWait(self.driver, 5).until(
                        lambda d: toggle_button_host.get_attribute("aria-expanded") == 'true'
                    )
                    time.sleep(0.5)
            except NoSuchElementException:
                pass

            events = self._extract_events_from_container(container)
            
            departure_event = self._find_event(events, "Vessel departure", pol)
            arrival_event = self._find_event(events, "Vessel arrival", pod)

            transit_ports = []
            for event in events:
                location = event.get('location', '').strip() if event.get('location') else ''
                if location and pol not in location and pod not in location:
                    if "arrival" in event.get('description','').lower() or "departure" in event.get('description','').lower():
                        if location not in transit_ports:
                           transit_ports.append(location)

            shipment_data = {
                "POL": pol,
                "POD": pod,
                "transit_port": ", ".join(transit_ports) if transit_ports else None,
                "ngay_tau_di": {
                    "ngay_du_kien": departure_event.get('date') if departure_event and 'ngay_du_kien' in departure_event.get('type') else None,
                    "ngay_thuc_te": departure_event.get('date') if departure_event and 'ngay_thuc_te' in departure_event.get('type') else None
                },
                "ngay_tau_den": {
                    "ngay_du_kien": arrival_event.get('date') if arrival_event else None,
                    "ngay_thuc_te": None 
                },
                "lich_su": events
            }
            all_shipments.append(shipment_data)
            
        return all_shipments

    def _get_summary_value(self, summary_element, data_test_id):
        try:
            return summary_element.find_element(By.CSS_SELECTOR, f"dd[data-test='{data_test_id}']").text
        except NoSuchElementException:
            return None

    def _extract_events_from_container(self, container_element):
        events = []
        try:
            transport_plan = container_element.find_element(By.CSS_SELECTOR, ".transport-plan__list")
            list_items = transport_plan.find_elements(By.CSS_SELECTOR, "li.transport-plan__list__item")
            
            for item in list_items:
                event_data = {}
                try:
                    location_text = item.find_element(By.CSS_SELECTOR, "div.location").text
                    event_data['location'] = location_text.replace('\n', ', ')
                except NoSuchElementException: 
                    event_data['location'] = None
                
                milestone_div = item.find_element(By.CSS_SELECTOR, "div.milestone")
                milestone_lines = milestone_div.text.split('\n')
                
                event_data['description'] = milestone_lines[0] if len(milestone_lines) > 0 else None
                event_data['date'] = milestone_lines[1] if len(milestone_lines) > 1 else None
                event_data['type'] = "ngay_du_kien" if "future" in item.get_attribute("class") else "ngay_thuc_te"
                
                events.append(event_data)
        except NoSuchElementException:
            print("Could not find transport plan for a container.")

        return events
    
    def _find_event(self, events, description_keyword, location_keyword):
        if not location_keyword:
            return {}
        for event in events:
            # Bổ sung kiểm tra để đảm bảo event['location'] không phải là None
            event_location = event.get("location") or ""
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = location_keyword.lower() in event_location.lower()
            if desc_match and loc_match:
                return event
        return {}