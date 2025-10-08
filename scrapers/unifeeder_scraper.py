import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
from .base_scraper import BaseScraper

class UnifeederScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Unifeeder và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.booking-details"))
            )

            normalized_data = self._extract_and_normalize_data()
            
            if not normalized_data:
                return None, f"Could not extract normalized data for '{tracking_number}'."

            results_df = pd.DataFrame(normalized_data)
            
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/unifeeder_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception as ss_e:
                return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            return None, f"Không tìm thấy kết quả cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self):
        try:
            route_container = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.route-display"))
            )
            route_spans = route_container.find_elements(By.TAG_NAME, "span")
            pol = route_spans[0].text.strip() if len(route_spans) > 0 else None
            pod = route_spans[1].text.strip() if len(route_spans) > 1 else None

            events = self._extract_events()
            
            departure_event = self._find_event(events, "LOAD FULL", pol, is_actual=True)
            arrival_event_projected = self._find_event(events, "DISCHARGE FULL", pod, is_projected=True)
            
            transit_ports = []
            for event in events:
                if "T/S" in event.get('description', ''):
                    port = event.get('location')
                    if port and port not in transit_ports:
                        transit_ports.append(port)
            
            shipment_data = {
                "POL": pol, "POD": pod,
                "transit_port": ", ".join(transit_ports) if transit_ports else None,
                "ngay_tau_di": {"ngay_du_kien": None, "ngay_thuc_te": departure_event.get('date') if departure_event else None},
                "ngay_tau_den": {"ngay_du_kien": arrival_event_projected.get('date') if arrival_event_projected else None, "ngay_thuc_te": None},
                "lich_su": events
            }
            
            return [shipment_data]

        except Exception as e:
            return []

    def _extract_events(self):
        events = []
        event_rows = self.driver.find_elements(By.CSS_SELECTOR, "div.row-item")
        
        for i, row in enumerate(event_rows):
            try:
                if row.find_elements(By.CSS_SELECTOR, ".table-title"):
                    continue

                cells = row.find_elements(By.CSS_SELECTOR, ".list-box > div")
                if len(cells) < 3:
                    continue

                date_text = cells[0].text.strip()
                description = cells[1].text.strip()
                location = cells[2].text.strip()
                
                event_type = "ngay_du_kien" if "(Projected)" in date_text else "ngay_thuc_te"
                
                event_data = {
                    "date": date_text.replace("(Projected)", "").strip(),
                    "type": event_type,
                    "description": description,
                    "location": location
                }
                events.append(event_data)
            except NoSuchElementException:
                continue
        return events

    def _find_event(self, events, description_keyword, location_keyword, is_projected=False, is_actual=False):
        if not location_keyword:
            return {}
        
        normalized_loc_keyword = location_keyword.lower()

        for event in reversed(events):
            event_location = (event.get("location") or "").lower()
            event_description = (event.get("description") or "").lower()
            event_type = event.get("type")

            desc_match = description_keyword.lower() in event_description
            loc_match = normalized_loc_keyword in event_location
            
            type_match = True
            if is_projected:
                type_match = (event_type == "ngay_du_kien")
            elif is_actual:
                type_match = (event_type == "ngay_thuc_te")

            if desc_match and loc_match and type_match:
                return event
        
        return {}