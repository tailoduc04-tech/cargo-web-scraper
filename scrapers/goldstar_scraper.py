import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper

class GoldstarScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Gold Star Line và chuẩn hóa kết quả
    theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD-Mon-YYYY' (ví dụ: '07-Oct-2025') sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Chuyển đổi từ định dạng '%d-%b-%Y'
            dt_obj = datetime.strptime(date_str, '%d-%b-%Y')
            # Format lại thành 'DD/MM/YYYY'
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            # Trả về chuỗi gốc hoặc None nếu không parse được
            print(f"[Goldstar Scraper] LOG: Không thể phân tích định dạng ngày: {date_str}")
            return None

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã tracking và trả về một dictionary JSON đã được chuẩn hóa.
        """
        print(f"[Goldstar Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # 1. Xử lý cookie nếu có
            try:
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "rcc-confirm-button"))
                )
                self.driver.execute_script("arguments[0].click();", cookie_button)
                print("[Goldstar Scraper] LOG: Đã chấp nhận cookies.")
            except Exception:
                print("[Goldstar Scraper] LOG: Banner cookie không xuất hiện hoặc không thể xử lý.")

            # 2. Nhập mã và tìm kiếm (với logic tránh bot)
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "containerid")))
            search_input.clear()
            search_input.send_keys(tracking_number)
            
            time.sleep(1.5)

            # --- GIẢI PHÁP TRÁNH BOT ---
            # Click vào một element khác để làm mất focus khỏi ô input
            self.driver.find_element(By.CSS_SELECTOR, "body > main > section.inner-center.mobile-margin > div > div > div > div > div > div.col-lg-5.col-md-12.col-sm-12.pot-form.p-0 > div > h3").click()
            print("[Goldstar Scraper] LOG: Đã click ra ngoài để tránh bot.")
            time.sleep(0.5)
            
            search_button = self.wait.until(EC.element_to_be_clickable((By.ID, "submitDetails")))
            self.driver.execute_script("arguments[0].click();", search_button)
            print(f"[Goldstar Scraper] LOG: Đang tìm kiếm mã: {tracking_number}")

            # 3. Đợi kết quả và mở rộng tất cả chi tiết
            self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "trackShipmentResultCard")))
            print("[Goldstar Scraper] LOG: Trang kết quả đã tải.")
            time.sleep(2)

            expand_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".accordion-item .arrowButton")
            print(f"[Goldstar Scraper] LOG: Tìm thấy {len(expand_buttons)} container để mở rộng.")
            for i, button in enumerate(expand_buttons):
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(0.5)
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                except Exception as e:
                    print(f"[Goldstar Scraper] WARN: Không thể nhấn nút mở rộng container #{i+1}: {e}")

            # 4. Trích xuất và chuẩn hóa dữ liệu
            print("[Goldstar Scraper] LOG: Bắt đầu trích xuất và chuẩn hóa dữ liệu.")
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[Goldstar Scraper] LOG: Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/goldstar_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"[Goldstar Scraper] ERROR: Timeout. Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"[Goldstar Scraper] ERROR: Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            print(f"[Goldstar Scraper] ERROR: Một lỗi không mong muốn đã xảy ra: {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định khi xử lý mã '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        try:
            # 1. Trích xuất thông tin chung từ summary card
            summary_card = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".trackShipmentResultRowInner")))
            bl_number = self.driver.find_element(By.XPATH, "//h3[contains(., 'GOSUXNG')]").text.strip()
            pol = summary_card.find_element(By.XPATH, ".//div[label='Port of Loading (POL)']/h3").text.strip()
            pod = summary_card.find_element(By.XPATH, ".//div[label='Port of Discharge (POD)']/h3").text.strip()
            etd = summary_card.find_element(By.XPATH, ".//div[label='Sailing Date']/h3").text.strip()
            eta = summary_card.find_element(By.XPATH, ".//div[label='ETD']/h3").text.strip() # Label này thực chất là ETA

            # 2. Thu thập tất cả các sự kiện từ tất cả các container
            all_events = []
            container_items = self.driver.find_elements(By.CSS_SELECTOR, ".accordion-item")
            for item in container_items:
                events = self._extract_events_from_container(item)
                all_events.extend(events)

            # 3. Tìm các ngày quan trọng và cảng trung chuyển từ lịch sử
            actual_departure = self._find_event(all_events, "Container was loaded at Port of Loading", pol)
            actual_arrival = self._find_event(all_events, "Container was discharged at Port of Destination", pod)
            
            transit_ports = []
            transit_discharge_events = []
            transit_load_events = []
            for event in all_events:
                location = event.get('location', '')
                description = event.get('description', '').lower()
                if location and pol not in location and pod not in location:
                    if "discharged" in description:
                        if location not in transit_ports: transit_ports.append(location)
                        transit_discharge_events.append(event)
                    elif "loaded" in description:
                        if location not in transit_ports: transit_ports.append(location)
                        transit_load_events.append(event)
            
            # 4. Xây dựng đối tượng JSON cuối cùng
            shipment_data = {
                "BookingNo": bl_number, # Sử dụng B/L No vì không có Booking No riêng
                "BlNumber": bl_number,
                "BookingStatus" : None, # Không có thông tin
                "Pol": pol,
                "Pod": pod,
                "Etd": self._format_date(etd),
                "Atd": self._format_date(actual_departure.get('date')) if actual_departure else None,
                "Eta": self._format_date(eta),
                "Ata": self._format_date(actual_arrival.get('date')) if actual_arrival else None,
                "TransitPort": ", ".join(transit_ports) if transit_ports else None,
                "EtdTransit": None, # Không có thông tin
                "AtdTrasit": self._format_date(transit_load_events[-1].get('date')) if transit_load_events else None,
                "EtaTransit": None, # Không có thông tin
                "AtaTrasit": self._format_date(transit_discharge_events[0].get('date')) if transit_discharge_events else None,
            }
            return shipment_data
        except Exception as e:
            print(f"[Goldstar Scraper] ERROR: Lỗi khi trích xuất và chuẩn hóa dữ liệu: {e}")
            traceback.print_exc()
            return None

    def _extract_events_from_container(self, container_item):
        events = []
        try:
            history_rows = container_item.find_elements(By.CSS_SELECTOR, ".accordion-body .grid-container")
            for row in history_rows:
                try:
                    description = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][2]").text.replace('Last Activity\n', '').strip()
                    location = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][3]").text.replace('Location\n', '').strip()
                    date = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][4]").text.replace('Date\n', '').strip()

                    events.append({
                        "date": date,
                        "description": description,
                        "location": location
                    })
                except NoSuchElementException:
                    continue
        except NoSuchElementException:
            print("[Goldstar Scraper] WARN: Không tìm thấy bảng lịch sử cho một container.")
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        if not events or not location_keyword:
            return {}
        
        normalized_loc_keyword = location_keyword.lower()
        for event in reversed(events): # Duyệt ngược để lấy sự kiện gần nhất
            event_location = event.get("location", "").lower()
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            
            if desc_match and normalized_loc_keyword in event_location:
                return event
        return {}