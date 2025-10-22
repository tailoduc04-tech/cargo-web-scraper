import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

class MscScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web MSC,
    tập trung vào tìm kiếm theo Booking Number và chuẩn hóa kết quả theo template JSON.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD/MM/YYYY' sang 'DD/MM/YYYY'.
        Hàm này chủ yếu để đảm bảo định dạng và xử lý các giá trị None.
        """
        if not date_str:
            return None
        try:
            # MSC đã cung cấp định dạng DD/MM/YYYY, chỉ cần xác thực lại
            datetime.strptime(date_str, '%d/%m/%Y')
            return date_str
        except (ValueError, TypeError):
            return None

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một Booking Number và trả về một dictionary JSON đã được chuẩn hóa.
        """
        print(f"[MSC Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 45)

            # 1. Xử lý cookie
            try:
                cookie_button = self.wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                cookie_button.click()
                print("[MSC Scraper] -> Đã chấp nhận cookies.")
            except TimeoutException:
                print("[MSC Scraper] -> Banner cookie không tìm thấy hoặc đã được chấp nhận.")

            # 2. Chuyển sang tìm kiếm bằng Booking Number
            booking_radio_button = self.wait.until(EC.presence_of_element_located((By.ID, "bookingradio")))
            self.driver.execute_script("arguments[0].click();", booking_radio_button)
            print("[MSC Scraper] -> Đã chuyển sang tìm kiếm bằng Booking Number.")
            time.sleep(0.5)

            # 3. Nhập Booking Number và tìm kiếm
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "trackingNumber")))
            search_input.clear()
            search_input.send_keys(tracking_number)
            
            search_form = self.driver.find_element(By.CSS_SELECTOR, "form.js-form")
            search_form.submit()
            print(f"[MSC Scraper] -> Đang tìm kiếm mã: {tracking_number}")

            # 4. Đợi kết quả và mở rộng tất cả chi tiết container
            self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "msc-flow-tracking__result")))
            print("[MSC Scraper] -> Trang kết quả đã tải.")
            time.sleep(2) # Chờ cho các animation

            more_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__more-button")
            print(f"[MSC Scraper] -> Tìm thấy {len(more_buttons)} container để mở rộng.")
            for i, button in enumerate(more_buttons):
                try:
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
                except Exception as e:
                    print(f"[MSC Scraper] -> Cảnh báo: Không thể nhấp vào nút mở rộng #{i+1}: {e}")
            
            print("[MSC Scraper] -> Đã mở rộng tất cả chi tiết container.")

            # 5. Trích xuất và chuẩn hóa dữ liệu thành một dictionary duy nhất
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[MSC Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/msc_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"[MSC Scraper] -> Lỗi Timeout. Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"[MSC Scraper] -> Lỗi: Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            print(f"[MSC Scraper] -> Lỗi không mong muốn: {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, booking_no):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả thành một dictionary duy nhất.
        """
        try:
            # 1. Trích xuất thông tin tóm tắt chung
            details_section = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".msc-flow-tracking__details ul")))
            
            pol = self._get_detail_value(details_section, "Port of Load")
            pod = self._get_detail_value(details_section, "Port of Discharge")
            bl_number = self.driver.find_element(By.CSS_SELECTOR, "#main > div.msc-flow-tracking.separator--bottom-medium > div > div:nth-child(3) > div > div > div > div.msc-flow-tracking__results > div > div > div.msc-flow-tracking__details > ul > li:nth-child(1) > div:nth-child(2) > span.msc-flow-tracking__details-value").text

            # Xử lý trường hợp có nhiều cảng trung chuyển
            transhipment_elements = details_section.find_elements(By.XPATH, ".//li[contains(., 'Transhipment')]/span[contains(@class, 'details-value')]")
            transit_ports = [elem.text for elem in transhipment_elements if elem.text]

            # 2. Thu thập tất cả các sự kiện từ tất cả các container vào một danh sách duy nhất
            all_events = []
            containers = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__container")
            for container in containers:
                events = self._extract_events_from_container(container)
                all_events.extend(events)

            # 3. Tìm các sự kiện quan trọng từ danh sách tổng hợp
            departure_event = self._find_event(all_events, "Export Loaded on Vessel", pol)
            arrival_event_estimated = self._find_event(all_events, "Estimated Time of Arrival", pod)
            arrival_event_actual = self._find_event(all_events, "Discharged from vessel", pod) # MSC dùng 'Discharged from vessel'
            
            ata_transit_event = None
            atd_transit_event = None
            eta_transit_event = None
            etd_transit_event = None
            if transit_ports:
                # Lấy sự kiện đến tại cảng trung chuyển đầu tiên
                eta_transit_event = self._find_event(all_events, "Estimated Time of Arrival", transit_ports[0]) or self._find_event(all_events, "Discharged from vessel", transit_ports[0])
                # Lấy sự kiện rời đi từ cảng trung chuyển cuối cùng
                etd_transit_event = self._find_event(all_events, "Full Intended Transshipment", transit_ports[-1])
                


            # 4. Xây dựng đối tượng JSON
            #shipment_data = {
            #    "BookingNo": booking_no,
            #    "BlNumber": bl_number,
            #    "BookingStatus": None,
            #    "Pol": pol,
            #    "Pod": pod,
            #    "Etd": None,
            #    "Atd": self._format_date(departure_event.get("date")) if departure_event else None,
            #    "Eta": self._format_date(arrival_event_estimated.get("date")) if arrival_event_estimated else None,
            #    "Ata": self._format_date(arrival_event_actual.get("date")) if arrival_event_actual else None,
            #    "TransitPort": ", ".join(transit_ports) if transit_ports else None,
            #    "EtdTransit": self._format_date(etd_transit_event.get("date")) if etd_transit_event else None,
            #    "AtdTrasit": self._format_date(atd_transit_event.get("date")) if atd_transit_event else None,
            #    "EtaTransit": self._format_date(eta_transit_event.get("date")) if eta_transit_event else None,
            #    "AtaTrasit": self._format_date(ata_transit_event.get("date")) if ata_transit_event else None,
            #}
            
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no,
                BlNumber= bl_number,
                BookingStatus= None,
                Pol= pol,
                Pod= pod,
                Etd= None,
                Atd= self._format_date(departure_event.get("date")) if departure_event else None,
                Eta= self._format_date(arrival_event_estimated.get("date")) if arrival_event_estimated else None,
                Ata= self._format_date(arrival_event_actual.get("date")) if arrival_event_actual else None,
                TransitPort= ", ".join(transit_ports) if transit_ports else None,
                EtdTransit= self._format_date(etd_transit_event.get("date")) if etd_transit_event else None,
                AtdTransit= self._format_date(atd_transit_event.get("date")) if atd_transit_event else None,
                EtaTransit= self._format_date(eta_transit_event.get("date")) if eta_transit_event else None,
                AtaTransit= self._format_date(ata_transit_event.get("date")) if ata_transit_event else None
            )
            
            return shipment_data

        except Exception as e:
            print(f"Lỗi khi trích xuất và chuẩn hóa dữ liệu: {e}")
            traceback.print_exc()
            return None

    def _get_detail_value(self, section, heading_text):
        """Lấy giá trị từ một mục trong phần details."""
        try:
            return section.find_element(By.XPATH, f".//li[contains(., '{heading_text}')]/span[contains(@class, 'details-value')]").text
        except NoSuchElementException:
            return None

    def _extract_events_from_container(self, container_element):
        """Trích xuất lịch sử di chuyển cho một container cụ thể."""
        events = []
        steps = container_element.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__step")
        
        for step in steps:
            try:
                date = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--two .data-value").text
                description = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--four .data-value").text
                location = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--three .data-value").text
                
                events.append({
                    "date": date,
                    "description": description,
                    "location": location
                })
            except NoSuchElementException:
                continue
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """Tìm một sự kiện cụ thể trong danh sách, trả về dictionary của sự kiện đó."""
        if not events:
            return {}
            
        for event in events:
            desc = event.get("description", "").lower()
            loc = (event.get("location") or "").lower()
            
            desc_match = description_keyword.lower() in desc
            
            loc_match = True # Mặc định là true nếu không cần kiểm tra location
            if location_keyword:
                loc_match = location_keyword.lower() in loc

            if desc_match and loc_match:
                return event
        return {}