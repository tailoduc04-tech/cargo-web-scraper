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
    Triển khai logic scraping cụ thể cho trang web Tailwind Shipping.
    Đầu ra được chuẩn hóa theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi các định dạng ngày tháng khác nhau sang 'DD/MM/YYYY'.
        Ví dụ: '14-Oct-2025 06:51' hoặc 'ETD: 01/10/2025 11:18 am'
        """
        if not date_str:
            return None
        
        # Loại bỏ các tiền tố không cần thiết
        if "ETD:" in date_str: date_str = date_str.replace("ETD:", "").strip()
        if "ETA:" in date_str: date_str = date_str.replace("ETA:", "").strip()

        # Thử các định dạng phổ biến
        for fmt in ('%d-%b-%Y %H:%M', '%d/%m/%Y %I:%M %p'):
            try:
                dt_obj = datetime.strptime(date_str, fmt)
                return dt_obj.strftime('%d/%m/%Y')
            except ValueError:
                continue
        return date_str # Trả về chuỗi gốc nếu không parse được

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
            except TimeoutException:
                print("[Tailwind] Không tìm thấy banner cookie hoặc đã được chấp nhận.")

            # 2. Nhập liệu và tìm kiếm
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "booking-number")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.search-icon")))
            self.driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].click();", search_button)
            print(f"[Tailwind] Đang tìm kiếm Booking Number: {tracking_number}")

            # 3. Chờ và chuyển sang tab mới
            self.wait.until(EC.number_of_windows_to_be(2))
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    self.driver.switch_to.window(window_handle)
                    break
            print("[Tailwind] Đã chuyển sang tab kết quả.")

            # 4. Đợi trang kết quả tải và trích xuất dữ liệu
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.stepwizard")))
            print("[Tailwind] Đã tải trang kết quả.")
            time.sleep(2)

            normalized_data = self._extract_and_normalize_data()

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            # Trả về dictionary duy nhất theo yêu cầu
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/tailwind_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"[Tailwind] Timeout. Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"[Tailwind] Lỗi: Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'. Trang web có thể đang chậm hoặc mã không hợp lệ."
        except Exception as e:
            print(f"[Tailwind] Lỗi không mong muốn: {e}")
            traceback.print_exc()
            return None, f"Lỗi không mong muốn cho '{tracking_number}': {e}"
        finally:
            # Dọn dẹp: Đóng các tab không cần thiết và quay về tab gốc
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    self.driver.switch_to.window(window_handle)
                    self.driver.close()
            self.driver.switch_to.window(original_window)
            print("[Tailwind] Đã dọn dẹp và quay về tab gốc.")

    def _extract_and_normalize_data(self):
        # --- 1. Trích xuất thông tin chung ---
        booking_no = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_bkgno").text
        bl_no = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_blno").text
        
        pol_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:first-child .txt_port_name")))
        pod_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:last-child .txt_port_name")))
        
        pol = self._get_tooltip_text(pol_element)
        pod = self._get_tooltip_text(pod_element)

        transit_elements = self.driver.find_elements(By.CSS_SELECTOR, ".stepwizard-step:not(:first-child):not(:last-child) .txt_port_name")
        transit_ports = [self._get_tooltip_text(el) for el in transit_elements]

        etd_element = self.driver.find_element(By.CSS_SELECTOR, ".stepwizard-step:first-child .tracker_icon")
        etd_raw = self._get_tooltip_text(etd_element)

        eta_element = self.driver.find_element(By.CSS_SELECTOR, ".stepwizard-step:last-child .tracker_icon")
        eta_raw = self._get_tooltip_text(eta_element)

        # --- 2. Xử lý chi tiết từ container đầu tiên ---
        # Giả định thông tin từ container đầu tiên là đại diện cho cả lô hàng
        first_container_row = self.driver.find_element(By.CSS_SELECTOR, "#datatablebytrack tbody tr:not(.mailcontent)")
        
        atd, ata, ata_transit, atd_transit = None, None, None, None
        
        try:
            view_details_button = first_container_row.find_element(By.CSS_SELECTOR, "button.view_details")
            self.driver.execute_script("arguments[0].click();", view_details_button)
            
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container .timeline-small")))
            
            events = self._extract_events_from_popup()
            
            # Tìm các sự kiện quan trọng
            departure_event = self._find_event(events, "LOAD FULL", pol)
            arrival_event = self._find_event(events, "DISCHARGE FULL", pod)
            atd = departure_event.get("date") if departure_event else None
            ata = arrival_event.get("date") if arrival_event else None

            if transit_ports:
                # Actual arrival at the first transit port
                ata_transit_event = self._find_event(events, "DISCHARGE TRANSHIPMENT FULL", transit_ports[0])
                ata_transit = ata_transit_event.get("date") if ata_transit_event else None
                # Actual departure from the last transit port
                atd_transit_event = self._find_event(events, "LOAD TRANSHIPMENT FULL", transit_ports[-1])
                atd_transit = atd_transit_event.get("date") if atd_transit_event else None

            # Đóng popup
            close_button = self.driver.find_element(By.CSS_SELECTOR, ".fancybox-container .fancybox-close-small")
            close_button.click()
            self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container")))

        except Exception as e:
            print(f"[Tailwind] Lỗi khi xử lý chi tiết container: {e}")
            traceback.print_exc()

        # --- 3. Xây dựng đối tượng JSON cuối cùng ---
        shipment_data = {
            "BookingNo": booking_no,
            "BlNumber": bl_no,
            "BookingStatus": None,
            "Pol": pol,
            "Pod": pod,
            "Etd": self._format_date(etd_raw),
            "Atd": self._format_date(atd),
            "Eta": self._format_date(eta_raw),
            "Ata": self._format_date(ata),
            "TransitPort": ", ".join(transit_ports) if transit_ports else None,
            "EtdTransit": None,
            "AtdTrasit": self._format_date(atd_transit),
            "EtaTransit": None,
            "AtaTrasit": self._format_date(ata_transit)
        }
                
        return shipment_data

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
                    "description": description,
                    "location": location
                })
            except NoSuchElementException:
                continue
        return events
        
    def _find_event(self, events, description_keyword, location_keyword):
        if not events or not location_keyword:
            return {}
        
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = location_keyword.lower() in event.get("location", "").lower()

            if desc_match and loc_match:
                return event
        return {}