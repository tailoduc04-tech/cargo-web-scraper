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
    Sử dụng phương pháp truy cập URL trực tiếp và chuẩn hóa kết quả
    theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD Mon YYYY HH:MM' sang 'DD/MM/YYYY'.
        Ví dụ: '24 Oct 2025 09:00' -> '24/10/2025'
        """
        if not date_str:
            return None
        try:
            # Tách chuỗi để xử lý các định dạng có thể có
            clean_date_str = date_str.split('(')[0].strip()
            # Định dạng chính là 'DD Mon YYYY HH:MM'
            dt_obj = datetime.strptime(clean_date_str, '%d %b %Y %H:%M')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            # Thử định dạng không có giờ
            try:
                dt_obj = datetime.strptime(clean_date_str, '%d %b %Y')
                return dt_obj.strftime('%d/%m/%Y')
            except (ValueError, IndexError):
                return date_str # Trả về chuỗi gốc nếu không phân tích được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính, truy cập URL trực tiếp và trả về một dictionary JSON duy nhất.
        """
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            print(f"Accessing direct URL: {direct_url}")
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 45) # Tăng thời gian chờ

            # 1. Xử lý cookie nếu có
            try:
                allow_all_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'coi-banner__accept') and contains(., 'Allow all')]"))
                )
                self.driver.execute_script("arguments[0].click();", allow_all_button)
                self.wait.until(EC.invisibility_of_element_located((By.ID, "coiOverlay")))
            except TimeoutException:
                print("Cookie banner not found or already accepted.")
            
            # 2. Chờ trang kết quả tải
            try:
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))

                # 3. Trích xuất và chuẩn hóa dữ liệu
                normalized_data = self._extract_and_normalize_data(tracking_number)
                
                if not normalized_data:
                    return None, f"Could not extract normalized data for '{tracking_number}'."

                return normalized_data, None

            except TimeoutException:
                # Kiểm tra xem có phải lỗi do tracking number sai không
                try:
                    error_script = "return document.querySelector('mc-input[data-test=\"track-input\"]').shadowRoot.querySelector('.mds-helper-text--negative').textContent"
                    error_message = self.driver.execute_script(error_script)
                    if error_message and "Incorrect format" in error_message:
                        return None, f"Không tìm thấy kết quả cho '{tracking_number}': {error_message.strip()}"
                except Exception:
                    pass
                raise TimeoutException("Results page did not load.")

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/maersk_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu thành một dictionary duy nhất
        dựa trên logic hoạt động cũ.
        """
        # 1. Trích xuất thông tin tóm tắt chung
        summary_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))
        
        try:
            pol = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-from-value']").text
        except NoSuchElementException:
            pol = None
        
        try:
            pod = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-to-value']").text
        except NoSuchElementException:
            pod = None
        
        # 2. Mở rộng và thu thập tất cả các sự kiện từ tất cả các container
        all_events = []
        containers = self.driver.find_elements(By.CSS_SELECTOR, "div.container--ocean")
        
        for container in containers:
            try:
                toggle_button_host = container.find_element(By.CSS_SELECTOR, "mc-button[data-test='container-toggle-details']")
                if toggle_button_host.get_attribute("aria-expanded") == 'false':
                    button_to_click = self.driver.execute_script("return arguments[0].shadowRoot.querySelector('button')", toggle_button_host)
                    self.driver.execute_script("arguments[0].click();", button_to_click)
                    WebDriverWait(self.driver, 10).until(
                        lambda d: toggle_button_host.get_attribute("aria-expanded") == 'true'
                    )
                    time.sleep(0.5)
            except (NoSuchElementException, TimeoutException):
                pass
            
            events = self._extract_events_from_container(container)
            all_events.extend(events)

        # 3. Tìm các sự kiện quan trọng và cảng trung chuyển từ danh sách tổng hợp
        departure_event_actual = self._find_event(all_events, "Vessel departure", pol, event_type="ngay_thuc_te")
        departure_event_estimated = self._find_event(all_events, "Vessel departure", pol, event_type="ngay_du_kien")
        arrival_event_actual = self._find_event(all_events, "Vessel arrival", pod, event_type="ngay_thuc_te")
        arrival_event_estimated = self._find_event(all_events, "Vessel arrival", pod, event_type="ngay_du_kien")

        transit_ports = []
        for event in all_events:
            location = event.get('location', '').strip() if event.get('location') else ''
            desc = event.get('description', '').lower()
            if location and pol not in location and pod not in location:
                if "arrival" in desc or "departure" in desc:
                    if location not in transit_ports:
                        transit_ports.append(location)

        # *** LOGIC CẬP NHẬT ĐỂ LẤY NGÀY TRUNG CHUYỂN ***
        etd_transit, atd_transit, eta_transit, ata_transit = None, None, None, None
        if transit_ports:
            # Lấy ngày đến tại cảng trung chuyển đầu tiên
            ata_transit_event = self._find_event(all_events, "Vessel arrival", transit_ports[0], event_type="ngay_thuc_te")
            eta_transit_event = self._find_event(all_events, "Vessel arrival", transit_ports[0], event_type="ngay_du_kien")
            ata_transit = ata_transit_event.get('date') if ata_transit_event else None
            eta_transit = eta_transit_event.get('date') if eta_transit_event else None

            # Lấy ngày đi từ cảng trung chuyển cuối cùng
            atd_transit_event = self._find_event(all_events, "Vessel departure", transit_ports[-1], event_type="ngay_thuc_te")
            etd_transit_event = self._find_event(all_events, "Vessel departure", transit_ports[-1], event_type="ngay_du_kien")
            atd_transit = atd_transit_event.get('date') if atd_transit_event else None
            etd_transit = etd_transit_event.get('date') if etd_transit_event else None
        # *** KẾT THÚC LOGIC CẬP NHẬT ***

        # 4. Xây dựng đối tượng JSON cuối cùng
        shipment_data = {
            "BookingNo": tracking_number,
            "BlNumber": tracking_number,
            "BookingStatus": None,
            "Pol": pol,
            "Pod": pod,
            "Etd": self._format_date(departure_event_estimated.get('date')) if departure_event_estimated else None,
            "Atd": self._format_date(departure_event_actual.get('date')) if departure_event_actual else None,
            "Eta": self._format_date(arrival_event_estimated.get('date')) if arrival_event_estimated else None,
            "Ata": self._format_date(arrival_event_actual.get('date')) if arrival_event_actual else None,
            "TransitPort": ", ".join(transit_ports) if transit_ports else None,
            "EtdTransit": self._format_date(etd_transit),
            "AtdTrasit": self._format_date(atd_transit),
            "EtaTransit": self._format_date(eta_transit),
            "AtaTrasit": self._format_date(ata_transit)
        }
        
        return shipment_data

    def _extract_events_from_container(self, container_element):
        """
        Trích xuất lịch sử sự kiện từ một khối container (Transport Plan).
        """
        events = []
        try:
            transport_plan = container_element.find_element(By.CSS_SELECTOR, ".transport-plan__list")
            list_items = transport_plan.find_elements(By.CSS_SELECTOR, "li.transport-plan__list__item")
            last_location = None
            for item in list_items:
                event_data = {}
                try:
                    location_text = item.find_element(By.CSS_SELECTOR, "div.location").text
                    event_data['location'] = location_text.split('\n')[0].strip()
                    last_location = location_text
                except NoSuchElementException: 
                    event_data['location'] = last_location
                
                milestone_div = item.find_element(By.CSS_SELECTOR, "div.milestone")
                milestone_lines = milestone_div.text.split('\n')
                
                event_data['description'] = milestone_lines[0] if len(milestone_lines) > 0 else None
                event_data['date'] = milestone_lines[1] if len(milestone_lines) > 1 else None
                event_data['type'] = "ngay_du_kien" if "future" in item.get_attribute("class") else "ngay_thuc_te"
                events.append(event_data)
        except NoSuchElementException:
            print("Could not find transport plan for a container.")
        return events
    
    def _find_event(self, events, description_keyword, location_keyword, event_type=None):
        """
        Tìm một sự kiện cụ thể, có thể lọc theo loại (thực tế/dự kiến).
        """
        if not location_keyword: return {}
        
        for event in reversed(events):
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            event_location = event.get("location") or ""
            loc_match = location_keyword.lower() in event_location.lower()
            
            type_match = True
            if event_type:
                type_match = (event.get("type") == event_type)

            if desc_match and loc_match and type_match:
                return event
        return {}