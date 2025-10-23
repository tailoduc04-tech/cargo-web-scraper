import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho module này
logger = logging.getLogger(__name__)

class TailwindScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web Tailwind Shipping.
    Sử dụng Selenium để trích xuất dữ liệu và chuẩn hóa theo
    schema N8nTrackingInfo.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi các định dạng ngày tháng khác nhau sang 'DD/MM/YYYY'.
        Ví dụ: '14-Oct-2025 06:51' hoặc 'ETD: 01/10/2025 11:18 am'
        """
        if not date_str or not isinstance(date_str, str):
            return None
        
        # Loại bỏ các tiền tố không cần thiết
        if "ETD:" in date_str: date_str = date_str.replace("ETD:", "").strip()
        if "ETA:" in date_str: date_str = date_str.replace("ETA:", "").strip()

        # Thử các định dạng phổ biến mà trang web này sử dụng
        for fmt in ('%d-%b-%Y %H:%M', '%d/%m/%Y %I:%M %p'):
            try:
                dt_obj = datetime.strptime(date_str, fmt)
                return dt_obj.strftime('%d/%m/%Y')
            except ValueError:
                continue
        
        logger.warning("Không thể phân tích định dạng ngày: %s. Trả về chuỗi gốc.", date_str)
        return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính.
        Thực hiện điều hướng, tìm kiếm, xử lý popup và trả về dữ liệu.
        """
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
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

            # 2. Nhập liệu và tìm kiếm (sử dụng selector từ HTML)
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "booking-number")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.search-icon")))
            self.driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].click();", search_button)
            logger.info("Đang tìm kiếm Booking Number: %s", tracking_number)

            # 3. Chờ và chuyển sang tab/window mới
            self.wait.until(EC.number_of_windows_to_be(2))
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    self.driver.switch_to.window(window_handle)
                    break
            logger.info("Đã chuyển sang tab kết quả.")
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.stepwizard")))
            logger.info("Trang kết quả đã tải thành công.")

            # 4. Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data()

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/tailwind_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"
        finally:
            try:
                for window_handle in self.driver.window_handles:
                    if window_handle != original_window:
                        self.driver.switch_to.window(window_handle)
                        self.driver.close()
                self.driver.switch_to.window(original_window)
                logger.info("Đã dọn dẹp và quay về tab gốc.")
            except Exception:
                logger.warning("Không thể dọn dẹp các cửa sổ phụ.")
            logger.info("Hoàn tất phiên scrape, quay về trang mặc định.")
            self.driver.get("about:blank") # Quay về trang trống để sẵn sàng cho lần scrape tiếp

    def _extract_and_normalize_data(self):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả của Tailwind.
        Hàm này sẽ lấy thông tin chung, sau đó mở popup 'View details'
        để lấy lịch sử di chuyển và xác định các ngày ATD, ATA, v.v.
        """
        try:
            # --- 1. Trích xuất thông tin chung (từ trang chính) ---
            booking_no = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_bkgno").text
            bl_no = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_blno").text
            booking_status = "" # Trang này không có trạng thái booking rõ ràng
            logger.info("BKG: %s, B/L: %s", booking_no, bl_no)

            # Lấy thông tin POL, POD và Transit từ step-wizard
            pol_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:first-child .txt_port_name")))
            pod_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:last-child .txt_port_name")))
            
            # Trang này dùng text rút gọn (NINGB..) nên ta sẽ lấy text đó
            pol = pol_element.text.replace("..", "").strip()
            pod = pod_element.text.replace("..", "").strip()
            logger.info("POL: %s, POD: %s", pol, pod)

            transit_elements = self.driver.find_elements(By.CSS_SELECTOR, ".stepwizard-step:not(:first-child):not(:last-child) .txt_port_name")
            transit_port_list = [el.text.replace("..", "").strip() for el in transit_elements]
            transit_ports_str = ", ".join(transit_port_list)
            
            # Lấy ETD (từ POL) và ETA (tới POD)
            eta_raw = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_eta").text
            # Loại bỏ phần (0d )
            if "(" in eta_raw:
                eta_raw = eta_raw.split("(")[0].strip()
            
            etd_raw = None # Không tìm thấy ETD trên trang chính

            # Khởi tạo các biến ngày tháng
            atd, ata, ata_transit, atd_transit, eta_transit = None, None, None, None, None
            # EtdTransit (future) không có trên trang này
            etd_transit = None

            # --- 2. Xử lý chi tiết từ popup container ---
            try:
                view_details_button = self.driver.find_element(By.CSS_SELECTOR, "button.view_details")
                self.driver.execute_script("arguments[0].click();", view_details_button)
                logger.info("Đã mở popup 'View details'.")
                
                # Chờ popup xuất hiện (dựa trên file HTML popup)
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container .timeline-small")))
                logger.info("Popup đã tải.")
                
                events = self._extract_events_from_popup()
                
                # Tìm các sự kiện quan trọng
                # ATD: Ngày "LOAD FULL" tại cảng POL
                departure_event = self._find_event(events, "LOAD FULL", pol)
                atd = departure_event.get("date") if departure_event else None

                # ATA: Ngày "DISCHARGE FULL" tại cảng POD
                arrival_event = self._find_event(events, "DISCHARGE FULL", pod)
                ata = arrival_event.get("date") if arrival_event else None
                
                if not ata:
                    # Thử tìm "UNLOAD" nếu "DISCHARGE FULL" không có
                    arrival_event_alt = self._find_event(events, "UNLOAD", pod)
                    ata = arrival_event_alt.get("date") if arrival_event_alt else None

                logger.info("ATD (từ popup): %s, ATA (từ popup): %s", atd, ata)

                # Xử lý transit
                if transit_port_list:
                    logger.info("Đang xử lý transit ports: %s", transit_ports_str)
                    
                    # AtaTransit: Actual arrival đầu tiên tại cảng transit
                    # (Tìm sự kiện DISCHARGE TRANSHIPMENT FULL tại cảng transit đầu tiên)
                    ata_transit_event = self._find_event(events, "DISCHARGE TRANSHIPMENT FULL", transit_port_list[0])
                    ata_transit = ata_transit_event.get("date") if ata_transit_event else None

                    # AtdTransit: Actual departure cuối cùng từ cảng transit
                    # (Tìm sự kiện LOAD TRANSHIPMENT FULL tại cảng transit cuối cùng)
                    atd_transit_event = self._find_event(events, "LOAD TRANSHIPMENT FULL", transit_port_list[-1])
                    atd_transit = atd_transit_event.get("date") if atd_transit_event else None
                    
                    # EtaTransit: Trang này không hiển thị ETA tại cảng transit
                    eta_transit = None
                
                logger.info("AtaTransit: %s, AtdTransit: %s", ata_transit, atd_transit)

                # Đóng popup
                close_button = self.driver.find_element(By.CSS_SELECTOR, ".fancybox-container .fancybox-close-small")
                close_button.click()
                self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container")))
                logger.info("Đã đóng popup.")

            except Exception as e:
                logger.error("Lỗi khi xử lý popup chi tiết container: %s", e, exc_info=True)
                # Tiếp tục mà không có dữ liệu từ popup

            # --- 3. Xây dựng đối tượng JSON cuối cùng ---
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no.strip(),
                BlNumber= bl_no.strip(),
                BookingStatus= booking_status.strip(),
                Pol= pol.strip(),
                Pod= pod.strip(),
                Etd= self._format_date(etd_raw) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta_raw) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= transit_ports_str,
                EtdTransit= self._format_date(etd_transit) or "",
                AtdTransit= self._format_date(atd_transit) or "",
                EtaTransit= self._format_date(eta_transit) or "",
                AtaTransit= self._format_date(ata_transit) or ""
            )
            
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            return shipment_data

        except Exception as e:
            logger.error("Lỗi trong quá trình trích xuất chi tiết: %s", e, exc_info=True)
            return None

    def _get_tooltip_text(self, element):
        """
        Helper để lấy text từ 'data-original-title' (nếu có),
        nếu không thì lấy .text
        (Lưu ý: file HTML được cung cấp không có data-original-title,
        nhưng mã nguồn cũ có, nên ta giữ lại logic này phòng trường hợp
        live site có thuộc tính đó.)
        """
        try:
            tooltip = element.get_attribute('data-original-title')
            if tooltip:
                return tooltip.strip()
        except:
            pass # Bỏ qua nếu không lấy được
        
        # Trả về text thông thường nếu không có tooltip
        return element.text.strip()

    def _extract_events_from_popup(self):
        """
        Trích xuất tất cả các sự kiện di chuyển từ popup 'View details'.
        """
        events = []
        try:
            # Tìm tất cả các khối 'media' trong popup
            movement_details = self.driver.find_elements(By.CSS_SELECTOR, ".fancybox-container .media")
            logger.info("Tìm thấy %d sự kiện trong popup.", len(movement_details))
            
            for detail in movement_details:
                try:
                    description = detail.find_element(By.CSS_SELECTOR, ".movement_title").text
                    date = detail.find_element(By.CSS_SELECTOR, ".date_track").text
                    # Dùng XPath để tìm span theo label đứng trước nó
                    location = detail.find_element(By.XPATH, ".//label[contains(text(), 'Activity Location:')]/following-sibling::span").text
                    
                    events.append({
                        "date": date.strip(),
                        "description": description.strip(),
                        "location": location.strip()
                    })
                except NoSuchElementException as inner_e:
                    logger.warning("Không thể trích xuất chi tiết từ một khối sự kiện: %s", inner_e)
                    continue
        except Exception as e:
            logger.error("Lỗi khi trích xuất sự kiện từ popup: %s", e, exc_info=True)
            
        return events
        
    def _find_event(self, events, description_keyword, location_keyword):
        """
        Tìm sự kiện đầu tiên khớp với TỪ KHÓA mô tả và TỪ KHÓA địa điểm.
        So sánh không phân biệt chữ hoa thường và kiểm tra 'contains' (chứa).
        """
        if not events or not location_keyword:
            return {}
        
        location_keyword_clean = location_keyword.lower().replace("..", "")
        description_keyword_lower = description_keyword.lower()
        
        for event in events:
            desc_match = description_keyword_lower in event.get("description", "").lower()
            loc_match = location_keyword_clean in event.get("location", "").lower()

            if desc_match and loc_match:
                logger.debug("Tìm thấy sự kiện khớp: %s tại %s", description_keyword, location_keyword)
                return event
                
        logger.debug("Không tìm thấy sự kiện khớp: %s tại %s", description_keyword, location_keyword)
        return {}