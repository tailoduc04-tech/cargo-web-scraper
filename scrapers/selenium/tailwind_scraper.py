import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time

from ..selenium_scraper import SeleniumScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho module này
logger = logging.getLogger(__name__)

class TailwindScraper(SeleniumScraper):
    # Triển khai logic scraping cho Tailwind Shipping. Sử dụng Selenium để trích xuất dữ liệu và chuẩn hóa.

    def _format_date(self, date_str):
        # Chuyển đổi các định dạng ngày tháng khác nhau sang 'DD/MM/YYYY'. Ví dụ: '14-Oct-2025 06:51' hoặc 'ETD: 01/10/2025 11:18 am'
        if not date_str or not isinstance(date_str, str):
            return None
        
        month_to_number = {
            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
        }
        
        # Thay thế tháng chữ bằng số
        for mon_str, mon_num in month_to_number.items():
            if mon_str in date_str:
                date_str = date_str.replace(mon_str, mon_num)
                break

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
        # Phương thức scrape chính. Thực hiện điều hướng, tìm kiếm, xử lý popup và trả về dữ liệu.
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time() # Tổng thời gian bắt đầu
        original_window = self.driver.current_window_handle

        try:
            t_nav_start = time.time()
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)
            logger.info("-> (Thời gian) Tải trang ban đầu: %.2fs", time.time() - t_nav_start)

            # 1. Xử lý cookie
            t_cookie_start = time.time()
            try:
                cookie_button = self.wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                self.driver.execute_script("arguments[0].click();", cookie_button)
                self.wait.until(EC.invisibility_of_element_located((By.ID, "onetrust-banner-sdk")))
                logger.info("-> Đã chấp nhận cookies. (Thời gian xử lý: %.2fs)", time.time() - t_cookie_start)
            except TimeoutException:
                logger.info("[Tailwind] Không tìm thấy banner cookie hoặc đã được chấp nhận. (Thời gian kiểm tra: %.2fs)", time.time() - t_cookie_start)


            # 2. Nhập liệu và tìm kiếm (sử dụng selector từ HTML)
            t_search_start = time.time()
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "booking-number")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.search-icon")))
            self.driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].click();", search_button)
            logger.info("Đang tìm kiếm Booking Number: %s. (Thời gian tìm kiếm: %.2fs)", tracking_number, time.time() - t_search_start)


            # 3. Chờ và chuyển sang tab/window mới
            t_wait_tab_start = time.time()
            self.wait.until(EC.number_of_windows_to_be(2))
            new_window = None
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    new_window = window_handle
                    self.driver.switch_to.window(window_handle)
                    break
            if new_window:
                 logger.info("Đã chuyển sang tab kết quả. (Thời gian chờ tab: %.2fs)", time.time() - t_wait_tab_start)
            else:
                 logger.warning("Không tìm thấy tab kết quả mới.")

            t_wait_result_start = time.time()
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.stepwizard")))
            logger.info("Trang kết quả đã tải thành công. (Thời gian chờ kết quả: %.2fs)", time.time() - t_wait_result_start)


            # 4. Trích xuất và chuẩn hóa dữ liệu
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(tracking_number) # Truyền tracking_number vào
            logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)


            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("Hoàn tất scrape thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/tailwind_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s (Tổng thời gian: %.2fs)",
                             tracking_number, screenshot_path, t_total_fail - t_total_start)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"
        finally:
            t_cleanup_start = time.time()
            try:
                # Đảm bảo đóng tab kết quả và quay về tab gốc
                current_handle = self.driver.current_window_handle
                if current_handle != original_window:
                    self.driver.close()
                    self.driver.switch_to.window(original_window)
                # Đóng các tab khác không phải tab gốc (nếu có lỗi mở thừa)
                for window_handle in self.driver.window_handles:
                    if window_handle != original_window:
                        try:
                            self.driver.switch_to.window(window_handle)
                            self.driver.close()
                        except: pass # Bỏ qua nếu tab đã bị đóng
                self.driver.switch_to.window(original_window)
                logger.info("Đã dọn dẹp và quay về tab gốc.")
            except Exception as e:
                logger.warning(f"Không thể dọn dẹp các cửa sổ phụ: {e}")
            logger.info("Hoàn tất phiên scrape, quay về trang mặc định. (Thời gian dọn dẹp: %.2fs)", time.time() - t_cleanup_start)
            try:
                # Chỉ điều hướng nếu driver chưa bị quit
                self.driver.get("about:blank") # Quay về trang trống
            except Exception:
                logger.info("Driver đã đóng, không thể điều hướng về about:blank.")


    def _extract_and_normalize_data(self, tracking_number): # Thêm tracking_number làm tham số
        # Trích xuất và chuẩn hóa dữ liệu từ trang kết quả của Tailwind. Lấy thông tin chung, sau đó mở popup 'View details' để lấy lịch sử di chuyển và xác định các ngày ATD, ATA, v.v.
        logger.info("--- Bắt đầu trích xuất chi tiết ---")
        t_extract_detail_start = time.time()
        try:
            # --- 1. Trích xuất thông tin chung (từ trang chính) ---
            t_basic_info_start = time.time()
            booking_no = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_bkgno").text
            bl_no = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_blno").text
            booking_status = "" # Trang này không có trạng thái booking rõ ràng
            logger.info("BKG: %s, B/L: %s", booking_no, bl_no)

            # Lấy thông tin POL, POD và Transit từ step-wizard
            pol_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:first-child .txt_port_name")))
            pod_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".stepwizard-step:last-child .txt_port_name")))

            pol = pol_element.text.replace("..", "").strip()
            pod = pod_element.text.replace("..", "").strip()
            logger.info("POL: %s, POD: %s", pol, pod)

            transit_elements = self.driver.find_elements(By.CSS_SELECTOR, ".stepwizard-step:not(:first-child):not(:last-child) .txt_port_name")
            transit_port_list = [el.text.replace("..", "").strip() for el in transit_elements]
            transit_ports_str = ", ".join(transit_port_list)

            # Lấy ETD (từ POL) và ETA (tới POD)
            eta_raw = self.driver.find_element(By.CSS_SELECTOR, ".txt_tra_data.mail_eta").text
            if "(" in eta_raw: eta_raw = eta_raw.split("(")[0].strip()

            etd_raw = None # Không tìm thấy ETD trên trang chính
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_basic_info_start)


            # Khởi tạo các biến ngày tháng
            atd, ata, ata_transit, atd_transit, eta_transit = None, None, None, None, None
            etd_transit = None # EtdTransit (future) không có trên trang này

            # --- 2. Xử lý chi tiết từ popup container ---
            t_popup_start = time.time()
            events = []
            try:
                view_details_button = self.driver.find_element(By.CSS_SELECTOR, "button.view_details")
                self.driver.execute_script("arguments[0].click();", view_details_button)
                logger.info("Đã mở popup 'View details'.")

                # Chờ popup xuất hiện
                t_wait_popup_start = time.time()
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container .timeline-small")))
                logger.info("Popup đã tải. (Thời gian chờ popup: %.2fs)", time.time() - t_wait_popup_start)

                t_extract_popup_start = time.time()
                events = self._extract_events_from_popup()
                logger.debug("-> (Thời gian) Trích xuất sự kiện từ popup: %.2fs", time.time() - t_extract_popup_start)

                # Tìm các sự kiện quan trọng
                t_find_events_start = time.time()
                departure_event = self._find_event(events, "LOAD FULL", pol)
                atd = departure_event.get("date") if departure_event else None

                arrival_event = self._find_event(events, "DISCHARGE FULL", pod)
                ata = arrival_event.get("date") if arrival_event else None
                if not ata:
                    arrival_event_alt = self._find_event(events, "UNLOAD", pod)
                    ata = arrival_event_alt.get("date") if arrival_event_alt else None

                logger.info("ATD (từ popup): %s, ATA (từ popup): %s", atd, ata)

                # Xử lý transit
                if transit_port_list:
                    logger.info("Đang xử lý transit ports: %s", transit_ports_str)
                    ata_transit_event = self._find_event(events, "DISCHARGE TRANSHIPMENT FULL", transit_port_list[0])
                    ata_transit = ata_transit_event.get("date") if ata_transit_event else None

                    atd_transit_event = self._find_event(events, "LOAD TRANSHIPMENT FULL", transit_port_list[-1])
                    atd_transit = atd_transit_event.get("date") if atd_transit_event else None

                    eta_transit = None # Trang này không hiển thị ETA tại cảng transit
                logger.info("AtaTransit: %s, AtdTransit: %s", ata_transit, atd_transit)
                logger.debug("-> (Thời gian) Tìm sự kiện chính và transit: %.2fs", time.time() - t_find_events_start)


                # Đóng popup
                t_close_popup_start = time.time()
                close_button = self.driver.find_element(By.CSS_SELECTOR, ".fancybox-container .fancybox-close-small")
                close_button.click()
                self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".fancybox-container")))
                logger.info("Đã đóng popup. (Thời gian đóng: %.2fs)", time.time() - t_close_popup_start)


            except Exception as e:
                logger.error("Lỗi khi xử lý popup chi tiết container: %s", e, exc_info=True)
                # Tiếp tục mà không có dữ liệu từ popup
            logger.debug("-> (Thời gian) Xử lý popup: %.2fs", time.time() - t_popup_start)


            # --- 3. Xây dựng đối tượng JSON cuối cùng ---
            t_normalize_start = time.time()
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
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("--- Hoàn tất trích xuất chi tiết --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)

            return shipment_data

        except Exception as e:
            logger.error("Lỗi trong quá trình trích xuất chi tiết: %s", e, exc_info=True)
            logger.info("--- Hoàn tất trích xuất chi tiết (lỗi) --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return None

    def _get_tooltip_text(self, element):
        # Helper để lấy text từ 'data-original-title' (nếu có), nếu không thì lấy .text
        try:
            tooltip = element.get_attribute('data-original-title')
            if tooltip:
                return tooltip.strip()
        except:
            pass # Bỏ qua nếu không lấy được
        return element.text.strip()

    def _extract_events_from_popup(self):
        # Trích xuất tất cả các sự kiện di chuyển từ popup 'View details'.
        events = []
        try:
            movement_details = self.driver.find_elements(By.CSS_SELECTOR, ".fancybox-container .media")
            logger.info("--> Tìm thấy %d sự kiện trong popup.", len(movement_details))

            for detail in movement_details:
                try:
                    description = detail.find_element(By.CSS_SELECTOR, ".movement_title").text
                    date = detail.find_element(By.CSS_SELECTOR, ".date_track").text
                    location = detail.find_element(By.XPATH, ".//label[contains(text(), 'Activity Location:')]/following-sibling::span").text

                    event_data = {
                        "date": date.strip(),
                        "description": description.strip(),
                        "location": location.strip()
                    }
                    events.append(event_data)
                    logger.debug("---> Trích xuất event: %s", event_data)
                except NoSuchElementException as inner_e:
                    logger.warning("---> Không thể trích xuất chi tiết từ một khối sự kiện: %s", inner_e)
                    continue
        except Exception as e:
            logger.error("--> Lỗi khi trích xuất sự kiện từ popup: %s", e, exc_info=True)

        return events

    def _find_event(self, events, description_keyword, location_keyword):
        # Tìm sự kiện đầu tiên khớp với từ khóa mô tả và từ khóa địa điểm. So sánh không phân biệt chữ hoa thường và kiểm tra 'contains'.
        if not events or not location_keyword:
             logger.debug("--> _find_event: Danh sách sự kiện rỗng hoặc thiếu location_keyword.")
             return {}

        location_keyword_clean = location_keyword.lower().replace("..", "")
        description_keyword_lower = description_keyword.lower()
        logger.debug("--> _find_event: Tìm '%s' tại '%s'", description_keyword_lower, location_keyword_clean)


        for event in events:
            desc_match = description_keyword_lower in event.get("description", "").lower()
            loc_match = location_keyword_clean in event.get("location", "").lower()

            if desc_match and loc_match:
                logger.debug("---> Khớp: %s", event)
                return event

        logger.debug("---> Không khớp.")
        return {}