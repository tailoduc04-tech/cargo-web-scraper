import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time  # <--- Tớ đã thêm module time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module này
logger = logging.getLogger(__name__)

class EmcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Evergreen (EMC)
    và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'MON-DD-YYYY' (ví dụ: SEP-21-2025) sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Đảm bảo tháng viết hoa chữ cái đầu để parse
            date_str_title = date_str.strip().title()
            # Định dạng của EMC là %b-%d-%Y (ví dụ: Sep-21-2025)
            dt_obj = datetime.strptime(date_str_title, '%b-%d-%Y')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scraping chính cho Evergreen.
        """
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time() # Tổng thời gian bắt đầu
        main_window = self.driver.current_window_handle
        try:
            t_nav_start = time.time()
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)
            logger.info("-> (Thời gian) Tải trang: %.2fs", time.time() - t_nav_start)
            
            t_cookie_start = time.time()
            try:
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btn_cookie_accept_all"))
                )
                cookie_button.click()
                logger.info("-> Đã chấp nhận cookies. (Thời gian xử lý: %.2fs)", time.time() - t_cookie_start)
            except TimeoutException:
                logger.info("-> Banner cookie không xuất hiện. (Thời gian kiểm tra: %.2fs)", time.time() - t_cookie_start)

            # --- 1. Thực hiện tìm kiếm ---
            logger.info("-> Điền thông tin tìm kiếm...")
            t_search_start = time.time()
            bl_radio = self.wait.until(EC.element_to_be_clickable((By.ID, "s_bl")))
            self.driver.execute_script("arguments[0].click();", bl_radio)

            search_input = self.driver.find_element(By.ID, "NO")
            search_input.clear()
            search_input.send_keys(tracking_number)

            submit_button = self.driver.find_element(By.CSS_SELECTOR, "#nav-quick > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(1) > td.ec-text-start > table > tbody > tr > td > div:nth-child(2) > input")
            submit_button.click()
            logger.info("-> (Thời gian) Gửi form tìm kiếm: %.2fs", time.time() - t_search_start)

            # --- 2. Chờ trang kết quả và trích xuất dữ liệu ---
            logger.info("-> Chờ trang kết quả tải...")
            t_wait_result_start = time.time()
            # Đợi một phần tử đáng tin cậy trên trang kết quả xuất hiện
            self.wait.until(EC.visibility_of_element_located((By.XPATH, "//th[contains(text(), 'B/L No.')]")))
            logger.info("-> Trang kết quả đã tải. (Thời gian chờ: %.2fs)", time.time() - t_wait_result_start)
            
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(tracking_number, main_window)
            logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("-> Hoàn tất scrape thành công cho mã %s. (Tổng thời gian: %.2fs)", 
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/emc_timeout_{tracking_number}_{timestamp}.png"
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
            # Đảm bảo quay về cửa sổ chính
            try:
                if self.driver.current_window_handle != main_window:
                    self.driver.close()
                    self.driver.switch_to.window(main_window)
            except Exception as switch_err:
                 logger.error("Lỗi khi chuyển về cửa sổ chính: %s", switch_err)


    def _extract_events_from_popup(self):
        """
        Trích xuất lịch sử di chuyển từ cửa sổ popup của container.
        """
        events = []
        try:
            # Chờ bảng trong popup xuất hiện (dựa trên ảnh, bảng có vẻ là `ec-table`)
            event_table = self.wait.until(EC.visibility_of_element_located((By.XPATH, "//td[contains(text(), 'Container Moves')]/ancestor::table")))
            # Tìm tất cả các hàng trong tbody (bỏ qua hàng header)
            rows = event_table.find_elements(By.XPATH, ".//tr[td]")
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3: # Cần ít nhất 3 cột: Date, Moves, Location
                    events.append({
                        "date": cells[0].text.strip(),
                        "description": cells[1].text.strip(),
                        "location": cells[2].text.strip(),
                    })
            logger.info(f"-> Đã trích xuất được {len(events)} sự kiện từ popup.")
        except (NoSuchElementException, TimeoutException):
            logger.warning("Không tìm thấy bảng sự kiện trong popup hoặc popup không tải kịp.")
        return events

    def _find_event_by_keywords(self, events, desc_keyword, loc_keyword):
        """
        Tìm một sự kiện cụ thể trong danh sách, trả về sự kiện đầu tiên khớp.
        """
        if not events or not loc_keyword:
            return {}
        
        # Làm sạch loc_keyword để so sánh (ví dụ: "HAIPHONG, VIETNAM (VN)" -> "HAIPHONG")
        simple_loc_keyword = loc_keyword.split(",")[0].strip().lower()

        for event in events:
            desc_match = desc_keyword.lower() in event.get("description", "").lower()
            
            # So sánh địa điểm đã làm sạch
            event_loc = event.get("location", "")
            simple_event_loc = event_loc.split(",")[0].strip().lower()
            loc_match = simple_loc_keyword in simple_event_loc

            if desc_match and loc_match:
                return event
        return {}
    
    def _parse_sortable_date(self, date_str):
        """Helper: Chuyển ngày 'Sep-21-2025' thành đối tượng datetime để sort."""
        if not date_str:
            return datetime.min
        try:
            return datetime.strptime(date_str.strip().title(), '%b-%d-%Y')
        except ValueError:
            logger.warning("Không thể parse ngày để sort: %s", date_str)
            return datetime.min # Đẩy các ngày lỗi về đầu

    def _extract_and_normalize_data(self, tracking_number, main_window):
        """
        Hàm chính để trích xuất dữ liệu từ trang kết quả và các popup.
        """
        try:
            # --- LẤY THÔNG TIN CƠ BẢN TỪ TRANG CHÍNH ---
            bl_number = self.driver.find_element(By.XPATH, "//th[contains(text(), 'B/L No.')]/following-sibling::td").text.strip()
            pol = self.driver.find_element(By.XPATH, "//th[contains(text(), 'Port of Loading')]/following-sibling::td").text.strip()
            pod = self.driver.find_element(By.XPATH, "//th[contains(text(), 'Port of Discharge')]/following-sibling::td").text.strip()
            etd_str = self.driver.find_element(By.XPATH, "//th[contains(text(), 'Estimated On Board Date')]/following-sibling::td").text.strip()
            eta_str = self.driver.find_element(By.XPATH, "//td[contains(., 'Estimated Date of Arrival at Destination')]/font").text.strip()
            
            logger.info(f"Thông tin cơ bản: B/L={bl_number}, POL={pol}, POD={pod}, ETD={etd_str}, ETA={eta_str}")

            # --- LẤY SỰ KIỆN TỪ 1 CONTAINER ĐẦU TIÊN ---
            all_events = []
            container_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'frmCntrMoveDetail')]")
            
            # --- THAY ĐỔI THEO YÊU CẦU ---
            logger.info(f"Tìm thấy {len(container_links)} container. Sẽ chỉ xử lý 1 container đầu tiên theo yêu cầu.")
            
            # Chỉ lặp qua 1 link đầu tiên
            for link in container_links[:1]: 
            # --- KẾT THÚC THAY ĐỔI ---
                try:
                    container_no = link.text.strip()
                    logger.info(f"Đang xử lý container: {container_no}")
                    link.click()
                    
                    # Chờ cửa sổ mới mở ra và chuyển sang nó
                    self.wait.until(EC.number_of_windows_to_be(2))
                    new_window = [window for window in self.driver.window_handles if window != main_window][0]
                    self.driver.switch_to.window(new_window)
                    logger.debug("-> Đã chuyển sang cửa sổ popup.")

                    events = self._extract_events_from_popup()
                    all_events.extend(events)

                    # Đóng cửa sổ popup và quay lại cửa sổ chính
                    self.driver.close()
                    logger.debug("-> Đã đóng popup.")
                    self.driver.switch_to.window(main_window)
                    logger.debug("-> Đã chuyển về cửa sổ chính.")
                except Exception as e:
                    logger.warning(f"Không thể xử lý popup cho container. Lỗi: {e}", exc_info=True)
                    # Cố gắng quay lại cửa sổ chính nếu có lỗi
                    if self.driver.current_window_handle != main_window:
                        try:
                            self.driver.close()
                        except: pass
                        self.driver.switch_to.window(main_window)

            logger.info(f"Tổng cộng đã thu thập được {len(all_events)} sự kiện (từ 1 container).")
            if not all_events:
                 logger.warning("Không thu thập được sự kiện nào từ các popup.")

            # --- TÌM CÁC SỰ KIỆN QUAN TRỌNG ---
            # Thêm "parsed_date" vào mỗi sự kiện và sort
            sorted_events = sorted(
                [{**event, "parsed_date": self._parse_sortable_date(event.get("date"))} for event in all_events],
                key=lambda x: x["parsed_date"]
            )
            
            atd_event = self._find_event_by_keywords(sorted_events, "Loaded", pol)
            ata_event = self._find_event_by_keywords(sorted_events, "Discharged", pod)

            # --- TÌM THÔNG TIN TRUNG CHUYỂN ---
            transit_ports = []
            ata_transit_event = None
            atd_transit_event = None
            
            # Đơn giản hóa POL/POD để so sánh
            simple_pol = pol.split(",")[0].strip().lower()
            simple_pod = pod.split(",")[0].strip().lower()

            for event in sorted_events:
                loc = event.get("location", "")
                simple_loc = loc.split(",")[0].strip().lower()
                desc = event.get("description", "").lower()
                
                # Bỏ qua nếu không có địa điểm hoặc là POL/POD
                if not simple_loc or simple_loc == simple_pol or simple_loc == simple_pod:
                    continue
                
                # Sự kiện dỡ hàng tại cảng transit
                if "discharged" in desc:
                    if loc not in transit_ports: 
                        transit_ports.append(loc)
                    if not ata_transit_event: # Chỉ lấy sự kiện đầu tiên
                        ata_transit_event = event
                        logger.info(f"Tìm thấy sự kiện AtaTransit: {event}")
                
                # Sự kiện xếp hàng lên tàu transit
                if "loaded on outbound vessel" in desc or "transship container loaded" in desc:
                    if loc not in transit_ports: 
                        transit_ports.append(loc)
                    atd_transit_event = event # Lấy sự kiện cuối cùng
                    logger.info(f"Tìm thấy sự kiện AtdTransit: {event}")
            
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= bl_number,
                BookingStatus= "", 
                Pol= pol,
                Pod= pod,
                Etd= self._format_date(etd_str) or "",
                Atd= self._format_date(atd_event.get("date")) if atd_event else "",
                Eta= self._format_date(eta_str) or "",
                Ata= self._format_date(ata_event.get("date")) if ata_event else "",
                TransitPort= ", ".join(transit_ports) if transit_ports else "",
                EtdTransit= "",
                AtdTransit= self._format_date(atd_transit_event.get("date")) if atd_transit_event else "",
                EtaTransit= "",
                AtaTransit= self._format_date(ata_transit_event.get("date")) if ata_transit_event else ""
            )
            
            return shipment_data
        except Exception as e:
            logger.error("Lỗi nghiêm trọng trong quá trình trích xuất: %s", e, exc_info=True)
            return None