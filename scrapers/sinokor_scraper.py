import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
import traceback
import logging

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho mô-đun này
logger = logging.getLogger(__name__)

class SinokorScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Sinokor và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD ...' (ví dụ: '2025-09-08 MON 05:00')
        sang 'DD/MM/YYYY'.
        """
        if not date_str:
            return ""
        try:
            # Chỉ lấy phần ngày tháng năm, bỏ qua thông tin khác
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể format ngày: '%s'. Trả về chuỗi rỗng.", date_str)
            return ""

    def _parse_event_datetime(self, date_str):
        """
        Helper: Chuyển đổi chuỗi ngày sự kiện (ví dụ: '2025-09-08 MON 05:00')
        thành đối tượng datetime để so sánh.
        """
        if not date_str:
            return None, None
        try:
            # Lấy phần 'YYYY-MM-DD HH:MM'
            date_part = " ".join(date_str.split(" ")[0:2])
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d %H:%M')
            return dt_obj, date_str
        except (ValueError, IndexError):
            logger.warning("Không thể parse event datetime: '%s'", date_str, exc_info=True)
            return None, None

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã B/L trên trang Sinokor.
        URL được xây dựng động dựa trên mã tracking.
        """
        logger.info("Bắt đầu scrape cho mã Sinokor: %s", tracking_number)
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            # Chờ cho panel schedule xuất hiện để chắc chắn trang đã tải
            logger.debug("Đang chờ panel 'divSchedule' xuất hiện...")
            self.wait.until(EC.visibility_of_element_located((By.ID, "divSchedule")))
            logger.info("Trang đã tải thành công. Bắt đầu trích xuất dữ liệu.")
            
            # Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                logger.warning("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/sinokor_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'. Trang web có thể đang chậm hoặc mã không hợp lệ."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn cho mã '%s': %s", tracking_number, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Hàm chính để trích xuất, xử lý và chuẩn hóa dữ liệu từ trang chi tiết.
        """
        try:
            today_dt = datetime.now()
            
            # 1. Trích xuất thông tin chung từ các panel
            logger.debug("Trích xuất thông tin B/L No và B/K Status...")
            bl_no = self._get_text_from_element(By.XPATH, "//label[contains(text(), 'B/L No.')]/../../div[contains(@class, 'font-bold')]/span")
            booking_status = self._get_text_from_element(By.XPATH, "//label[contains(text(), 'B/K Status')]/../../div[contains(@class, 'font-bold')]/span")
            
            # 2. Trích xuất thông tin Schedule (chỉ chứa ETD và ETA)
            logger.debug("Trích xuất thông tin Schedule (ETD, ETA)...")
            schedule_panel = self.wait.until(EC.presence_of_element_located((By.ID, "divSchedule")))
            etd_str = self._get_text_from_element(By.CSS_SELECTOR, "li.col-sm-8 .col-sm-6:nth-child(1)", parent=schedule_panel)
            eta_str = self._get_text_from_element(By.CSS_SELECTOR, "li.col-sm-8 .col-sm-6:nth-child(2)", parent=schedule_panel)
            
            pol, etd = split_location_and_datetime(etd_str)
            pod, eta = split_location_and_datetime(eta_str)
            
            logger.info("Thông tin Schedule: POL=%s, POD=%s, ETD=%s, ETA=%s", pol, pod, etd, eta)

            # 3. Mở rộng bảng chi tiết Cargo Tracking
            logger.debug("Mở rộng panel Cargo Tracking...")
            cargo_tracking_panel_selector = "#wrapper > div > div > div:nth-child(5).panel.hpanel"
            cargo_tracking_panel = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, cargo_tracking_panel_selector)))
            try:
                toggle_button = cargo_tracking_panel.find_element(By.ID, "tglDetailInfo")
                if 'fa-chevron-down' in toggle_button.get_attribute('class'):
                     self.driver.execute_script("arguments[0].click();", toggle_button)
                     self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, f"{cargo_tracking_panel_selector} #divDetailInfo div.splitTable")))
                     logger.info("Đã mở rộng panel Cargo Tracking.")
            except Exception:
                logger.info("Panel Cargo Tracking đã mở sẵn hoặc không thể nhấn nút.")
                pass

            # 4. Trích xuất lịch sử sự kiện và tìm các ngày thực tế / transit
            history_events = self._extract_history_events(cargo_tracking_panel)
            
            atd = None
            ata = None
            transit_port_list = []
            eta_transit = None
            ata_transit = None
            atd_transit = None
            future_etd_transits = [] # (datetime, date_str)

            # Tìm sự kiện Departure tại POL và Arrival tại POD
            atd_event = self._find_event(history_events, "Departure", pol)
            pod_arrival_event = self._find_event(history_events, "Arrival", pod)

            # Xử lý ATD (so sánh với ngày hiện tại)
            if atd_event:
                atd_dt, atd_str_full = self._parse_event_datetime(atd_event.get("date"))
                if atd_dt and atd_dt <= today_dt:
                    atd = atd_str_full # Đây là ngày Actual
                    logger.info("Tìm thấy ATD (Actual): %s", atd)
                elif atd_dt:
                    etd = atd_str_full # Đây là ngày Estimated mới, cập nhật ETD
                    logger.info("Cập nhật ETD từ lịch sử: %s", etd)
            
            # Xử lý ATA (so sánh với ngày hiện tại)
            if pod_arrival_event:
                ata_dt, ata_str_full = self._parse_event_datetime(pod_arrival_event.get("date"))
                if ata_dt and ata_dt <= today_dt:
                    ata = ata_str_full # Đây là ngày Actual
                    logger.info("Tìm thấy ATA (Actual): %s", ata)
                elif ata_dt:
                    eta = ata_str_full # Đây là ngày Estimated mới, cập nhật ETA
                    logger.info("Cập nhật ETA từ lịch sử: %s", eta)

            # 5. Xử lý logic Transit (dựa trên các sự kiện không phải POL/POD)
            logger.debug("Bắt đầu xử lý logic transit...")
            for event in history_events:
                desc = event.get("description", "").lower()
                loc = event.get("location", "")
                
                if ("departure" in desc or "arrival" in desc):
                    is_pol_event = (event == atd_event)
                    is_pod_event = (event == pod_arrival_event)
                    
                    # Bỏ qua nếu là sự kiện ở POL hoặc POD
                    if is_pol_event or is_pod_event:
                        continue

                    # Nếu không phải POL/POD, đây là sự kiện transit
                    logger.debug("Phát hiện sự kiện transit: %s tại %s", desc, loc)
                    if loc and loc not in transit_port_list:
                        transit_port_list.append(loc)
                    
                    event_dt, event_str = self._parse_event_datetime(event.get("date"))
                    if not event_dt:
                        continue

                    if "arrival" in desc:
                        if event_dt <= today_dt:
                            # Lấy ngày *actual* arrival đầu tiên tại cảng transit
                            if not ata_transit:
                                ata_transit = event_str
                                logger.debug("Tìm thấy AtaTransit (Actual): %s", ata_transit)
                        else:
                            # Lấy ngày *estimated* arrival đầu tiên tại cảng transit
                            if not eta_transit:
                                eta_transit = event_str
                                logger.debug("Tìm thấy EtaTransit (Estimated): %s", eta_transit)
                    
                    if "departure" in desc:
                        if event_dt <= today_dt:
                            # Lấy ngày *actual* departure cuối cùng từ cảng transit
                            atd_transit = event_str
                            logger.debug("Tìm thấy AtdTransit (Actual): %s", atd_transit)
                        else:
                            # Thêm vào danh sách các ngày departure trong tương lai
                            future_etd_transits.append((event_dt, event_str))
                            logger.debug("Thêm EtdTransit (Estimated) vào danh sách: %s", event_str)

            # Sắp xếp và chọn EtdTransit gần nhất trong tương lai
            etd_transit_final = ""
            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit_final = future_etd_transits[0][1] # Lấy chuỗi ngày
                logger.info("EtdTransit (Estimated) gần nhất được chọn: %s", etd_transit_final)
            else:
                 logger.info("Không tìm thấy EtdTransit nào trong tương lai.")

            # 6. Xây dựng đối tượng JSON theo template N8nTrackingInfo
            shipment_data = N8nTrackingInfo(
                  BookingNo= tracking_number, # Sinokor không hiển thị Booking No, dùng tracking_number
                  BlNumber= bl_no or "",
                  BookingStatus= booking_status or "",
                  Pol= pol or "",
                  Pod= pod or "",
                  Etd= self._format_date(etd) or "",
                  Atd= self._format_date(atd) or "",
                  Eta= self._format_date(eta) or "",
                  Ata= self._format_date(ata) or "",
                  TransitPort= ", ".join(transit_port_list) if transit_port_list else "",
                  EtdTransit= self._format_date(etd_transit_final) or "",
                  AtdTransit= self._format_date(atd_transit) or "",
                  EtaTransit= self._format_date(eta_transit) or "",
                  AtaTransit= self._format_date(ata_transit) or ""
            )
            
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            return shipment_data

        except Exception as e:
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, exc_info=True)
            return None

    def _extract_history_events(self, cargo_tracking_panel):
        """
        Trích xuất tất cả các sự kiện từ bảng Cargo Tracking.
        """
        events = []
        try:
            logger.debug("Đang tìm bảng chi tiết sự kiện...")
            detail_table_body = cargo_tracking_panel.find_element(By.CSS_SELECTOR, "#divDetailInfo .splitTable table tbody")
            rows = detail_table_body.find_elements(By.TAG_NAME, "tr")
            logger.info("Tìm thấy %d hàng trong bảng lịch sử sự kiện.", len(rows))
            
            current_event_group = ""
            is_container_event = False
            
            for row in rows:
                header_th = row.find_elements(By.CSS_SELECTOR, "th.firstTh")
                if header_th:
                    current_event_group = header_th[0].text.strip()
                    # Xác định xem đây là nhóm sự kiện của container (Pickup/Return) hay tàu (Departure/Arrival)
                    is_container_event = "pickup" in current_event_group.lower() or "return" in current_event_group.lower()
                    continue
                
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells or len(cells) < 3:
                    continue # Bỏ qua các hàng không hợp lệ
                
                date_text, location, description = "", "", ""
                
                if is_container_event:
                    # Cấu trúc: CNTR No., Location, Date & Time
                    cntr_no, location, date_text = [c.text.strip() for c in cells]
                    description = f"{current_event_group}: {cntr_no}"
                else:
                    # Cấu trúc: Vessel / Voyage, Location, Date & Time
                    vessel_voyage, location, date_text = [c.text.strip() for c in cells]
                    # Chúng ta cần description là 'Departure' hoặc 'Arrival'
                    # current_event_group chính là 'Departure' hoặc 'Arrival'
                    description = f"{current_event_group}: {vessel_voyage}"
                
                if date_text:
                    events.append({"description": description, "location": location, "date": date_text})
                    
        except NoSuchElementException:
            logger.info("Không tìm thấy bảng chi tiết sự kiện (#divDetailInfo), có thể không có dữ liệu.")
            pass
        except Exception as e:
            logger.warning("Lỗi khi trích xuất lịch sử sự kiện: %s", e, exc_info=True)
            pass
            
        logger.info("Trích xuất được %d sự kiện từ lịch sử.", len(events))
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        """
        Tìm sự kiện cụ thể (ví dụ: 'Departure' tại 'Ningbo').
        """
        if not location_keyword:
            return None
        
        for event in events:
            # Kiểm tra từ khóa (ví dụ: 'Departure') ở phần đầu của mô tả (ví dụ: 'Departure: TS HONGKONG...')
            desc_match = description_keyword.lower() in event.get("description", "").lower().split(':')[0]
            # Kiểm tra địa điểm
            loc_match = location_keyword.lower() in event.get("location", "").lower()
            
            if desc_match and loc_match:
                logger.debug("Tìm thấy sự kiện khớp: %s tại %s", description_keyword, location_keyword)
                return event
                
        logger.debug("Không tìm thấy sự kiện khớp: %s tại %s", description_keyword, location_keyword)
        return None

    def _get_text_from_element(self, by, value, parent=None):
        """
        Hàm trợ giúp được cập nhật để trả về chuỗi rỗng thay vì None.
        """
        try:
            source = parent or self.driver
            return source.find_element(by, value).text.strip()
        except NoSuchElementException:
            logger.debug("Không tìm thấy element: %s, %s. Trả về chuỗi rỗng.", by, value)
            return ""

# Giữ nguyên hàm tiện ích này, cập nhật trả về chuỗi rỗng
def split_location_and_datetime(input_string):
    """
    Tách chuỗi "Location... YYYY-MM-DD HH:MM" thành (Location, Datetime).
    """
    if not input_string:
        return "", ""
    
    # Pattern tìm kiếm ngày giờ YYYY-MM-DD HH:MM
    pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2})'
    match = re.search(pattern, input_string)
    
    if match:
        split_index = match.start()
        location_part = input_string[:split_index].strip()
        datetime_part = match.group(0)
        return location_part, datetime_part
    else:
        # Nếu không tìm thấy, giả sử toàn bộ là location
        return input_string.strip(), ""