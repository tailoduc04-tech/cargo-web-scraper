import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
import traceback
import logging
import time # <--- Thêm import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module
logger = logging.getLogger(__name__)

def _split_location_and_datetime(input_string):
    """
    Tách chuỗi đầu vào thành (vị trí, ngày giờ).
    """
    if not input_string:
        return None, None
    # Pattern tìm kiếm ngày giờ YYYY-MM-DD HH:MM
    pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}'
    match = re.search(pattern, input_string)
    if match:
        split_index = match.start()
        # Lấy phần location là mọi thứ trước ngày giờ
        location_part = input_string[:split_index].strip()
        # Lấy phần ngày giờ
        datetime_part = match.group(0)
        return location_part, datetime_part
    else:
        # Nếu không tìm thấy, trả về toàn bộ chuỗi là location
        return input_string.strip(), None

class HeungALineScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Heung-A Line và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD ...' sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Tách lấy phần ngày YYYY-MM-DD
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning(f"Không thể phân tích định dạng ngày: {date_str}")
            return date_str # Trả về chuỗi gốc nếu lỗi

    @staticmethod
    def _extract_code(location_text):
        """
        Trích xuất mã trong dấu ngoặc đơn từ tên vị trí (ví dụ: 'KIT' từ '... (KIT)').
        Sử dụng để so khớp linh hoạt.
        """
        if not location_text:
            return ""
        # Tìm văn bản bên trong dấu ngoặc đơn
        match = re.search(r'\((.*?)\)', location_text)
        if match:
            # Trả về mã đã được trích xuất (ví dụ: "kit")
            return match.group(1).lower()
        # Nếu không có mã, trả về toàn bộ văn bản
        return location_text.lower()

    def _get_text_from_element(self, by, value, parent=None):
        """
        Helper lấy text từ element, trả về None nếu không tìm thấy.
        """
        try:
            source = parent or self.driver
            return source.find_element(by, value).text.strip()
        except NoSuchElementException:
            logger.warning(f"Không tìm thấy element: {by} = {value}")
            return None

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã B/L trên trang Heung-A Line.
        """
        logger.info(f"Bắt đầu scrape cho mã: {tracking_number}")
        t_total_start = time.time() # Tổng thời gian bắt đầu
        try:
            # Heung-A sử dụng URL trực tiếp
            direct_url = f"{self.config['url']}{tracking_number}"
            t_nav_start = time.time()
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)
            logger.info("-> (Thời gian) Tải trang: %.2fs", time.time() - t_nav_start)


            # Chờ cho đến khi panel Schedule xuất hiện
            t_wait_panel_start = time.time()
            self.wait.until(EC.visibility_of_element_located((By.ID, "divSchedule")))
            logger.info("Trang chi tiết đã tải. (Thời gian chờ panel: %.2fs)", time.time() - t_wait_panel_start)

            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(tracking_number)
            logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)


            if not normalized_data:
                logger.warning(f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'.")
                return None, f"Could not extract normalized data for '{tracking_number}'."

            t_total_end = time.time()
            logger.info(f"Hoàn tất scrape thành công cho mã: {tracking_number} (Tổng thời gian: %.2fs)",
                         t_total_end - t_total_start)
            return normalized_data, None

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/heungaline_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning(f"Timeout khi scrape mã '{tracking_number}'. Đã lưu ảnh chụp màn hình vào {screenshot_path} (Tổng thời gian: %.2fs)",
                             t_total_fail - t_total_start)
            except Exception as ss_e:
                logger.error(f"Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '{tracking_number}': {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'. Trang web có thể đang chậm hoặc mã không hợp lệ."
        except Exception as e:
            t_total_fail = time.time()
            logger.error(f"Đã xảy ra lỗi không mong muốn khi scrape mã '{tracking_number}': {e} (Tổng thời gian: %.2fs)",
                         t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_history_events(self, cargo_tracking_panel):
        """
        Trích xuất tất cả các sự kiện từ bảng Cargo Tracking.
        """
        events = []
        try:
            logger.debug("Bắt đầu trích xuất lịch sử sự kiện...")
            detail_table_body = cargo_tracking_panel.find_element(By.CSS_SELECTOR, "#divDetailInfo .splitTable table tbody")
            rows = detail_table_body.find_elements(By.TAG_NAME, "tr")
            logger.debug("-> Tìm thấy %d hàng trong bảng lịch sử.", len(rows))
            current_event_group = "" # Ví dụ: "Pickup (2/2)", "Departure"
            is_container_event = False

            for row in rows:
                # Kiểm tra xem đây có phải là hàng header của nhóm sự kiện không
                header_th = row.find_elements(By.CSS_SELECTOR, "th.firstTh")
                if header_th:
                    current_event_group = header_th[0].text.strip()
                    # Sự kiện container (Pickup, Return) có cấu trúc cột khác
                    is_container_event = "pickup" in current_event_group.lower() or "return" in current_event_group.lower()
                    logger.debug("-> Đang xử lý nhóm sự kiện: %s (Container event: %s)", current_event_group, is_container_event)
                    continue

                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells or len(cells) < 3: continue

                date_text, location, description = "", "", ""
                if is_container_event:
                    # Cấu trúc: CNTR No. | Location | Date & Time
                    cntr_no, location, date_text = [c.text.strip() for c in cells]
                    description = f"{current_event_group}: {cntr_no}"
                else:
                    # Cấu trúc: Vessel / Voyage | Location | Date & Time
                    vessel_voyage, location, date_text = [c.text.strip() for c in cells]
                    description = f"{current_event_group}: {vessel_voyage}"

                if date_text:
                    event_data = {"description": description, "location": location, "date": date_text}
                    events.append(event_data)
                    logger.debug("--> Đã trích xuất sự kiện: %s", event_data)

        except NoSuchElementException:
            logger.warning("Không tìm thấy bảng chi tiết Cargo Tracking (#divDetailInfo).")
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất lịch sử sự kiện: {e}", exc_info=True)

        logger.debug("-> Hoàn tất trích xuất %d sự kiện.", len(events))
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        """
        Tìm một sự kiện cụ thể (ví dụ: 'Departure') tại một vị trí cụ thể.
        """
        if not location_keyword:
            logger.warning(f"Thiếu location_keyword khi tìm '{description_keyword}'")
            return None

        # Sử dụng mã (ví dụ: "KIT") để so khớp, vì nó đáng tin cậy hơn
        location_code = self._extract_code(location_keyword)
        if not location_code:
             logger.warning(f"Không thể trích xuất mã từ: '{location_keyword}'")
             return None

        logger.debug("-> _find_event: Tìm '%s' tại location_code '%s'", description_keyword, location_code)
        for event in events:
            # Kiểm tra từ khóa mô tả (ví dụ: 'Departure')
            desc_match = description_keyword.lower() in event.get("description", "").lower().split(':')[0]
            # Kiểm tra mã vị trí
            loc_match = location_code in event.get("location", "").lower()

            if desc_match and loc_match:
                logger.debug("--> Tìm thấy sự kiện khớp: %s", event)
                return event

        logger.debug("--> Không tìm thấy sự kiện khớp.")
        return None

    def _extract_and_normalize_data(self, tracking_number):
        """
        Hàm chính để trích xuất, xử lý và chuẩn hóa dữ liệu từ trang chi tiết.
        """
        logger.info(f"Đang trích xuất dữ liệu cho {tracking_number}")
        try:
            # 1. Trích xuất thông tin chung
            logger.debug("Trích xuất thông tin chung...")
            bl_no = self._get_text_from_element(By.XPATH, "//label[contains(text(), 'B/L No.')]/ancestor::div[1]/following-sibling::div//span")
            booking_status = self._get_text_from_element(By.XPATH, "//label[contains(text(), 'B/K Status')]/ancestor::div[1]/following-sibling::div//span")

            # 2. Trích xuất thông tin lịch trình (POL, POD, ETD, ETA)
            logger.debug("Trích xuất thông tin lịch trình...")
            schedule_panel = self.wait.until(EC.presence_of_element_located((By.ID, "divSchedule")))

            pol_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#divSchedule li.col-sm-8 .col-sm-6:nth-child(1)")))
            pod_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#divSchedule li.col-sm-8 .col-sm-6:nth-child(2)")))

            # Lấy toàn bộ text và tách location/datetime
            pol_full_text, etd = _split_location_and_datetime(pol_element.text)
            pod_full_text, eta = _split_location_and_datetime(pod_element.text)

            # Tách riêng tên cảng (POL/POD) và tên terminal
            pol = pol_element.find_element(By.TAG_NAME, "b").text.strip()
            pol_terminal = pol_element.find_element(By.TAG_NAME, "a").text.strip() # 'KIT(한국국제터미널)'

            pod = pod_element.find_element(By.TAG_NAME, "b").text.strip()
            pod_terminal = pod_element.find_element(By.XPATH, ".//span[not(@class)]").text.strip() # 'PAT TERMINAL 2 (PORT AUTHORITY OF THAILAND)'

            logger.info(f"POL: {pol} ({pol_terminal}), ETD (Scheduled): {etd}")
            logger.info(f"POD: {pod} ({pod_terminal}), ETA (Scheduled): {eta}")

            # 3. Mở rộng bảng chi tiết Cargo Tracking
            logger.debug("Kiểm tra và mở rộng Cargo Tracking...")
            cargo_tracking_panel = self.wait.until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Cargo Tracking')]/ancestor::div[contains(@class, 'panel')]")))
            try:
                toggle_button = cargo_tracking_panel.find_element(By.ID, "tglDetailInfo")
                # 'fa-chevron-down' nghĩa là đang đóng -> cần click
                if 'fa-chevron-down' in toggle_button.get_attribute('class'):
                     self.driver.execute_script("arguments[0].click();", toggle_button)
                     self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#divDetailInfo div.splitTable")))
                     logger.info("Đã mở rộng chi tiết Cargo Tracking.")
                else:
                     logger.debug("Chi tiết Cargo Tracking đã được mở sẵn.")
            except Exception:
                logger.warning("Không tìm thấy nút 'tglDetailInfo' hoặc panel đã mở.", exc_info=True)
                pass

            # 4. Trích xuất lịch sử và tìm các ngày thực tế (ATD, ATA)
            logger.debug("Trích xuất lịch sử sự kiện...")
            history_events = self._extract_history_events(cargo_tracking_panel)

            logger.debug("Tìm ATD/ATA...")
            actual_departure = self._find_event(history_events, "Departure", pol_terminal)
            actual_arrival = self._find_event(history_events, "Arrival", pod_terminal)

            atd = actual_departure.get('date') if actual_departure else None
            ata = actual_arrival.get('date') if actual_arrival else None

            logger.info(f"ATD (Actual): {atd}, ATA (Actual): {ata}")

            # 5. Xác định cảng trung chuyển (Logic COSCO)
            logger.debug("Xử lý logic transit...")
            transit_ports_list = []
            ata_transit_date = None
            atd_transit_date = None

            # Trang Heung-A không cung cấp ngày dự kiến cho transit
            eta_transit_str = ""
            etd_transit_str = ""

            pol_code = self._extract_code(pol_terminal)
            pod_code = self._extract_code(pod_terminal)

            logger.debug(f"Matching codes - POL: {pol_code}, POD: {pod_code}")

            for event in history_events:
                loc = event.get('location', '')
                if not loc: continue

                loc_lower = loc.lower()
                # Chỉ lấy phần mô tả chính (ví dụ: 'arrival', 'departure')
                desc = event.get('description', '').lower().split(':')[0]

                is_pol = pol_code in loc_lower
                is_pod = pod_code in loc_lower

                # Bỏ qua nếu là sự kiện tại POL hoặc POD
                if is_pol or is_pod:
                    continue

                # Chỉ quan tâm đến sự kiện tàu (Arrival/Departure) tại các cảng khác
                if "arrival" in desc or "departure" in desc:
                     if loc not in transit_ports_list:
                        transit_ports_list.append(loc)
                        logger.debug(f"Tìm thấy cảng transit: {loc}")

                event_date = event.get('date')
                if not event_date: continue

                # Áp dụng logic COSCO:
                # AtaTransit: Ngày *thực tế* *đầu tiên* tàu đến cảng transit
                if "arrival" in desc:
                    if not ata_transit_date:
                        ata_transit_date = event_date
                        logger.debug(f"Tìm thấy AtaTransit đầu tiên: {event_date} tại {loc}")

                # AtdTransit: Ngày *thực tế* *cuối cùng* tàu rời cảng transit
                elif "departure" in desc:
                    atd_transit_date = event_date # Luôn cập nhật để lấy cái cuối cùng
                    logger.debug(f"Cập nhật AtdTransit cuối cùng: {event_date} tại {loc}")

            # 6. Xây dựng đối tượng JSON chuẩn hóa
            logger.debug("Tạo đối tượng N8nTrackingInfo...")
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= bl_no or "",
                BookingStatus= booking_status or "",
                Pol= pol or "",
                Pod= pod or "",
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= ", ".join(transit_ports_list) if transit_ports_list else "",
                EtdTransit= etd_transit_str,
                AtdTransit= self._format_date(atd_transit_date) or "",
                EtaTransit= eta_transit_str,
                AtaTransit= self._format_date(ata_transit_date) or ""
            )

            logger.info(f"Trích xuất dữ liệu thành công cho {tracking_number}.")
            return shipment_data

        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng trong quá trình trích xuất dữ liệu cho '{tracking_number}': {e}", exc_info=True)
            return None