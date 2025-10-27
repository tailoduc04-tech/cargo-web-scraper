import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time # <--- Thêm import time
import traceback

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class MscScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web MSC,
    tập trung vào tìm kiếm theo Booking Number và chuẩn hóa kết quả theo template JSON.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD/MM/YYYY' sang 'DD/MM/YYYY'.
        Trả về chuỗi rỗng "" nếu định dạng không hợp lệ hoặc đầu vào là None.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # MSC đã cung cấp định dạng DD/MM/YYYY, chỉ cần xác thực lại
            datetime.strptime(date_str, '%d/%m/%Y')
            return date_str
        except (ValueError, TypeError):
            logger.warning("Không thể phân tích định dạng ngày: %s. Trả về chuỗi rỗng.", date_str)
            return ""

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một Booking Number và trả về một đối tượng N8nTrackingInfo hoặc lỗi.
        """
        logger.info("[MSC Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time() # Tổng thời gian bắt đầu
        try:
            t_nav_start = time.time()
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 45)
            # Log thời gian tải trang sẽ chính xác hơn khi chờ element đầu tiên
            # logger.info("-> (Thời gian) Tải trang: %.2fs", time.time() - t_nav_start)

            # 1. Xử lý cookie
            t_cookie_start = time.time()
            try:
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                logger.info("-> (Thời gian) Tải trang và tìm nút cookie: %.2fs", time.time() - t_nav_start)
                cookie_button.click()
                logger.info("[MSC Scraper] Đã chấp nhận cookies. (Thời gian xử lý: %.2fs)", time.time() - t_cookie_start)
            except TimeoutException:
                 logger.info("-> (Thời gian) Tải trang (không có cookie): %.2fs", time.time() - t_nav_start)
                 logger.info("[MSC Scraper] Banner cookie không tìm thấy hoặc đã được chấp nhận. (Thời gian kiểm tra: %.2fs)", time.time() - t_cookie_start)

            # 2. Chuyển sang tìm kiếm bằng Booking Number
            t_switch_search_start = time.time()
            booking_radio_button = self.wait.until(EC.presence_of_element_located((By.ID, "bookingradio")))
            self.driver.execute_script("arguments[0].click();", booking_radio_button)
            logger.info("[MSC Scraper] Đã chuyển sang tìm kiếm bằng Booking Number. (Thời gian: %.2fs)", time.time() - t_switch_search_start)

            # 3. Nhập Booking Number và tìm kiếm
            t_search_start = time.time()
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "trackingNumber")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_form = self.driver.find_element(By.CSS_SELECTOR, "form.js-form")
            search_form.submit()
            logger.info("[MSC Scraper] Đang tìm kiếm mã: %s. (Thời gian tìm kiếm: %.2fs)", tracking_number, time.time() - t_search_start)

            # 4. Đợi kết quả và mở rộng chi tiết container ĐẦU TIÊN
            t_wait_result_start = time.time()
            self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "msc-flow-tracking__result")))
            logger.info("[MSC Scraper] Trang kết quả đã tải. (Thời gian chờ: %.2fs)", time.time() - t_wait_result_start)

            t_expand_start = time.time()
            more_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__more-button")
            logger.info("[MSC Scraper] Tìm thấy %d container. Sẽ chỉ mở rộng container đầu tiên.", len(more_buttons))

            # --- THAY ĐỔI: Chỉ xử lý nút đầu tiên ---
            if more_buttons:
                button = more_buttons[0] # Chỉ lấy nút đầu tiên
                try:
                    # Kiểm tra xem nút có class 'open' không, nếu có thì bỏ qua
                    parent_div = button.find_element(By.XPATH, "..") # Lấy div cha chứa nút
                    if 'open' not in parent_div.get_attribute('class'):
                         self.driver.execute_script("arguments[0].click();", button)
                         logger.debug("Đã click mở rộng container đầu tiên.")
                    else:
                         logger.debug("Container đầu tiên đã được mở rộng.")
                except Exception as e:
                    logger.warning("[MSC Scraper] Không thể nhấp vào nút mở rộng container đầu tiên: %s", e, exc_info=True)
            else:
                 logger.warning("[MSC Scraper] Không tìm thấy container nào để mở rộng.")
            logger.info("[MSC Scraper] -> (Thời gian) Mở rộng container: %.2fs", time.time() - t_expand_start)
            # --- KẾT THÚC THAY ĐỔI ---

            # 5. Trích xuất và chuẩn hóa dữ liệu
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(tracking_number)
            logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)


            if not normalized_data:
                # Lỗi đã được log bên trong hàm _extract_and_normalize_data
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[MSC Scraper] Hoàn tất scrape thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/msc_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("[MSC Scraper] Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s (Tổng thời gian: %.2fs)",
                             tracking_number, screenshot_path, t_total_fail - t_total_start)
            except Exception as ss_e:
                logger.error("[MSC Scraper] Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[MSC Scraper] Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _get_detail_value(self, section, heading_text):
        """Helper: Lấy giá trị từ một mục trong phần details."""
        try:
            # Tìm li chứa heading_text, sau đó tìm span giá trị trong li đó
            value_span = section.find_element(By.XPATH, f".//li[contains(., '{heading_text}')]/span[contains(@class, 'details-value')]")
            full_text = value_span.text.strip()
            # Xử lý trường hợp POL/POD có mã trong ngoặc
            if '(' in full_text and heading_text in ["Port of Load", "Port of Discharge"]:
                return full_text.split('(')[0].strip()
            return full_text
        except NoSuchElementException:
            logger.debug("Không tìm thấy giá trị cho mục detail: '%s'", heading_text)
            return None


    def _extract_events_from_container(self, container_element):
        """Helper: Trích xuất lịch sử di chuyển (events) cho một container cụ thể."""
        events = []
        try:
            steps = container_element.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__step")
            logger.debug("-> Tìm thấy %d step(s) trong container.", len(steps))

            for step in steps:
                try:
                    date_str = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--two .data-value").text.strip()
                    description = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--four .data-value").text.strip()
                    location = step.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__cell--three .data-value").text.strip()

                    # Cố gắng parse ngày để validate và dùng cho sắp xếp sau này
                    event_datetime = None
                    try:
                        event_datetime = datetime.strptime(date_str, '%d/%m/%Y')
                    except ValueError:
                        logger.warning("Định dạng ngày không hợp lệ '%s' trong step: %s, %s", date_str, location, description)
                        continue # Bỏ qua event này nếu ngày không hợp lệ

                    event_data = {
                        "date": date_str, # Giữ lại chuỗi gốc để format
                        "datetime": event_datetime, # Dùng để sắp xếp và so sánh
                        "description": description,
                        "location": location
                    }
                    events.append(event_data)
                    logger.debug("--> Trích xuất event: %s", event_data)
                except NoSuchElementException as e:
                    logger.warning("Thiếu thông tin trong một step của container: %s", e)
                    continue # Bỏ qua step này nếu thiếu phần tử
        except Exception as e:
             logger.error("Lỗi khi trích xuất events từ container: %s", e, exc_info=True)
        logger.debug("-> Hoàn tất trích xuất %d sự kiện từ container.", len(events))
        return events

    def _find_event(self, sorted_events, description_keyword, location_keyword_or_list=None, find_first=False, find_last=False):
        """
        Helper: Tìm một sự kiện cụ thể trong danh sách ĐÃ SẮP XẾP theo datetime.
        Trả về dictionary của event hoặc {} nếu không tìm thấy.
        location_keyword_or_list: chuỗi hoặc list chuỗi, chỉ cần location chứa 1 trong các keyword.
        """
        matches = []
        if not sorted_events:
            logger.debug("-> _find_event: Danh sách sự kiện rỗng.")
            return {}

        desc_keyword_lower = description_keyword.lower()

        keywords_to_check = []
        if location_keyword_or_list:
            if isinstance(location_keyword_or_list, list):
                keywords_to_check = [k.strip().lower() for k in location_keyword_or_list if k and k.strip()]
            elif isinstance(location_keyword_or_list, str) and location_keyword_or_list.strip():
                 keywords_to_check = [location_keyword_or_list.strip().lower()]

        logger.debug("-> _find_event: Tìm '%s' tại '%s' (Keywords: %s), first: %s, last: %s",
                     description_keyword, location_keyword_or_list, keywords_to_check, find_first, find_last)

        for event in sorted_events:
            desc = event.get("description", "").lower()
            loc = (event.get("location") or "").strip().lower()

            desc_match = desc_keyword_lower in desc

            # Chỉ kiểm tra location nếu có keywords_to_check
            loc_match = not keywords_to_check or any(keyword in loc for keyword in keywords_to_check)

            if desc_match and loc_match:
                matches.append(event)
                if find_first:
                    # Vì list đã sort, match đầu tiên là match cần tìm
                    logger.debug("--> Tìm thấy event (first): %s", event)
                    return event

        if not matches:
            logger.debug("--> Không tìm thấy event khớp.")
            return {}

        # Nếu không yêu cầu first, hoặc yêu cầu last (hoặc mặc định)
        result = matches[-1] # Match cuối cùng trong list đã sort
        logger.debug("--> Tìm thấy event (last/default): %s", result)
        return result


    def _extract_and_normalize_data(self, booking_no):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả thành một đối tượng N8nTrackingInfo.
        Chỉ xử lý container đầu tiên.
        """
        try:
            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT CHUNG ===
            logger.debug("Bắt đầu trích xuất thông tin tóm tắt...")
            result_div = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".msc-flow-tracking__result")))
            details_section = result_div.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__details ul")

            pol = self._get_detail_value(details_section, "Port of Load") or ""
            pod = self._get_detail_value(details_section, "Port of Discharge") or ""

            bl_number_span = details_section.find_element(By.XPATH, ".//span[contains(text(), 'Bill of Lading:')]/following-sibling::span[contains(@class, 'msc-flow-tracking__details-value')][1]")
            bl_number = bl_number_span.text.strip() if bl_number_span else ""

            transit_port_spans = details_section.find_elements(By.XPATH, ".//li[contains(., 'Transhipment')]/span[contains(@class, 'details-value')]")
            transit_ports = [elem.text.strip() for elem in transit_port_spans if elem.text.strip()]

            logger.info("Thông tin tóm tắt: POL='%s', POD='%s', BL='%s', TransitPorts=%s", pol, pod, bl_number, transit_ports)

            # === BƯỚC 2: THU THẬP VÀ SẮP XẾP EVENTS TỪ CONTAINER ĐẦU TIÊN ===
            all_events = []
            containers = result_div.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__container")
            logger.info("Bắt đầu thu thập events từ container đầu tiên (trong số %d).", len(containers))
            # --- THAY ĐỔI: Chỉ xử lý container đầu tiên ---
            if containers:
                container_events = self._extract_events_from_container(containers[0]) # Chỉ lấy container đầu tiên
                logger.debug("Container đầu tiên có %d event(s).", len(container_events))
                all_events.extend(container_events)
            else:
                logger.warning("Không tìm thấy container nào.")
            # --- KẾT THÚC THAY ĐỔI ---

            # Sắp xếp tất cả events theo thời gian tăng dần
            all_events.sort(key=lambda x: x.get('datetime', datetime.max))
            logger.info("Đã thu thập và sắp xếp tổng cộng %d event(s) (từ container đầu tiên).", len(all_events))

            # === BƯỚC 3: TÌM CÁC SỰ KIỆN CHÍNH (ETD/ATD POL, ETA/ATA POD) ===
            logger.debug("Tìm kiếm các sự kiện chính...")
            # ATD POL: Sự kiện "Export Loaded on Vessel" cuối cùng tại POL
            atd_event = self._find_event(all_events, "Export Loaded on Vessel", pol, find_last=True)
            # ETA POD: Sự kiện "Estimated Time of Arrival" cuối cùng tại POD
            eta_event = self._find_event(all_events, "Estimated Time of Arrival", pod, find_last=True)
            # ATA POD: Sự kiện "Discharged from vessel" đầu tiên tại POD
            ata_event = self._find_event(all_events, "Discharged from vessel", pod, find_first=True)

            etd_pol = "" # ETD POL: MSC thường không hiển thị rõ ràng, để trống
            atd_pol = self._format_date(atd_event.get("date")) if atd_event else ""
            eta_pod = self._format_date(eta_event.get("date")) if eta_event else ""
            ata_pod = self._format_date(ata_event.get("date")) if ata_event else ""

            logger.info("Sự kiện chính: ATD POL='%s', ETA POD='%s', ATA POD='%s'", atd_pol, eta_pod, ata_pod)

            # === BƯỚC 4: TÌM CÁC SỰ KIỆN TRANSIT (LOGIC TƯƠNG TỰ COSCO) ===
            ata_transit_event = None
            eta_transit_event = None
            atd_transit_event = None
            etd_transit_final_str = None
            future_etd_transits = []
            today = date.today()

            if transit_ports:
                logger.info("Bắt đầu xử lý logic transit cho các cảng: %s", transit_ports)
                # Tìm AtaTransit: Sự kiện "Discharged from vessel" đầu tiên tại BẤT KỲ cảng transit nào
                ata_transit_event = self._find_event(all_events, "Discharged from vessel", transit_ports, find_first=True)

                # Tìm EtaTransit: Nếu không có AtaTransit, tìm "Estimated Time of Arrival" đầu tiên tại BẤT KỲ cảng transit nào
                if not ata_transit_event:
                    eta_transit_event = self._find_event(all_events, "Estimated Time of Arrival", transit_ports, find_first=True)

                # Tìm AtdTransit: Sự kiện "Export Loaded on Vessel" cuối cùng tại BẤT KỲ cảng transit nào
                atd_transit_event = self._find_event(all_events, "Export Loaded on Vessel", transit_ports, find_last=True)

                # Tìm EtdTransit: Tìm tất cả "Full Intended Transshipment" tại các cảng transit có ngày > hôm nay
                logger.debug("Tìm kiếm EtdTransit trong tương lai...")
                for event in all_events:
                    loc = (event.get('location') or "").strip().lower()
                    desc = event.get('description', '').lower()
                    event_datetime = event.get('datetime')

                    is_transit_loc = any(tp_lower in loc for tp_lower in [tp.lower() for tp in transit_ports])

                    if is_transit_loc and "full intended transshipment" in desc and event_datetime:
                        if event_datetime.date() > today:
                            future_etd_transits.append((event_datetime, event['location'], event['date']))
                            logger.debug("--> Tìm thấy ứng viên EtdTransit tương lai: %s", event)

                if future_etd_transits:
                    future_etd_transits.sort() # Sắp xếp theo ngày tăng dần
                    etd_transit_final_str = future_etd_transits[0][2] # Lấy chuỗi ngày của event gần nhất
                    logger.info("EtdTransit gần nhất trong tương lai được chọn: %s", etd_transit_final_str)
                else:
                    logger.info("Không tìm thấy EtdTransit nào trong tương lai.")
            else:
                 logger.info("Không có cảng transit, bỏ qua xử lý transit.")

            # Format các ngày transit
            ata_transit = self._format_date(ata_transit_event.get("date")) if ata_transit_event else ""
            eta_transit = self._format_date(eta_transit_event.get("date")) if eta_transit_event else ""
            atd_transit = self._format_date(atd_transit_event.get("date")) if atd_transit_event else ""
            etd_transit = self._format_date(etd_transit_final_str) or "" # Đã là chuỗi DD/MM/YYYY

            logger.info("Sự kiện Transit: Ata='%s', Eta='%s', Atd='%s', Etd='%s'", ata_transit, eta_transit, atd_transit, etd_transit)

            # === BƯỚC 5: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            logger.debug("Tạo đối tượng N8nTrackingInfo...")
            shipment_data = N8nTrackingInfo(
                BookingNo=booking_no or "",
                BlNumber=bl_number or "",
                BookingStatus="",
                Pol=pol,
                Pod=pod,
                Etd=etd_pol,
                Atd=atd_pol,
                Eta=eta_pod,
                Ata=ata_pod,
                TransitPort=", ".join(transit_ports) or "",
                EtdTransit=etd_transit,
                AtdTransit=atd_transit,
                EtaTransit=eta_transit,
                AtaTransit=ata_transit
            )

            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công cho mã '%s'.", booking_no)
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", booking_no, e, exc_info=True)
            return None