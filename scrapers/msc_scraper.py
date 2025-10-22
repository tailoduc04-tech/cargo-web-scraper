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

logger = logging.getLogger(__name__)

class MscScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web MSC,
    tập trung vào tìm kiếm theo Booking Number và chuẩn hóa kết quả theo template JSON.
    Sử dụng logging và logic xử lý transit tương tự CoscoScraper.
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
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 45)

            # 1. Xử lý cookie
            try:
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                cookie_button.click()
                logger.info("[MSC Scraper] Đã chấp nhận cookies.")
            except TimeoutException:
                logger.info("[MSC Scraper] Banner cookie không tìm thấy hoặc đã được chấp nhận.")

            # 2. Chuyển sang tìm kiếm bằng Booking Number
            booking_radio_button = self.wait.until(EC.presence_of_element_located((By.ID, "bookingradio")))
            # Sử dụng JavaScript click để đảm bảo click thành công
            self.driver.execute_script("arguments[0].click();", booking_radio_button)
            logger.info("[MSC Scraper] Đã chuyển sang tìm kiếm bằng Booking Number.")
            time.sleep(1) # Chờ một chút để UI cập nhật

            # 3. Nhập Booking Number và tìm kiếm
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "trackingNumber")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            # Sử dụng submit của form thay vì click nút tìm kiếm đôi khi ổn định hơn
            search_form = self.driver.find_element(By.CSS_SELECTOR, "form.js-form")
            search_form.submit()
            logger.info("[MSC Scraper] Đang tìm kiếm mã: %s", tracking_number)

            # 4. Đợi kết quả và mở rộng tất cả chi tiết container
            self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "msc-flow-tracking__result")))
            logger.info("[MSC Scraper] Trang kết quả đã tải.")
            # Chờ thêm một chút cho các animation hoàn tất và các nút xuất hiện đầy đủ
            time.sleep(3)

            more_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__more-button")
            logger.info("[MSC Scraper] Tìm thấy %d container để mở rộng.", len(more_buttons))
            for i, button in enumerate(more_buttons):
                try:
                    # Kiểm tra xem nút có class 'open' không, nếu có thì bỏ qua
                    if 'open' not in button.find_element(By.XPATH, "..").get_attribute('class'):
                         self.driver.execute_script("arguments[0].click();", button)
                         logger.debug("Đã click mở rộng container #%d", i+1)
                         time.sleep(1.5) # Chờ animation mở rộng
                    else:
                         logger.debug("Container #%d đã được mở rộng.", i+1)
                except Exception as e:
                    logger.warning("[MSC Scraper] Không thể nhấp vào nút mở rộng container #%d: %s", i + 1, e)

            logger.info("[MSC Scraper] Đã mở rộng (hoặc kiểm tra) tất cả chi tiết container.")

            # 5. Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                # Lỗi đã được log bên trong hàm _extract_and_normalize_data
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("[MSC Scraper] Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/msc_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("[MSC Scraper] Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("[MSC Scraper] Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("[MSC Scraper] Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _get_detail_value(self, section, heading_text):
        """Helper: Lấy giá trị từ một mục trong phần details."""
        try:
            # Tìm li chứa heading_text, sau đó tìm span giá trị trong li đó
            value_span = section.find_element(By.XPATH, f".//li[contains(., '{heading_text}')]/span[contains(@class, 'details-value')]")
            # MSC có thể có cấu trúc span > span bên trong, lấy text của span cha để bao gồm tất cả
            full_text = value_span.text.strip()
            # Xử lý trường hợp POL/POD có mã trong ngoặc: "Houston, US (USHOU)" -> "Houston, US"
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
            logger.debug("Tìm thấy %d step(s) trong container.", len(steps))

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

                    events.append({
                        "date": date_str, # Giữ lại chuỗi gốc để format
                        "datetime": event_datetime, # Dùng để sắp xếp và so sánh
                        "description": description,
                        "location": location
                    })
                except NoSuchElementException as e:
                    logger.warning("Thiếu thông tin trong một step của container: %s", e)
                    continue # Bỏ qua step này nếu thiếu phần tử
        except Exception as e:
             logger.error("Lỗi khi trích xuất events từ container: %s", e, exc_info=True)
        return events

    def _find_event(self, sorted_events, description_keyword, location_keyword_or_list=None, find_first=False, find_last=False):
        """
        Helper: Tìm một sự kiện cụ thể trong danh sách ĐÃ SẮP XẾP theo datetime.
        Trả về dictionary của event hoặc {} nếu không tìm thấy.
        location_keyword_or_list: chuỗi hoặc list chuỗi, chỉ cần location chứa 1 trong các keyword.
        """
        matches = []
        if not sorted_events:
            return {}

        desc_keyword_lower = description_keyword.lower()

        keywords_to_check = []
        if location_keyword_or_list:
            if isinstance(location_keyword_or_list, list):
                keywords_to_check = [k.strip().lower() for k in location_keyword_or_list if k and k.strip()]
            elif isinstance(location_keyword_or_list, str) and location_keyword_or_list.strip():
                 keywords_to_check = [location_keyword_or_list.strip().lower()]

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
                    logger.debug("Tìm thấy event (first): desc='%s', loc='%s', event=%s", description_keyword, location_keyword_or_list, event)
                    return event

        if not matches:
            logger.debug("Không tìm thấy event: desc='%s', loc='%s'", description_keyword, location_keyword_or_list)
            return {}

        # Nếu không yêu cầu first, hoặc yêu cầu last (hoặc mặc định)
        logger.debug("Tìm thấy event (last/default): desc='%s', loc='%s', event=%s", description_keyword, location_keyword_or_list, matches[-1])
        return matches[-1] # Match cuối cùng trong list đã sort


    def _extract_and_normalize_data(self, booking_no):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả thành một đối tượng N8nTrackingInfo.
        Áp dụng logic tìm transit tương tự COSCO.
        """
        try:
            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT CHUNG ===
            result_div = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".msc-flow-tracking__result")))
            details_section = result_div.find_element(By.CSS_SELECTOR, ".msc-flow-tracking__details ul")

            # Sử dụng _get_detail_value để lấy POL và POD (đã xử lý loại bỏ mã cảng)
            pol = self._get_detail_value(details_section, "Port of Load") or ""
            pod = self._get_detail_value(details_section, "Port of Discharge") or ""

            # Lấy BL Number
            bl_number_span = details_section.find_element(By.XPATH, ".//span[contains(text(), 'Bill of Lading:')]/following-sibling::span[contains(@class, 'msc-flow-tracking__details-value')][1]")
            bl_number = bl_number_span.text.strip() if bl_number_span else ""

            # Xử lý trường hợp có nhiều cảng trung chuyển
            transit_port_spans = details_section.find_elements(By.XPATH, ".//li[contains(., 'Transhipment')]/span[contains(@class, 'details-value')]")
            transit_ports = [elem.text.strip() for elem in transit_port_spans if elem.text.strip()]

            logger.info("Thông tin tóm tắt: POL='%s', POD='%s', BL='%s', TransitPorts=%s", pol, pod, bl_number, transit_ports)

            # === BƯỚC 2: THU THẬP VÀ SẮP XẾP TẤT CẢ EVENTS ===
            all_events = []
            containers = result_div.find_elements(By.CSS_SELECTOR, ".msc-flow-tracking__container")
            logger.info("Bắt đầu thu thập events từ %d container(s).", len(containers))
            for i, container in enumerate(containers):
                container_events = self._extract_events_from_container(container)
                logger.debug("Container #%d có %d event(s).", i+1, len(container_events))
                all_events.extend(container_events)

            # Sắp xếp tất cả events theo thời gian tăng dần
            all_events.sort(key=lambda x: x.get('datetime', datetime.max)) # Sắp xếp sử dụng datetime object
            logger.info("Đã thu thập và sắp xếp tổng cộng %d event(s).", len(all_events))

            # === BƯỚC 3: TÌM CÁC SỰ KIỆN CHÍNH (ETD/ATD POL, ETA/ATA POD) ===
            # ATD POL: Sự kiện "Export Loaded on Vessel" cuối cùng tại POL
            atd_event = self._find_event(all_events, "Export Loaded on Vessel", pol, find_last=True)
            # ETA POD: Sự kiện "Estimated Time of Arrival" cuối cùng tại POD
            eta_event = self._find_event(all_events, "Estimated Time of Arrival", pod, find_last=True)
            # ATA POD: Sự kiện "Discharged from vessel" đầu tiên tại POD
            ata_event = self._find_event(all_events, "Discharged from vessel", pod, find_first=True)

            # ETD POL: MSC thường không hiển thị ETD dự kiến rõ ràng ở POL, để trống
            etd_pol = ""
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
                for event in all_events:
                    loc = (event.get('location') or "").strip().lower()
                    desc = event.get('description', '').lower()
                    event_datetime = event.get('datetime')

                    is_transit_loc = any(tp_lower in loc for tp_lower in [tp.lower() for tp in transit_ports])

                    if is_transit_loc and "full intended transshipment" in desc and event_datetime:
                        if event_datetime.date() > today:
                            future_etd_transits.append((event_datetime, event['location'], event['date']))
                            logger.debug("Tìm thấy ứng viên EtdTransit tương lai: %s", event)

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