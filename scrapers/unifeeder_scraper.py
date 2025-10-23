import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho module này
logger = logging.getLogger(__name__)

class UnifeederScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Unifeeder và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD-Mon-YY HH:MM AM/PM' sang 'DD/MM/YYYY'.
        Ví dụ: '29-Oct-25 07:00 AM' -> '29/10/2025'
        """
        if not date_str:
            return None
        try:
            # Loại bỏ phần '(Projected)' nếu có để phân tích ngày
            clean_date_str = date_str.replace("(Projected)", "").strip()
            # Phân tích chuỗi ngày tháng
            dt_obj = datetime.strptime(clean_date_str, '%d-%b-%y %I:%M %p')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            # Trả về None nếu không phân tích được
            return None

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính, truy cập URL trực tiếp và trả về JSON.
        """
        logger.info("Bắt đầu scrape cho mã: %s (Unifeeder)", tracking_number)
        try:
            # Unifeeder cho phép truy cập URL trực tiếp với mã tracking
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            # Chờ cho đến khi chi tiết booking (div.booking-details) được tải
            # Đây là dấu hiệu trang đã tải xong dữ liệu
            logger.info("Đang chờ trang tải dữ liệu cho mã: %s", tracking_number)
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.booking-details"))
            )
            logger.info("Trang đã tải xong. Bắt đầu trích xuất dữ liệu.")

            # Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                logger.warning("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                return None, f"Could not extract normalized data for '{tracking_number}'."

            logger.info("Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/unifeeder_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn cho '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ trang và ánh xạ vào template JSON (N8nTrackingInfo).
        """
        try:
            logger.info("Bắt đầu trích xuất chi tiết cho mã: %s", tracking_number)
            
            # Khởi tạo các biến
            pol, pod = "", ""
            etd, atd, eta, ata = "", "", "", ""
            etd_transit, atd_transit, eta_transit, ata_transit = "", "", "", ""
            transit_ports_list = []
            today = date.today()

            # 1. Trích xuất POL và POD
            try:
                route_container = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.route-display")))
                route_spans = route_container.find_elements(By.TAG_NAME, "span")
                pol = route_spans[0].text.strip() if len(route_spans) > 0 else ""
                pod = route_spans[1].text.strip() if len(route_spans) > 1 else ""
                logger.info("Trích xuất POL: %s, POD: %s", pol, pod)
            except Exception as e:
                logger.warning("Không thể trích xuất POL/POD: %s", e, exc_info=True)

            # 2. Trích xuất tất cả các sự kiện
            events = self._extract_events()
            if not events:
                logger.warning("Không tìm thấy sự kiện tracking nào cho mã: %s", tracking_number)
                return None
            
            # Đảo ngược danh sách sự kiện để xử lý theo thứ tự thời gian (từ cũ nhất -> mới nhất)
            events.reverse()
            logger.info("Đã trích xuất và đảo ngược %d sự kiện.", len(events))

            # 3. Tìm các sự kiện quan trọng (POL/POD)
            # _find_event duyệt ngược, nên nó sẽ tìm sự kiện "cuối cùng" (gần nhất) khớp
            departure_actual = self._find_event(events, "LOAD", pol, event_type="ngay_thuc_te")
            departure_projected = self._find_event(events, "LOAD", pol, event_type="ngay_du_kien")
            arrival_actual = self._find_event(events, "DISCHARGE", pod, event_type="ngay_thuc_te")
            arrival_projected = self._find_event(events, "DISCHARGE", pod, event_type="ngay_du_kien")
            
            atd = self._format_date(departure_actual.get('date'))
            etd = self._format_date(departure_projected.get('date'))
            ata = self._format_date(arrival_actual.get('date'))
            eta = self._format_date(arrival_projected.get('date'))

            logger.debug("ATD (raw): %s, ETD (raw): %s", departure_actual.get('date'), departure_projected.get('date'))
            logger.debug("ATA (raw): %s, ETA (raw): %s", arrival_actual.get('date'), arrival_projected.get('date'))

            # 4. Xử lý logic Transit (tương tự Cosco)
            ts_discharge_events = []
            ts_load_events = []
            future_etd_transits = []

            for event in events:
                desc = event.get('description', '').upper()
                # Unifeeder dùng "T/S" để đánh dấu sự kiện transit
                if "T/S" in desc:
                    port = event.get('location')
                    if port and port not in transit_ports_list:
                        transit_ports_list.append(port)
                    
                    if "DISCHARGE" in desc:
                        ts_discharge_events.append(event)
                    elif "LOAD" in desc:
                        ts_load_events.append(event)
            
            logger.info("Tìm thấy %d cảng transit: %s", len(transit_ports_list), ", ".join(transit_ports_list))
            logger.info("Tìm thấy %d sự kiện T/S Discharge và %d sự kiện T/S Load.", len(ts_discharge_events), len(ts_load_events))

            # Tìm AtaTransit (sự kiện T/S Discharge thực tế đầu tiên)
            first_ts_discharge_actual = next((e for e in ts_discharge_events if e['type'] == 'ngay_thuc_te'), None)
            if first_ts_discharge_actual:
                ata_transit = self._format_date(first_ts_discharge_actual.get('date'))
                logger.debug("Tìm thấy AtaTransit: %s", ata_transit)

            # Tìm EtaTransit (sự kiện T/S Discharge dự kiến đầu tiên)
            first_ts_discharge_projected = next((e for e in ts_discharge_events if e['type'] == 'ngay_du_kien'), None)
            if first_ts_discharge_projected:
                eta_transit = self._format_date(first_ts_discharge_projected.get('date'))
                logger.debug("Tìm thấy EtaTransit: %s", eta_transit)
            
            # Tìm AtdTransit (sự kiện T/S Load thực tế cuối cùng)
            last_ts_load_actual = next((e for e in reversed(ts_load_events) if e['type'] == 'ngay_thuc_te'), None)
            if last_ts_load_actual:
                atd_transit = self._format_date(last_ts_load_actual.get('date'))
                logger.debug("Tìm thấy AtdTransit: %s", atd_transit)

            # Tìm EtdTransit (sự kiện T/S Load dự kiến gần nhất trong tương lai)
            for e in ts_load_events:
                if e['type'] == 'ngay_du_kien':
                    date_str = e.get('date')
                    if not date_str:
                        continue
                    try:
                        clean_date_str = date_str.replace("(Projected)", "").strip()
                        dt_obj = datetime.strptime(clean_date_str, '%d-%b-%y %I:%M %p')
                        etd_date = dt_obj.date()
                        if etd_date > today:
                            future_etd_transits.append((etd_date, self._format_date(date_str)))
                            logger.debug("Thêm ETD transit trong tương lai: %s", date_str)
                    except (ValueError, IndexError):
                        logger.warning("Không thể parse EtdTransit: %s", date_str)
            
            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit = future_etd_transits[0][1]
                logger.info("EtdTransit gần nhất trong tương lai được chọn: %s", etd_transit)
            else:
                logger.info("Không tìm thấy EtdTransit nào trong tương lai.")

            # 5. Tạo đối tượng JSON và điền dữ liệu
            # Đảm bảo 14 trường đều là chuỗi, rỗng nếu không có dữ liệu
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= tracking_number,
                BookingStatus= "", # Không có thông tin
                Pol= pol or "",
                Pod= pod or "",
                Etd= etd or "",
                Atd= atd or "",
                Eta= eta or "",
                Ata= ata or "",
                TransitPort= ", ".join(transit_ports_list) if transit_ports_list else "",
                EtdTransit= etd_transit or "",
                AtdTransit= atd_transit or "",
                EtaTransit= eta_transit or "",
                AtaTransit= ata_transit or "",
            )
            
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            return shipment_data

        except Exception as e:
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None

    def _extract_events(self):
        """
        Trích xuất toàn bộ lịch sử từ mục "Tracking".
        Trả về một danh sách các dictionary sự kiện.
        """
        events = []
        try:
            event_rows = self.driver.find_elements(By.CSS_SELECTOR, "div.row-item")
            logger.info("Tìm thấy %d hàng sự kiện (bao gồm cả tiêu đề).", len(event_rows))
            
            for row in event_rows:
                # Bỏ qua hàng tiêu đề
                if row.find_elements(By.CSS_SELECTOR, ".table-title"):
                    continue

                try:
                    cells = row.find_elements(By.CSS_SELECTOR, ".list-box > div")
                    if len(cells) < 3: 
                        logger.warning("Một hàng sự kiện có ít hơn 3 ô, bỏ qua.")
                        continue

                    # Ô đầu tiên là div.time-display
                    date_text = cells[0].text.strip()
                    description = cells[1].text.strip()
                    location = cells[2].text.strip()
                    
                    # Xác định loại sự kiện dựa trên (Projected)
                    event_type = "ngay_du_kien" if "(Projected)" in date_text else "ngay_thuc_te"
                    
                    event_data = {
                        "date": date_text,
                        "type": event_type,
                        "description": description,
                        "location": location
                    }
                    events.append(event_data)
                    logger.debug("Đã trích xuất sự kiện: %s", event_data)

                except NoSuchElementException:
                    logger.warning("Lỗi khi đọc một hàng sự kiện, bỏ qua.", exc_info=True)
                    continue
        except Exception as e:
            logger.error("Lỗi nghiêm trọng khi trích xuất danh sách sự kiện: %s", e, exc_info=True)
            
        return events

    def _find_event(self, events, description_keyword, location_keyword, event_type=None):
        """
        Tìm một sự kiện cụ thể trong danh sách, có thể lọc theo loại (dự kiến/thực tế).
        Duyệt ngược (reversed) để tìm sự kiện GẦN NHẤT/CUỐI CÙNG khớp với tiêu chí.
        """
        if not location_keyword: 
            logger.debug("Bỏ qua _find_event vì location_keyword rỗng.")
            return {}
        
        # Duyệt ngược danh sách (đã được sắp xếp từ cũ -> mới) để tìm sự kiện cuối cùng
        for event in reversed(events):
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            
            # Xử lý trường hợp location_keyword là "Cntao" và event.location là "Cntao Cntao"
            event_location = (event.get("location") or "").lower()
            # Kiểm tra xem location_keyword có nằm trong event_location không
            loc_match = location_keyword.lower() in event_location
            
            type_match = True
            if event_type:
                type_match = (event.get("type") == event_type)

            if desc_match and loc_match and type_match:
                logger.debug("Tìm thấy sự kiện khớp: %s (cho keyword: %s, %s, %s)", event, description_keyword, location_keyword, event_type)
                return event
        
        logger.debug("Không tìm thấy sự kiện khớp cho: %s, %s, %s", description_keyword, location_keyword, event_type)
        return {}