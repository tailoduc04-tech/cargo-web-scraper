import logging
import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup
import re

from ..api_scraper import ApiScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module
logger = logging.getLogger(__name__)

def _split_location_and_datetime(input_string):
    """
    Tách chuỗi đầu vào thành (vị trí, ngày giờ). Trả về chuỗi rỗng nếu lỗi.
    """
    if not input_string:
        return "", ""
    pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2})'
    match = re.search(pattern, input_string)
    if match:
        split_index = match.start()
        location_part = input_string[:split_index].strip()
        datetime_part = match.group(0)
        return location_part, datetime_part
    else:
        return input_string.strip(), ""

class HeungALineScraper(ApiScraper):
    """
    Triển khai logic scraping cụ thể cho trang Heung-A Line và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu. Sử dụng requests và BeautifulSoup.
    """
    def __init__(self, driver, config):
        super().__init__(config=config)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://ebiz.heungaline.com/',
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD ...' sang 'DD/MM/YYYY'.
        Trả về "" nếu lỗi hoặc đầu vào không hợp lệ.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning(f"[HeungA Scraper] Không thể phân tích định dạng ngày: {date_str}")
            return "" # Trả về chuỗi rỗng nếu lỗi

    @staticmethod
    def _extract_code(location_text):
        """
        Trích xuất mã trong dấu ngoặc đơn từ tên vị trí.
        """
        if not location_text:
            return ""
        match = re.search(r'\((.*?)\)', location_text)
        if match:
            return match.group(1).lower()
        return location_text.lower()

    def _get_text_safe_soup(self, soup_element, selector, attribute=None):
        """
        Helper lấy text hoặc attribute từ phần tử BeautifulSoup một cách an toàn.
        Trả về chuỗi rỗng "" nếu không tìm thấy.
        """
        if not soup_element:
            return ""
        try:
            target = soup_element.select_one(selector)
            if target:
                if attribute:
                    return target.get(attribute, "").strip()
                else:
                    return ' '.join(target.stripped_strings)
            else:
                return ""
        except Exception as e:
            logger.warning(f"[HeungA Scraper] Lỗi khi lấy text/attribute từ soup selector '{selector}': {e}")
            return ""

    def _parse_event_datetime(self, date_str):
        """
        Helper: Chuyển đổi chuỗi ngày sự kiện thành đối tượng datetime để so sánh.
        """
        if not date_str:
            return None, None
        try:
            match = re.search(r'(\d{4}-\d{2}-\d{2})\s+[A-Z]{3}\s+(\d{2}:\d{2})', date_str)
            if match:
                 date_part = f"{match.group(1)} {match.group(2)}"
                 dt_obj = datetime.strptime(date_part, '%Y-%m-%d %H:%M')
                 return dt_obj, date_str
            else:
                 match_simple = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})', date_str)
                 if match_simple:
                     dt_obj = datetime.strptime(match_simple.group(1), '%Y-%m-%d %H:%M')
                     return dt_obj, date_str
                 else:
                     logger.warning("[HeungA Scraper] Pattern không khớp với event datetime: '%s'", date_str)
                     return None, None
        except (ValueError, IndexError):
            logger.warning("[HeungA Scraper] Không thể parse event datetime: '%s'", date_str, exc_info=True)
            return None, None

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã B/L trên trang Heung-A Line bằng requests và BeautifulSoup.
        """
        logger.info(f"[HeungA Scraper] Bắt đầu scrape cho mã: {tracking_number} (sử dụng requests)")
        t_total_start = time.time()
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            t_req_start = time.time()
            # Gửi request để lấy HTML
            response = self.session.get(direct_url, timeout=30)
            response.raise_for_status() # Kiểm tra lỗi HTTP
            
            # with open("output/heunga_response.html", 'w', encoding='utf-8') as f:
            #     print("Saving raw HTML response to output/heunga_response.html")
            #     f.write(response.text)
            
            logger.info("-> (Thời gian) Tải HTML: %.2fs", time.time() - t_req_start)

            # Parse HTML bằng BeautifulSoup
            t_parse_start = time.time()
            soup = BeautifulSoup(response.text, 'lxml')
            logger.info("-> (Thời gian) Parse HTML bằng BeautifulSoup: %.2fs", time.time() - t_parse_start)

            # Kiểm tra xem có panel schedule không (dấu hiệu trang tải đúng)
            schedule_panel_check = soup.select_one("#divSchedule")
            if not schedule_panel_check:
                 logger.warning("[HeungA Scraper] Không tìm thấy panel '#divSchedule'. Có thể mã tracking không hợp lệ hoặc trang lỗi.")
                 # Thử tìm thông báo lỗi (HeungA và Sinokor dùng chung ID này)
                 error_alert = soup.select_one('#e-alert-message')
                 if error_alert:
                     error_msg = error_alert.get_text(strip=True)
                     logger.error("[HeungA Scraper] Trang trả về lỗi: %s", error_msg)
                     return None, f"Trang Heung-A báo lỗi: {error_msg}"
                 else:
                    return None, f"Không tìm thấy dữ liệu hoặc trang lỗi cho '{tracking_number}'."

            # Trích xuất và chuẩn hóa dữ liệu từ soup
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_soup(soup, tracking_number) # Hàm mới dùng soup
            logger.info("-> (Thời gian) Trích xuất dữ liệu từ soup: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                logger.warning("[HeungA Scraper] Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info(f"[HeungA Scraper] Hoàn tất scrape thành công cho mã: {tracking_number} (Tổng thời gian: %.2fs)",
                         t_total_end - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
            t_total_fail = time.time()
            logger.warning("[HeungA Scraper] Timeout khi scrape mã '%s' (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_fail - t_total_start)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'. Request bị timeout."
        except requests.exceptions.HTTPError as e:
             t_total_fail = time.time()
             logger.error("[HeungA Scraper] Lỗi HTTP %s khi scrape mã '%s' (Tổng thời gian: %.2fs)",
                          e.response.status_code, tracking_number, t_total_fail - t_total_start)
             return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
            t_total_fail = time.time()
            logger.error("[HeungA Scraper] Lỗi kết nối khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except Exception as e:
            t_total_fail = time.time()
            logger.error(f"[HeungA Scraper] Đã xảy ra lỗi không mong muốn khi scrape mã '{tracking_number}': {e} (Tổng thời gian: %.2fs)",
                         t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_history_events_soup(self, tbody_soup):
        """
        Trích xuất tất cả các sự kiện từ tbody của bảng Cargo Tracking (đã parse bằng BeautifulSoup).
        """
        events = []
        if not tbody_soup:
             logger.warning("[HeungA Scraper] --> Không tìm thấy tbody của bảng lịch sử sự kiện.")
             return events
        rows = tbody_soup.find_all("tr", recursive=False)
        logger.info("[HeungA Scraper] --> Tìm thấy %d hàng trong bảng lịch sử sự kiện.", len(rows))
        current_event_group = ""
        is_container_event = False
        for row in rows:
            header_th = row.find("th", class_="firstTh")
            if header_th:
                current_event_group = header_th.get_text(strip=True)
                is_container_event = "pickup" in current_event_group.lower() or "return" in current_event_group.lower()
                logger.debug("---> Processing event group: %s (Container: %s)", current_event_group, is_container_event)
                continue
            cells = row.find_all("td", recursive=False)
            if not cells or len(cells) < 3:
                logger.debug("---> Skipping invalid row in history table.")
                continue
            date_text, location, description = "", "", ""
            cell_texts = [c.get_text(strip=True) for c in cells]
            if is_container_event:
                cntr_no, location, date_text = cell_texts[:3]
                description = f"{current_event_group}: {cntr_no}"
            else:
                vessel_voyage, location, date_text = cell_texts[:3]
                description = f"{current_event_group}: {vessel_voyage}"
            if date_text:
                event_data = {"description": description, "location": location, "date": date_text}
                events.append(event_data)
                logger.debug("---> Extracted event: %s", event_data)
        logger.info("[HeungA Scraper] --> Trích xuất được %d sự kiện từ lịch sử.", len(events))
        return events

    def _find_event_soup(self, events, description_keyword, location_keyword):
        """
        Tìm sự kiện cụ thể trong list events (đã trích xuất từ soup).
        """
        if not location_keyword:
            logger.debug("[HeungA Scraper] --> _find_event_soup: Thiếu location_keyword cho '%s'", description_keyword)
            return None
        match = re.search(r'\((.*?)\)', location_keyword)
        location_code_keyword = match.group(1).lower() if match else location_keyword.lower()
        logger.debug("[HeungA Scraper] --> _find_event_soup: Tìm '%s' tại code '%s'", description_keyword, location_code_keyword)
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower().split(':')[0]
            loc_match = location_code_keyword in event.get("location", "").lower()
            if desc_match and loc_match:
                logger.debug("---> Khớp: %s", event)
                return event
        logger.debug("---> Không khớp.")
        return None

    def _extract_and_normalize_data_soup(self, soup, tracking_number):
        """
        Hàm chính để trích xuất, xử lý và chuẩn hóa dữ liệu từ đối tượng BeautifulSoup.
        """
        logger.info(f"[HeungA Scraper] Đang trích xuất dữ liệu cho {tracking_number}")
        t_extract_detail_start = time.time()
        try:
            today_dt = datetime.now()

            # 1. Trích xuất thông tin chung
            t_basic_info_start = time.time()
            logger.debug("Trích xuất thông tin B/L No và B/K Status...")
            bl_no_label = soup.find('label', string=lambda text: text and 'B/L No.' in text)
            bl_no = ""
            if bl_no_label:
                value_div = bl_no_label.find_parent('div', class_='form-group').find_next_sibling('div')
                if value_div: bl_no = self._get_text_safe_soup(value_div, 'span')

            bk_status_label = soup.find('label', string=lambda text: text and 'B/K Status' in text)
            booking_status = ""
            if bk_status_label:
                 value_div = bk_status_label.find_parent('div', class_='form-group').find_next_sibling('div')
                 if value_div: booking_status = self._get_text_safe_soup(value_div, 'span')

            logger.info(f"[HeungA Scraper] BlNumber: {bl_no}, BookingStatus: {booking_status}")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_basic_info_start)

            # 2. Trích xuất thông tin Schedule (ETD và ETA)
            t_schedule_start = time.time()
            logger.debug("Trích xuất thông tin Schedule (ETD, ETA)...")
            schedule_panel = soup.select_one("#divSchedule")
            etd_str_raw = self._get_text_safe_soup(schedule_panel, "li.col-sm-8 .col-sm-6:nth-child(1)")
            eta_str_raw = self._get_text_safe_soup(schedule_panel, "li.col-sm-8 .col-sm-6:nth-child(2)")

            pol, etd = _split_location_and_datetime(etd_str_raw)
            pod, eta = _split_location_and_datetime(eta_str_raw)

            pol_terminal = self._get_text_safe_soup(schedule_panel.select_one("li.col-sm-8 .col-sm-6:nth-child(1)"), "a")
            pod_terminal = self._get_text_safe_soup(schedule_panel.select_one("li.col-sm-8 .col-sm-6:nth-child(2)"), "span:not([class])")

            logger.info("[HeungA Scraper] Schedule Info: POL=%s, POD=%s, ETD=%s, ETA=%s", pol, pod, etd, eta)
            logger.debug("-> (Thời gian) Trích xuất schedule: %.2fs", time.time() - t_schedule_start)

            # 3. Trích xuất lịch sử sự kiện từ bảng Cargo Tracking
            t_history_start = time.time()
            logger.debug("Trích xuất lịch sử sự kiện từ Cargo Tracking...")
            cargo_tracking_table = soup.select_one("#divDetailInfo .splitTable table tbody")
            history_events = self._extract_history_events_soup(cargo_tracking_table)
            logger.debug("-> (Thời gian) Trích xuất lịch sử sự kiện: %.2fs", time.time() - t_history_start)

            # 4. Tìm các ngày thực tế / transit từ lịch sử
            t_process_events_start = time.time()
            atd = ""
            ata = ""
            transit_port_list = []
            eta_transit = ""
            ata_transit = ""
            atd_transit = ""
            etd_transit_final = ""
            future_etd_transits = []

            atd_event = self._find_event_soup(history_events, "Departure", pol_terminal or pol)
            pod_arrival_event = self._find_event_soup(history_events, "Arrival", pod_terminal or pod)

            # Xử lý ATD
            if atd_event:
                atd_dt, atd_str_full = self._parse_event_datetime(atd_event.get("date"))
                if atd_dt and atd_dt <= today_dt: atd = atd_str_full
                elif atd_dt: etd = atd_str_full # Cập nhật ETD nếu event > today

            # Xử lý ATA
            if pod_arrival_event:
                ata_dt, ata_str_full = self._parse_event_datetime(pod_arrival_event.get("date"))
                if ata_dt and ata_dt <= today_dt: ata = ata_str_full
                elif ata_dt: eta = ata_str_full # Cập nhật ETA nếu event > today

            logger.info(f"[HeungA Scraper] ATD (Actual): {atd}, ATA (Actual): {ata}")
            logger.info(f"[HeungA Scraper] ETD (Updated): {etd}, ETA (Updated): {eta}")


            # 5. Xử lý logic Transit
            logger.debug("Bắt đầu xử lý logic transit...")
            pol_compare_str = (pol_terminal or pol).lower()
            pod_compare_str = (pod_terminal or pod).lower()

            for event in history_events:
                desc = event.get("description", "").lower().split(':')[0].strip()
                loc = event.get("location", "")
                loc_lower = loc.lower()

                is_pol_event = pol_compare_str in loc_lower and "departure" in desc
                is_pod_event = pod_compare_str in loc_lower and "arrival" in desc

                if is_pol_event or is_pod_event: continue

                if ("departure" in desc or "arrival" in desc):
                    logger.debug("[HeungA Scraper] Phát hiện sự kiện transit: %s tại %s", desc, loc)
                    if loc and loc not in transit_port_list:
                        transit_port_list.append(loc)

                    event_dt, event_str = self._parse_event_datetime(event.get("date"))
                    if not event_dt: continue

                    if "arrival" in desc:
                        if event_dt <= today_dt: # Actual Arrival
                            if not ata_transit: ata_transit = event_str
                        else: # Estimated Arrival
                            if not ata_transit and not eta_transit: eta_transit = event_str

                    if "departure" in desc:
                        if event_dt <= today_dt: # Actual Departure
                            atd_transit = event_str # Lấy cái cuối
                        else: # Estimated Departure
                            future_etd_transits.append((event_dt, event_str))

            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit_final = future_etd_transits[0][1]
                logger.info("[HeungA Scraper] EtdTransit (Estimated) gần nhất được chọn: %s", etd_transit_final)
            else:
                 logger.info("[HeungA Scraper] Không tìm thấy EtdTransit nào trong tương lai.")
            logger.debug("-> (Thời gian) Xử lý sự kiện và transit: %.2fs", time.time() - t_process_events_start)

            # 6. Xây dựng đối tượng JSON
            t_normalize_start = time.time()
            shipment_data = N8nTrackingInfo(
                  BookingNo= tracking_number,
                  BlNumber= bl_no or tracking_number,
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

            logger.info(f"[HeungA Scraper] Trích xuất dữ liệu thành công cho {tracking_number}.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("[HeungA Scraper] --- Hoàn tất trích xuất chi tiết. (Tổng thời gian trích xuất: %.2fs) ---", time.time() - t_extract_detail_start)

            return shipment_data

        except Exception as e:
            logger.error(f"[HeungA Scraper] Lỗi nghiêm trọng trong quá trình trích xuất dữ liệu cho '{tracking_number}': {e}", exc_info=True)
            logger.info("[HeungA Scraper] --- Hoàn tất trích xuất chi tiết (lỗi). (Tổng thời gian trích xuất: %.2fs) ---", time.time() - t_extract_detail_start)
            return None