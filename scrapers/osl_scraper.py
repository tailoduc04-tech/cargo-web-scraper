import logging
import requests
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup 

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo
logger = logging.getLogger(__name__)

class OslScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Oceanic Star Line (OSL)
    bằng cách gọi API trực tiếp và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def __init__(self, driver, config):
        self.config = config
        self.api_url = "https://star-liners.com/wp-admin/admin-ajax.php"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://star-liners.com',
            'Referer': 'https://star-liners.com/track-my-shipment/',
            'Sec-Ch-Ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        })


    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'Weekday, DD-Mon-YYYY' (ví dụ: Sunday, 20-Jul-2025)
        sang định dạng 'DD/MM/YYYY'. Trả về "" nếu lỗi.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            date_part = date_str.split(", ")[1]
            dt_obj = datetime.strptime(date_part, '%d-%b-%Y')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[OSL API Scraper] Cảnh báo: Không thể phân tích định dạng ngày: %s", date_str)
            return ""

    def scrape(self, tracking_number):
        """
        Phương thức scraping chính cho Oceanic Star Line bằng API.
        """
        logger.info("[OSL API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time() # Tổng thời gian bắt đầu

        payload = {
            'nonce': '23a2b8b108',
            'container_no': '',
            'bl_no': tracking_number,
            'action': 'search'
        }

        try:
            logger.info(f"[OSL API Scraper] -> Gửi POST request đến: {self.api_url}")
            t_request_start = time.time()
            response = self.session.post(self.api_url, data=payload, timeout=30)
            logger.info("-> (Thời gian) Gọi API: %.2fs", time.time() - t_request_start)
            response.raise_for_status()

            t_parse_start = time.time()
            api_response = response.json()
            logger.info("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)

            # Kiểm tra trạng thái và dữ liệu trả về từ API
            if api_response.get("status") == 1 and api_response.get("data"):
                html_data = api_response["data"]
                # Parse HTML string bằng BeautifulSoup
                soup = BeautifulSoup(html_data, 'lxml')

                t_extract_start = time.time()
                normalized_data = self._extract_and_normalize_data(soup, tracking_number)
                logger.info("-> (Thời gian) Trích xuất dữ liệu từ HTML: %.2fs", time.time() - t_extract_start)

                if not normalized_data:
                    return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}' từ HTML response."

                t_total_end = time.time()
                logger.info("[OSL API Scraper] -> Hoàn tất scrape thành công. (Tổng thời gian: %.2fs)", t_total_end - t_total_start)
                return normalized_data, None
            else:
                error_message = api_response.get("response", "API không trả về dữ liệu thành công.")
                logger.warning(f"[OSL API Scraper] API trả về lỗi hoặc không có dữ liệu cho '{tracking_number}': {error_message}")
                return None, f"Không tìm thấy dữ liệu cho '{tracking_number}': {error_message}"

        except requests.exceptions.Timeout:
            t_total_fail = time.time()
            logger.warning("[OSL API Scraper] Timeout khi gọi API. (Tổng thời gian: %.2fs)", t_total_fail - t_total_start)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[OSL API Scraper] Lỗi HTTP %s khi gọi API: %s (Tổng thời gian: %.2fs)",
                         e.response.status_code, e.response.text, t_total_fail - t_total_start)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[OSL API Scraper] Lỗi kết nối khi gọi API: %s (Tổng thời gian: %.2fs)", e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except json.JSONDecodeError:
            t_total_fail = time.time()
            logger.error("[OSL API Scraper] Không thể parse JSON từ response API. Response text: %s (Tổng thời gian: %.2fs)", response.text, t_total_fail - t_total_start)
            return None, f"API Response không phải JSON hợp lệ cho '{tracking_number}'."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[OSL API Scraper] Lỗi không mong muốn: %s (Tổng thời gian: %.2fs)", e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_all_events(self, soup):
        """
        Trích xuất toàn bộ lịch sử di chuyển từ HTML table trong response API.
        Input là đối tượng BeautifulSoup đã parse từ HTML string.
        """
        events = []
        # HTML được trả về trực tiếp là các thẻ <tr>
        rows = soup.find_all("tr")
        logger.debug("Tìm thấy %d hàng sự kiện trong HTML data.", len(rows))

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5: # Cần ít nhất 5 cột theo response mẫu
                logger.debug("Bỏ qua hàng không đủ cột: %s", row.prettify())
                continue

            # Cột 0: Container No, 1: BL No, 2: Date, 3: Description, 4: Location
            event_data = {
                "container_no": cells[0].text.strip(),
                "bl_number": cells[1].text.strip(),
                "date": cells[2].text.strip(),
                "description": cells[3].text.strip().upper(),
                "location": cells[4].text.strip()
            }
            # Lấy thông tin Vessel/Voyage nếu có (cột 5, 6)
            if len(cells) > 5:
                event_data["vessel"] = cells[5].text.strip()
            if len(cells) > 6:
                 event_data["voyage"] = cells[6].text.strip()

            events.append(event_data)
        logger.debug("Đã trích xuất %d sự kiện từ HTML.", len(events))
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """
        Tìm một sự kiện cụ thể trong danh sách, có thể lọc theo cảng.
        Tìm sự kiện ĐẦU TIÊN khớp (vì dữ liệu trả về từ mới đến cũ).
        """
        logger.debug("Tìm event: desc='%s', loc='%s'", description_keyword, location_keyword)
        for event in events: # Duyệt từ mới đến cũ (như trong response)
            desc_match = description_keyword.upper() in event.get("description", "")

            loc_match = True # Mặc định là khớp nếu không cần kiểm tra cảng
            if location_keyword:
                loc_match = location_keyword.upper() in event.get("location", "").upper()

            if desc_match and loc_match:
                logger.debug("--> Khớp: %s", event)
                return event # Trả về sự kiện đầu tiên tìm thấy
        logger.debug("--> Không khớp.")
        return {}

    def _extract_and_normalize_data(self, soup, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ soup HTML, áp dụng logic tìm kiếm sự kiện.
        """
        try:
            all_events = self._extract_all_events(soup)
            if not all_events:
                logger.warning("[OSL API Scraper] Không tìm thấy sự kiện nào trong HTML response.")
                return None

            # --- Xác định các thông tin cơ bản ---
            # Lấy BL Number từ sự kiện đầu tiên có BL (vì một số sự kiện đầu có thể trống)
            bl_number = next((event.get("bl_number") for event in all_events if event.get("bl_number")), tracking_number)
            booking_status = all_events[0].get("description") # Sự kiện mới nhất

            # --- Tìm sự kiện đi và đến chính ---
            # Dữ liệu từ mới -> cũ, nên _find_event sẽ lấy sự kiện gần nhất (mới nhất)
            departure_event = self._find_event(all_events, "LOAD FULL")
            arrival_event = self._find_event(all_events, "DISCHARGE FULL")

            pol = departure_event.get("location")
            pod = arrival_event.get("location")

            logger.info(f"POL: {pol}, POD: {pod}")

            # --- Tìm kiếm cảng và ngày trung chuyển ---
            transit_ports = []
            ts_discharge_events = []
            ts_load_events = []

            # Duyệt ngược để xử lý theo thứ tự thời gian (cũ -> mới) cho logic transit
            for event in reversed(all_events):
                port = event.get('location')
                desc = event.get('description', '')

                # Nếu sự kiện dỡ/tải hàng không diễn ra tại POL hoặc POD, đó là cảng trung chuyển
                is_transit_event = "DISCHARGE" in desc or "LOAD" in desc
                # Kiểm tra cẩn thận None trước khi so sánh
                is_at_pol = bool(port and pol and port.upper() == pol.upper())
                is_at_pod = bool(port and pod and port.upper() == pod.upper())

                if is_transit_event and not is_at_pol and not is_at_pod:
                    if port and port not in transit_ports:
                        transit_ports.append(port)
                        logger.debug(f"Thêm cảng transit: {port}")
                    if "DISCHARGE" in desc:
                        ts_discharge_events.append(event)
                    elif "LOAD" in desc:
                        ts_load_events.append(event)

            # AtaTransit là DISCHARGE đầu tiên ở cảng transit (trong list đã sort)
            # AtdTransit là LOAD cuối cùng ở cảng transit (trong list đã sort)
            ata_transit_date = ts_discharge_events[0].get('date') if ts_discharge_events else None
            atd_transit_date = ts_load_events[-1].get('date') if ts_load_events else None

            logger.info(f"Transit Ports: {transit_ports}, AtaT: {ata_transit_date}, AtdT: {atd_transit_date}")

            # === BƯỚC 4: Xây dựng đối tượng JSON cuối cùng ===
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number, # API không trả về BookingNo, dùng tracking_number
                BlNumber= bl_number or "",
                BookingStatus= booking_status or "",
                Pol= pol or "",
                Pod= pod or "",
                Etd= "",
                Atd= self._format_date(departure_event.get("date")) or "",
                Eta= "",
                Ata= self._format_date(arrival_event.get("date")) or "",
                TransitPort= ", ".join(transit_ports) if transit_ports else "",
                EtdTransit= "",
                AtdTransit= self._format_date(atd_transit_date) or "",
                EtaTransit= "",
                AtaTransit= self._format_date(ata_transit_date) or ""
            )
            logger.info("Đã tạo đối tượng N8nTrackingInfo.")
            return shipment_data
        except Exception as e:
            logger.error("[OSL API Scraper] Lỗi trong quá trình trích xuất từ HTML: %s", e, exc_info=True)
            return None