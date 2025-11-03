import logging
import requests
import json
import time
from datetime import datetime, date

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module này
logger = logging.getLogger(__name__)

class MscScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web MSC,
    bằng cách gọi API trực tiếp và chuẩn hóa kết quả theo template JSON.
    """

    def __init__(self, driver, config):
        self.config = config
        self.api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://www.msc.com',
            'Referer': 'https://www.msc.com/en/track-a-shipment',
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
        Chuyển đổi chuỗi ngày từ 'DD/MM/YYYY' sang 'DD/MM/YYYY'.
        Trả về chuỗi rỗng "" nếu định dạng không hợp lệ hoặc đầu vào là None/rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            datetime.strptime(date_str, '%d/%m/%Y')
            return date_str
        except (ValueError, TypeError):
            logger.warning("[MSC API Scraper] Không thể phân tích định dạng ngày: %s. Trả về chuỗi rỗng.", date_str)
            return ""

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một Booking Number bằng cách gọi API
        và trả về một đối tượng N8nTrackingInfo hoặc lỗi.
        """
        logger.info("[MSC API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time() # Tổng thời gian bắt đầu

        # Payload
        payload = {
            "trackingNumber": tracking_number,
            "trackingMode": "1"
        }

        try:
            logger.info(f"[MSC API Scraper] Gửi POST request đến: {self.api_url}")
            t_request_start = time.time()
            response = self.session.post(self.api_url, json=payload, timeout=30)
            logger.info("-> (Thời gian) Gọi API: %.2fs", time.time() - t_request_start)
            response.raise_for_status() # Kiểm tra lỗi HTTP (4xx, 5xx)

            t_parse_start = time.time()
            data = response.json()
            logger.debug("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)

            # Kiểm tra response thành công và có dữ liệu
            if not data or not data.get("IsSuccess") or not data.get("Data", {}).get("BillOfLadings"):
                 error_msg = "API không trả về dữ liệu thành công hoặc không có thông tin BillOfLadings."
                 logger.warning("[MSC API Scraper] %s Response: %s", error_msg, data)
                 return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên API MSC."

            # Trích xuất và chuẩn hóa dữ liệu từ response JSON
            t_extract_start = time.time()
            # Chỉ lấy dữ liệu từ Bill of Lading đầu tiên trong danh sách
            bill_of_lading_data = data["Data"]["BillOfLadings"][0]
            normalized_data = self._extract_and_normalize_data_api(bill_of_lading_data, tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                logger.warning("[MSC API Scraper] Không thể chuẩn hóa dữ liệu từ API cho mã: %s.", tracking_number)
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[MSC API Scraper] Hoàn tất thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
             t_total_fail = time.time()
             logger.warning("[MSC API Scraper] Timeout khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                          tracking_number, t_total_fail - t_total_start)
             return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[MSC API Scraper] Lỗi HTTP %s khi gọi API cho mã '%s'. Response: %s (Tổng thời gian: %.2fs)",
                         e.response.status_code, tracking_number, e.response.text, t_total_fail - t_total_start, exc_info=False)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[MSC API Scraper] Lỗi kết nối khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except json.JSONDecodeError:
             t_total_fail = time.time()
             logger.error("[MSC API Scraper] Không thể parse JSON từ response API cho mã '%s'. Response text: %s (Tổng thời gian: %.2fs)",
                          tracking_number, response.text, t_total_fail - t_total_start)
             return None, f"API Response không phải JSON hợp lệ cho '{tracking_number}'."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[MSC API Scraper] Lỗi không mong muốn cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _find_event_api(self, events, description_keyword, location_keyword_or_list=None, find_first=False, find_last=False):
        """
        Helper: Tìm một sự kiện cụ thể trong danh sách sự kiện từ API.
        Sự kiện trong API được sắp xếp theo 'Order' giảm dần (mới nhất trước).
        Trả về dictionary của event hoặc {} nếu không tìm thấy.
        location_keyword_or_list: chuỗi hoặc list chuỗi, chỉ cần location chứa 1 trong các keyword.
        """
        matches = []
        if not events:
            logger.debug("[MSC API Scraper] -> _find_event_api: Danh sách sự kiện rỗng.")
            return {}

        desc_keyword_lower = description_keyword.lower()

        keywords_to_check = []
        if location_keyword_or_list:
            if isinstance(location_keyword_or_list, list):
                keywords_to_check = [k.strip().lower() for k in location_keyword_or_list if k and k.strip()]
            elif isinstance(location_keyword_or_list, str) and location_keyword_or_list.strip():
                 keywords_to_check = [location_keyword_or_list.strip().lower()]

        logger.debug("[MSC API Scraper] -> _find_event_api: Tìm '%s' tại '%s' (Keywords: %s), first: %s, last: %s",
                     description_keyword, location_keyword_or_list, keywords_to_check, find_first, find_last)

        # Sắp xếp lại sự kiện theo Order tăng dần (từ cũ đến mới) để logic first/last hoạt động đúng
        sorted_events = sorted(events, key=lambda x: x.get('Order', float('inf')))

        for event in sorted_events:
            desc = event.get("Description", "").lower()
            loc = (event.get("Location") or "").strip().lower()

            desc_match = desc_keyword_lower in desc

            # Chỉ kiểm tra location nếu có keywords_to_check
            loc_match = not keywords_to_check or any(keyword in loc for keyword in keywords_to_check)

            if desc_match and loc_match:
                matches.append(event)
                if find_first:
                    # Vì list đã sort, match đầu tiên là match cần tìm
                    logger.debug("---> Tìm thấy event (first): %s", event)
                    return event

        if not matches:
            logger.debug("---> Không tìm thấy event khớp.")
            return {}

        # Nếu không yêu cầu first, hoặc yêu cầu last (hoặc mặc định)
        result = matches[-1] # Match cuối cùng trong list đã sort
        logger.debug("---> Tìm thấy event (last/default): %s", result)
        return result

    def _extract_and_normalize_data_api(self, bl_data, booking_no_input):
        """
        Trích xuất và chuẩn hóa dữ liệu từ dictionary JSON của một BillOfLading.
        Chỉ xử lý container đầu tiên.
        """
        logger.info("[MSC API Scraper] --- Bắt đầu _extract_and_normalize_data_api ---")
        t_extract_detail_start = time.time()
        try:
            general_info = bl_data.get("GeneralTrackingInfo", {})
            containers_info = bl_data.get("ContainersInfo", [])

            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT CHUNG ===
            logger.debug("Bắt đầu trích xuất thông tin tóm tắt...")
            bl_number = bl_data.get("BillOfLadingNumber", "")
            pol = general_info.get("PortOfLoad", "")
            pod = general_info.get("PortOfDischarge", "")
            # API không có Booking Status rõ ràng, để trống
            booking_status = ""
            transit_ports = general_info.get("Transshipments", []) # Danh sách các cảng transit

            logger.info("Thông tin tóm tắt: POL='%s', POD='%s', BL='%s', TransitPorts=%s", pol, pod, bl_number, transit_ports)

            # === BƯỚC 2: THU THẬP EVENTS TỪ CONTAINER ĐẦU TIÊN ===
            all_events = []
            if containers_info:
                logger.info("Bắt đầu thu thập events từ container đầu tiên (trong số %d).", len(containers_info))
                first_container = containers_info[0]
                container_events = first_container.get("Events", [])
                logger.debug("Container đầu tiên ('%s') có %d event(s).", first_container.get("ContainerNumber"), len(container_events))
                all_events = container_events # API đã trả về list events
            else:
                logger.warning("Không tìm thấy thông tin container (ContainersInfo) trong response.")

            logger.info("Đã thu thập tổng cộng %d event(s) (từ container đầu tiên).", len(all_events))

            # === BƯỚC 3: TÌM CÁC SỰ KIỆN CHÍNH (ETD/ATD POL, ETA/ATA POD) ===
            logger.debug("Tìm kiếm các sự kiện chính...")
            # ATD POL: Sự kiện "Export Loaded on Vessel" cuối cùng tại POL
            atd_event = self._find_event_api(all_events, "Export Loaded on Vessel", pol, find_last=True)
            # ETA POD: Sự kiện "Estimated Time of Arrival" cuối cùng tại POD
            eta_event = self._find_event_api(all_events, "Estimated Time of Arrival", pod, find_last=True)
            # ATA POD: Sự kiện "Discharged from vessel" hoặc tương tự đầu tiên tại POD (API dùng chữ hoa)
            # MSC API dùng "Full container discharged" thay vì "Discharged from vessel"
            ata_event = self._find_event_api(all_events, "Full container discharged", pod, find_first=True)
            # Nếu không thấy "Full container discharged", thử tìm "Discharged From Vessel" (trường hợp cũ?)
            if not ata_event:
                 ata_event = self._find_event_api(all_events, "Discharged From Vessel", pod, find_first=True)


            etd_pol = "" # ETD POL: API không cung cấp rõ ràng, để trống
            atd_pol = self._format_date(atd_event.get("Date")) if atd_event else ""
            eta_pod = self._format_date(eta_event.get("Date")) if eta_event else ""
            ata_pod = self._format_date(ata_event.get("Date")) if ata_event else ""

            logger.info("Sự kiện chính: ATD POL='%s', ETA POD='%s', ATA POD='%s'", atd_pol, eta_pod, ata_pod)

            # === BƯỚC 4: TÌM CÁC SỰ KIỆN TRANSIT ===
            ata_transit_event = None
            eta_transit_event = None
            atd_transit_event = None
            etd_transit_final_str = None
            future_etd_transits = []
            today = date.today()

            if transit_ports:
                logger.info("Bắt đầu xử lý logic transit cho các cảng: %s", transit_ports)
                # Tìm AtaTransit: Sự kiện "Full Transshipment Discharged" đầu tiên tại BẤT KỲ cảng transit nào
                ata_transit_event = self._find_event_api(all_events, "Full Transshipment Discharged", transit_ports, find_first=True)

                # Tìm EtaTransit: Nếu không có AtaTransit, tìm "Estimated Time of Arrival" đầu tiên tại BẤT KỲ cảng transit nào
                if not ata_transit_event:
                    eta_transit_event = self._find_event_api(all_events, "Estimated Time of Arrival", transit_ports, find_first=True)

                # Tìm AtdTransit: Sự kiện "Full Transshipment Loaded" cuối cùng tại BẤT KỲ cảng transit nào
                atd_transit_event = self._find_event_api(all_events, "Full Transshipment Loaded", transit_ports, find_last=True)

                # Tìm EtdTransit: Tìm tất cả "Estimated Time of Departure" (không có trong API này)
                # HOẶC "Full Intended Transshipment"
                logger.debug("Tìm kiếm EtdTransit trong tương lai (dựa trên 'Full Intended Transshipment')...")
                transit_ports_lower = [tp.lower() for tp in transit_ports]
                for event in all_events: # Duyệt qua all_events gốc (đã sort theo Order)
                    loc = (event.get('Location') or "").strip().lower()
                    desc = event.get('Description', '').lower()
                    date_str = event.get('Date')

                    is_transit_loc = any(tp_lower in loc for tp_lower in transit_ports_lower)

                if future_etd_transits:
                    future_etd_transits.sort() # Sắp xếp theo ngày tăng dần
                    etd_transit_final_str = future_etd_transits[0][2] # Lấy chuỗi ngày của event gần nhất
                    logger.info("EtdTransit gần nhất trong tương lai được chọn: %s", etd_transit_final_str)
                else:
                    logger.info("Không tìm thấy EtdTransit nào trong tương lai (API mới không cung cấp?).")
            else:
                 logger.info("Không có cảng transit, bỏ qua xử lý transit.")

            # Format các ngày transit
            ata_transit = self._format_date(ata_transit_event.get("Date")) if ata_transit_event else ""
            eta_transit = self._format_date(eta_transit_event.get("Date")) if eta_transit_event else ""
            atd_transit = self._format_date(atd_transit_event.get("Date")) if atd_transit_event else ""
            etd_transit = self._format_date(etd_transit_final_str) or "" # Sẽ là "" vì logic trên

            logger.info("Sự kiện Transit: Ata='%s', Eta='%s', Atd='%s', Etd='%s'", ata_transit, eta_transit, atd_transit, etd_transit)

            # === BƯỚC 5: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            logger.debug("Tạo đối tượng N8nTrackingInfo...")
            shipment_data = N8nTrackingInfo(
                BookingNo=booking_no_input or "", # Input là booking no
                BlNumber=bl_number or "",
                BookingStatus=booking_status, # ""
                Pol=pol or "",
                Pod=pod or "",
                Etd=etd_pol, # ""
                Atd=atd_pol,
                Eta=eta_pod,
                Ata=ata_pod,
                TransitPort=", ".join(transit_ports) or "",
                EtdTransit=etd_transit, # ""
                AtdTransit=atd_transit,
                EtaTransit=eta_transit,
                AtaTransit=ata_transit
            )

            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công cho mã '%s'.", booking_no_input)
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_extract_detail_start)
            logger.info("[MSC API Scraper] --- Hoàn tất _extract_and_normalize_data_api --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)

            return shipment_data

        except Exception as e:
            logger.error("[MSC API Scraper] Lỗi trong quá trình trích xuất chi tiết từ API cho mã '%s': %s", booking_no_input, e, exc_info=True)
            logger.info("[MSC API Scraper] --- Hoàn tất _extract_and_normalize_data_api (lỗi) --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)

            return None