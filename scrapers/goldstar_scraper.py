import logging
import requests
import time
from datetime import datetime, date
import json

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module
logger = logging.getLogger(__name__)

class GoldstarScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Gold Star Line bằng cách gọi API trực tiếp
    và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def __init__(self, driver, config): # driver không còn được sử dụng
        self.config = config
        # API endpoint lấy từ file test_goldstar.py
        self.api_url = "https://www.goldstarline.com/api/cms"
        self.session = requests.Session()
        # Headers cơ bản dựa trên file test_goldstar.py
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json;charset=UTF-8', # Dùng application/json vì payload là JSON
            'Origin': 'https://www.goldstarline.com',
            'Referer': 'https://www.goldstarline.com/tools/track_shipment',
            'Sec-Ch-Ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ API format 'YYYY-MM-DDTHH:MM:SS' hoặc
        'YYYY-MM-DDTHH:MM:SS.000' sang 'DD/MM/YYYY'.
        Trả về "" nếu đầu vào không hợp lệ hoặc rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Lấy phần ngày tháng năm, bỏ qua phần giờ và mili giây
            date_part = date_str.split('T')[0]
            if not date_part:
                return ""
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[Goldstar API Scraper] Không thể phân tích định dạng ngày: %s", date_str)
            return ""

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính bằng API.
        Thực hiện gọi API tracking.
        """
        logger.info("[Goldstar API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()

        # Payload dựa trên file test_goldstar.py
        payload = {
            "action": "get_track_shipment_val",
            "country_code": "HK", # Mã này có thể cần thay đổi hoặc không quan trọng
            "containerid": tracking_number # API dùng key là 'containerid' cho cả B/L và container
        }

        try:
            logger.info(f"[Goldstar API Scraper] Gửi POST request đến API: {self.api_url}")
            t_api_start = time.time()
            response = self.session.post(self.api_url, json=payload, timeout=30)
            logger.info("-> (Thời gian) Gọi API tracking: %.2fs", time.time() - t_api_start)
            response.raise_for_status()

            t_parse_start = time.time()
            data = response.json()
            logger.info("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)

            # Kiểm tra response thành công và có dữ liệu cần thiết
            if data.get("status") != "OK" or not data.get("data", {}).get("message", {}).get("response"):
                error_msg = data.get("message", "API không trả về dữ liệu thành công.")
                logger.warning("[Goldstar API Scraper] API không trả về dữ liệu thành công cho '%s': %s", tracking_number, error_msg)
                # Kiểm tra xem có phải lỗi do không tìm thấy B/L/Container không
                if isinstance(data.get("data"), dict) and data["data"].get("status") == 0:
                     error_msg = data["data"].get("message", "Không tìm thấy thông tin.")
                     logger.warning("[Goldstar API Scraper] API báo không tìm thấy thông tin: %s", error_msg)
                     return None, f"Không tìm thấy dữ liệu cho '{tracking_number}': {error_msg}"
                return None, f"Không tìm thấy dữ liệu cho '{tracking_number}': {error_msg}"

            # Trích xuất và chuẩn hóa từ phần response bên trong
            api_response_data = data["data"]["message"]["response"]
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_api(api_response_data, tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                # Lỗi đã được log bên trong _extract_and_normalize_data_api
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[Goldstar API Scraper] Hoàn tất scrape thành công. (Tổng thời gian: %.2fs)", t_total_end - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
             t_total_fail = time.time()
             logger.warning("[Goldstar API Scraper] Timeout khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                          tracking_number, t_total_fail - t_total_start)
             return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[Goldstar API Scraper] Lỗi HTTP %s khi gọi API cho mã '%s'. Response: %s (Tổng thời gian: %.2fs)",
                         e.response.status_code, tracking_number, e.response.text, t_total_fail - t_total_start, exc_info=False)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[Goldstar API Scraper] Lỗi kết nối khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except json.JSONDecodeError:
            t_total_fail = time.time()
            logger.error("[Goldstar API Scraper] Không thể parse JSON từ response API cho mã '%s'. Response text: %s (Tổng thời gian: %.2fs)",
                         tracking_number, response.text, t_total_fail - t_total_start)
            return None, f"API Response không phải JSON hợp lệ cho '{tracking_number}'."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[Goldstar API Scraper] Lỗi không mong muốn cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data_api(self, api_data, tracking_number_input):
        """
        Trích xuất và chuẩn hóa dữ liệu từ dictionary JSON trả về của API Goldstar.
        Áp dụng logic transit tương tự COSCO.
        """
        logger.info("[Goldstar API Scraper] --- Bắt đầu _extract_and_normalize_data_api ---")
        t_extract_detail_start = time.time()
        try:
            consignment_details = api_data.get("consignmentDetails", {})
            bl_route_legs = api_data.get("blRouteLegs", [])

            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT ===
            t_basic_info_start = time.time()
            # API trả về B/L hoặc Container ID trong key 'containerid' của response gốc,
            # nhưng không có trong phần 'response' bên trong. Dùng tracking_number_input làm fallback.
            bl_number = tracking_number_input
            booking_no = tracking_number_input # Giả định B/L No là Booking No
            booking_status = "" # API không cung cấp trạng thái booking chung

            pol = consignment_details.get("consPolDesc", "")
            pod = consignment_details.get("consPodDesc", "") # Đây là Final POD theo logic API

            logger.info(f"[Goldstar API Scraper] -> BL/Booking: {bl_number}, POL: {pol}, POD: {pod}")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_basic_info_start)

            # === BƯỚC 2: XỬ LÝ LỊCH TRÌNH (blRouteLegs) ===
            t_schedule_start = time.time()
            etd, atd, eta, ata = "", "", "", ""
            etd_transit_final, atd_transit, transit_port_list, eta_transit, ata_transit = "", "", [], "", ""
            future_etd_transits = []
            today = date.today()

            if not bl_route_legs:
                logger.warning("[Goldstar API Scraper] Không tìm thấy dữ liệu lịch trình (blRouteLegs) cho mã: %s", tracking_number_input)
                # Vẫn trả về thông tin cơ bản
                shipment_data = N8nTrackingInfo(
                    BookingNo=booking_no, BlNumber=bl_number, BookingStatus=booking_status, Pol=pol, Pod=pod,
                    **{k: "" for k in N8nTrackingInfo.__fields__ if k not in ['BookingNo', 'BlNumber', 'BookingStatus', 'Pol', 'Pod']}
                )
            else:
                logger.info("[Goldstar API Scraper] Tìm thấy %d chặng trong lịch trình.", len(bl_route_legs))
                # Xử lý chặng đầu tiên (POL)
                first_leg = bl_route_legs[0]
                atd = first_leg.get("sailingDateDT") # Luôn coi là Actual Departure Date
                etd = "" # API không có ETD rõ ràng cho POL

                # Xử lý chặng cuối cùng (POD)
                last_leg = bl_route_legs[-1]
                eta = last_leg.get("arrivalDateDT") # Estimated Arrival Date
                ata = last_leg.get("actualArrivalDateDT") # Actual Arrival Date

                # Xử lý các chặng trung chuyển (nếu có)
                logger.debug("[Goldstar API Scraper] Bắt đầu xử lý thông tin transit...")
                for i in range(len(bl_route_legs) - 1):
                    current_leg = bl_route_legs[i]
                    next_leg = bl_route_legs[i+1]

                    current_pod_name = current_leg.get("portNameTo", "").strip()
                    next_pol_name = next_leg.get("portNameFrom", "").strip()

                    # Nếu cảng dỡ chặng này = cảng xếp chặng sau -> transit
                    if current_pod_name and next_pol_name and current_pod_name == next_pol_name:
                         transit_port = current_pod_name
                         logger.debug(f"[Goldstar API Scraper] Tìm thấy cảng transit '{transit_port}'")
                         if transit_port not in transit_port_list:
                            transit_port_list.append(transit_port)

                         # AtaTransit / EtaTransit (Ngày đến cảng transit)
                         temp_ata_transit = current_leg.get("actualArrivalDateDT")
                         temp_eta_transit = current_leg.get("arrivalDateDT")
                         if temp_ata_transit and not ata_transit: # Lấy Ata đầu tiên
                             ata_transit = temp_ata_transit
                             logger.debug(f"Tìm thấy AtaTransit đầu tiên: {ata_transit}")
                         elif temp_eta_transit and not ata_transit and not eta_transit: # Lấy Eta đầu tiên nếu chưa có Ata
                             eta_transit = temp_eta_transit
                             logger.debug(f"Tìm thấy EtaTransit đầu tiên: {eta_transit}")

                         # AtdTransit / EtdTransit (Ngày rời cảng transit)
                         temp_atd_transit = next_leg.get("sailingDateDT") # API chỉ có Sailing Date (coi là Actual)
                         temp_etd_transit_str = "" # API không có ETD rời transit

                         if temp_atd_transit:
                             atd_transit = temp_atd_transit # Lấy Atd cuối cùng
                             logger.debug(f"Cập nhật AtdTransit cuối cùng: {atd_transit}")

                # API này không cung cấp ETD rời cảng transit nên etd_transit_final sẽ luôn rỗng
                logger.info("[Goldstar API Scraper] Không tìm thấy ETD transit nào trong tương lai (API không cung cấp).")
                etd_transit_final = ""
                logger.debug("-> (Thời gian) Xử lý lịch trình và transit: %.2fs", time.time() - t_schedule_start)


                # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
                t_normalize_start = time.time()
                shipment_data = N8nTrackingInfo(
                    BookingNo= booking_no,
                    BlNumber= bl_number,
                    BookingStatus= booking_status, # ""
                    Pol= pol or "",
                    Pod= pod or "",
                    Etd= self._format_date(etd) or "", # ""
                    Atd= self._format_date(atd) or "",
                    Eta= self._format_date(eta) or "",
                    Ata= self._format_date(ata) or "",
                    TransitPort= ", ".join(transit_port_list) if transit_port_list else "",
                    EtdTransit= self._format_date(etd_transit_final) or "", # ""
                    AtdTransit= self._format_date(atd_transit) or "",
                    EtaTransit= self._format_date(eta_transit) or "",
                    AtaTransit= self._format_date(ata_transit) or ""
                )
            logger.info("[Goldstar API Scraper] Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("[Goldstar API Scraper] --- Hoàn tất _extract_and_normalize_data_api --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)

            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("[Goldstar API Scraper] Lỗi trong quá trình trích xuất chi tiết từ API cho mã '%s': %s",
                         tracking_number_input, e, exc_info=True)
            logger.info("[Goldstar API Scraper] --- Hoàn tất _extract_and_normalize_data_api (lỗi) --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return None