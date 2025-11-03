import logging
import requests
import json
import time
from datetime import datetime, date

from ..api_scraper import ApiScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho module này
logger = logging.getLogger(__name__)

class UnifeederScraper(ApiScraper):
    """
    Triển khai logic scraping cụ thể cho trang Unifeeder (Avana) bằng cách gọi API trực tiếp
    và chuẩn hóa kết quả theo định dạng JSON yêu cầu.
    """

    def __init__(self, driver, config): # driver không còn được dùng
        self.config = config
        self.api_url = "https://api-fr.cargoes.com/track/avana" # URL API mới
        self.session = requests.Session()
        # Headers cơ bản dựa trên script test
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.unifeeder.cargoes.com',
            'Referer': 'https://www.unifeeder.cargoes.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            # Content-Type không cần cho GET request với params
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ API format 'YYYY-MM-DDTHH:MM:SS' sang 'DD/MM/YYYY'.
        Trả về "" nếu đầu vào không hợp lệ hoặc rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Lấy phần ngày tháng năm, bỏ qua phần giờ và timezone
            date_part = date_str.split('T')[0]
            if not date_part:
                return ""
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[Unifeeder API Scraper] Không thể phân tích định dạng ngày: %s", date_str)
            return "" # Trả về chuỗi rỗng nếu lỗi

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính bằng API.
        """
        logger.info("[Unifeeder API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()

        params = {
            "trackingId": tracking_number,
            "tenant": "AVANA-LINER" # Hardcode tenant này dựa trên request mẫu
        }

        try:
            logger.info(f"[Unifeeder API Scraper] Gửi GET request đến: {self.api_url}")
            t_request_start = time.time()
            response = self.session.get(self.api_url, params=params, timeout=30)
            logger.info("-> (Thời gian) Gọi API: %.2fs", time.time() - t_request_start)
            response.raise_for_status() # Kiểm tra lỗi HTTP (4xx, 5xx)

            t_parse_start = time.time()
            data = response.json()
            logger.debug("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)

            # Kiểm tra response có dữ liệu cần thiết không
            if not data or not data.get("bookingRelatedDetails") or not data.get("bookingTrackingEvents"):
                 error_msg = "API không trả về dữ liệu thành công hoặc thiếu thông tin booking/events."
                 logger.warning("[Unifeeder API Scraper] %s Response: %s", error_msg, data)
                 # Kiểm tra thêm nếu có lỗi cụ thể từ API (mặc dù response mẫu không có)
                 # api_error = data.get("error") or data.get("message")
                 # if api_error: error_msg = api_error
                 return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên API Unifeeder."

            # Trích xuất và chuẩn hóa dữ liệu từ response JSON
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_api(data, tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                logger.warning("[Unifeeder API Scraper] Không thể chuẩn hóa dữ liệu từ API cho mã: %s.", tracking_number)
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[Unifeeder API Scraper] Hoàn tất thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        # --- Xử lý lỗi (tương tự các scraper API khác) ---
        except requests.exceptions.Timeout:
            t_total_fail = time.time()
            logger.warning("[Unifeeder API Scraper] Timeout khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_fail - t_total_start)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[Unifeeder API Scraper] Lỗi HTTP %s khi gọi API cho mã '%s'. Response: %s (Tổng thời gian: %.2fs)",
                         e.response.status_code, tracking_number, e.response.text, t_total_fail - t_total_start, exc_info=False)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[Unifeeder API Scraper] Lỗi kết nối khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except json.JSONDecodeError:
             t_total_fail = time.time()
             logger.error("[Unifeeder API Scraper] Không thể parse JSON từ response API cho mã '%s'. Response text: %s (Tổng thời gian: %.2fs)",
                          tracking_number, response.text, t_total_fail - t_total_start)
             return None, f"API Response không phải JSON hợp lệ cho '{tracking_number}'."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[Unifeeder API Scraper] Lỗi không mong muốn cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data_api(self, api_data, tracking_number_input):
        """
        Trích xuất và chuẩn hóa dữ liệu từ dictionary JSON trả về của API Unifeeder/Avana.
        """
        logger.info("[Unifeeder API Scraper] --- Bắt đầu _extract_and_normalize_data_api ---")
        t_extract_detail_start = time.time()
        try:
            booking_details = api_data.get("bookingRelatedDetails", {})
            events = api_data.get("bookingTrackingEvents", [])

            # === BƯỚC 1: LẤY THÔNG TIN CƠ BẢN ===
            logger.debug("Bắt đầu trích xuất thông tin cơ bản...")
            # API trả về mã cảng, không có tên đầy đủ, tạm dùng mã cảng
            pol = booking_details.get("originLocationName", "")
            pod = booking_details.get("destinationLocationName", "")
            bl_number = booking_details.get("bolNumber", "")
            # API có bookingNumber, ưu tiên dùng nó
            booking_no = booking_details.get("bookingNumber", tracking_number_input)
            # Lấy trạng thái từ sự kiện mới nhất (danh sách trả về đã sắp xếp)
            booking_status = events[0].get("event_desc", "") if events else ""

            logger.info(f"Thông tin cơ bản: POL='{pol}', POD='{pod}', BL='{bl_number}', Booking='{booking_no}', Status='{booking_status}'")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_extract_detail_start) # Log tạm thời gian

            # === BƯỚC 2: XỬ LÝ SỰ KIỆN ===
            t_event_start = time.time()
            etd, atd, eta, ata = "", "", "", ""
            etd_transit_final, atd_transit, transit_port_list, eta_transit, ata_transit = "", "", [], "", ""
            future_etd_transits = []
            today = date.today()

            if not events:
                logger.warning("[Unifeeder API Scraper] Không có sự kiện nào được trả về từ API.")
            else:
                logger.info("[Unifeeder API Scraper] Tìm thấy %d sự kiện từ API. Bắt đầu xử lý...", len(events))
                # Sắp xếp sự kiện theo thời gian tăng dần (cũ -> mới) dựa vào event_time hoặc ata/eta
                events.sort(key=lambda x: x.get("event_time") or x.get("ata") or x.get("eta") or "")

                atd_transit_found = None
                ata_transit_found = None
                eta_transit_found = None

                for event in events:
                    loc_info = event.get("event_location", {})
                    # Dùng formattedDescription hoặc code nếu không có tên đầy đủ
                    loc_name = loc_info.get("formattedDescription") or loc_info.get("code") or ""
                    loc_name_lower = loc_name.lower()
                    desc = event.get("event_desc", "").lower()
                    # Xác định ngày thực tế/dự kiến
                    event_status = event.get("event_status", "").lower()
                    is_actual = event_status == "actual"
                    # Lấy ngày phù hợp (ata cho actual, eta cho projected)
                    date_str = event.get("ata") if is_actual else event.get("eta")
                    # Nếu không có ata/eta, thử lấy event_time
                    if not date_str: date_str = event.get("event_time")


                    is_pol = pol.lower() in loc_name_lower if pol else False
                    is_pod = pod.lower() in loc_name_lower if pod else False
                    is_ts_discharge = "tsdf" in event.get("event_type", "").lower() or "discharge" in desc and ("t/s" in desc or "transshipment" in desc)
                    is_ts_load = "tslf" in event.get("event_type", "").lower() or "load" in desc and ("t/s" in desc or "transshipment" in desc)


                    # --- Xử lý POL ---
                    if is_pol and ("load on vessel" in desc):
                        if is_actual: atd = date_str
                        else: etd = date_str

                    # --- Xử lý POD ---
                    elif is_pod and ("discharge from vessel" in desc):
                        if is_actual: ata = date_str
                        else: eta = date_str

                    # --- Xử lý Transit ---
                    elif is_ts_discharge or is_ts_load:
                        if loc_name and loc_name not in transit_port_list:
                             transit_port_list.append(loc_name)
                             logger.debug("Tìm thấy cảng transit: %s", loc_name)

                        if is_ts_discharge:
                            if is_actual and not ata_transit_found:
                                ata_transit_found = date_str
                                logger.debug("Tìm thấy AtaTransit đầu tiên: %s", date_str)
                            elif not is_actual and not ata_transit_found and not eta_transit_found:
                                eta_transit_found = date_str
                                logger.debug("Tìm thấy EtaTransit đầu tiên: %s", date_str)

                        if is_ts_load:
                            if is_actual:
                                atd_transit_found = date_str # Lấy cái cuối cùng
                                logger.debug("Cập nhật AtdTransit cuối cùng: %s", date_str)
                            else: # Projected Load at Transit
                                try:
                                    # Parse ngày YYYY-MM-DD từ chuỗi date_str (YYYY-MM-DDTHH:MM:SS)
                                    etd_date = datetime.strptime((date_str or "").split('T')[0], '%Y-%m-%d').date()
                                    if etd_date > today:
                                        future_etd_transits.append((etd_date, loc_name, date_str))
                                        logger.debug("Thêm ETD transit trong tương lai: %s (%s)", date_str, loc_name)
                                except (ValueError, IndexError, AttributeError):
                                    logger.warning("Không thể parse ETD transit: %s", date_str)

                # Xử lý kết quả transit
                if future_etd_transits:
                    future_etd_transits.sort()
                    etd_transit_final = future_etd_transits[0][2] # Lấy chuỗi ngày
                    logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
                else:
                    logger.info("Không tìm thấy ETD transit nào trong tương lai.")

                atd_transit = atd_transit_found
                eta_transit = eta_transit_found
                ata_transit = ata_transit_found

            logger.debug("-> (Thời gian) Xử lý sự kiện và transit: %.2fs", time.time() - t_event_start)

            # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            t_normalize_start = time.time()
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no or "",
                BlNumber= bl_number or "",
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
            logger.info("[Unifeeder API Scraper] Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("[Unifeeder API Scraper] --- Hoàn tất _extract_and_normalize_data_api --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("[Unifeeder API Scraper] Lỗi trong quá trình trích xuất chi tiết từ API cho mã '%s': %s", tracking_number_input, e, exc_info=True)
            logger.info("[Unifeeder API Scraper] --- Hoàn tất _extract_and_normalize_data_api (lỗi) --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return None