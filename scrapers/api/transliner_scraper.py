import logging
import requests
import json
import time
from datetime import datetime, date

from ..api_scraper import ApiScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class TranslinerScraper(ApiScraper):
    """
    Triển khai logic scraping cụ thể cho trang Transliner bằng cách gọi API trực tiếp
    và chuẩn hóa kết quả theo template JSON.
    """

    def __init__(self, driver, config):
        super().__init__(config=config)
        self.api_url_template = "https://translinergroup.track.tigris.systems/api/bookings/{booking_number}"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://translinergroup.track.tigris.systems',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ API format 'YYYY-MM-DDTHH:MM:SSZ' sang 'DD/MM/YYYY'.
        Trả về "" nếu đầu vào không hợp lệ hoặc rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            date_part = date_str.split('T')[0]
            if not date_part:
                return ""
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[Transliner API Scraper] Không thể phân tích định dạng ngày: %s", date_str)
            return ""

    def scrape(self, tracking_number):
        """
        Phương thức scraping chính cho Transliner bằng API.
        """
        logger.info("[Transliner API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()

        # Xây dựng URL API động
        api_url = self.api_url_template.format(booking_number=tracking_number)

        # Parameters cho request
        params = {
            "include_emails": "true"
        }
        # Cập nhật Referer động
        self.session.headers['Referer'] = f'https://translinergroup.track.tigris.systems/?ref={tracking_number}'

        try:
            logger.info(f"[Transliner API Scraper] Gửi GET request đến: {api_url}")
            t_request_start = time.time()
            response = self.session.get(api_url, params=params, timeout=30)
            logger.info("-> (Thời gian) Gọi API: %.2fs", time.time() - t_request_start)
            response.raise_for_status() # Kiểm tra lỗi HTTP (4xx, 5xx)

            t_parse_start = time.time()
            data = response.json()
            logger.debug("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)

            # Kiểm tra response có dữ liệu cần thiết không (ví dụ: booking_number)
            if not data or "booking_number" not in data:
                 error_msg = "API không trả về dữ liệu thành công hoặc thiếu thông tin booking."
                 logger.warning("[Transliner API Scraper] %s Response: %s", error_msg, data)
                 return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên API Transliner."

            # Trích xuất và chuẩn hóa dữ liệu từ response JSON
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_api(data, tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                logger.warning("[Transliner API Scraper] Không thể chuẩn hóa dữ liệu từ API cho mã: %s.", tracking_number)
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[Transliner API Scraper] Hoàn tất thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
            t_total_fail = time.time()
            logger.warning("[Transliner API Scraper] Timeout khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_fail - t_total_start)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[Transliner API Scraper] Lỗi HTTP %s khi gọi API cho mã '%s'. Response: %s (Tổng thời gian: %.2fs)",
                         e.response.status_code, tracking_number, e.response.text, t_total_fail - t_total_start, exc_info=False)
            if e.response.status_code == 404:
                 return None, f"Không tìm thấy thông tin cho mã '{tracking_number}' trên API Transliner."
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[Transliner API Scraper] Lỗi kết nối khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except json.JSONDecodeError:
             t_total_fail = time.time()
             logger.error("[Transliner API Scraper] Không thể parse JSON từ response API cho mã '%s'. Response text: %s (Tổng thời gian: %.2fs)",
                          tracking_number, response.text, t_total_fail - t_total_start)
             return None, f"API Response không phải JSON hợp lệ cho '{tracking_number}'."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[Transliner API Scraper] Lỗi không mong muốn cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data_api(self, api_data, tracking_number_input):
        """
        Trích xuất và chuẩn hóa dữ liệu từ dictionary JSON trả về của API Transliner/Tigris.
        """
        logger.info("[Transliner API Scraper] --- Bắt đầu _extract_and_normalize_data_api ---")
        t_extract_detail_start = time.time()
        try:
            milestones = api_data.get("milestones", [])
            logger.debug("Bắt đầu trích xuất thông tin cơ bản...")
            # API trả về booking_number và bill_of_lading giống nhau
            booking_no = api_data.get("booking_number", tracking_number_input)
            bl_number = api_data.get("bill_of_lading", booking_no)
            # Lấy trạng thái từ milestone cuối cùng (mới nhất)
            booking_status = milestones[-1].get("type", "") if milestones else ""

            # API này không trả về POL/POD trực tiếp trong thông tin cơ bản
            pol = ""
            pod = ""

            logger.info(f"Thông tin cơ bản: Booking='{booking_no}', BL='{bl_number}', Status='{booking_status}'")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_extract_detail_start) # Log tạm

            t_event_start = time.time()
            etd, atd, eta, ata = "", "", "", ""
            transit_port = ""
            etd_transit, atd_transit, eta_transit, ata_transit = "", "", "", ""

            if not milestones:
                logger.warning("[Transliner API Scraper] Không có sự kiện (milestones) nào được trả về từ API.")
            else:
                logger.info("[Transliner API Scraper] Tìm thấy %d sự kiện (milestones) từ API. Bắt đầu xử lý...", len(milestones))
                milestones.sort(key=lambda x: x.get("event_date") or "0000")

                for event in milestones:
                    event_type = event.get("type", "").upper()
                    event_date = event.get("event_date")
                    estimated_departure = event.get("estimated_departure_date")
                    actual_departure = event.get("actual_departure_date")
                    estimated_arrival = event.get("estimated_arrival_date")
                    actual_arrival = event.get("actual_arrival_date")

                    # Xác định ATD từ sự kiện VESSEL_DEPARTURE
                    if event_type == "VESSEL_DEPARTURE":
                        atd = actual_departure or event_date

                    # Xác định ATA/ETA từ sự kiện DISCHARGED
                    if event_type == "DISCHARGED":
                        ata = actual_arrival or event_date # Ưu tiên actual_arrival
                        eta = estimated_arrival # Lấy ETA nếu có

            logger.info(f"Kết quả xử lý sự kiện: ETD='{etd}', ATD='{atd}', ETA='{eta}', ATA='{ata}'")
            logger.info("API Transliner không cung cấp thông tin chi tiết về POL, POD, và Transit.")
            logger.debug("-> (Thời gian) Xử lý sự kiện: %.2fs", time.time() - t_event_start)

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
                TransitPort= transit_port or "",
                EtdTransit= etd_transit or "",
                AtdTransit= atd_transit or "",
                EtaTransit= eta_transit or "",
                AtaTransit= ata_transit or ""
            )
            logger.info("[Transliner API Scraper] Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("[Transliner API Scraper] --- Hoàn tất _extract_and_normalize_data_api --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return shipment_data

        except Exception as e:
            logger.error("[Transliner API Scraper] Lỗi trong quá trình trích xuất chi tiết từ API cho mã '%s': %s", tracking_number_input, e, exc_info=True)
            logger.info("[Transliner API Scraper] --- Hoàn tất _extract_and_normalize_data_api (lỗi) --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return None