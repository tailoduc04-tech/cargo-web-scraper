import logging
import requests
import time
from datetime import datetime
from schemas import N8nTrackingInfo
import json

from .base_scraper import BaseScraper # Vẫn kế thừa từ BaseScraper

logger = logging.getLogger(__name__)

class CordeliaScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Cordelia Line bằng cách gọi API trực tiếp
    và chuẩn hóa kết quả.
    """

    def __init__(self, driver, config):
        super().__init__(config=config)
        self.api_url_template = "https://erp.cordelialine.com/cordelia/app/bltracking/bltracingweb?blno={blno}"

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD/MM/YYYY HH:MM...' hoặc 'DD/MM/YYYY'
        sang 'DD/MM/YYYY'.
        Trả về "" nếu đầu vào không hợp lệ hoặc rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Lấy phần ngày, bỏ qua phần giờ nếu có
            date_part = date_str.split(' ')[0]
            # Parse ngày theo định dạng DD/MM/YYYY
            dt_obj = datetime.strptime(date_part, '%d/%m/%Y')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return ""

    def scrape(self, tracking_number):
        """
        Lấy dữ liệu cho một số theo dõi bằng cách gọi API trực tiếp của Cordelia Line
        và trả về ở định dạng JSON chuẩn hóa.
        """
        logger.info("[Cordelia API Scraper] Bắt đầu lấy dữ liệu cho mã: %s", tracking_number)
        t_total_start = time.time() # Tổng thời gian bắt đầu

        api_url = self.api_url_template.format(blno=tracking_number)

        # Thêm headers giống như trình duyệt gửi AJAX request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': f"{self.config.get('url', 'https://cordelialine.com/bltracking/?blno=')}{tracking_number}" # Giả lập trang gốc
        }

        try:
            t_request_start = time.time()
            response = requests.get(api_url, headers=headers, timeout=30) # Timeout 30 giây

            # with open("cordelia_response.json", 'w', encoding='utf-8') as f:
            #     print("Saving raw API response to cordelia_response.json")
            #     json.dump(response.json(), f, indent=2, ensure_ascii=False, default=str)

            logger.info("-> (Thời gian) Gọi API: %.2fs", time.time() - t_request_start)
            response.raise_for_status() # Kiểm tra lỗi HTTP (4xx, 5xx)

            t_parse_start = time.time()
            data = response.json()
            logger.debug("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)


            # Kiểm tra dữ liệu trả về có hợp lệ không
            if not data or not data.get("searchList"):
                 logger.warning("[Cordelia API Scraper] API trả về dữ liệu trống hoặc không có 'searchList' cho mã: %s", tracking_number)
                 return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên API Cordelia."

            # Trích xuất và chuẩn hóa dữ liệu
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(data, tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)


            if not normalized_data:
                logger.warning("[Cordelia API Scraper] Không thể chuẩn hóa dữ liệu từ API cho mã: %s.", tracking_number)
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[Cordelia API Scraper] Hoàn tất thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
             t_total_fail = time.time()
             logger.warning("[Cordelia API Scraper] Timeout khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                          tracking_number, t_total_fail - t_total_start)
             return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[Cordelia API Scraper] Lỗi HTTP khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[Cordelia API Scraper] Lỗi kết nối khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[Cordelia API Scraper] Lỗi không mong muốn cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, data, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ JSON trả về của API Cordelia.
        """
        try:
            # Lấy phần tử đầu tiên trong danh sách kết quả
            item = data["searchList"][0]

            # === BƯỚC 1: TRÍCH XUẤT DỮ LIỆU THÔ TỪ JSON ===
            bl_number = item.get("blNo")
            pol = item.get("pol") # Port of Loading
            sob_date_str = item.get("sobDate") # Shipped on Board Date (Actual Departure)
            pod = item.get("pod") # Port of Discharge (thường là cảng transit đầu tiên nếu có)
            fpod = item.get("webFpod") # Final Port of Discharge
            trans_voyage_flag = item.get("transVesselVoyage") # '1', '2', '3' nếu có transit

            # Lấy ETA tại FPOD - Logic phức tạp dựa trên JavaScript
            eta_fpod_str = None
            if pod == fpod: # Direct service
                eta_fpod_str = item.get("flEta")
            elif trans_voyage_flag == '1': # 1 transit
                eta_fpod_str = item.get("slEta") # Second leg ETA
            elif trans_voyage_flag == '2': # 2 transits
                eta_fpod_str = item.get("tlEta") # Third leg ETA
            elif trans_voyage_flag == '3': # 3 transits
                eta_fpod_str = item.get("frleta") # Fourth leg ETA

            current_status = item.get("containerStatusDescription")
            current_location = item.get("currentlocation")

            # === BƯỚC 2: XỬ LÝ LOGIC & CHUẨN HÓA ===

            # Transit Port: Nếu có transit, cảng transit đầu tiên là 'pod' trong JSON
            transit_port_list = []
            if pod != fpod:
                # Dựa vào transVesselVoyage để xác định các cảng transit
                if pod: # POT 1
                    transit_port_list.append(pod)
                pot2 = item.get("slpot") # POT 2 (nếu có 2+ transit)
                if trans_voyage_flag in ['2', '3'] and pot2:
                    transit_port_list.append(pot2)
                pot3 = item.get("tlpot") # POT 3 (nếu có 3 transit)
                if trans_voyage_flag == '3' and pot3:
                    transit_port_list.append(pot3)
            transit_port = ", ".join(transit_port_list)

            # AtaTransit: Arrival at first transit port (POD Discharge Date)
            ata_transit_str = item.get("flDischargedate") if pod != fpod else None

            # AtdTransit: Departure from last transit port
            atd_transit_str = None
            if trans_voyage_flag == '1':
                atd_transit_str = item.get("etdSecond") # ETD from POT 1
            elif trans_voyage_flag == '2':
                atd_transit_str = item.get("etdThird") # ETD from POT 2
            elif trans_voyage_flag == '3':
                 atd_transit_str = item.get("etdFourth") # ETD from POT 3

            # EtaTransit: ETA at first transit port
            eta_transit_str = item.get("flEta") if pod != fpod else None

            # EtdTransit: Không có thông tin ETD (dự kiến) rời cảng transit
            etd_transit_str = ""

            # Ata (Actual arrival at FPOD): Dùng discharge date tương ứng với leg cuối
            ata_str = None
            if pod == fpod:
                 ata_str = item.get("flDischargedate")
            elif trans_voyage_flag == '1':
                 ata_str = item.get("slDischargedate")
            elif trans_voyage_flag == '2':
                 ata_str = item.get("tlDischargedate")
            elif trans_voyage_flag == '3':
                 ata_str = item.get("frlDischargedate")


            # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            shipment_data = N8nTrackingInfo(
                BookingNo= item.get("bookingNo") or "", # Dùng bookingNo nếu có
                BlNumber= bl_number or "",
                BookingStatus= current_status or "",
                Pol= pol or "",
                Pod= fpod or "", # Luôn dùng final POD
                Etd= "", # API không cung cấp ETD rõ ràng, chỉ có ATD
                Atd= self._format_date(sob_date_str) or "", # SOB Date là ATD
                Eta= self._format_date(eta_fpod_str) or "", # ETA tại Final POD
                Ata= self._format_date(ata_str) or "", # Actual Arrival tại Final POD
                TransitPort= transit_port or "",
                EtdTransit= self._format_date(etd_transit_str) or "", # Sẽ là ""
                AtdTransit= self._format_date(atd_transit_str) or "", # ETD from last transit port
                EtaTransit= self._format_date(eta_transit_str) or "", # ETA at first transit port
                AtaTransit= self._format_date(ata_transit_str) or "" # Actual arrival at first transit
            )

            logger.info("Đã chuẩn hóa dữ liệu thành công cho BL: %s", bl_number)
            return shipment_data

        except (IndexError, KeyError, TypeError) as e:
            logger.error("Lỗi key hoặc index khi xử lý JSON cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None
        except Exception as e:
            logger.error("Lỗi không mong muốn khi chuẩn hóa dữ liệu cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None