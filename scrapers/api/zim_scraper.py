import logging
import requests
import time
from datetime import datetime, date

from ..api_scraper import ApiScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class ZimScraper(ApiScraper):
    # Triển khai logic scraping cho ZIM bằng API trực tiếp và chuẩn hóa kết quả theo template JSON.

    def __init__(self, driver, config):
        super().__init__(config=config)
        
        # ZIM API base URL - tracking_number sẽ được nối vào
        self.api_base_url = self.config.get('api_url', 'https://apigw.zim.com/digital/TrackShipment/v1/')
        self.subscription_key = self.config.get('subscription_key', '9d63cf020a4c4708a7b0ebfe39578300')
        
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
            'Culture': 'en-US',
            'Expires': '0',
            'Origin': 'https://www.zim.com',
            'Pageid': '1220',
            'Pragma': 'no-cache',
            'Priority': 'u=1, i',
            'Referer': 'https://www.zim.com/',
            'Sec-Ch-Ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ ISO format '2025-08-11T19:55:00' sang 'DD/MM/YYYY'.
        Trả về chuỗi rỗng nếu định dạng không hợp lệ hoặc đầu vào là None/rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Parse ISO format và lấy phần ngày
            dt_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, TypeError):
            logger.warning("[ZIM API Scraper] Không thể phân tích định dạng ngày: %s. Trả về chuỗi rỗng.", date_str)
            return ""

    def _parse_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày thành đối tượng date để so sánh.
        """
        if not date_str:
            return None
        try:
            dt_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt_obj.date()
        except (ValueError, TypeError):
            return None

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một Booking Number bằng cách gọi API và trả về một đối tượng N8nTrackingInfo hoặc lỗi.
        """
        logger.info("[ZIM API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()

        # Xây dựng URL đầy đủ
        api_url = f"{self.api_base_url}{tracking_number}/result"
        params = {
            'subscription-key': self.subscription_key
        }

        try:
            logger.info(f"[ZIM API Scraper] Gửi GET request đến: {api_url}")
            t_request_start = time.time()
            response = self.session.get(api_url, params=params, timeout=30)
            logger.info("-> (Thời gian) Gọi API: %.2fs", time.time() - t_request_start)
            response.raise_for_status()

            t_parse_start = time.time()
            data = response.json()
            logger.debug("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)

            # Kiểm tra response thành công và có dữ liệu
            if not data or not data.get("isSuccess"):
                error_msg = "API không trả về dữ liệu thành công."
                logger.warning("[ZIM API Scraper] %s Response: %s", error_msg, data)
                return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên API ZIM."

            # Kiểm tra có dữ liệu data
            if not data.get("data"):
                error_msg = "API không có trường 'data'."
                logger.warning("[ZIM API Scraper] %s", error_msg)
                return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên API ZIM."

            # Trích xuất và chuẩn hóa dữ liệu từ response JSON
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_api(data["data"], tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                logger.warning("[ZIM API Scraper] Không thể chuẩn hóa dữ liệu từ API cho mã: %s.", tracking_number)
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[ZIM API Scraper] Hoàn tất thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
            t_total_fail = time.time()
            logger.warning("[ZIM API Scraper] Timeout khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                          tracking_number, t_total_fail - t_total_start)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[ZIM API Scraper] Lỗi HTTP %s khi gọi API cho mã '%s'. Response: %s (Tổng thời gian: %.2fs)",
                         e.response.status_code, tracking_number, e.response.text, t_total_fail - t_total_start, exc_info=False)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
            t_total_fail = time.time()
            logger.error("[ZIM API Scraper] Lỗi kết nối khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[ZIM API Scraper] Lỗi không mong muốn cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data_api(self, data, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ JSON response của ZIM API.
        
        Logic:
        1. Lấy ETD transit gần nhất, lớn hơn today
        2. Khi ngày tháng không nêu rõ là actual hay estimated:
           - Nếu lớn hơn today: mặc định là estimated
           - Nếu nhỏ hơn today: mặc định là actual
        """
        try:
            today = date.today()
            
            # Lấy thông tin cơ bản từ consignmentDetails
            consignment = data.get("consignmentDetails", {})
            
            # Booking/BL Number
            booking_no = tracking_number
            bl_number = tracking_number
            
            # POL và POD
            pol_code = consignment.get("consPol", "")
            pol_desc = consignment.get("consPolDesc", "")
            pol = pol_desc if pol_desc else pol_code
            
            pod_code = consignment.get("consPod", "")
            pod_desc = consignment.get("consPodDesc", "")
            pod = pod_desc if pod_desc else pod_code
            
            # Lấy route legs (chặng đường)
            route_legs = data.get("blRouteLegs", [])
            
            if not route_legs:
                logger.warning("[ZIM API Scraper] Không có thông tin route legs cho mã: %s", tracking_number)
                return None
            
            # Khởi tạo biến
            etd, atd, eta, ata = "", "", "", ""
            transit_ports = []
            etd_transit, atd_transit, eta_transit, ata_transit = "", "", "", ""
            future_etd_transits = []
            
            # Sắp xếp legs theo thứ tự (dựa vào sailingDateTz)
            sorted_legs = sorted(route_legs, key=lambda x: x.get("sailingDateTz", ""))
            
            # --- Xử lý chặng đầu tiên (POL) ---
            first_leg = sorted_legs[0]
            
            # Departure từ POL
            sailing_date = first_leg.get("sailingDateTz", "")
            actual_sailing = first_leg.get("actualArrivalDateTZ")  # Nếu có actual thì là ATD
            
            if actual_sailing and self._parse_date(actual_sailing):
                atd = self._format_date(actual_sailing)
            elif sailing_date:
                sailing_date_obj = self._parse_date(sailing_date)
                if sailing_date_obj:
                    if sailing_date_obj <= today:
                        atd = self._format_date(sailing_date)
                    else:
                        etd = self._format_date(sailing_date)
            
            # --- Xử lý chặng cuối cùng (POD) ---
            last_leg = sorted_legs[-1]
            
            # Arrival tại POD
            arrival_date = last_leg.get("arrivalDateTz", "")
            actual_arrival = last_leg.get("actualArrivalDateTZ")
            
            if actual_arrival and self._parse_date(actual_arrival):
                ata = self._format_date(actual_arrival)
            elif arrival_date:
                arrival_date_obj = self._parse_date(arrival_date)
                if arrival_date_obj:
                    if arrival_date_obj <= today:
                        ata = self._format_date(arrival_date)
                    else:
                        eta = self._format_date(arrival_date)
            
            # --- Xử lý Transit Ports (các chặng giữa) ---
            if len(sorted_legs) > 1:
                # Tìm tất cả các cảng có portToType = "Transshipment"
                for leg in sorted_legs:
                    port_type_to = leg.get("portToType", "")
                    
                    # Chỉ lấy các cảng transit (portToType = "Transshipment")
                    if port_type_to.lower() == "transshipment":
                        port_name = leg.get("portNameTo", "")
                        if port_name and port_name not in transit_ports:
                            transit_ports.append(port_name)
                        
                        # --- Xử lý ETA/ATA Transit (Arrival tại cảng transit) ---
                        arrival_transit = leg.get("arrivalDateTz", "")
                        actual_arrival_transit = leg.get("actualArrivalDateTZ")
                        
                        if actual_arrival_transit:
                            ata_transit_date = self._parse_date(actual_arrival_transit)
                            if ata_transit_date and ata_transit_date <= today:
                                # Luôn cập nhật để lấy cái cuối cùng (most recent)
                                ata_transit = self._format_date(actual_arrival_transit)
                        elif arrival_transit:
                            arr_transit_obj = self._parse_date(arrival_transit)
                            if arr_transit_obj:
                                if arr_transit_obj <= today:
                                    # Luôn cập nhật để lấy cái cuối cùng
                                    ata_transit = self._format_date(arrival_transit)
                                else:
                                    if not eta_transit:
                                        eta_transit = self._format_date(arrival_transit)
                        
                        # --- Xử lý ETD/ATD Transit (Departure từ cảng transit) ---
                        sailing_transit = leg.get("sailingDateTz", "")
                        
                        if sailing_transit:
                            sail_transit_obj = self._parse_date(sailing_transit)
                            if sail_transit_obj:
                                if sail_transit_obj <= today:
                                    atd_transit = self._format_date(sailing_transit)  # Lấy cái cuối cùng
                                else:
                                    # Lưu vào danh sách ETD transit tương lai
                                    future_etd_transits.append((sail_transit_obj, port_name, self._format_date(sailing_transit)))
            
            # --- Chọn ETD transit gần nhất > today ---
            etd_transit_final = ""
            if future_etd_transits:
                future_etd_transits.sort()  # Sắp xếp theo ngày tăng dần
                etd_transit_final = future_etd_transits[0][2]  # Lấy ngày gần nhất
                logger.info(f"[ZIM API Scraper] ETD transit được chọn: {etd_transit_final} tại {future_etd_transits[0][1]}")
            
            # Tạo đối tượng N8nTrackingInfo
            return N8nTrackingInfo(
                BookingNo=booking_no,
                BlNumber=bl_number,
                BookingStatus="",
                Pol=pol,
                Pod=pod,
                Etd=etd,
                Atd=atd,
                Eta=eta,
                Ata=ata,
                TransitPort=", ".join(transit_ports),
                EtdTransit=etd_transit_final,
                AtdTransit=atd_transit,
                EtaTransit=eta_transit,
                AtaTransit=ata_transit
            )
            
        except Exception as e:
            logger.error("[ZIM API Scraper] Lỗi khi trích xuất và chuẩn hóa dữ liệu: %s", e, exc_info=True)
            return None
