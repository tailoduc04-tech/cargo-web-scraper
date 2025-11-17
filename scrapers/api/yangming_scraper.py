import logging
import requests
import time
import re
from datetime import datetime, date

from ..api_scraper import ApiScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class YangmingScraper(ApiScraper):
    # Triển khai logic scraping cho Yang Ming (YML) bằng API.

    def __init__(self, driver, config):
        super().__init__(config=config)
        self.landing_url = self.config.get('landing_url', 'https://www.yangming.com/en/esolution/cargo_tracking')
        self.api_url = self.config.get('api_url', 'https://www.yangming.com/api/CargoTracking/GetTracking')
        
        # Cập nhật headers giả lập trình duyệt để vượt qua lớp bảo mật
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'www.yangming.com',
            'Referer': 'https://www.yangming.com/en/esolution/cargo_tracking',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest' 
        })

    def _format_date(self, date_str):
        # Chuyển đổi 'YYYY/MM/DD HH:MM' -> 'DD/MM/YYYY'. Ví dụ: '2025/10/04 20:30' -> '04/10/2025'
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Lấy phần ngày, bỏ qua giờ
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y/%m/%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[YML API] Không thể format ngày: %s", date_str)
            return ""

    def scrape(self, tracking_number):
        logger.info(f"[YML API] Bắt đầu scrape cho mã: {tracking_number}")
        t_start = time.time()

        try:
            # BƯỚC 1: Session Priming - Truy cập trang chủ để lấy Cookie & Token
            logger.info(f"[YML API] Đang truy cập landing page để khởi tạo session...")
            self.session.get(self.landing_url, timeout=20)
            
            # BƯỚC 2: Gọi API
            params = {
                "paramTrackNo": tracking_number,
                "paramTrackPosition": "SEARCH",
                "paramRefNo": ""
            }
            
            logger.info(f"[YML API] Gửi request đến API GetTracking...")
            t_api = time.time()
            response = self.session.get(self.api_url, params=params, timeout=30)
            logger.info(f"-> Gọi API mất: {time.time() - t_api:.2f}s")
            response.raise_for_status()
            
            data = response.json()
            
            # Kiểm tra dữ liệu trả về
            if not data.get("blList") or len(data["blList"]) == 0:
                return None, f"Không tìm thấy thông tin vận đơn cho mã {tracking_number}"
            
            # BƯỚC 3: Trích xuất và chuẩn hóa
            # Lấy phần tử đầu tiên trong danh sách B/L
            bl_data = data["blList"][0]
            normalized_data = self._extract_and_normalize(bl_data, tracking_number)
            
            logger.info(f"[YML API] Hoàn tất thành công. Tổng thời gian: {time.time() - t_start:.2f}s")
            return normalized_data, None

        except Exception as e:
            logger.error(f"[YML API] Lỗi: {e}", exc_info=True)
            return None, f"Lỗi khi scrape YML: {e}"

    def _extract_and_normalize(self, bl_data, tracking_number):
        """
        Mapping dữ liệu JSON từ API sang schema N8nTrackingInfo.
        """
        basic_info = bl_data.get("basicInfo", {})
        routing_schedule = bl_data.get("routingInfo", {}).get("routingSchedule", [])
        
        # 1. Thông tin cơ bản
        booking_no = bl_data.get("queryTrackNo", tracking_number)
        bl_number = bl_data.get("returnTrackNo", tracking_number)
        
        # API trả về dạng "HAIPHONG (VNHPH)", ta cắt lấy tên
        pol = basic_info.get("loading", "").split("(")[0].strip()
        pod = basic_info.get("discharge", "").split("(")[0].strip()
        
        # 2. Xử lý lịch trình
        etd, atd, eta, ata = "", "", "", ""
        transit_ports = []
        etd_transit_final, atd_transit, eta_transit, ata_transit = "", "", "", ""
        future_etd_transits = []
        today = date.today()
        
        if routing_schedule:
            # Sắp xếp theo thứ tự seq
            sorted_legs = sorted(routing_schedule, key=lambda x: x.get("seq", 0))
            
            # --- Chặng đầu (POL) ---
            # DateTime ở chặng đầu là thời gian khởi hành (Departure)
            first_leg = sorted_legs[0]
            dep_date = first_leg.get("dateTime", "")
            is_actual_dep = first_leg.get("dateQlfr") == "Actual"
            
            if is_actual_dep:
                atd = self._format_date(dep_date)
            else:
                etd = self._format_date(dep_date)
                
            # --- Chặng cuối (POD) ---
            # DateTime ở chặng cuối là thời gian đến (Arrival)
            last_leg = sorted_legs[-1]
            arr_date = last_leg.get("dateTime", "")
            is_actual_arr = last_leg.get("dateQlfr") == "Actual"
            
            if is_actual_arr:
                ata = self._format_date(arr_date)
            else:
                eta = self._format_date(arr_date)
                
            # --- Xử lý Transit ---
            # Các chặng nằm giữa là transit ports
            if len(sorted_legs) > 2:
                transit_legs = sorted_legs[1:-1]
                
                for leg in transit_legs:
                    port_name = leg.get("placeName", "").strip()
                    if port_name and port_name not in transit_ports:
                        transit_ports.append(port_name)
                    
                    # Xử lý Arrival (Đến cảng transit) - Dựa vào berthInfo
                    # Ví dụ: "Berthing time at terminal: 2025/10/08 14:50 (Actual)"
                    berth_info = leg.get("berthInfo")
                    if berth_info:
                        # Tìm ngày trong chuỗi berthInfo
                        match = re.search(r'(\d{4}/\d{2}/\d{2})', berth_info)
                        if match:
                            berth_date_str = match.group(1)
                            berth_date_fmt = self._format_date(berth_date_str)
                            
                            if "(Actual)" in berth_info:
                                if not ata_transit: ata_transit = berth_date_fmt
                            else:
                                if not ata_transit and not eta_transit: eta_transit = berth_date_fmt

                    # Xử lý Departure (Rời cảng transit) - Dựa vào dateTime chính
                    main_date = leg.get("dateTime", "")
                    is_actual_main = leg.get("dateQlfr") == "Actual"
                    main_date_fmt = self._format_date(main_date)
                    
                    if is_actual_main:
                        atd_transit = main_date_fmt # Lấy ATD cuối cùng
                    else:
                        # Logic lấy ETD transit gần nhất trong tương lai
                        if main_date:
                            try:
                                dt_obj = datetime.strptime(main_date.split(" ")[0], '%Y/%m/%d').date()
                                if dt_obj > today:
                                    future_etd_transits.append((dt_obj, port_name, main_date_fmt))
                            except ValueError:
                                pass

        # Chọn ETD transit gần nhất > hôm nay
        if future_etd_transits:
            future_etd_transits.sort()
            etd_transit_final = future_etd_transits[0][2]
            logger.info(f"ETD transit được chọn: {etd_transit_final}")

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