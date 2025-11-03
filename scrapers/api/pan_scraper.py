import logging
import requests
import time
from datetime import datetime, date

from ..api_scraper import ApiScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class PanScraper(ApiScraper):
    """
    Triển khai logic scraping cụ thể cho trang Pan Continental Shipping
    bằng cách gọi API trực tiếp và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def __init__(self, driver, config):
        super().__init__(config=config)
        self.api_url = "https://www.pancon.co.kr/pan/selectWeb212AR.pcl"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json;charset=UTF-8',
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Origin': 'https://www.pancon.co.kr',
            'Referer': 'https://www.pancon.co.kr/pan/pageLink.do?pageId=tracking'
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYYMMDDHHMM' (API format) sang 'DD/MM/YYYY'.
        Trả về "" nếu lỗi hoặc đầu vào không hợp lệ.
        """
        if not date_str or not isinstance(date_str, str) or len(date_str) < 8:
                try:
                    now_timestamp_str = datetime.now().strftime('%Y%m%d%H%M')
                    if date_str[:12] == now_timestamp_str:
                         logger.warning("Phát hiện giá trị ngày giống timestamp hiện tại: %s. Bỏ qua.", date_str)
                         return ""
                except Exception:
                    pass
                
        if date_str and len(date_str) >= 8:
            pass
        else:
                return ""

        try:
            # Chỉ lấy phần YYYYMMDD
            date_part = date_str[:8]
            dt_obj = datetime.strptime(date_part, '%Y%m%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày API: %s. Trả về chuỗi rỗng.", date_str)
            return ""

    def _parse_date_obj(self, date_str):
        """
        Chuyển đổi chuỗi ngày 'YYYYMMDDHHMM' sang đối tượng date
        để so sánh. Trả về None nếu lỗi.
        """
        if not date_str or not isinstance(date_str, str) or len(date_str) < 8:
            if date_str and len(date_str) > 10 :
                try:
                    now_timestamp_str = datetime.now().strftime('%Y%m%d%H%M')
                    if date_str[:12] == now_timestamp_str:
                         logger.warning("Phát hiện giá trị ngày giống timestamp hiện tại khi parse object: %s. Bỏ qua.", date_str)
                         return None
                except Exception:
                    pass
            elif date_str and len(date_str) >= 8:
                 pass
            else:
                return None

        try:
            date_part = date_str[:8]
            return datetime.strptime(date_part, '%Y%m%d').date()
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích chuỗi ngày API sang object: %s", date_str)
            return None

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho Pan Continental bằng API.
        """
        logger.info(f"[PanCont API Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        t_total_start = time.time()

        payload = {
            "I_AS_ARGU1": tracking_number,
            "I_AS_KIND": "BL",
            "I_AS_COUNTRY_CD": ""
        }

        try:
            t_request_start = time.time()
            logger.info(f"Gửi POST request đến: {self.api_url} với payload: {payload}")
            response = self.session.post(self.api_url, json=payload, timeout=30)
            logger.info("-> (Thời gian) Gọi API: %.2fs", time.time() - t_request_start)
            response.raise_for_status() # Kiểm tra lỗi HTTP

            t_parse_start = time.time()
            data = response.json()
            logger.info("-> (Thời gian) Parse JSON: %.2fs", time.time() - t_parse_start)

            # Kiểm tra cấu trúc response và dữ liệu
            if not data or "rows" not in data or not data["rows"]:
                logger.warning(f"API không trả về dữ liệu hợp lệ hoặc 'rows' rỗng cho mã: {tracking_number}")
                return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên API Pan Continental."

            # Chỉ lấy dữ liệu từ dòng đầu tiên (vì các dòng có vẻ giống nhau chỉ khác CNTR_NO)
            api_data = data["rows"][0]

            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(api_data, tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa dữ liệu: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[PanCont API Scraper] -> Hoàn tất scrape thành công. (Tổng thời gian: %.2fs)", t_total_end - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
            t_total_fail = time.time()
            logger.warning(f"Timeout khi gọi API cho '{tracking_number}'. (Tổng thời gian: %.2fs)", t_total_fail - t_total_start)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error(f"Lỗi HTTP {e.response.status_code} khi gọi API cho '{tracking_number}': {e.response.text} (Tổng thời gian: %.2fs)", t_total_fail - t_total_start)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error(f"Lỗi kết nối khi gọi API cho '{tracking_number}': {e} (Tổng thời gian: %.2fs)", t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except Exception as e:
            t_total_fail = time.time()
            logger.error(f"Đã xảy ra lỗi không mong muốn khi scrape API cho '{tracking_number}': {e} (Tổng thời gian: %.2fs)", t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"


    def _extract_and_normalize_data(self, api_data, tracking_number_input):
        """
        Trích xuất dữ liệu từ JSON API và ánh xạ vào template JSON.
        """
        try:
            bl_number = api_data.get("BL_NO")
            booking_no = api_data.get("BKG_NO")
            pol = api_data.get("POL")
            pod = api_data.get("POD")

            # --- Thu thập thông tin các chặng từ API data ---
            legs_data = [
                {
                    'vsl': api_data.get("VSL_1"),
                    'voy': api_data.get("VOY_1"),
                    'pol': api_data.get("POL_1"),
                    'etd': api_data.get("POL_ETD_1"),
                    'pod': api_data.get("POD_1"),
                    'eta': api_data.get("POD_ETA_1")
                },
                {
                    'vsl': api_data.get("VSL_2"),
                    'voy': api_data.get("VOY_2"),
                    'pol': api_data.get("POL_2"),
                    'etd': api_data.get("POL_ETD_2"),
                    'pod': api_data.get("POD_2"),
                    'eta': api_data.get("POD_ETA_2")
                },
                {
                    'vsl': api_data.get("VSL_3"),
                    'voy': api_data.get("VOY_3"),
                    'pol': api_data.get("POL_3"),
                    'etd': api_data.get("POL_ETD_3"),
                    'pod': api_data.get("POD_3"),
                    'eta': api_data.get("POD_ETA_3")
                }
            ]

            # Lọc ra các chặng hợp lệ (có thông tin tàu VSL_x)
            valid_legs = [leg for leg in legs_data if leg.get('vsl')]

            if not valid_legs:
                logger.warning(f"Không tìm thấy chặng tàu hợp lệ nào trong dữ liệu API cho mã: {tracking_number_input}")
                # Vẫn trả về thông tin cơ bản nếu có
                return N8nTrackingInfo(
                    BookingNo= booking_no or tracking_number_input,
                    BlNumber= bl_number or tracking_number_input,
                    BookingStatus= "", # API không có trường này
                    Pol= pol or "",
                    Pod= pod or "",
                    **{k: "" for k in N8nTrackingInfo.__fields__ if k not in ['BookingNo', 'BlNumber', 'BookingStatus', 'Pol', 'Pod']}
                )

            logger.info(f"Tìm thấy {len(valid_legs)} chặng tàu hợp lệ từ API.")

            # --- Khởi tạo biến ---
            etd, atd, eta, ata = "", "", "", ""
            transit_port_list = []
            eta_transit, ata_transit = "", ""
            etd_transit, atd_transit = "", ""
            future_etd_transits = []
            today = date.today()

            # --- Xử lý chặng đầu (ETD/ATD) ---
            first_leg = valid_legs[0]
            etd_str = first_leg.get('etd')
            etd_date = self._parse_date_obj(etd_str)
            if etd_date and etd_date <= today:
                atd = etd_str # Đã xảy ra -> là Actual
            else:
                etd = etd_str # Chưa xảy ra -> là Expected

            # --- Xử lý chặng cuối (ETA/ATA) ---
            last_leg = valid_legs[-1]
            eta_str = last_leg.get('eta')
            eta_date = self._parse_date_obj(eta_str)
            if eta_date and eta_date <= today:
                ata = eta_str # Đã xảy ra -> là Actual
            else:
                eta = eta_str # Chưa xảy ra -> là Expected

            # --- Xử lý các chặng trung chuyển ---
            logger.info("Bắt đầu xử lý thông tin transit từ API...")
            for i in range(len(valid_legs) - 1):
                current_leg = valid_legs[i]
                next_leg = valid_legs[i+1]

                current_pod = current_leg.get('pod')
                next_pol = next_leg.get('pol')

                # Nếu cảng dỡ của chặng này = cảng xếp của chặng sau -> đây là transit
                if current_pod and next_pol and current_pod == next_pol:
                    transit_port = current_pod
                    logger.debug(f"Tìm thấy cảng transit '{transit_port}'")
                    if transit_port not in transit_port_list:
                        transit_port_list.append(transit_port)

                    # 1. Xử lý Ngày đến cảng transit (AtaTransit / EtaTransit)
                    temp_eta_transit_str = current_leg.get('eta')
                    temp_eta_date = self._parse_date_obj(temp_eta_transit_str)

                    if temp_eta_date and temp_eta_date <= today:
                        # Đã đến (Actual)
                        if not ata_transit:
                            ata_transit = temp_eta_transit_str
                            logger.debug(f"Tìm thấy AtaTransit đầu tiên: {ata_transit}")
                    else:
                        # Sắp đến (Expected)
                        if not ata_transit and not eta_transit:
                            eta_transit = temp_eta_transit_str
                            logger.debug(f"Tìm thấy EtaTransit đầu tiên: {eta_transit}")

                    # 2. Xử lý Ngày rời cảng transit (AtdTransit / EtdTransit)
                    temp_etd_transit_str = next_leg.get('etd')
                    temp_etd_date = self._parse_date_obj(temp_etd_transit_str)

                    if temp_etd_date and temp_etd_date <= today:
                        # Đã rời (Actual)
                        atd_transit = temp_etd_transit_str # Lấy ngày *cuối cùng*
                        logger.debug(f"Cập nhật AtdTransit cuối cùng: {atd_transit}")
                    else:
                        # Sắp rời (Expected)
                        if temp_etd_date and temp_etd_date > today:
                            future_etd_transits.append((temp_etd_date, transit_port, temp_etd_transit_str))
                            logger.debug(f"Thêm ETD transit trong tương lai: {temp_etd_transit_str} tại {transit_port}")

            # Chọn EtdTransit gần nhất trong tương lai
            if future_etd_transits:
                future_etd_transits.sort() # Sắp xếp theo ngày
                etd_transit = future_etd_transits[0][2] # Lấy chuỗi ngày của ngày sớm nhất
                logger.info(f"ETD transit gần nhất trong tương lai được chọn: {etd_transit}")

            # --- Chuẩn hóa kết quả ---
            shipment_data = N8nTrackingInfo(
                BookingNo= (booking_no or tracking_number_input).strip(),
                BlNumber= (bl_number or tracking_number_input).strip(),
                BookingStatus= "", # API không có trường này
                Pol= pol.strip() if pol else "",
                Pod= pod.strip() if pod else "",
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= ", ".join(transit_port_list) if transit_port_list else "",
                EtdTransit= self._format_date(etd_transit) or "",
                AtdTransit= self._format_date(atd_transit) or "",
                EtaTransit= self._format_date(eta_transit) or "",
                AtaTransit= self._format_date(ata_transit) or ""
            )

            logger.info(f"Trích xuất dữ liệu thành công từ API cho: {tracking_number_input}")
            return shipment_data

        except Exception as e:
            logger.error(f"[PanCont API Scraper] -> Lỗi trong quá trình trích xuất từ JSON: {e}", exc_info=True)
            return None