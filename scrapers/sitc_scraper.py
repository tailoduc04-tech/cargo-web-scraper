import logging
import requests
import time
from datetime import datetime, date

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class SitcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang SITC bằng cách gọi API trực tiếp
    và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def __init__(self, driver, config):
        self.config = config
        self.base_url = "https://ebusiness.sitcline.com/"
        self.api_url = "https://ebusiness.sitcline.com/api/equery/cargoTrack/searchTrack"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'ebusiness.sitcline.com',
            'Referer': 'https://ebusiness.sitcline.com/',
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
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD HH:MM' sang 'DD/MM/YYYY'.
        Trả về "" nếu đầu vào không hợp lệ hoặc rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            date_part = date_str.split(" ")[0]
            if not date_part:
                return ""
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[SITC API Scraper] Không thể phân tích định dạng ngày: %s", date_str)
            return ""

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính bằng API.
        Thực hiện lấy cookie và gọi API tracking.
        """
        logger.info("[SITC API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()

        params = {'blNo': tracking_number}

        try:
            
            logger.info(f"[SITC API Scraper] Gửi GET request đến {self.base_url} để khởi tạo session...")
            t_init_start = time.time()
            initial_response = self.session.get(self.base_url, timeout=30)
            initial_response.raise_for_status()
            logger.info("-> (Thời gian) Khởi tạo session: %.2fs", time.time() - t_init_start)

            
            logger.info(f"[SITC API Scraper] Gửi GET request đến API: {self.api_url}")
            t_api_start = time.time()
            response = self.session.get(self.api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info("-> (Thời gian) Gọi API tracking: %.2fs", time.time() - t_api_start)

            # Kiểm tra response thành công và có dữ liệu
            if not data.get("success") or not data.get("data"):
                error_msg = data.get("message", "API không trả về dữ liệu thành công.")
                logger.warning("[SITC API Scraper] API không trả về dữ liệu thành công cho '%s': %s", tracking_number, error_msg)
                return None, f"Không tìm thấy dữ liệu cho '{tracking_number}': {error_msg}"

            # Trích xuất và chuẩn hóa
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_api(data["data"], tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[SITC API Scraper] Hoàn tất scrape thành công. (Tổng thời gian: %.2fs)", time.time() - t_total_start)
            return normalized_data, None

        except requests.exceptions.Timeout:
             t_total_fail = time.time()
             logger.warning("[SITC API Scraper] Timeout khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                          tracking_number, t_total_fail - t_total_start)
             return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout API)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[SITC API Scraper] Lỗi HTTP %s khi gọi API cho mã '%s' (Tổng thời gian: %.2fs)",
                         e.response.status_code, tracking_number, t_total_fail - t_total_start, exc_info=False)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[SITC API Scraper] Lỗi kết nối khi gọi API cho mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[SITC API Scraper] Lỗi không mong muốn cho mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data_api(self, api_data, tracking_number_input):
        """
        Trích xuất và chuẩn hóa dữ liệu từ dictionary JSON trả về của API SITC.
        """
        logger.info("[SITC API Scraper] --- Bắt đầu _extract_and_normalize_data_api ---")
        t_extract_detail_start = time.time()
        try:
            t_basic_info_start = time.time()

            list1 = api_data.get("list1", [])
            bl_number = list1[0].get("blNo") if list1 else tracking_number_input
            booking_no = bl_number
            logger.info("[SITC API Scraper] Đã tìm thấy BlNumber: %s", bl_number)

            # Lấy BookingStatus từ list3 (lấy status của cont đầu tiên)
            booking_status = ""
            list3 = api_data.get("list3", [])
            if list3:
                # Dùng movementNameEn làm trạng thái
                booking_status = list3[0].get("movementNameEn", "")
                logger.info("[SITC API Scraper] Đã tìm thấy BookingStatus (từ cont đầu tiên): %s", booking_status)
            else:
                logger.warning("[SITC API Scraper] Không tìm thấy thông tin container (list3) để lấy BookingStatus.")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_basic_info_start)


            t_schedule_start = time.time()
            schedule_list = api_data.get("list2", [])

            if not schedule_list:
                logger.warning("[SITC API Scraper] Không tìm thấy dữ liệu lịch trình (list2) cho mã: %s", tracking_number_input)
                # Vẫn trả về thông tin cơ bản nếu có
                return N8nTrackingInfo(
                    BookingNo= booking_no,
                    BlNumber= bl_number,
                    BookingStatus= booking_status,
                    **{k: "" for k in N8nTrackingInfo.__fields__ if k not in ['BookingNo', 'BlNumber', 'BookingStatus']}
                )

            # Lấy POL từ chặng đầu tiên và POD từ chặng cuối cùng
            pol = schedule_list[0].get("portFromName", "")
            logger.info("[SITC API Scraper] Đã tìm thấy POL: %s", pol)
            pod = schedule_list[-1].get("portToName", "")
            logger.info("[SITC API Scraper] Đã tìm thấy POD: %s", pod)

            etd, atd, eta, ata = "", "", "", ""
            etd_transit_final, atd_transit, transit_port_list, eta_transit, ata_transit = "", "", [], "", ""

            # Xử lý chặng đầu tiên
            first_leg = schedule_list[0]
            etd = first_leg.get("etd", "") # Schedule ETD
            atd = first_leg.get("atd", "") # ETD/ATD (Actual)

            # Xử lý chặng cuối
            last_leg = schedule_list[-1]
            eta = last_leg.get("eta", "") # Schedule ETA
            ata = last_leg.get("ata", "") # ETA/ATA (Actual)

            # Xử lý các chặng trung chuyển (Áp dụng logic COSCO/SITC cũ)
            future_etd_transits = []
            today = date.today()
            logger.info("[SITC API Scraper] Bắt đầu xử lý thông tin transit...")

            for i in range(len(schedule_list) - 1):
                current_leg = schedule_list[i]
                next_leg = schedule_list[i+1]

                current_pod_name = current_leg.get("portToName", "").strip()
                next_pol_name = next_leg.get("portFromName", "").strip()

                if current_pod_name and next_pol_name and current_pod_name == next_pol_name:
                    logger.debug("[SITC API Scraper] Tìm thấy cảng transit '%s' giữa chặng %d và %d", current_pod_name, i, i+1)
                    if current_pod_name not in transit_port_list:
                         transit_port_list.append(current_pod_name)

                    temp_eta_transit = current_leg.get("eta", "") # Schedule ETA
                    temp_ata_transit = current_leg.get("ata", "") # ETA/ATA

                    if temp_ata_transit and not ata_transit: # Chỉ lấy ATA transit đầu tiên
                         ata_transit = temp_ata_transit
                         logger.debug("[SITC API Scraper] Tìm thấy AtaTransit đầu tiên: %s", ata_transit)
                    elif temp_eta_transit and not ata_transit and not eta_transit: # Chỉ lấy ETA transit đầu tiên nếu chưa có ATA
                         eta_transit = temp_eta_transit
                         logger.debug("[SITC API Scraper] Tìm thấy EtaTransit đầu tiên: %s", eta_transit)

                    temp_etd_transit_str = next_leg.get("etd", "") # Schedule ETD
                    temp_atd_transit = next_leg.get("atd", "") # ETD/ATD

                    if temp_atd_transit: # Luôn lấy ATD transit cuối cùng
                         atd_transit = temp_atd_transit
                         logger.debug("[SITC API Scraper] Cập nhật AtdTransit cuối cùng: %s", atd_transit)

                    if temp_etd_transit_str:
                        try:
                            etd_transit_date = datetime.strptime(temp_etd_transit_str.split(' ')[0], '%Y-%m-%d').date()
                            if etd_transit_date > today:
                                future_etd_transits.append((etd_transit_date, current_pod_name, temp_etd_transit_str))
                                logger.debug("[SITC API Scraper] Thêm ETD transit trong tương lai: %s (%s)", temp_etd_transit_str, current_pod_name)
                        except (ValueError, IndexError):
                            logger.warning("[SITC API Scraper] Không thể parse ETD transit: %s", temp_etd_transit_str)

            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit_final = future_etd_transits[0][2] # Lấy chuỗi ngày
                logger.info("[SITC API Scraper] ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
            else:
                 logger.info("[SITC API Scraper] Không tìm thấy ETD transit nào trong tương lai.")
            logger.debug("-> (Thời gian) Xử lý sailing schedule và transit: %.2fs", time.time() - t_schedule_start)

            t_normalize_start = time.time()
            # Đảm bảo mọi giá trị None hoặc lỗi đều trở thành ""
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no,
                BlNumber= bl_number,
                BookingStatus= booking_status,
                Pol= pol,
                Pod= pod,
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
            logger.info("[SITC API Scraper] Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("[SITC API Scraper] --- Hoàn tất _extract_and_normalize_data_api --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)

            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("[SITC API Scraper] Lỗi trong quá trình trích xuất chi tiết từ API cho mã '%s': %s",
                         tracking_number_input, e, exc_info=True)
            logger.info("[SITC API Scraper] --- Hoàn tất _extract_and_normalize_data_api (lỗi) --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return None