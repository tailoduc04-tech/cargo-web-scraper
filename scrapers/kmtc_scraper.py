import logging
import requests
import time
from datetime import datetime, date

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho module
logger = logging.getLogger(__name__)

class KmtcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web eKMTC bằng cách gọi API trực tiếp,
    sử dụng logging, cấu trúc chuẩn và chuẩn hóa kết quả đầu ra.
    """

    def __init__(self, driver, config): # driver không còn được sử dụng nhưng giữ để tương thích
        self.config = config # config có thể vẫn cần nếu URL thay đổi
        self.step1_url = "https://api.ekmtc.com/trans/trans/cargo-tracking/"
        self.step2_url_template = "https://api.ekmtc.com/trans/trans/cargo-tracking/{bkgNo}/close-info"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Host': 'api.ekmtc.com',
            'Origin': 'https://www.ekmtc.com',
            'Referer': 'https://www.ekmtc.com/',
            'Sec-Ch-Ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ API format 'YYYYMMDDHHMM' sang 'DD/MM/YYYY'.
        Trả về chuỗi rỗng "" nếu đầu vào không hợp lệ hoặc rỗng.
        """
        if not date_str or not isinstance(date_str, str) or len(date_str) < 8:
            return ""
        try:
            # Chỉ lấy phần YYYYMMDD
            date_part = date_str[:8]
            dt_obj = datetime.strptime(date_part, '%Y%m%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[KMTC API Scraper] Không thể phân tích định dạng ngày: %s", date_str)
            return "" # Trả về chuỗi rỗng nếu lỗi

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính bằng API.
        Thực hiện 2 bước gọi API và trả về dữ liệu đã chuẩn hóa.
        """
        logger.info("[KMTC API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()
        bkg_no = None
        data_step1 = None

        # --- BƯỚC 1: Lấy bkgNo ---
        try:
            logger.info("[KMTC API Scraper] Bước 1: Gửi POST request để lấy bkgNo...")
            t_step1_start = time.time()
            payload_step1 = {"dtKnd": "BL", "blNo": tracking_number}
            response_step1 = self.session.post(self.step1_url, json=payload_step1, timeout=30)
            response_step1.raise_for_status()
            data_step1 = response_step1.json()
            logger.info("-> (Thời gian) Gọi API Bước 1: %.2fs", time.time() - t_step1_start)

            # Trích xuất bkgNo
            if data_step1 and "cntrList" in data_step1 and data_step1["cntrList"]:
                bkg_no = data_step1["cntrList"][0].get("bkgNo")
                if bkg_no:
                    logger.info("[KMTC API Scraper] Bước 1: Trích xuất thành công bkgNo: %s", bkg_no)
                else:
                    logger.error("[KMTC API Scraper] Bước 1: Không tìm thấy 'bkgNo' trong 'cntrList'.")
                    return None, f"Không tìm thấy dữ liệu chi tiết (bkgNo) cho B/L '{tracking_number}'."
            else:
                logger.error("[KMTC API Scraper] Bước 1: Response không chứa 'cntrList' hợp lệ.")
                return None, f"Không tìm thấy dữ liệu (cntrList) cho B/L '{tracking_number}'."

        except requests.exceptions.Timeout:
            logger.error("[KMTC API Scraper] Bước 1: Request bị timeout.")
            return None, f"Request timeout khi lấy thông tin ban đầu cho '{tracking_number}'."
        except requests.exceptions.HTTPError as e:
            logger.error(f"[KMTC API Scraper] Bước 1: Lỗi HTTP: {e.response.status_code} - {e.response.reason}")
            return None, f"Lỗi HTTP {e.response.status_code} khi lấy thông tin ban đầu cho '{tracking_number}'."
        except Exception as e:
            logger.error(f"[KMTC API Scraper] Bước 1: Lỗi không xác định: {e}", exc_info=True)
            return None, f"Lỗi không xác định khi lấy thông tin ban đầu cho '{tracking_number}': {e}"

        # --- BƯỚC 2: Lấy thông tin chi tiết bằng bkgNo ---
        if bkg_no:
            try:
                step2_url = self.step2_url_template.format(bkgNo=bkg_no)
                logger.info("[KMTC API Scraper] Bước 2: Gửi GET request để lấy chi tiết...")
                t_step2_start = time.time()
                response_step2 = self.session.get(step2_url, timeout=30)
                response_step2.raise_for_status()
                data_step2 = response_step2.json()
                logger.info("-> (Thời gian) Gọi API Bước 2: %.2fs", time.time() - t_step2_start)

                logger.info("[KMTC API Scraper] Bước 2: Request thành công.")

                # --- BƯỚC 3: Trích xuất và chuẩn hóa ---
                t_extract_start = time.time()
                normalized_data = self._extract_and_normalize_data(data_step1, data_step2, tracking_number)
                logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

                if not normalized_data:
                    logger.warning(f"[KMTC API Scraper] Lỗi: Không thể chuẩn hóa dữ liệu cho '{tracking_number}'.")
                    return None, f"Không thể chuẩn hóa dữ liệu đã lấy từ API cho '{tracking_number}'."

                t_total_end = time.time()
                logger.info("[KMTC API Scraper] Hoàn tất scrape thành công. (Tổng thời gian: %.2fs)",
                             time.time() - t_total_start)
                return normalized_data, None

            except requests.exceptions.Timeout:
                logger.error("[KMTC API Scraper] Bước 2: Request bị timeout.")
                return None, f"Request timeout khi lấy thông tin chi tiết cho '{tracking_number}' (bkgNo: {bkg_no})."
            except requests.exceptions.HTTPError as e:
                logger.error(f"[KMTC API Scraper] Bước 2: Lỗi HTTP: {e.response.status_code} - {e.response.reason}")
                return None, f"Lỗi HTTP {e.response.status_code} khi lấy thông tin chi tiết cho '{tracking_number}' (bkgNo: {bkg_no})."
            except Exception as e:
                logger.error(f"[KMTC API Scraper] Bước 2: Lỗi không xác định: {e}", exc_info=True)
                return None, f"Lỗi không xác định khi lấy thông tin chi tiết cho '{tracking_number}': {e}"
        else:
            # Trường hợp này không nên xảy ra do đã kiểm tra ở Bước 1
            logger.error("[KMTC API Scraper] Lỗi logic: Không có bkgNo để thực hiện Bước 2.")
            return None, f"Lỗi logic: Không có bkgNo cho '{tracking_number}'."


    def _extract_and_normalize_data(self, data_step1, data_step2, tracking_number_input):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ response JSON của API KMTC.
        """
        logger.info("[KMTC API Scraper] --- Bắt đầu _extract_and_normalize_data ---")
        try:
            # --- Lấy thông tin từ data_step2 (chi tiết) ---
            bkg_no = data_step2.get("bkgNo")
            bl_no_raw = data_step1["cntrList"][0].get("blNo") if data_step1 and data_step1.get("cntrList") else None
            # API trả về BL No không có tiền tố hãng tàu, thêm vào nếu cần
            bl_no = f"KMT{bl_no_raw}" if bl_no_raw else tracking_number_input

            pol = data_step2.get("polPortEnm", "").split(',')[0].strip() # Lấy tên cảng chính
            pod = data_step2.get("podPortEnm", "").split(',')[0].strip()
            etd_api = data_step2.get("etd") # YYYYMMDDHHMM
            eta_api = data_step2.get("eta")

            # Booking Status: Có thể dùng bkgStsCd hoặc issueStatus từ data_step1
            # '01' là Booked, '02' là BL Issued (tạm lấy từ issueStatus)
            issue_status_code = data_step1["cntrList"][0].get("issueStatus") if data_step1 and data_step1.get("cntrList") else None
            booking_status = "B/L Issued" if issue_status_code == "02" else "Booked" # Đơn giản hóa

            logger.info(f"[KMTC API Scraper] -> Thông tin cơ bản: BKG='{bkg_no}', BL='{bl_no}', POL='{pol}', POD='{pod}', Status='{booking_status}'")
            logger.info(f"[KMTC API Scraper] -> ETD (API): '{etd_api}', ETA (API): '{eta_api}'")

            # --- API KMTC không trả về lịch sử chi tiết (Actual dates, Transit) ---
            # Do đó, ATD sẽ lấy từ ETD nếu ngày đó đã qua, ATA lấy từ ETA nếu đã qua
            # Các trường transit sẽ để trống

            etd = self._format_date(etd_api)
            eta = self._format_date(eta_api)
            atd = ""
            ata = ""

            today = date.today()
            try:
                if etd_api:
                    etd_dt = datetime.strptime(etd_api[:8], '%Y%m%d').date()
                    if etd_dt <= today:
                        atd = etd # Nếu ngày ETD đã qua, coi như là ATD
                        etd = "" # Xóa ETD đi
            except (ValueError, IndexError):
                 logger.warning("[KMTC API Scraper] Không thể so sánh ngày ETD với hôm nay.")

            try:
                 if eta_api:
                    eta_dt = datetime.strptime(eta_api[:8], '%Y%m%d').date()
                    if eta_dt <= today:
                        ata = eta # Nếu ngày ETA đã qua, coi như là ATA
                        eta = "" # Xóa ETA đi
            except (ValueError, IndexError):
                 logger.warning("[KMTC API Scraper] Không thể so sánh ngày ETA với hôm nay.")


            # Các trường transit không có thông tin từ API này
            transit_port = ""
            etd_transit = ""
            atd_transit = ""
            eta_transit = ""
            ata_transit = ""

            # --- Xây dựng đối tượng JSON cuối cùng ---
            logger.debug("[KMTC API Scraper] Xây dựng đối tượng N8nTrackingInfo...")
            normalized_data = N8nTrackingInfo(
                BookingNo= bkg_no or tracking_number_input,
                BlNumber= bl_no, # Đã có tiền tố KMT
                BookingStatus= booking_status,
                Pol= pol,
                Pod= pod,
                Etd= etd, # Đã xử lý nếu là quá khứ
                Atd= atd, # Lấy từ ETD nếu đã qua
                Eta= eta, # Đã xử lý nếu là quá khứ
                Ata= ata, # Lấy từ ETA nếu đã qua
                TransitPort= transit_port,
                EtdTransit= etd_transit,
                AtdTransit= atd_transit,
                EtaTransit= eta_transit,
                AtaTransit= ata_transit
            )

            logger.info("[KMTC API Scraper] --- Hoàn tất, đã chuẩn hóa dữ liệu ---")
            return normalized_data

        except Exception as e:
            logger.error(f"[KMTC API Scraper] Lỗi trong quá trình trích xuất và chuẩn hóa: {e}", exc_info=True)
            return None