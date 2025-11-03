import logging
import requests # <--- Dùng requests
import time
from datetime import datetime, date
from bs4 import BeautifulSoup # <--- Dùng BeautifulSoup
import re
import traceback

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Khởi tạo logger cho module này
logger = logging.getLogger(__name__)

class SealeadScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web SeaLead bằng requests và BeautifulSoup,
    chuẩn hóa kết quả theo định dạng JSON yêu cầu.
    """
    def __init__(self, driver, config): # driver không còn được sử dụng
        self.config = config
        # Tạo session để quản lý headers và cookies (nếu cần)
        self.session = requests.Session()
        # Headers cơ bản để giả lập trình duyệt
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.sea-lead.com/track-shipment/', # Referer quan trọng
        })

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'Month DD, YYYY' hoặc 'YYYY-MM-DD HH:MM:SS' sang 'DD/MM/YYYY'.
        Trả về "" nếu lỗi.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        # Thử format '%B %d, %Y' trước
        try:
            dt_obj = datetime.strptime(date_str.strip(), '%B %d, %Y')
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            # Thử format 'YYYY-MM-DD HH:MM:SS' (cho ATA từ bảng container)
            try:
                # Chỉ lấy phần date
                date_part = date_str.strip().split(" ")[0]
                dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
                return dt_obj.strftime('%d/%m/%Y')
            except ValueError:
                logger.warning("[SeaLead Scraper] Không thể phân tích định dạng ngày: %s", date_str)
                return "" # Trả về chuỗi rỗng

    # --- Hàm helper lấy text từ soup ---
    def _get_text_safe_soup(self, soup_element, selector, attribute=None):
        """
        Helper lấy text hoặc attribute từ phần tử BeautifulSoup một cách an toàn.
        Trả về chuỗi rỗng "" nếu không tìm thấy.
        """
        if not soup_element:
            return ""
        try:
            target = soup_element.select_one(selector) if selector else soup_element # Cho phép truyền trực tiếp element
            if target:
                if attribute:
                    return target.get(attribute, "").strip()
                else:
                    return ' '.join(target.stripped_strings)
            else:
                return ""
        except Exception as e:
            selector_str = selector if selector else "element itself"
            logger.warning(f"[SeaLead Scraper] Lỗi khi lấy text/attribute từ soup selector '{selector_str}': {e}")
            return ""

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho SeaLead bằng requests và BeautifulSoup.
        Trang này dùng POST request để tìm kiếm.
        """
        logger.info(f"[SeaLead Scraper] Bắt đầu scrape cho mã: {tracking_number} (sử dụng requests)")
        t_total_start = time.time()
        try:
            search_url = self.config['url']
            payload = {'bl_number': tracking_number}
            logger.info(f"[SeaLead Scraper] -> Gửi POST request đến: {search_url} với payload: {payload}")
            t_req_start = time.time()
            post_headers = self.session.headers.copy()
            post_headers.update({
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://www.sea-lead.com',
            })
            response = self.session.post(search_url, data=payload, headers=post_headers, timeout=30)
            response.raise_for_status()
            logger.info("-> (Thời gian) Gửi POST và nhận HTML: %.2fs", time.time() - t_req_start)

            t_parse_start = time.time()
            soup = BeautifulSoup(response.text, 'lxml')
            logger.info("-> (Thời gian) Parse HTML bằng BeautifulSoup: %.2fs", time.time() - t_parse_start)

            bl_header = soup.find('h4', string=lambda text: text and 'Bill of lading number:' in text)
            if not bl_header:
                 logger.warning("[SeaLead Scraper] Không tìm thấy header B/L trên trang response. Mã tracking có thể không hợp lệ hoặc trang lỗi.")
                 # Kiểm tra xem có phải trang tìm kiếm ban đầu không
                 search_input_check = soup.select_one("form input#bl_number") # Kiểm tra input tìm kiếm
                 if search_input_check:
                      logger.warning("[SeaLead Scraper] Trang trả về vẫn là trang tìm kiếm, mã tracking không đúng.")
                      return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên trang SeaLead (mã không đúng?)."
                 # Tìm thông báo lỗi chung nếu có
                 error_div = soup.select_one(".error-message-class") # Thay selector nếu biết
                 if error_div:
                     error_msg = error_div.get_text(strip=True)
                     logger.error("[SeaLead Scraper] Trang trả về lỗi: %s", error_msg)
                     return None, f"Trang SeaLead báo lỗi: {error_msg}"

                 return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên trang SeaLead."

            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_soup(soup, tracking_number)
            logger.info("-> (Thời gian) Trích xuất dữ liệu từ soup: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                return None, f"Không thể chuẩn hóa dữ liệu từ trang cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info(f"[SeaLead Scraper] -> Hoàn tất scrape thành công cho mã: {tracking_number}. (Tổng thời gian: %.2fs)",
                         t_total_end - t_total_start)
            return normalized_data, None

        # --- Xử lý lỗi (giống phiên bản trước) ---
        except requests.exceptions.Timeout:
             t_total_fail = time.time()
             logger.warning("[SeaLead Scraper] Timeout khi scrape mã '%s' (Tổng thời gian: %.2fs)",
                          tracking_number, t_total_fail - t_total_start)
             return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout Request)."
        except requests.exceptions.HTTPError as e:
            t_total_fail = time.time()
            logger.error("[SeaLead Scraper] Lỗi HTTP %s khi scrape mã '%s' (Tổng thời gian: %.2fs)",
                         e.response.status_code, tracking_number, t_total_fail - t_total_start)
            return None, f"Lỗi HTTP {e.response.status_code} khi truy vấn '{tracking_number}'."
        except requests.exceptions.RequestException as e:
             t_total_fail = time.time()
             logger.error("[SeaLead Scraper] Lỗi kết nối khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                          tracking_number, e, t_total_fail - t_total_start, exc_info=True)
             return None, f"Lỗi kết nối khi truy vấn '{tracking_number}': {e}"
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[SeaLead Scraper] Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    # --- Hàm _extract_and_normalize_data_soup ---
    def _extract_and_normalize_data_soup(self, soup, tracking_number_input):
        """
        Trích xuất dữ liệu từ trang kết quả đã parse bằng BeautifulSoup và ánh xạ vào template JSON.
        Cập nhật dựa trên cấu trúc HTML mới.
        """
        logger.debug("[SeaLead Scraper] --- Bắt đầu _extract_and_normalize_data_soup ---")
        try:
            t_extract_detail_start = time.time()
            today = date.today()

            # === BƯỚC 1: KHỞI TẠO BIẾN ===
            etd, atd, eta, ata = "", "", "", ""
            transit_port_list = []
            etd_transit_final, atd_transit, eta_transit, ata_transit = "", "", "", ""
            future_etd_transits = []

            # === BƯỚC 2: TRÍCH XUẤT THÔNG TIN CƠ BẢN ===
            t_basic_info_start = time.time()
            bl_header = soup.find('h4', string=lambda text: text and 'Bill of lading number' in text)
            bl_number = bl_header.get_text(strip=True).replace("Bill of lading number:", "").strip() if bl_header else tracking_number_input
            booking_no = bl_number
            booking_status = ""

            # Sử dụng HTML snippet mới để tìm POL, POD trong table.route-table-bill
            info_table = soup.select_one("div#custom-table-track table.route-table-bill")
            pol, pod = "", ""
            if info_table:
                pol_th = info_table.find('th', string='Port of Loading')
                if pol_th: pol = self._get_text_safe_soup(pol_th.find_next_sibling('td'), None)
                pod_th = info_table.find('th', string='Port of Discharge')
                if pod_th: pod = self._get_text_safe_soup(pod_th.find_next_sibling('td'), None)
            else:
                 logger.warning("[SeaLead Scraper] Không tìm thấy bảng thông tin tóm tắt (div#custom-table-track table.route-table-bill).")

            logger.info(f"[SeaLead Scraper] -> BL: {bl_number}, POL: {pol}, POD: {pod}")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_basic_info_start)

            # === BƯỚC 3: TRÍCH XUẤT LỊCH TRÌNH VÀ THÔNG TIN TRUNG CHUYỂN ===
            t_schedule_start = time.time()
            # Bảng lịch trình chính nằm trong div.single-container-main div#custom-table-track-full table.route-table
            main_schedule_table = soup.select_one("div.single-container-main div#custom-table-track-full table.route-table")

            if not main_schedule_table:
                logger.warning(f"[SeaLead Scraper] Không tìm thấy bảng lịch trình chính (div#custom-table-track-full table.route-table) cho mã: {tracking_number_input}")
                # Trả về thông tin cơ bản
                return N8nTrackingInfo(
                    BookingNo=booking_no, BlNumber=bl_number, BookingStatus=booking_status,
                    Pol=pol, Pod=pod,
                    **{k: "" for k in N8nTrackingInfo.__fields__ if k not in ['BookingNo', 'BlNumber', 'BookingStatus', 'Pol', 'Pod']}
                )
            rows = main_schedule_table.select("tr")

            if not rows:
                logger.warning(f"[SeaLead Scraper] Không tìm thấy chặng nào trong bảng lịch trình chính cho mã: {tracking_number_input}")
                # Trả về thông tin cơ bản
                return N8nTrackingInfo(
                    BookingNo=booking_no, BlNumber=bl_number, BookingStatus=booking_status,
                    Pol=pol, Pod=pod,
                    **{k: "" for k in N8nTrackingInfo.__fields__ if k not in ['BookingNo', 'BlNumber', 'BookingStatus', 'Pol', 'Pod']}
                )

            logger.debug("-> Tìm thấy %d chặng trong bảng lịch trình chính.", len(rows))

            # Xử lý chặng đầu tiên
            first_leg_cells = rows[1].find_all("td", recursive=False)
            # ETD: cột 6 (index 5) - '(Estimated) Departure Time'
            etd = self._get_text_safe_soup(first_leg_cells[5], None) if len(first_leg_cells) > 5 else ""
            atd = "" # Không có

            # Xử lý chặng cuối
            last_leg_cells = rows[-1].find_all("td", recursive=False)
            eta = self._get_text_safe_soup(last_leg_cells[7], None) if len(last_leg_cells) > 7 else ""
            logger.debug("-> ETD (dự kiến): %s, ETA (dự kiến): %s", etd, eta)

            logger.info("[SeaLead Scraper] Bắt đầu xử lý thông tin transit...")
            for i in range(len(rows) - 1):
                current_leg_cells = rows[i].find_all("td", recursive=False)
                next_leg_cells = rows[i+1].find_all("td", recursive=False)

                current_pod = self._get_text_safe_soup(current_leg_cells[6], None) if len(current_leg_cells) > 6 else "" # Destination Location
                next_pol = self._get_text_safe_soup(next_leg_cells[4], None) if len(next_leg_cells) > 4 else "" # Origin location

                if current_pod and next_pol and current_pod == next_pol:
                    logger.debug(f"[SeaLead Scraper] Tìm thấy cảng transit '{current_pod}' giữa chặng {i} và {i+1}")
                    if current_pod not in transit_port_list:
                         transit_port_list.append(current_pod)

                    # EtaTransit: cột 8 (index 7) - '(Estimated) Arrival Time'
                    temp_eta_transit = self._get_text_safe_soup(current_leg_cells[7], None) if len(current_leg_cells) > 7 else ""
                    if temp_eta_transit and not eta_transit:
                         eta_transit = temp_eta_transit
                         logger.debug(f"[SeaLead Scraper] Tìm thấy EtaTransit đầu tiên: {eta_transit}")
                    ata_transit = ""

                    # EtdTransit: cột 6 (index 5) - '(Estimated) Departure Time'
                    temp_etd_transit_str = self._get_text_safe_soup(next_leg_cells[5], None) if len(next_leg_cells) > 5 else ""
                    atd_transit = ""

                    if temp_etd_transit_str:
                        try:
                            etd_transit_date = datetime.strptime(temp_etd_transit_str.strip(), '%B %d, %Y').date()
                            if etd_transit_date > today:
                                future_etd_transits.append((etd_transit_date, current_pod, temp_etd_transit_str))
                                logger.debug(f"[SeaLead Scraper] Thêm ETD transit trong tương lai: {temp_etd_transit_str} ({current_pod})")
                        except (ValueError, IndexError):
                            logger.warning(f"[SeaLead Scraper] Không thể parse ETD transit: {temp_etd_transit_str}")

            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit_final = future_etd_transits[0][2]
                logger.info(f"[SeaLead Scraper] ETD transit gần nhất trong tương lai được chọn: {etd_transit_final}")
            else:
                 logger.info("[SeaLead Scraper] Không tìm thấy ETD transit nào trong tương lai.")
            logger.debug("-> (Thời gian) Xử lý lịch trình và transit: %.2fs", time.time() - t_schedule_start)


            # === BƯỚC 4: TRÍCH XUẤT ATA TỪ CHI TIẾT CONTAINER (NẾU CÓ) ===
            container_details_table = None
            all_route_tables = soup.select("div.single-container-main table.route-table")
            
            if len(all_route_tables) > 1:
                 for table in all_route_tables[1:]:
                    header_th = table.find('th', string=lambda text: text and 'Container No.' in text)
                    if header_th:
                        container_details_table = table
                        logger.debug("[SeaLead Scraper] Tìm thấy bảng chi tiết container.")
                        break

            t_container_detail_start = time.time()
            if container_details_table:
                try:
                    first_container_row = container_details_table.select_one("tbody tr")
                    if first_container_row:
                        container_cells = first_container_row.find_all("td", recursive=False)
                        ata = self._get_text_safe_soup(container_cells[4], None) if len(container_cells) > 4 else ""
                        logger.info(f"[SeaLead Scraper] Tìm thấy Ata (Latest Move Time) từ chi tiết container: {ata}")
                    else:
                        logger.warning("[SeaLead Scraper] Không tìm thấy hàng dữ liệu trong bảng chi tiết container.")
                except Exception as e_ata:
                    logger.warning("[SeaLead Scraper] Lỗi khi lấy Ata từ chi tiết container: %s", e_ata)
            else:
                logger.info("[SeaLead Scraper] Không có bảng chi tiết container, không thể lấy Ata.")
            logger.debug("-> (Thời gian) Trích xuất chi tiết container (Ata): %.2fs", time.time() - t_container_detail_start)

            # === BƯỚC 5: XÂY DỰNG ĐỐI TƯỢNG JSON ===
            t_normalize_start = time.time()
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no,
                BlNumber= bl_number,
                BookingStatus= booking_status, # ""
                Pol= pol,
                Pod= pod,
                Etd= self._format_date(etd) or "",
                Atd= atd or "", # ""
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= ", ".join(transit_port_list) if transit_port_list else "",
                EtdTransit= self._format_date(etd_transit_final) or "",
                AtdTransit= atd_transit or "", # ""
                EtaTransit= self._format_date(eta_transit) or "",
                AtaTransit= ata_transit or "" # ""
            )

            logger.info("[SeaLead Scraper] Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("-> (Thời gian) Tổng thời gian trích xuất chi tiết: %.2fs", time.time() - t_extract_detail_start)

            return shipment_data

        except Exception as e:
            logger.error(f"[SeaLead Scraper] Lỗi trong quá trình trích xuất chi tiết từ soup cho mã '{tracking_number_input}': {e}", exc_info=True)
            return None