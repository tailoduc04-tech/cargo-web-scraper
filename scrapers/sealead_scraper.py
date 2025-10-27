import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time # <--- Thêm import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class SealeadScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web SeaLead và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'Month DD, YYYY' (ví dụ: August 7, 2025) sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Định dạng của SeaLead là '%B %d, %Y'
            dt_obj = datetime.strptime(date_str, '%B %d, %Y')
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            logger.warning("[SeaLead Scraper] Không thể phân tích định dạng ngày: %s", date_str)
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho SeaLead.
        """
        logger.info(f"[SeaLead Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        t_total_start = time.time() # Tổng thời gian bắt đầu
        try:
            t_nav_start = time.time()
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)
            logger.info("-> (Thời gian) Tải trang: %.2fs", time.time() - t_nav_start)

            # --- 1. Nhập mã B/L và tìm kiếm ---
            logger.info("[SeaLead Scraper] -> Đang điền thông tin tìm kiếm...")
            t_search_start = time.time()
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "bl_number")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            track_button = self.driver.find_element(By.CSS_SELECTOR, ".track-form-container button")
            self.driver.execute_script("arguments[0].click();", track_button)
            logger.info("-> (Thời gian) Điền và gửi form tìm kiếm: %.2fs", time.time() - t_search_start)


            # --- 2. Chờ trang kết quả và trích xuất ---
            logger.info("[SeaLead Scraper] -> Đang chờ trang kết quả tải...")
            t_wait_result_start = time.time()
            # Chờ header chứa số B/L xuất hiện
            self.wait.until(
                EC.visibility_of_element_located((By.XPATH, f"//h4[contains(text(), 'Bill of lading number:')]"))
            )
            logger.info("[SeaLead Scraper] -> Trang kết quả đã tải. (Thời gian chờ: %.2fs)", time.time() - t_wait_result_start)

            t_extract_start = time.time()
            logger.info("[SeaLead Scraper] -> Bắt đầu trích xuất.")
            normalized_data = self._extract_and_normalize_data(tracking_number)
            logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)


            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info(f"[SeaLead Scraper] -> Hoàn tất scrape thành công cho mã: {tracking_number}. (Tổng thời gian: %.2fs)",
                         t_total_end - t_total_start)
            return normalized_data, None

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/sealead_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("[SeaLead Scraper] Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s (Tổng thời gian: %.2fs)",
                             tracking_number, screenshot_path, t_total_fail - t_total_start)
            except Exception as ss_e:
                logger.error("[SeaLead Scraper] Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("[SeaLead Scraper] Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ trang kết quả và ánh xạ vào template JSON.
        """
        logger.debug("--- Bắt đầu _extract_and_normalize_data ---")
        try:
            t_extract_detail_start = time.time() # Thời gian bắt đầu trích xuất chi tiết
            # === BƯỚC 1: KHỞI TẠO BIẾN ===
            etd, atd, eta, ata = None, None, None, None
            transit_port_list = []
            etd_transit_final, atd_transit, eta_transit, ata_transit = None, None, None, None
            today = date.today()

            # === BƯỚC 2: TRÍCH XUẤT THÔNG TIN CƠ BẢN ===
            t_basic_info_start = time.time()
            bl_number = self.driver.find_element(By.XPATH, "//h4[contains(text(), 'Bill of lading number')]").text.replace("Bill of lading number:", "").strip()
            booking_no = bl_number # SeaLead không có BKG no riêng
            booking_status = None # SeaLead không có trạng thái booking rõ ràng

            info_table = self.driver.find_element(By.CSS_SELECTOR, "table.route-table-bill")
            pol = info_table.find_element(By.XPATH, ".//th[contains(text(), 'Port of Loading')]/following-sibling::td").text.strip()
            pod = info_table.find_element(By.XPATH, ".//th[contains(text(), 'Port of Discharge')]/following-sibling::td").text.strip()
            logger.info(f"[SeaLead Scraper] -> BL: {bl_number}, POL: {pol}, POD: {pod}")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_basic_info_start)

            # === BƯỚC 3: TRÍCH XUẤT LỊCH TRÌNH VÀ THÔNG TIN TRUNG CHUYỂN ===
            t_schedule_start = time.time()
            schedule_tables = self.driver.find_elements(By.CSS_SELECTOR, "table.route-table")

            if not schedule_tables:
                logger.warning(f"[SeaLead Scraper] Không tìm thấy bảng lịch trình (route-table) cho mã: {tracking_number}")
                return None

            main_schedule_table = schedule_tables[0]
            rows = main_schedule_table.find_elements(By.CSS_SELECTOR, "tbody tr")

            if not rows:
                logger.warning(f"[SeaLead Scraper] Không tìm thấy chặng nào trong bảng lịch trình chính cho mã: {tracking_number}")
                return None

            logger.debug("-> Tìm thấy %d chặng trong bảng lịch trình chính.", len(rows))

            # Xử lý chặng đầu tiên
            first_leg_cells = rows[0].find_elements(By.TAG_NAME, "td")
            etd = first_leg_cells[5].text.strip() if len(first_leg_cells) > 5 else None
            # atd = None (Không có thông tin)

            # Xử lý chặng cuối
            last_leg_cells = rows[-1].find_elements(By.TAG_NAME, "td")
            eta = last_leg_cells[7].text.strip() if len(last_leg_cells) > 7 else None
            logger.debug("-> ETD (dự kiến): %s, ETA (dự kiến): %s", etd, eta)


            # Xử lý các chặng trung chuyển (Logic từ COSCO)
            future_etd_transits = []
            logger.info("[SeaLead Scraper] Bắt đầu xử lý thông tin transit...")

            for i in range(len(rows) - 1):
                current_leg_cells = rows[i].find_elements(By.TAG_NAME, "td")
                next_leg_cells = rows[i+1].find_elements(By.TAG_NAME, "td")

                # Cột [6] là "Destination Location", Cột [4] là "Origin location"
                current_pod = current_leg_cells[6].text.strip() if len(current_leg_cells) > 6 else None
                next_pol = next_leg_cells[4].text.strip() if len(next_leg_cells) > 4 else None

                if current_pod and next_pol and current_pod == next_pol:
                    logger.debug(f"[SeaLead Scraper] Tìm thấy cảng transit '{current_pod}' giữa chặng {i} và {i+1}")
                    if current_pod not in transit_port_list:
                         transit_port_list.append(current_pod)

                    # Sealead chỉ có (Estimated) Arrival Time (Cột [7])
                    temp_eta_transit = current_leg_cells[7].text.strip() if len(current_leg_cells) > 7 else None
                    if temp_eta_transit and not eta_transit: # Lấy ETA transit đầu tiên
                         eta_transit = temp_eta_transit
                         logger.debug(f"[SeaLead Scraper] Tìm thấy EtaTransit đầu tiên: {eta_transit}")
                    # temp_ata_transit = None (Không có)

                    # Sealead chỉ có (Estimated) Departure Time (Cột [5])
                    temp_etd_transit_str = next_leg_cells[5].text.strip() if len(next_leg_cells) > 5 else None
                    # temp_atd_transit = None (Không có)

                    if temp_etd_transit_str:
                        try:
                            # Parse ngày theo format của Sealead
                            etd_transit_date = datetime.strptime(temp_etd_transit_str, '%B %d, %Y').date()
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


            # === BƯỚC 4: TRÍCH XUẤT THỜI GIAN ĐẾN THỰC TẾ (Ata) TỪ CHI TIẾT CONTAINER ===
            t_container_detail_start = time.time()
            if len(schedule_tables) > 1:
                container_details_table = schedule_tables[1]
                try:
                    # Lấy Ata từ "Latest Move Time" (Cột [4]) của container đầu tiên
                    first_container_row = container_details_table.find_element(By.CSS_SELECTOR, "tbody tr")
                    container_cells = first_container_row.find_elements(By.TAG_NAME, "td")
                    ata = container_cells[4].text.strip() if len(container_cells) > 4 else None
                    logger.info(f"[SeaLead Scraper] Tìm thấy Ata (Latest Move Time) từ chi tiết container: {ata}")
                except NoSuchElementException:
                    logger.warning("[SeaLead Scraper] Không tìm thấy chi tiết container để lấy Ata.")
            else:
                logger.info("[SeaLead Scraper] Không có bảng chi tiết container, không thể lấy Ata.")
            logger.debug("-> (Thời gian) Trích xuất chi tiết container (Ata): %.2fs", time.time() - t_container_detail_start)


            # === BƯỚC 5: XÂY DỰNG ĐỐI TƯỢNG JSON (ĐẢM BẢO `or ""`) ===
            t_normalize_start = time.time()
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no.strip(),
                BlNumber= bl_number.strip(),
                BookingStatus= booking_status or "",
                Pol= pol.strip() or "",
                Pod= pod.strip() or "",
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "", # Sẽ là "" vì không có dữ liệu
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= ", ".join(transit_port_list) if transit_port_list else "",
                EtdTransit= self._format_date(etd_transit_final) or "",
                AtdTransit= self._format_date(atd_transit) or "", # Sẽ là "" vì không có dữ liệu
                EtaTransit= self._format_date(eta_transit) or "",
                AtaTransit= self._format_date(ata_transit) or "" # Sẽ là "" vì không có dữ liệu
            )

            logger.info("[SeaLead Scraper] Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("-> (Thời gian) Tổng thời gian trích xuất chi tiết: %.2fs", time.time() - t_extract_detail_start)

            return shipment_data

        except Exception as e:
            logger.error(f"[SeaLead Scraper] Lỗi trong quá trình trích xuất chi tiết cho mã '{tracking_number}': {e}", exc_info=True)
            return None