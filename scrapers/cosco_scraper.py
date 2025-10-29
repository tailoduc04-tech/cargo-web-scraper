import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class CoscoScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang COSCO Shipping Lines
    và chuẩn hóa kết quả theo template JSON.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD HH:MM:SS...' sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            date_part = date_str.split(' ')[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return date_str

    def _extract_date_from_text(self, text, date_type):
        """
        Trích xuất ngày 'Actual' hoặc 'Expected' từ một chuỗi.
        """
        if not text or date_type not in text:
            return None
        match = re.search(fr'{date_type}:\s*(\d{{4}}-\d{{2}}-\d{{2}}\s*\d{{2}}:\d{{2}}:\d{{2}})', text)
        if match:
            return match.group(1).strip()
        return None

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính. Thực hiện tìm kiếm và trả về dữ liệu đã chuẩn hóa.
        """
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()
        
        try:
            t_nav_start = time.time()
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 45)
            logger.info("-> (Thời gian) Tải trang: %.2fs", time.time() - t_nav_start)

            # 1. Xử lý cookie nếu có
            t_cookie_start = time.time()
            try:
                cookie_button = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".btnBlue.ivu-btn-primary"))
                )
                cookie_button.click()
                logger.info("Đã chấp nhận cookies. (Thời gian xử lý cookie: %.2fs)", time.time() - t_cookie_start)
            except TimeoutException:
                logger.info("Banner cookie không xuất hiện hoặc đã được chấp nhận. (Thời gian kiểm tra: %.2fs)", time.time() - t_cookie_start)

            # 2. Tìm kiếm
            t_search_start = time.time()
            iframe = self.wait.until(EC.presence_of_element_located((By.ID, "scctCargoTracking")))
            self.driver.switch_to.frame(iframe)
            logger.info("Đã chuyển vào iframe tìm kiếm. (Thời gian tìm iframe: %.2fs)", time.time() - t_search_start)

            search_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.ant-input")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.driver.find_element(By.CSS_SELECTOR, "button.css-1tiubaq")
            search_button.click()
            logger.info("Đang tìm kiếm mã: %s. (Thời gian tìm kiếm: %.2fs)", tracking_number, time.time() - t_search_start)

            # 3. Chờ kết quả và trích xuất
            logger.info("Chờ trang kết quả tải...")
            t_wait_result_start = time.time()
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div#rc-tabs-0-panel-ocean")))
            logger.info("Trang kết quả đã tải. (Thời gian chờ kết quả: %.2fs)", time.time() - t_wait_result_start)

            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(tracking_number)
            logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("Hoàn tất scrape thành công cho mã: %s (Tổng thời gian: %.2fs)", 
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/cosco_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s (Tổng thời gian: %.2fs)", 
                             tracking_number, screenshot_path, t_total_fail - t_total_start)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s (Tổng thời gian: %.2fs)", 
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"
        finally:
            try:
                 self.driver.switch_to.default_content()
            except Exception as switch_err:
                 logger.error("Lỗi khi chuyển về default content: %s", switch_err)


    def _extract_schedule_date(self, cell, date_type):
        """
        Helper mới: Trích xuất ngày từ cấu trúc table cell của trang kết quả mới.
        """
        try:
            xpath_selector = f".//span[contains(text(), '{date_type}')]/following-sibling::span"
            date_str = cell.find_element(By.XPATH, xpath_selector).text
            if "Not yet" in date_str:
                return None
            return date_str.strip()
        except NoSuchElementException:
            return None

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả của COSCO (Layout mới).
        Đã cập nhật logic tìm EtdTransit gần nhất > hôm nay.
        """
        try:
            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT ===
            bkg_no_text = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.ct-side-bar span.side-bar-title span"))).text
            logger.info("Đã tìm thấy booking number text: %s", bkg_no_text)
            booking_no = re.search(r'BKG#"(\d+)"', bkg_no_text).group(1) if bkg_no_text else tracking_number
            bl_number = booking_no

            booking_status = self.driver.find_element(By.CSS_SELECTOR, "div.booking-status").text
            logger.info("Đã tìm thấy booking status: %s", booking_status)

            # === BƯỚC 2: LẤY THÔNG TIN TỪ BẢNG "SCHEDULE DETAIL" ===
            schedule_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.ant-table-content")))
            logger.info("Đã tìm thấy schedule_table")
            rows = schedule_table.find_elements(By.CSS_SELECTOR, "tbody tr")

            if not rows:
                logger.warning("Không tìm thấy chặng nào trong Schedule Detail cho mã: %s", tracking_number)
                return None

            pol = rows[0].find_elements(By.TAG_NAME, "td")[2].text
            logger.info("Đã tìm thấy POL: %s", pol)
            pod = rows[-1].find_elements(By.TAG_NAME, "td")[4].text
            logger.info("Đã tìm thấy POD: %s", pod)

            etd, atd, eta, ata = None, None, None, None
            etd_transit_final, atd_transit, transit_port_list, eta_transit, ata_transit = None, None, [], None, None

            # Xử lý chặng đầu tiên
            first_leg_cells = rows[0].find_elements(By.TAG_NAME, "td")
            departure_cell = first_leg_cells[3]
            etd = self._extract_schedule_date(departure_cell, 'Expected')
            atd = self._extract_schedule_date(departure_cell, 'Actual')

            # Xử lý chặng cuối
            last_leg_cells = rows[-1].find_elements(By.TAG_NAME, "td")
            arrival_cell = last_leg_cells[5]
            eta = self._extract_schedule_date(arrival_cell, 'Expected')
            ata = self._extract_schedule_date(arrival_cell, 'Actual')
            
            # Xử lý các chặng trung chuyển
            future_etd_transits = []
            today = date.today()
            logger.info("Bắt đầu xử lý thông tin transit...")

            for i in range(len(rows) - 1):
                current_leg_cells = rows[i].find_elements(By.TAG_NAME, "td")
                next_leg_cells = rows[i+1].find_elements(By.TAG_NAME, "td")

                current_pod = current_leg_cells[4].text.strip()
                next_pol = next_leg_cells[2].text.strip()

                if current_pod == next_pol and current_pod:
                    logger.debug("Tìm thấy cảng transit '%s' giữa chặng %d và %d", current_pod, i, i+1)
                    if current_pod not in transit_port_list:
                         transit_port_list.append(current_pod)

                    arrival_at_transit_cell = current_leg_cells[5]
                    temp_eta_transit = self._extract_schedule_date(arrival_at_transit_cell, 'Expected')
                    temp_ata_transit = self._extract_schedule_date(arrival_at_transit_cell, 'Actual')
                    if temp_ata_transit and not ata_transit:
                         ata_transit = temp_ata_transit
                         logger.debug("Tìm thấy AtaTransit đầu tiên: %s", ata_transit)
                    elif temp_eta_transit and not ata_transit and not eta_transit:
                         eta_transit = temp_eta_transit
                         logger.debug("Tìm thấy EtaTransit đầu tiên: %s", eta_transit)

                    departure_from_transit_cell = next_leg_cells[3]
                    temp_etd_transit_str = self._extract_schedule_date(departure_from_transit_cell, 'Expected')
                    temp_atd_transit = self._extract_schedule_date(departure_from_transit_cell, 'Actual')
                    if temp_atd_transit:
                         atd_transit = temp_atd_transit
                         logger.debug("Cập nhật AtdTransit cuối cùng: %s", atd_transit)

                    if temp_etd_transit_str:
                        try:
                            etd_transit_date = datetime.strptime(temp_etd_transit_str.split(' ')[0], '%Y-%m-%d').date()
                            if etd_transit_date > today:
                                future_etd_transits.append((etd_transit_date, current_pod, temp_etd_transit_str))
                                logger.debug("Thêm ETD transit trong tương lai: %s (%s)", temp_etd_transit_str, current_pod)
                        except (ValueError, IndexError):
                            logger.warning("Không thể parse ETD transit: %s", temp_etd_transit_str)

            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit_final = future_etd_transits[0][2]
                logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
            else:
                 logger.info("Không tìm thấy ETD transit nào trong tương lai.")

            # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no.strip(),
                BlNumber= bl_number.strip(),
                BookingStatus= booking_status.strip(),
                Pol= pol.strip(),
                Pod= pod.strip(),
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
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None