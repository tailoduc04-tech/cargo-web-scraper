import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class SitcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang SITC và chuẩn hóa kết quả
    theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD ...' sang 'DD/MM/YYYY'.
        Nếu không chuyển được, trả về None.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Lấy phần ngày tháng, bỏ qua phần giờ nếu có
            date_part = date_str.split(" ")[0]
            if not date_part:
                return None
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return None # Trả về None, sẽ được chuẩn hóa thành "" sau

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính. Thực hiện tìm kiếm và trả về dữ liệu đã chuẩn hóa.
        """
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
        try:
            logger.info("1. Điều hướng đến URL: %s", self.config['url'])
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30) # Chờ tối đa 30s

            logger.info("2. Điền thông tin vào form tìm kiếm...")
            # Selector này nhắm vào input thứ 2 trong form, sau dropdown
            search_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "form.search-form div.el-col:nth-child(2) input.el-input__inner"))
            )
            search_input.clear()
            search_input.send_keys(tracking_number)
            logger.info("-> Đã điền mã: %s", tracking_number)

            search_button = self.driver.find_element(By.CSS_SELECTOR, "form.search-form button.btn-primary")
            # Dùng execute_script để click an toàn hơn
            self.driver.execute_script("arguments[0].click();", search_button)
            logger.info("-> Đã nhấn nút tìm kiếm.")

            logger.info("3. Chờ trang kết quả tải...")
            # Chờ cho đến khi thấy tiêu đề "Basic Information"
            self.wait.until(
                EC.visibility_of_element_located((By.XPATH, "//h4[text()='Basic Information']"))
            )
            logger.info("-> Trang kết quả cho '%s' đã tải xong.", tracking_number)
            # Chờ thêm 2s để đảm bảo mọi thứ render xong (giữ lại từ code cũ)
            time.sleep(2)

            logger.info("4. Bắt đầu trích xuất và chuẩn hóa dữ liệu...")
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                logger.error("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("5. Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/sitc_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"


    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả của SITC.
        Áp dụng logic transit tương tự COSCO.
        """
        logger.info("--- Bắt đầu _extract_and_normalize_data ---")
        try:
            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT ===

            # 1.1. Lấy B/L No từ bảng Basic Info
            basic_info_table = self.wait.until(EC.presence_of_element_located((By.XPATH, "//h4[text()='Basic Information']/following-sibling::div[contains(@class, 'el-table')]")))
            bl_number = basic_info_table.find_element(By.CSS_SELECTOR, "tbody tr td:nth-child(1)").text.strip()
            # Giả định BookingNo giống BlNumber vì không có trường riêng
            booking_no = bl_number
            logger.info("Đã tìm thấy BlNumber: %s", bl_number)

            # 1.2. Lấy BookingStatus từ bảng Container (lấy status của cont đầu tiên)
            booking_status = ""
            try:
                status_xpath = "//h4[text()='Containers Information']/following-sibling::div[contains(@class, 'el-table')]//tbody/tr[1]/td[9]//span"
                booking_status = self.driver.find_element(By.XPATH, status_xpath).text.strip()
                logger.info("Đã tìm thấy BookingStatus: %s", booking_status)
            except (NoSuchElementException, TimeoutException):
                logger.warning("Không tìm thấy BookingStatus. Sẽ để trống.")

            # === BƯỚC 2: LẤY THÔNG TIN TỪ BẢNG "SAILING SCHEDULE" ===
            schedule_table = self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[h4[contains(text(), 'Sailing Schedule')]]/following-sibling::div[contains(@class, 'el-table')]")))
            logger.info("Đã tìm thấy schedule_table")
            rows = schedule_table.find_elements(By.CSS_SELECTOR, "tbody tr")

            if not rows:
                logger.warning("Không tìm thấy chặng nào trong Schedule Detail cho mã: %s", tracking_number)
                return None

            # Lấy POL từ chặng đầu tiên và POD từ chặng cuối cùng
            pol = rows[0].find_elements(By.TAG_NAME, "td")[2].text.strip()
            logger.info("Đã tìm thấy POL: %s", pol)
            pod = rows[-1].find_elements(By.TAG_NAME, "td")[5].text.strip()
            logger.info("Đã tìm thấy POD: %s", pod)

            etd, atd, eta, ata = None, None, None, None
            etd_transit_final, atd_transit, transit_port_list, eta_transit, ata_transit = None, None, [], None, None

            # Xử lý chặng đầu tiên
            first_leg_cells = rows[0].find_elements(By.TAG_NAME, "td")
            etd = first_leg_cells[3].text.strip() # Schedule ETD
            atd = first_leg_cells[4].text.strip() # ETD/ATD (Actual)

            # Xử lý chặng cuối
            last_leg_cells = rows[-1].find_elements(By.TAG_NAME, "td")
            eta = last_leg_cells[6].text.strip() # Schedule ETA
            ata = last_leg_cells[7].text.strip() # ETA/ATA (Actual)
            
            # Xử lý các chặng trung chuyển (Áp dụng logic COSCO)
            future_etd_transits = []
            today = date.today()
            logger.info("Bắt đầu xử lý thông tin transit...")

            for i in range(len(rows) - 1):
                current_leg_cells = rows[i].find_elements(By.TAG_NAME, "td")
                next_leg_cells = rows[i+1].find_elements(By.TAG_NAME, "td")

                # Cột [5] là POD của chặng hiện tại
                current_pod = current_leg_cells[5].text.strip()
                # Cột [2] là POL của chặng kế tiếp
                next_pol = next_leg_cells[2].text.strip()

                # Nếu POD chặng này == POL chặng sau -> đây là cảng transit
                if current_pod == next_pol and current_pod:
                    logger.debug("Tìm thấy cảng transit '%s' giữa chặng %d và %d", current_pod, i, i+1)
                    if current_pod not in transit_port_list:
                         transit_port_list.append(current_pod)

                    # Lấy thông tin đến cảng transit (từ chặng hiện tại)
                    temp_eta_transit = current_leg_cells[6].text.strip() # Schedule ETA
                    temp_ata_transit = current_leg_cells[7].text.strip() # ETA/ATA
                    
                    # Lấy AtaTransit/EtaTransit *đầu tiên* tìm thấy
                    if temp_ata_transit and not ata_transit:
                         ata_transit = temp_ata_transit
                         logger.debug("Tìm thấy AtaTransit đầu tiên: %s", ata_transit)
                    elif temp_eta_transit and not ata_transit and not eta_transit:
                         eta_transit = temp_eta_transit
                         logger.debug("Tìm thấy EtaTransit đầu tiên: %s", eta_transit)

                    # Lấy thông tin rời cảng transit (từ chặng kế tiếp)
                    temp_etd_transit_str = next_leg_cells[3].text.strip() # Schedule ETD
                    temp_atd_transit = next_leg_cells[4].text.strip() # ETD/ATD

                    # Lấy AtdTransit *cuối cùng* (mới nhất)
                    if temp_atd_transit:
                         atd_transit = temp_atd_transit
                         logger.debug("Cập nhật AtdTransit cuối cùng: %s", atd_transit)

                    # Thu thập các EtdTransit trong tương lai
                    if temp_etd_transit_str:
                        try:
                            etd_transit_date = datetime.strptime(temp_etd_transit_str.split(' ')[0], '%Y-%m-%d').date()
                            if etd_transit_date > today:
                                future_etd_transits.append((etd_transit_date, current_pod, temp_etd_transit_str))
                                logger.debug("Thêm ETD transit trong tương lai: %s (%s)", temp_etd_transit_str, current_pod)
                        except (ValueError, IndexError):
                            logger.warning("Không thể parse ETD transit: %s", temp_etd_transit_str)

            # Sắp xếp và chọn EtdTransit gần nhất
            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit_final = future_etd_transits[0][2]
                logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
            else:
                 logger.info("Không tìm thấy ETD transit nào trong tương lai.")

            # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            # Đảm bảo mọi giá trị None đều trở thành ""
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
            logger.info("--- Hoàn tất _extract_and_normalize_data ---")
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None