import logging
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from schemas import N8nTrackingInfo
import re

from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class CordeliaScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Cordelia Line và chuẩn hóa kết quả.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD/MM/YYYY HH:MM...' hoặc 'DD/MM/YYYY'
        sang 'DD/MM/YYYY'.
        Trả về "" nếu đầu vào không hợp lệ hoặc rỗng.
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Lấy phần ngày, bỏ qua phần giờ nếu có (ví dụ: "16/10/2025 20:10")
            date_part = date_str.split(' ')[0]
            # Parse ngày theo định dạng DD/MM/YYYY
            dt_obj = datetime.strptime(date_part, '%d/%m/%Y')
            # Trả về định dạng DD/MM/YYYY
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return "" # Trả về chuỗi rỗng nếu parse lỗi

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một số theo dõi nhất định từ trang web của Cordelia Line
        và trả về ở định dạng JSON chuẩn hóa.
        """
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
        try:
            url = f"{self.config['url']}{tracking_number}"
            self.driver.get(url)
            self.wait = WebDriverWait(self.driver, 30)

            # Chờ cho bảng kết quả được JavaScript tải xong
            # Trang web có một div với id="loader" sẽ biến mất khi tải xong
            logger.info("Chờ 'loader' biến mất...")
            self.wait.until(EC.invisibility_of_element_located((By.ID, "loader")))
            logger.info("'loader' đã biến mất.")
            
            # Sau đó, đợi bảng dữ liệu chính xuất hiện
            result_table = self.wait.until(
                EC.visibility_of_element_located((By.ID, "checkShedTable"))
            )
            logger.info("Cordelia: Trang kết quả đã được tải và bảng dữ liệu đã hiển thị.")

            # Trích xuất và chuẩn hóa dữ liệu sang định dạng JSON mong muốn
            normalized_data = self._extract_and_normalize_data(result_table, tracking_number)

            if not normalized_data:
                logger.warning("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/cordelia_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, table, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ bảng kết quả (cấu trúc 1 hàng) của Cordelia.
        """
        try:
            # Tìm hàng dữ liệu đầu tiên trong phần thân của bảng
            row = table.find_element(By.CSS_SELECTOR, "tbody tr")
            cells = row.find_elements(By.TAG_NAME, "td")

            if len(cells) < 9:
                logger.warning("Cordelia: Hàng dữ liệu có ít hơn 9 ô. Không thể parse cho mã: %s", tracking_number)
                return None

            # === BƯỚC 1: TRÍCH XUẤT DỮ LIỆU THÔ ===
            bl_number = cells[0].text.strip()
            pol = cells[1].text.strip()
            sob_date_str = cells[4].text.strip() # Shipped on Board Date
            pod = cells[5].text.strip()          # Final Port of Discharge
            eta_fpod_str = cells[6].text.strip() # ETA at Final Port
            current_status = cells[7].text.strip()
            current_location = cells[8].text.strip() # Đây có thể là tàu hoặc cảng
            
            logger.info("Trích xuất dữ liệu thô thành công cho BL: %s", bl_number)

            # === BƯỚC 2: XỬ LÝ LOGIC & CHUẨN HÓA ===
            
            # Logic Transit Port: Thử parse từ 'Current Status'
            # Ví dụ: "Loaded 2nd Leg INMICT" -> "INMICT"
            # Tìm từ cuối cùng trong chuỗi status mà là chữ IN HOA (thường là mã cảng)
            transit_port = ""
            if "leg" in current_status.lower():
                matches = re.findall(r'\b([A-Z]{5,})\b', current_status) # Tìm mã cảng (5+ chữ hoa)
                if matches:
                    transit_port = matches[-1] # Lấy mã cuối cùng tìm được
                    logger.info("Đã parse được Transit Port '%s' từ status '%s'", transit_port, current_status)
            
            # HTML của Cordelia không cung cấp lịch trình chi tiết từng chặng (multi-leg schedule).
            # Do đó, các trường transit date (Ata, Eta, Atd, Etd) không thể trích xuất.
            # Chúng sẽ được để là chuỗi rỗng theo yêu cầu.
            
            # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            shipment_data = N8nTrackingInfo(
                BookingNo= "",
                BlNumber= bl_number,
                BookingStatus= current_status,
                Pol= pol,
                Pod= pod,
                Etd= "", # Không có ETD, chỉ có ATD (SOB Date)
                Atd= self._format_date(sob_date_str), # SOB Date là Actual Departure
                Eta= self._format_date(eta_fpod_str), # ETA FPOD là Estimated Arrival
                Ata= "", # Không có ATA
                TransitPort= transit_port, # Đã parse ở trên
                EtdTransit= "", # Không có dữ liệu
                AtdTransit= "", # Không có dữ liệu
                EtaTransit= "", # Không có dữ liệu
                AtaTransit= ""  # Không có dữ liệu
            )
            
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công cho BL: %s", bl_number)
            return shipment_data

        except Exception as e:
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None