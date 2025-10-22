import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

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
        if not date_str:
            return None
        try:
            # Lấy phần ngày tháng, bỏ qua phần giờ
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            return None # Trả về None nếu định dạng không hợp lệ

    def _get_date_from_cell(self, cell):
        """
        Trích xuất ngày và xác định xem đó là ngày thực tế (màu đỏ) hay dự kiến.
        Trả về một tuple (date_string, is_actual).
        """
        try:
            # Ngày có màu thường nằm trong thẻ span
            span = cell.find_element(By.TAG_NAME, "span")
            date_str = span.text.strip()
            # Font màu đỏ chỉ ra ngày thực tế
            is_actual = "color: red" in span.get_attribute("style")
            return date_str, is_actual
        except NoSuchElementException:
            # Nếu không có thẻ span, đó là ngày dự kiến trong cột Schedule
            return cell.text.strip(), False

    def scrape(self, tracking_number):
        print(f"[SITC Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            print("[SITC Scraper] 1. Điều hướng đến URL...")
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            print("[SITC Scraper] 2. Điền thông tin vào form tìm kiếm...")
            search_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "form.search-form input[placeholder='B/L No.']"))
            )
            search_input.clear()
            search_input.send_keys(tracking_number)
            print(f"-> Đã điền mã: {tracking_number}")

            search_button = self.driver.find_element(By.CSS_SELECTOR, "form.search-form button")
            self.driver.execute_script("arguments[0].click();", search_button)
            print("-> Đã nhấn nút tìm kiếm.")

            print("[SITC Scraper] 3. Chờ trang kết quả tải...")
            self.wait.until(
                EC.visibility_of_element_located((By.XPATH, "//h4[text()='Basic Information']"))
            )
            print(f"-> Trang kết quả cho '{tracking_number}' đã tải xong.")
            time.sleep(2)

            print("[SITC Scraper] 4. Bắt đầu trích xuất và chuẩn hóa dữ liệu...")
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                print(f"[SITC Scraper] Lỗi: Không thể trích xuất dữ liệu cho '{tracking_number}'.")
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[SITC Scraper] 5. Trả về kết quả thành công.")
            # Trả về một dictionary duy nhất theo yêu cầu
            return normalized_data, None

        except TimeoutException:
            print(f"[SITC Scraper] Lỗi: TimeoutException xảy ra cho mã '{tracking_number}'.")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/sitc_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"  -> Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"  -> Lỗi: Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            print(f"[SITC Scraper] Lỗi: Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        print("[SITC Scraper] --- Bắt đầu _extract_and_normalize_data ---")
        try:
            # 1. Trích xuất thông tin cơ bản
            basic_info_table = self.wait.until(EC.presence_of_element_located((By.XPATH, "//h4[text()='Basic Information']/following-sibling::div[contains(@class, 'el-table')]")))
            bl_number = basic_info_table.find_element(By.CSS_SELECTOR, "tbody tr td:nth-child(1)").text.strip()
            pol = basic_info_table.find_element(By.CSS_SELECTOR, "tbody tr td:nth-child(2)").text.strip()
            pod = basic_info_table.find_element(By.CSS_SELECTOR, "tbody tr td:nth-child(3)").text.strip()

            # 2. Trích xuất lịch trình
            schedule_table = self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[h4[contains(text(), 'Sailing Schedule')]]/following-sibling::div[contains(@class, 'el-table')]")))
            rows = schedule_table.find_elements(By.CSS_SELECTOR, "tbody tr")

            if not rows:
                print("[SITC Scraper] Lỗi: Không tìm thấy chặng nào trong lịch trình.")
                return None

            # Khởi tạo các biến
            etd, atd, eta, ata = None, None, None, None
            etd_transit, atd_transit, transit_port, eta_transit, ata_transit = None, None, None, None, None

            # Xử lý chặng đầu tiên (POL)
            first_leg_cells = rows[0].find_elements(By.TAG_NAME, "td")
            etd = first_leg_cells[3].text.strip()
            etd_atd_val, etd_is_actual = self._get_date_from_cell(first_leg_cells[4])
            if etd_is_actual:
                atd = etd_atd_val

            # Xử lý chặng cuối cùng (POD)
            last_leg_cells = rows[-1].find_elements(By.TAG_NAME, "td")
            eta = last_leg_cells[6].text.strip()
            eta_ata_val, eta_is_actual = self._get_date_from_cell(last_leg_cells[7])
            if eta_is_actual:
                ata = eta_ata_val

            # Xử lý cảng trung chuyển (nếu có)
            if len(rows) > 1:
                # Chỉ lấy cảng trung chuyển đầu tiên để phù hợp với template
                transit_port = first_leg_cells[5].text.strip()
                
                # Arrival at Transit Port (cuối chặng 1)
                eta_transit = first_leg_cells[6].text.strip()
                ata_transit_val, ata_transit_is_actual = self._get_date_from_cell(first_leg_cells[7])
                if ata_transit_is_actual:
                    ata_transit = ata_transit_val

                # Departure from Transit Port (đầu chặng 2)
                second_leg_cells = rows[1].find_elements(By.TAG_NAME, "td")
                etd_transit = second_leg_cells[3].text.strip()
                atd_transit_val, atd_transit_is_actual = self._get_date_from_cell(second_leg_cells[4])
                if atd_transit_is_actual:
                    atd_transit = atd_transit_val

            # 3. Xây dựng đối tượng JSON
            #shipment_data = {
            #    "BookingNo": tracking_number, # Dùng tracking_number làm BookingNo vì không có dữ liệu riêng
            #    "BlNumber": bl_number,
            #    "BookingStatus": None, # Không có thông tin này
            #    "Pol": pol,
            #    "Pod": pod,
            #    "Etd": self._format_date(etd),
            #    "Atd": self._format_date(atd),
            #    "Eta": self._format_date(eta),
            #    "Ata": self._format_date(ata),
            #    "TransitPort": transit_port,
            #    "EtdTransit": self._format_date(etd_transit),
            #    "AtdTrasit": self._format_date(atd_transit),
            #    "EtaTransit": self._format_date(eta_transit),
            #    "AtaTrasit": self._format_date(ata_transit)
            #}
            
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= bl_number,
                BookingStatus= None,
                Pol= pol,
                Pod= pod,
                Etd= self._format_date(etd),
                Atd= self._format_date(atd),
                Eta= self._format_date(eta),
                Ata= self._format_date(ata),
                TransitPort= transit_port,
                EtdTransit= self._format_date(etd_transit),
                AtdTransit= self._format_date(atd_transit),
                EtaTransit= self._format_date(eta_transit),
                AtaTransit= self._format_date(ata_transit)
            )
            
            print("[SITC Scraper] --- Hoàn tất _extract_and_normalize_data ---")
            return shipment_data

        except Exception as e:
            print(f"[SITC Scraper] Lỗi trong quá trình trích xuất dữ liệu: {e}")
            traceback.print_exc()
            return None