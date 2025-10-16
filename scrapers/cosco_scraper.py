import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import re

from .base_scraper import BaseScraper

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
            # Lấy phần ngày, bỏ qua phần giờ và múi giờ
            date_part = date_str.split(' ')[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            print(f"    [COScO Scraper] Cảnh báo: Không thể phân tích định dạng ngày: {date_str}")
            return date_str

    def _extract_date_from_text(self, text, date_type):
        """
        Trích xuất ngày 'Actual' hoặc 'Expected' từ một chuỗi.
        Ví dụ chuỗi: 'Actual: 2025-09-26 07:02:27 CDT'
        """
        if not text or date_type not in text:
            return None
        
        # Sử dụng regex để tìm ngày tháng sau 'Actual:' hoặc 'Expected:'
        match = re.search(fr'{date_type}:\s*(\d{{4}}-\d{{2}}-\d{{2}}\s*\d{{2}}:\d{{2}}:\d{{2}})', text)
        if match:
            return match.group(1).strip()
        return None

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính. Thực hiện tìm kiếm và trả về dữ liệu đã chuẩn hóa.
        """
        print(f"[COSCO Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 45)

            # 1. Xử lý cookie nếu có
            try:
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".btnBlue.ivu-btn-primary"))
                )
                cookie_button.click()
                print("[COSCO Scraper] -> Đã chấp nhận cookies.")
            except TimeoutException:
                print("[COSCO Scraper] -> Banner cookie không xuất hiện.")

            # 2. Tìm kiếm
            # Chờ iframe chứa form tìm kiếm tải xong và chuyển vào đó
            iframe = self.wait.until(EC.presence_of_element_located((By.ID, "scctCargoTracking")))
            self.driver.switch_to.frame(iframe)
            print("[COSCO Scraper] -> Đã chuyển vào iframe tìm kiếm.")

            search_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.ant-input")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.driver.find_element(By.CSS_SELECTOR, "button.css-1tiubaq")
            search_button.click()
            print(f"[COSCO Scraper] -> Đang tìm kiếm mã: {tracking_number}")

            # 3. Chờ kết quả và trích xuất
            print("[COSCO Scraper] -> Chờ trang kết quả tải...")
            # Đợi một element đặc trưng của trang kết quả, ví dụ box thông tin booking
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div#rc-tabs-0-panel-ocean")))
            print("[COSCO Scraper] -> Trang kết quả đã tải.")

            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[COSCO Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/cosco_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception as ss_e:
                print(f"Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"
        finally:
            # Luôn chuyển về context mặc định sau khi xong việc
            self.driver.switch_to.default_content()

    def _extract_schedule_date(self, cell, date_type):
        """
        Helper mới: Trích xuất ngày từ cấu trúc table cell của trang kết quả mới.
        Tìm 'Expected：' hoặc 'Actual：' và lấy text của thẻ span kế tiếp.
        """
        try:
            # XPath để tìm span chứa text và lấy span anh em ngay sau nó
            date_str = cell.find_element(By.XPATH, f".//span[contains(text(), '{date_type}')]/following-sibling::span").text
            # Trả về None nếu không có dữ liệu thực tế
            if "Not yet" in date_str:
                return None
            return date_str.strip()
        except NoSuchElementException:
            # Trả về None nếu không tìm thấy element
            return None

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả của COSCO (Layout mới).
        """
        try:
            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT ===
            # Lấy Booking No từ sidebar vì đây là nơi hiển thị đáng tin cậy
            bkg_no_text = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.ct-side-bar span.side-bar-title span"))).text
            print("[COSCO Scraper] Đã tìm thấy booking number")
            booking_no = re.search(r'BKG#"(\d+)"', bkg_no_text).group(1) if bkg_no_text else tracking_number

            # B/L No thường giống Booking No nếu không được cung cấp riêng
            bl_number = booking_no
            
            booking_status = self.driver.find_element(By.CSS_SELECTOR, "div.booking-status").text
            print("[COSCO Scraper] Đã tìm thấy booking status")

            # === BƯỚC 2: LẤY THÔNG TIN TỪ BẢNG "SCHEDULE DETAIL" ===
            schedule_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.ant-table-content")))
            print("[COSCO Scraper] Đã tìm thấy schedule_table")
            rows = schedule_table.find_elements(By.CSS_SELECTOR, "tbody tr")

            if not rows:
                print("[COSCO Scraper] Cảnh báo: Không tìm thấy chặng nào trong Schedule Detail.")
                return None

            # Lấy POL từ chặng đầu tiên và POD từ chặng cuối cùng
            pol = rows[0].find_elements(By.TAG_NAME, "td")[2].text
            print("[COSCO Scraper] Đã tìm thấy pol")
            pod = rows[-1].find_elements(By.TAG_NAME, "td")[4].text
            print("[COSCO Scraper] Đã tìm thấy pod")
            
            # Khởi tạo tất cả các biến ngày tháng
            etd, atd, eta, ata = None, None, None, None
            etd_transit, atd_transit, transit_port, eta_transit, ata_transit = None, None, None, None, None

            # Xử lý chặng đầu tiên để lấy ETD và ATD
            first_leg_cells = rows[0].find_elements(By.TAG_NAME, "td")
            departure_cell = first_leg_cells[3]
            etd = self._extract_schedule_date(departure_cell, 'Expected')
            atd = self._extract_schedule_date(departure_cell, 'Actual')

            # Xử lý chặng cuối để lấy ETA và ATA
            last_leg_cells = rows[-1].find_elements(By.TAG_NAME, "td")
            arrival_cell = last_leg_cells[5]
            eta = self._extract_schedule_date(arrival_cell, 'Expected')
            ata = self._extract_schedule_date(arrival_cell, 'Actual')

            # Xử lý thông tin transit nếu có nhiều hơn một chặng
            if len(rows) > 1:
                # Cảng transit là POD của chặng đầu tiên
                transit_port = first_leg_cells[4].text
                
                # Ngày đến cảng transit (Arrival at transit) là ngày đến của chặng đầu tiên
                arrival_at_transit_cell = first_leg_cells[5]
                eta_transit = self._extract_schedule_date(arrival_at_transit_cell, 'Expected')
                ata_transit = self._extract_schedule_date(arrival_at_transit_cell, 'Actual')

                # Ngày đi từ cảng transit (Departure from transit) là ngày đi của chặng thứ hai
                second_leg_cells = rows[1].find_elements(By.TAG_NAME, "td")
                departure_from_transit_cell = second_leg_cells[3]
                etd_transit = self._extract_schedule_date(departure_from_transit_cell, 'Expected')
                atd_transit = self._extract_schedule_date(departure_from_transit_cell, 'Actual')

            # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            shipment_data = {
                "BookingNo": booking_no.strip(),
                "BlNumber": bl_number.strip(),
                "BookingStatus": booking_status.strip(),
                "Pol": pol.strip(),
                "Pod": pod.strip(),
                "Etd": self._format_date(etd),
                "Atd": self._format_date(atd),
                "Eta": self._format_date(eta),
                "Ata": self._format_date(ata),
                "TransitPort": transit_port.strip() if transit_port else None,
                "EtdTransit": self._format_date(etd_transit),
                "AtdTransit": self._format_date(atd_transit),
                "EtaTransit": self._format_date(eta_transit),
                "AtaTransit": self._format_date(ata_transit)
            }
            
            return shipment_data

        except Exception as e:
            print(f"    [COSCO Scraper] Lỗi trong quá trình trích xuất chi tiết: {e}")
            traceback.print_exc()
            return None