import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper

class PanScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Pan Continental Shipping
    và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY/MM/DD HH:mm' sang 'DD/MM/YYYY'.
        Hàm cũng xử lý các giá trị 'null' hoặc trống.
        """
        if not date_str or date_str.lower() == 'null':
            return None
        try:
            # Lấy phần ngày, bỏ qua phần giờ
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y/%m/%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho Pan Continental.
        """
        print(f"[PanCont Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            self.driver.get(self.config['url'].format(BL_NUMBER = tracking_number))
            self.wait = WebDriverWait(self.driver, 30)

            # --- 2. Chờ và chuyển vào iframe chứa kết quả ---
            print("[PanCont Scraper] -> Đang chờ iframe kết quả tải...")
            # Đợi cho iframe xuất hiện và chuyển vào đó
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
            self.driver.switch_to.frame(self.driver.find_element(By.TAG_NAME, "iframe"))
            
            # --- 3. Chờ kết quả trong iframe và trích xuất ---
            self.wait.until(EC.visibility_of_element_located((By.ID, "bl_no")))
            print("[PanCont Scraper] -> Trang kết quả đã tải. Bắt đầu trích xuất.")
            
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[PanCont Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/pancont_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception:
                pass
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"
            
    def _get_text_by_id(self, element_id):
        """Hàm trợ giúp lấy text từ element bằng ID, trả về None nếu không tìm thấy."""
        try:
            return self.driver.find_element(By.ID, element_id).text.strip()
        except NoSuchElementException:
            return None

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ trang kết quả và ánh xạ vào template JSON.
        """
        try:
            bl_number = self._get_text_by_id("bl_no")
            booking_no = self._get_text_by_id("bkg_no")
            pol = self._get_text_by_id("pol")
            pod = self._get_text_by_id("pod")

            # --- Mặc định cho chặng trực tiếp ---
            atd = self._get_text_by_id("pol_etd_1")
            ata = self._get_text_by_id("pod_eta_1")
            
            transit_port, ata_transit, atd_transit = None, None, None

            # --- Kiểm tra thông tin trung chuyển ---
            # Nếu có thông tin về tàu thứ 2, lô hàng có trung chuyển
            vsl_2 = self._get_text_by_id("vsl_2")
            if vsl_2 and vsl_2.lower() != 'null':
                # Cảng trung chuyển là cảng dỡ của chặng 1
                transit_port = self._get_text_by_id("pod_1")
                # Thời gian đến cảng trung chuyển (AtaTrasit) là ngày đến của chặng 1
                ata_transit = self._get_text_by_id("pod_eta_1")
                # Thời gian rời cảng trung chuyển (AtdTrasit) là ngày đi của chặng 2
                atd_transit = self._get_text_by_id("pol_etd_2")
                # Thời gian đến cuối cùng (Ata) bây giờ là của chặng 2
                ata = self._get_text_by_id("pod_eta_2")

            shipment_data = {
                "BookingNo": booking_no or tracking_number,
                "BlNumber": bl_number or tracking_number,
                "BookingStatus": None,
                "Pol": pol,
                "Pod": pod,
                "Etd": None,
                "Atd": self._format_date(atd),
                "Eta": None,
                "Ata": self._format_date(ata),
                "TransitPort": transit_port,
                "EtdTransit": None,
                "AtdTrasit": self._format_date(atd_transit),
                "EtaTransit": None,
                "AtaTrasit": self._format_date(ata_transit),
            }
            return shipment_data

        except Exception as e:
            print(f"[PanCont Scraper] -> Lỗi trong quá trình trích xuất: {e}")
            traceback.print_exc()
            return None