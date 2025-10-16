import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper

class TranslinerScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Transliner (theo yêu cầu)
    và chuẩn hóa kết quả theo template JSON.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD Mon YYYY' (ví dụ: 8 Sept 2025) sang 'DD/MM/YYYY'.
        """
        if not date_str or date_str.strip() == '-':
            return None
        try:
            # Định dạng của trang web là '%d %b %Y'
            dt_obj = datetime.strptime(date_str, '%d %b %Y')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            print(f"    [Transliner Scraper] Cảnh báo: Không thể phân tích định dạng ngày: {date_str}")
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scraping chính cho Transliner.
        """
        print(f"[Transliner Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            # URL được cung cấp nhận mã theo dõi trực tiếp
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30)

            # Chờ cho phần nội dung chính của kết quả được tải
            print("[Transliner Scraper] -> Chờ trang kết quả tải...")
            self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.m_1b7284a3.mantine-Paper-root"))
            )
            print("[Transliner Scraper] -> Trang kết quả đã tải. Bắt đầu trích xuất.")
            time.sleep(1) # Chờ một chút để đảm bảo mọi thứ ổn định

            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[Transliner Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả.
        """
        try:
            # Định vị khối chứa thông tin tóm tắt
            summary_paper = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.m_1b7284a3.mantine-Paper-root"))
            )

            # Helper function để lấy text an toàn
            def get_text_safe(element, selector):
                try:
                    return element.find_element(By.CSS_SELECTOR, selector).text.strip()
                except NoSuchElementException:
                    return None

            # Trích xuất dữ liệu
            bl_number = get_text_safe(summary_paper, "div.__m__-_r_33_ p:last-child") or tracking_number
            booking_no = get_text_safe(summary_paper, "div.__m__-_r_2k_ p:last-child") or tracking_number
            booking_status = get_text_safe(summary_paper, "div.__m__-_r_3a_ span.mantine-Badge-label")
            
            # Trích xuất ATD và ATA, các giá trị ETD/ETA không có
            pol_atd = get_text_safe(summary_paper, "div.__m__-_r_3f_ p:last-child")
            pod_ata = get_text_safe(summary_paper, "div.__m__-_r_3m_ p:last-child")
            
            # Làm sạch chuỗi ngày tháng
            atd = pol_atd.replace("POL ATD:", "").strip() if pol_atd else None
            ata = pod_ata.replace("POD ATA:", "").strip() if pod_ata else None

            # Xây dựng đối tượng JSON theo template
            shipment_data = {
                "BookingNo": booking_no,
                "BlNumber": bl_number,
                "BookingStatus": booking_status,
                "Pol": None,
                "Pod": None,
                "Etd": None,
                "Atd": self._format_date(atd),
                "Eta": None,
                "Ata": self._format_date(ata),
                "TransitPort": None,
                "EtdTransit": None,
                "AtdTrasit": None,
                "EtaTransit": None,
                "AtaTrasit": None
            }
            return shipment_data
            
        except Exception as e:
            print(f"    [Transliner Scraper] Lỗi trong quá trình trích xuất: {e}")
            traceback.print_exc()
            return None