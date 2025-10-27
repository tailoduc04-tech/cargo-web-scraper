import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time
import re

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Khởi tạo logger cho module này
logger = logging.getLogger(__name__)

class TranslinerScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Transliner
    và chuẩn hóa kết quả theo template JSON.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD Mon YYYY' (ví dụ: 8 Sept 2025) sang 'DD/MM/YYYY'.
        Trả về None nếu đầu vào không hợp lệ.
        """
        if not date_str or not isinstance(date_str, str) or date_str.strip() == '-':
            return None
        try:
            # Định dạng của trang web là '%d %b %Y'
            dt_obj = datetime.strptime(date_str.strip(), '%d %b %Y')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return None # Trả về None để logic gọi hàm xử lý (thường là `or ""`)

    def scrape(self, tracking_number):
        """
        Phương thức scraping chính cho Transliner.
        Thực hiện tải trang, chờ đợi và gọi hàm trích xuất.
        """
        logger.info("Bắt đầu scrape Transliner cho mã: %s", tracking_number)
        try:
            # Transliner hỗ trợ URL trực tiếp với mã tracking
            direct_url = f"{self.config['url']}{tracking_number}"
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 30) # Chờ tối đa 30 giây

            # Chờ cho phần nội dung chính (khối tóm tắt) được tải
            logger.info("-> Chờ trang kết quả tải...")
            self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.m_1b7284a3.mantine-Paper-root"))
            )
            logger.info("-> Trang kết quả đã tải. Bắt đầu trích xuất...")

            # Gọi hàm trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                # _extract_and_normalize_data đã tự log lỗi
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("-> Hoàn tất scrape Transliner thành công cho mã: %s.", tracking_number)
            return normalized_data, None

        except TimeoutException:
            logger.warning("Không tìm thấy kết quả cho '%s' (Timeout) trên Transliner.", tracking_number)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            # Ghi lại lỗi không mong muốn
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape Transliner cho '%s': %s", 
                         tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả.
        """
        try:
            # Định vị khối tóm tắt chính chứa tất cả thông tin
            summary_paper = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.m_1b7284a3.mantine-Paper-root"))
            )

            # Helper function (định nghĩa nội bộ) để lấy text an toàn
            def get_text_safe(element, selector):
                """Lấy text, trả về None nếu không tìm thấy."""
                try:
                    return element.find_element(By.CSS_SELECTOR, selector).text.strip()
                except NoSuchElementException:
                    logger.warning("Không tìm thấy selector: %s", selector)
                    return None

            # === BƯỚC 1: TRÍCH XUẤT DỮ LIỆU TÓM TẮT ===
            
            # BookingNo và BlNumber
            bl_number_raw = get_text_safe(summary_paper, "div.__m__-_r_33_ p:last-child")
            bl_number = bl_number_raw or tracking_number # Dùng tracking_number nếu không tìm thấy
            logger.info("Đã tìm thấy BlNumber: %s", bl_number)

            booking_no_raw = get_text_safe(summary_paper, "div.__m__-_r_2k_ p:last-child")
            booking_no = booking_no_raw or tracking_number # Dùng tracking_number nếu không tìm thấy
            logger.info("Đã tìm thấy BookingNo: %s", booking_no)
            
            # BookingStatus
            booking_status = get_text_safe(summary_paper, "div.__m__-_r_3a_ span.mantine-Badge-label")
            logger.info("Đã tìm thấy BookingStatus: %s", booking_status)
            
            # POL và POD (Không có trên trang này)
            pol_route = get_text_safe(summary_paper, "div.__m__-_r_2p_ p:last-child")
            pod_route = get_text_safe(summary_paper, "div.__m__-_r_2u_ p:last-child")
            
            pol = pol_route or ""
            pod = pod_route or ""
            if not pol:
                logger.info("Không tìm thấy thông tin POL (như dự kiến).")
            if not pod:
                logger.info("Không tìm thấy thông tin POD (như dự kiến).")

            # ETD / ATD (từ cảng đi)
            pol_etd_raw = get_text_safe(summary_paper, "div.__m__-_r_3f_ p:first-child")
            pol_atd_raw = get_text_safe(summary_paper, "div.__m__-_r_3f_ p:last-child")
            
            # Làm sạch tiền tố "POL ETD:" và "POL ATD:"
            etd_str = pol_etd_raw.replace("POL ETD:", "").strip() if pol_etd_raw else None
            atd_str = pol_atd_raw.replace("POL ATD:", "").strip() if pol_atd_raw else None
            
            logger.info("Trích xuất được ETD string: %s", etd_str)
            logger.info("Trích xuất được ATD string: %s", atd_str)

            # ETA / ATA (tới cảng đích)
            pod_eta_raw = get_text_safe(summary_paper, "div.__m__-_r_3m_ p:first-child")
            pod_ata_raw = get_text_safe(summary_paper, "div.__m__-_r_3m_ p:last-child")

            # Làm sạch tiền tố "POD ETA:" và "POD ATA:"
            eta_str = pod_eta_raw.replace("POD ETA:", "").strip() if pod_eta_raw else None
            ata_str = pod_ata_raw.replace("POD ATA:", "").strip() if pod_ata_raw else None
            
            logger.info("Trích xuất được ETA string: %s", eta_str)
            logger.info("Trích xuất được ATA string: %s", ata_str)

            # === BƯỚC 2: XỬ LÝ TRANSIT ===
            logger.info("Không tìm thấy dữ liệu transit chi tiết. Các trường transit sẽ để trống.")
            transit_port = ""
            etd_transit = ""
            atd_transit = ""
            eta_transit = ""
            ata_transit = ""
            
            # === BƯỚC 3: CHUẨN HÓA VÀ TRẢ VỀ ===
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no,
                BlNumber= bl_number,
                BookingStatus= booking_status or "",
                Pol= pol, # Đã là ""
                Pod= pod, # Đã là ""
                Etd= self._format_date(etd_str) or "",
                Atd= self._format_date(atd_str) or "",
                Eta= self._format_date(eta_str) or "",
                Ata= self._format_date(ata_str) or "",
                TransitPort= transit_port,  # ""
                EtdTransit= etd_transit,   # ""
                AtdTransit= atd_transit,   # ""
                EtaTransit= eta_transit,   # ""
                AtaTransit= ata_transit    # ""
            )
            
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công cho mã: %s", tracking_number)
            return shipment_data
            
        except Exception as e:
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho '%s': %s", 
                         tracking_number, e, exc_info=True)
            return None