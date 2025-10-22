import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Bắt đầu file với logger
logger = logging.getLogger(__name__)

class PanScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Pan Continental Shipping
    và chuẩn hóa kết quả theo template JSON yêu cầu.
    
    Sử dụng logging và logic xử lý transit/ngày tháng (Actual/Expected)
    tương tự như CoscoScraper.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY/MM/DD HH:mm' (hoặc các biến thể)
        sang 'DD/MM/YYYY'.
        """
        if not date_str or date_str.lower() == 'null':
            return None
        try:
            # Lấy phần ngày, bỏ qua phần giờ
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y/%m/%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s. Trả về chuỗi gốc.", date_str)
            return date_str # Trả về chuỗi gốc nếu không parse được

    def _parse_date_obj(self, date_str):
        """
        Chuyển đổi chuỗi ngày 'YYYY/MM/DD ...' sang đối tượng date
        để so sánh. Trả về None nếu lỗi.
        """
        if not date_str or date_str.lower() == 'null':
            return None
        try:
            date_part = date_str.split(" ")[0]
            return datetime.strptime(date_part, '%Y/%m/%d').date()
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích chuỗi ngày sang object: %s", date_str)
            return None

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho Pan Continental.
        """
        logger.info(f"[PanCont Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            self.driver.get(self.config['url'].format(BL_NUMBER = tracking_number))
            self.wait = WebDriverWait(self.driver, 30)

            # --- 2. Chờ và chuyển vào iframe chứa kết quả ---
            logger.info("[PanCont Scraper] -> Đang chờ iframe kết quả tải...")
            # Đợi cho iframe xuất hiện và chuyển vào đó
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
            self.driver.switch_to.frame(self.driver.find_element(By.TAG_NAME, "iframe"))
            
            # --- 3. Chờ kết quả trong iframe và trích xuất ---
            self.wait.until(EC.visibility_of_element_located((By.ID, "bl_no")))
            logger.info("[PanCont Scraper] -> Trang kết quả đã tải. Bắt đầu trích xuất.")
            
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                # Lỗi này đã được log bên trong _extract_and_normalize_data
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("[PanCont Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/pancont_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning(f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout). Đã lưu ảnh chụp màn hình: {screenshot_path}")
            except Exception as ss_e:
                logger.error(f"Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '{tracking_number}': {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error(f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}", exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"
        finally:
            # Luôn chuyển về context mặc định
            try:
                 self.driver.switch_to.default_content()
                 logger.info("[PanCont Scraper] -> Đã chuyển về default content.")
            except Exception as switch_err:
                 logger.error(f"[PanCont Scraper] -> Lỗi khi chuyển về default content: {switch_err}")
            
    def _get_text_by_id(self, element_id):
        """Hàm trợ giúp lấy text từ element bằng ID, trả về None nếu không tìm thấy."""
        try:
            return self.driver.find_element(By.ID, element_id).text.strip()
        except NoSuchElementException:
            logger.warning(f"Không tìm thấy element với ID: {element_id}")
            return None

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ trang kết quả và ánh xạ vào template JSON.
        Áp dụng logic Actual/Expected dựa trên ngày hiện tại và xử lý transit
        nhiều chặng.
        """
        try:
            bl_number = self._get_text_by_id("bl_no")
            booking_no = self._get_text_by_id("bkg_no")
            pol = self._get_text_by_id("pol")
            pod = self._get_text_by_id("pod")
            
            # --- Thu thập thông tin các chặng ---
            # Trang PanCont có cấu trúc phẳng (leg_1, leg_2, leg_3)
            # Chúng ta sẽ thu thập chúng vào một danh sách
            
            legs_data = [
                {
                    'vsl': self._get_text_by_id("vsl_1"),
                    'pol': self._get_text_by_id("pol_1"),
                    'etd': self._get_text_by_id("pol_etd_1"),
                    'pod': self._get_text_by_id("pod_1"),
                    'eta': self._get_text_by_id("pod_eta_1")
                },
                {
                    'vsl': self._get_text_by_id("vsl_2"),
                    'pol': self._get_text_by_id("pol_2"),
                    'etd': self._get_text_by_id("pol_etd_2"),
                    'pod': self._get_text_by_id("pod_2"),
                    'eta': self._get_text_by_id("pod_eta_2")
                },
                {
                    'vsl': self._get_text_by_id("vsl_3"),
                    'pol': self._get_text_by_id("pol_3"),
                    'etd': self._get_text_by_id("pol_etd_3"),
                    'pod': self._get_text_by_id("pod_3"),
                    'eta': self._get_text_by_id("pod_eta_3")
                }
            ]
            
            # Lọc ra các chặng hợp lệ (có thông tin tàu và không phải 'null')
            valid_legs = [leg for leg in legs_data if leg.get('vsl') and leg['vsl'].lower() != 'null']
            
            if not valid_legs:
                logger.warning(f"Không tìm thấy chặng tàu hợp lệ nào cho mã: {tracking_number}")
                # Vẫn trả về thông tin cơ bản nếu có
                return N8nTrackingInfo(
                    BookingNo= booking_no or tracking_number,
                    BlNumber= bl_number or tracking_number,
                    BookingStatus= "",
                    Pol= pol or "",
                    Pod= pod or "",
                    **{k: "" for k in N8nTrackingInfo.__fields__ if k not in ['BookingNo', 'BlNumber', 'BookingStatus', 'Pol', 'Pod']}
                )

            logger.info(f"Tìm thấy {len(valid_legs)} chặng tàu hợp lệ.")

            # --- Khởi tạo biến ---
            etd, atd, eta, ata = None, None, None, None
            transit_port_list = []
            eta_transit, ata_transit = None, None
            etd_transit, atd_transit = None, None
            future_etd_transits = [] # (date_obj, port_name, date_str)
            today = date.today()

            # --- Xử lý chặng đầu (ETD/ATD) ---
            first_leg = valid_legs[0]
            etd_str = first_leg.get('etd')
            etd_date = self._parse_date_obj(etd_str)
            if etd_date and etd_date <= today:
                atd = etd_str # Đã xảy ra -> là Actual
            else:
                etd = etd_str # Chưa xảy ra -> là Expected

            # --- Xử lý chặng cuối (ETA/ATA) ---
            last_leg = valid_legs[-1]
            eta_str = last_leg.get('eta')
            eta_date = self._parse_date_obj(eta_str)
            if eta_date and eta_date <= today:
                ata = eta_str # Đã xảy ra -> là Actual
            else:
                eta = eta_str # Chưa xảy ra -> là Expected

            # --- Xử lý các chặng trung chuyển ---
            logger.info("Bắt đầu xử lý thông tin transit...")
            for i in range(len(valid_legs) - 1):
                current_leg = valid_legs[i]
                next_leg = valid_legs[i+1]
                
                current_pod = current_leg.get('pod')
                next_pol = next_leg.get('pol')
                
                # Nếu cảng dỡ của chặng này = cảng xếp của chặng sau -> đây là transit
                if current_pod and current_pod.lower() != 'null' and current_pod == next_pol:
                    transit_port = current_pod
                    logger.debug(f"Tìm thấy cảng transit '{transit_port}'")
                    if transit_port not in transit_port_list:
                        transit_port_list.append(transit_port)
                        
                    # 1. Xử lý Ngày đến cảng transit (AtaTransit / EtaTransit)
                    # Đây là ngày ETA của chặng hiện tại (current_leg)
                    temp_eta_transit_str = current_leg.get('eta')
                    temp_eta_date = self._parse_date_obj(temp_eta_transit_str)
                    
                    if temp_eta_date and temp_eta_date <= today:
                        # Đã đến (Actual)
                        if not ata_transit: # Lấy ngày *đầu tiên*
                            ata_transit = temp_eta_transit_str
                            logger.debug(f"Tìm thấy AtaTransit đầu tiên: {ata_transit}")
                    else:
                        # Sắp đến (Expected)
                        if not ata_transit and not eta_transit: # Chỉ lấy nếu chưa có actual
                            eta_transit = temp_eta_transit_str
                            logger.debug(f"Tìm thấy EtaTransit đầu tiên: {eta_transit}")

                    # 2. Xử lý Ngày rời cảng transit (AtdTransit / EtdTransit)
                    # Đây là ngày ETD của chặng tiếp theo (next_leg)
                    temp_etd_transit_str = next_leg.get('etd')
                    temp_etd_date = self._parse_date_obj(temp_etd_transit_str)
                    
                    if temp_etd_date and temp_etd_date <= today:
                        # Đã rời (Actual)
                        atd_transit = temp_etd_transit_str # Lấy ngày *cuối cùng*
                        logger.debug(f"Cập nhật AtdTransit cuối cùng: {atd_transit}")
                    else:
                        # Sắp rời (Expected)
                        if temp_etd_date and temp_etd_date > today:
                            future_etd_transits.append((temp_etd_date, transit_port, temp_etd_transit_str))
                            logger.debug(f"Thêm ETD transit trong tương lai: {temp_etd_transit_str} tại {transit_port}")
            
            # Chọn EtdTransit gần nhất trong tương lai
            if future_etd_transits:
                future_etd_transits.sort() # Sắp xếp theo ngày
                etd_transit = future_etd_transits[0][2] # Lấy chuỗi ngày của ngày sớm nhất
                logger.info(f"ETD transit gần nhất trong tương lai được chọn: {etd_transit}")

            # --- Chuẩn hóa kết quả ---
            shipment_data = N8nTrackingInfo(
                BookingNo= (booking_no or tracking_number).strip(),
                BlNumber= (bl_number or tracking_number).strip(),
                BookingStatus= "", # Không có trường này
                Pol= pol.strip() if pol else "",
                Pod= pod.strip() if pod else "",
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= ", ".join(transit_port_list) if transit_port_list else "",
                EtdTransit= self._format_date(etd_transit) or "",
                AtdTransit= self._format_date(atd_transit) or "",
                EtaTransit= self._format_date(eta_transit) or "",
                AtaTransit= self._format_date(ata_transit) or ""
            ) 
            
            logger.info(f"Trích xuất dữ liệu thành công cho: {tracking_number}")
            return shipment_data

        except Exception as e:
            logger.error(f"[PanCont Scraper] -> Lỗi trong quá trình trích xuất: {e}", exc_info=True)
            return None