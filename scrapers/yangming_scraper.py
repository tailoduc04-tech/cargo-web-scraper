import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import re
from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

class YangmingScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Yang Ming và chuẩn hóa kết quả
    theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY/MM/DD ...' sang 'DD/MM/YYYY'.
        Ví dụ: '2025/10/04 20:30 (Actual)' -> '04/10/2025'
        """
        if not date_str:
            return None
        try:
            # Lấy phần ngày tháng năm, bỏ qua thông tin giờ và trạng thái
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y/%m/%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính, thực hiện tìm kiếm và trích xuất dữ liệu.
        """
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # 1. Xử lý cookie
            try:
                cookie_button = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "cc-dismiss")))
                cookie_button.click()
            except TimeoutException:
                print("Cookie banner not found or already accepted.")

            # 2. Chọn loại tìm kiếm và nhập mã
            bl_radio_button = self.wait.until(EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_rdolType_1")))
            bl_radio_button.click()

            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_num1")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            # 3. Nhấn nút tìm kiếm
            track_button = self.wait.until(EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_btnTrack")))
            track_button.click()

            # 4. Chờ trang kết quả và trích xuất
            self.wait.until(EC.visibility_of_element_located((By.ID, "ContentPlaceHolder1_divResult")))
            
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                return None, f"Could not extract normalized data for '{tracking_number}'."

            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/yangming_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
            except Exception:
                pass
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ trang kết quả và ánh xạ vào template JSON.
        """
        try:
            # 1. Trích xuất thông tin cơ bản
            bl_number = self.wait.until(EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_rptBLNo_lblBLNo_0"))).text.strip()
            pol_raw = self.driver.find_element(By.ID, "ContentPlaceHolder1_rptBLNo_gvBasicInformation_0_lblLoading_0").text.strip()
            pod_raw = self.driver.find_element(By.ID, "ContentPlaceHolder1_rptBLNo_gvBasicInformation_0_lblDischarge_0").text.strip()

            pol = pol_raw.split('(')[0].strip()
            pod = pod_raw.split('(')[0].strip()
            
            # 2. Trích xuất lịch trình
            routing_places = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'lblRouting_')]")
            routing_dates = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'lblDateTime_')]")
            
            etd, atd, eta, ata = None, None, None, None
            transit_ports = []
            ata_transit = None

            if routing_dates:
                # Lấy ETD/ATD từ chặng đầu tiên
                first_leg_text = routing_dates[0].text.lower()
                if "(actual)" in first_leg_text:
                    atd = self._format_date(routing_dates[0].text)
                elif "(estimated)" in first_leg_text:
                    etd = self._format_date(routing_dates[0].text)

                # Lấy ETA/ATA từ chặng cuối cùng
                last_leg_text = routing_dates[-1].text.lower()
                if "(actual)" in last_leg_text:
                    ata = self._format_date(routing_dates[-1].text)
                elif "(estimated)" in last_leg_text:
                    eta = self._format_date(routing_dates[-1].text)
            
            # Xử lý các cảng trung chuyển (nếu có)
            if len(routing_places) > 2:
                for i in range(1, len(routing_places) - 1):
                    transit_ports.append(routing_places[i].text.strip())
                    
                    # Tìm AtaTransit (Berthing time)
                    transit_date_text = routing_dates[i].text
                    berthing_match = re.search(r'Berthing time at terminal: (.*?)\(Actual\)', transit_date_text, re.IGNORECASE)
                    if berthing_match:
                        ata_transit = self._format_date(berthing_match.group(1).strip())
                        # Chỉ lấy Ata của cảng trung chuyển đầu tiên để phù hợp template
                        break 
                        
            # 3. Tạo đối tượng JSON và điền dữ liệu
            #shipment_data = {
            #    "BookingNo": tracking_number, # Không có Booking No riêng, dùng mã đã search
            #    "BlNumber": bl_number,
            #    "BookingStatus": None, # Không có thông tin
            #    "Pol": pol,
            #    "Pod": pod,
            #    "Etd": etd,
            #    "Atd": atd,
            #    "Eta": eta,
            #    "Ata": ata,
            #    "TransitPort": ", ".join(transit_ports) if transit_ports else None,
            #    "EtdTransit": None, # Không có thông tin
            #    "AtdTrasit": None, # Không có thông tin
            #    "EtaTransit": None, # Không có thông tin
            #    "AtaTrasit": ata_transit,
            #}
            
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= bl_number,
                BookingStatus= None,
                Pol= pol,
                Pod= pod,
                Etd= etd,
                Atd= atd,
                Eta= eta,
                Ata= ata,
                TransitPort= ", ".join(transit_ports) if transit_ports else None,
                EtdTransit= None, 
                AtdTrasit= None, 
                EtaTransit= None,
                AtaTrasit= ata_transit,
            )
            
            return shipment_data

        except Exception as e:
            traceback.print_exc()
            return None