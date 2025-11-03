import logging
import re
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from ..selenium_scraper import SeleniumScraper
from schemas import N8nTrackingInfo

# Khởi tạo logger cho module này
logger = logging.getLogger(__name__)

class YangmingScraper(SeleniumScraper):
    """
    Triển khai logic scraping cụ thể cho trang Yang Ming (YM) và chuẩn hóa
    kết quả theo template JSON (N8nTrackingInfo).
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY/MM/DD ...' sang 'DD/MM/YYYY'.
        Ví dụ: '2025/10/04 20:30 (Actual)' -> '04/10/2025'
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Lấy phần ngày tháng năm, bỏ qua thông tin giờ và trạng thái
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y/%m/%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính.
        Thực hiện truy cập URL, xử lý cookie, tìm kiếm và gọi hàm trích xuất.
        """
        logger.info("Bắt đầu scrape hãng YM cho mã: %s", tracking_number)
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # 1. Xử lý cookie nếu có
            try:
                cookie_button = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "body > div > div > div.fixed.w-full.bg-slate-200.bottom-0.opacity-100.z-50.text-black.text-justify > div > div.mt-5.w-full.flex.justify-end > button"))
                )
                cookie_button.click()
                logger.info("Đã chấp nhận cookies.")
            except TimeoutException:
                logger.info("Banner cookie không xuất hiện hoặc đã được chấp nhận.")

            # 2. Chọn loại tìm kiếm (B/L No.) và nhập mã
            bl_radio_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_rdolType_1"))
            )
            bl_radio_button.click()

            search_input = self.wait.until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_num1"))
            )
            search_input.clear()
            search_input.send_keys(tracking_number)
            logger.info("Đã nhập mã tracking: %s", tracking_number)

            # 3. Nhấn nút tìm kiếm
            track_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "ContentPlaceHolder1_btnTrack"))
            )
            track_button.click()
            logger.info("Đang tìm kiếm...")

            # 4. Chờ trang kết quả và trích xuất
            # Chờ cho đến khi phần kết quả chính hiển thị
            self.wait.until(
                EC.visibility_of_element_located((By.ID, "ContentPlaceHolder1_divResult"))
            )
            logger.info("Trang kết quả đã tải.")
            
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                logger.warning("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("Hoàn tất scrape YM thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/yangming_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape YM mã '%s'. Đã lưu ảnh chụp màn hình vào %s", 
                             tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", 
                             tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape YM mã '%s': %s", 
                         tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả của Yang Ming.
        """
        try:
            # === BƯỚC 1: LẤY THÔNG TIN CƠ BẢN ===
            
            # Lấy BlNumber
            bl_number = self.wait.until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_rptBLNo_lblBLNo_0"))
            ).text.strip()
            
            # Lấy BookingNo (nếu có), nếu không dùng tracking_number làm fallback
            booking_no = ""
            try:
                booking_no = self.driver.find_element(By.ID, "ContentPlaceHolder1_rptBLNo_lblBKNo_0").text.strip()
            except NoSuchElementException:
                logger.warning("Không tìm thấy ID 'ContentPlaceHolder1_rptBLNo_lblBKNo_0' cho Booking No.")
                
            if not booking_no:
                booking_no = tracking_number # Fallback
                logger.info("Không tìm thấy Booking No, sử dụng tracking_number '%s' làm BookingNo.", tracking_number)
            
            logger.info("Đã trích xuất BlNumber: %s, BookingNo: %s", bl_number, booking_no)

            # BookingStatus (Không có trường này trên YM, để trống)
            booking_status = ""
            
            # Lấy POL và POD từ bảng Basic Information
            pol_raw = self.driver.find_element(
                By.ID, "ContentPlaceHolder1_rptBLNo_gvBasicInformation_0_lblLoading_0"
            ).text.strip()
            pod_raw = self.driver.find_element(
                By.ID, "ContentPlaceHolder1_rptBLNo_gvBasicInformation_0_lblDischarge_0"
            ).text.strip()

            # Tách lấy tên cảng (phần trước dấu ngoặc)
            pol = pol_raw.split('(')[0].strip()
            pod = pod_raw.split('(')[0].strip()
            logger.info("Đã trích xuất POL: %s, POD: %s", pol, pod)

            # === BƯỚC 2: LẤY THÔNG TIN TỪ BẢNG "ROUTING SCHEDULE" ===

            # Khởi tạo tất cả các biến ngày tháng và transit
            etd, atd, eta, ata = "", "", "", ""
            etd_transit_final, atd_transit, eta_transit, ata_transit = "", "", "", ""
            transit_port_list = []
            today = date.today()

            # Lấy danh sách các chặng (địa điểm và thời gian)
            routing_places_elems = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'lblRouting_')]")
            routing_dates_elems = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'lblDateTime_')]")

            if not routing_places_elems or len(routing_places_elems) != len(routing_dates_elems):
                logger.warning("Không tìm thấy thông tin lịch trình hoặc số lượng không khớp. Các trường lịch trình có thể bị trống.")
            else:
                logger.info("Đã tìm thấy %d chặng trong lịch trình.", len(routing_places_elems))
                
                # Tạo một danh sách các "chặng" để dễ xử lý
                legs = []
                for i in range(len(routing_places_elems)):
                    place = routing_places_elems[i].text.strip()
                    # Lấy HTML để phân tích (Actual)/(Estimated) và Berthing time
                    date_text_html = routing_dates_elems[i].get_attribute('innerHTML')
                    legs.append({"place": place, "html": date_text_html})

                # --- Xử lý chặng đầu tiên (POL) ---
                first_leg = legs[0]
                first_leg_html_lower = first_leg['html'].lower()
                # Lấy phần text ngày tháng (trước thẻ <br> nếu có)
                first_leg_date_str = first_leg['html'].split('<')[0].strip()
                
                if "(actual)" in first_leg_html_lower:
                    atd = self._format_date(first_leg_date_str)
                elif "(estimated)" in first_leg_html_lower:
                    etd = self._format_date(first_leg_date_str)
                logger.info("POL Info - ETD: '%s', ATD: '%s'", etd, atd)

                # --- Xử lý chặng cuối cùng (POD) ---
                last_leg = legs[-1]
                last_leg_html_lower = last_leg['html'].lower()
                last_leg_date_str = last_leg['html'].split('<')[0].strip()

                if "(actual)" in last_leg_html_lower:
                    ata = self._format_date(last_leg_date_str)
                elif "(estimated)" in last_leg_html_lower:
                    eta = self._format_date(last_leg_date_str)
                logger.info("POD Info - ETA: '%s', ATA: '%s'", eta, ata)

                # --- Xử lý các chặng trung chuyển (nếu có) ---
                future_etd_transits = []
                if len(legs) > 2:
                    logger.info("Đang xử lý %d cảng transit...", len(legs) - 2)
                    
                    # Loop qua các chặng ở giữa (từ index 1 đến len-2)
                    for i in range(1, len(legs) - 1):
                        current_transit_leg = legs[i]
                        transit_port = current_transit_leg['place']
                        
                        if transit_port not in transit_port_list:
                            transit_port_list.append(transit_port)
                        
                        logger.debug("Đang xử lý cảng transit: %s", transit_port)
                        transit_date_html = current_transit_leg['html']
                        transit_date_html_lower = transit_date_html.lower()

                        # --- Logic cho AtaTransit / EtaTransit (Arrival at transit) ---
                        # YM gọi đây là "Berthing time at terminal"
                        berthing_match = re.search(
                            r'Berthing time at terminal: (.*?)(?:\(Actual\)|\(Estimated\))', 
                            transit_date_html, 
                            re.IGNORECASE
                        )
                        if berthing_match:
                            berthing_time_str = berthing_match.group(1).strip()
                            berthing_time_formatted = self._format_date(berthing_time_str)
                            
                            if "(actual)" in berthing_match.group(0).lower():
                                # Chỉ lấy AtaTransit của cảng transit ĐẦU TIÊN
                                if not ata_transit: 
                                    ata_transit = berthing_time_formatted
                                    logger.debug("Tìm thấy AtaTransit đầu tiên: %s", ata_transit)
                            else: # Estimated
                                # Chỉ lấy EtaTransit nếu AtaTransit chưa được set
                                if not ata_transit and not eta_transit: 
                                    eta_transit = berthing_time_formatted
                                    logger.debug("Tìm thấy EtaTransit đầu tiên: %s", eta_transit)

                        # --- Logic cho AtdTransit / EtdTransit (Departure from transit) ---
                        # Thời gian chính ở dòng đầu tiên là thời gian rời cảng (Departure)
                        departure_time_str = transit_date_html.split('<')[0].strip()
                        departure_time_formatted = self._format_date(departure_time_str)

                        if "(actual)" in transit_date_html_lower:
                            # Lấy AtdTransit của cảng transit CUỐI CÙNG
                            atd_transit = departure_time_formatted 
                            logger.debug("Cập nhật AtdTransit cuối cùng: %s", atd_transit)
                        
                        elif "(estimated)" in transit_date_html_lower:
                            # Đây là một ETD transit, kiểm tra xem nó có ở tương lai không
                            try:
                                date_part = departure_time_str.split(" ")[0]
                                etd_transit_date_obj = datetime.strptime(date_part, '%Y/%m/%d').date()
                                
                                # Nếu ngày dự kiến > hôm nay, thêm vào danh sách
                                if etd_transit_date_obj > today:
                                    future_etd_transits.append((etd_transit_date_obj, transit_port, departure_time_formatted))
                                    logger.debug("Thêm ETD transit tương lai: %s (%s)", departure_time_formatted, transit_port)
                            except (ValueError, IndexError) as e:
                                logger.warning("Không thể parse ETD transit: '%s'. Lỗi: %s", departure_time_str, e)
                    
                    # Sắp xếp và chọn ETD transit gần nhất
                    if future_etd_transits:
                        future_etd_transits.sort() # Sắp xếp theo ngày
                        etd_transit_final = future_etd_transits[0][2] # Lấy ngày (mục thứ 3) của ETD gần nhất
                        logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
                    else:
                        logger.info("Không tìm thấy ETD transit nào trong tương lai.")

            # === BƯỚC 3: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            shipment_data = N8nTrackingInfo(
                BookingNo=booking_no,
                BlNumber=bl_number,
                BookingStatus=booking_status, # Luôn là ""
                Pol=pol,
                Pod=pod,
                Etd=etd,
                Atd=atd,
                Eta=eta,
                Ata=ata,
                TransitPort=", ".join(transit_port_list) if transit_port_list else "",
                EtdTransit=etd_transit_final,
                AtdTransit=atd_transit,
                EtaTransit=eta_transit,
                AtaTransit=ata_transit
            )
            
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", 
                         tracking_number, e, exc_info=True)
            return None