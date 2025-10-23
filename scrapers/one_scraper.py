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

logger = logging.getLogger(__name__)

class OneScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Ocean Network Express (ONE)
    và chuẩn hóa kết quả theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD HH:MM' sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Lấy phần ngày, bỏ qua phần giờ
            date_part = date_str.split(' ')[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            # Trả về chuỗi gốc nếu không parse được
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho ONE. Truy cập URL, nhấn nút mở rộng,
        chờ dữ liệu động tải và trích xuất thông tin.
        """
        logger.info("[ONE Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        try:
            # Xây dựng URL trực tiếp từ cấu hình và tracking number
            direct_url = f"{self.config['url']}{tracking_number}"
            logger.info("[ONE Scraper] -> Đang truy cập URL: %s", direct_url)
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 45)

            # 1. Chờ cho container chính của bảng kết quả xuất hiện
            logger.info("[ONE Scraper] -> Chờ bảng kết quả chính tải...")
            self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.mb-20"))
            )
            logger.info("[ONE Scraper] -> Bảng kết quả đã tải. Tìm nút mở rộng...")

            # 2. Tìm và nhấn vào nút mở rộng (link container) của hàng đầu tiên
            # Trang ONE hiển thị nhiều container cho 1 booking, ta chỉ cần xem 1 cái
            try:
                expand_button = self.wait.until(EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    # Selector này nhắm vào link container number ở hàng đầu tiên
                    "div.Table_body__JrCVh div.Table_tr__oVzeh span.uppercase.text-ds-secondary.cursor-pointer"
                )))
                
                self.driver.execute_script("arguments[0].click();", expand_button)
                logger.info("[ONE Scraper] -> Đã nhấn mở rộng chi tiết cho container đầu tiên.")
            except TimeoutException:
                 logger.warning("[ONE Scraper] -> Không tìm thấy nút mở rộng chi tiết (link container).")
                 # Vẫn tiếp tục thử, có thể trang đã mở sẵn
            except Exception as e:
                logger.warning("[ONE Scraper] -> Lỗi khi nhấn nút mở rộng: %s", e)

            # 3. Chờ cho phần thông tin chi tiết của container được hiển thị
            logger.info("[ONE Scraper] -> Chờ dữ liệu chi tiết (Sailing Information) hiển thị...")
            self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.SailingInformation_sailing-table-wrap__PL8Px"))
            )
            logger.info("[ONE Scraper] -> Dữ liệu chi tiết đã hiển thị.")

            # 4. Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("[ONE Scraper] -> Hoàn tất scrape thành công cho mã: %s.", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/one_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("[ONE Scraper] Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("[ONE Scraper] Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("[ONE Scraper] Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ các phần đã được tải trên trang và ánh xạ vào template JSON.
        """
        try:
            logger.info("--- Bắt đầu trích xuất và chuẩn hóa dữ liệu ---")
            today = date.today()

            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT ===
            booking_no = tracking_number
            bl_number = tracking_number
            
            try:
                # Lấy trạng thái mới nhất từ hàng đầu tiên của bảng tóm tắt
                booking_status = self.driver.find_element(
                    By.CSS_SELECTOR, 
                    "div.Table_body__JrCVh .Table_event-status-selector-alias__25gMz > div"
                ).text.strip()
                logger.info("Đã tìm thấy BookingStatus: %s", booking_status)
            except NoSuchElementException:
                logger.warning("Không tìm thấy BookingStatus tóm tắt.")
                booking_status = ""

            # === BƯỚC 2: LẤY POL/POD TỪ BẢNG "SAILING INFORMATION" ===
            # Bảng này rõ ràng hơn
            sailing_table = self.driver.find_element(By.ID, "sailing-table-wrap")
            pol = sailing_table.find_element(By.CSS_SELECTOR, "div.SailingTable_port-of-loading-td__nnGHt").text.strip()
            pod = sailing_table.find_element(By.CSS_SELECTOR, "div.SailingTable_port-of-discharge-td__kSfvf").text.strip()
            logger.info("Đã tìm thấy POL: %s", pol)
            logger.info("Đã tìm thấy POD: %s", pod)

            # === BƯỚC 3: PHÂN TÍCH BẢNG SỰ KIỆN (EVENT TABLE) ===
            etd, atd, eta, ata = None, None, None, None
            etd_transit_final, atd_transit, transit_port_list, eta_transit, ata_transit = None, None, [], None, None
            
            future_etd_transits = []
            atd_transit_found = None
            ata_transit_found = None
            eta_transit_found = None

            event_table = self.driver.find_element(By.ID, "event-table-container-id")
            rows = event_table.find_elements(By.CSS_SELECTOR, "tr.EventTable_table-row__yVKnA")
            logger.info("Đã tìm thấy %d hàng sự kiện. Bắt đầu phân tích...", len(rows))
            
            current_location = None
            all_events = []

            # Vòng 1: Build danh sách sự kiện chuẩn hóa
            for row in rows:
                try:
                    # Cập nhật vị trí nếu hàng này có định nghĩa vị trí mới
                    try:
                        location_text = row.find_element(By.CSS_SELECTOR, "div.EventTable_country-name__fax8l").text.strip()
                        if location_text:
                            current_location = location_text
                    except NoSuchElementException:
                        pass # Giữ nguyên current_location của hàng trước
                    
                    description = row.find_element(By.CSS_SELECTOR, ".EventTable_event-name-vessel-group__sDbkT > div").text.strip().lower()
                    date_container = row.find_element(By.CSS_SELECTOR, ".EventTable_actual-estimate-schedule__dlQ1t")
                    date_str = date_container.text.replace('\n', ' ').strip()
                    
                    date_type = "Estimated" # Mặc định
                    try:
                        # 'text-ds-grey-darker-1' là class cho ngày Actual
                        date_container.find_element(By.CSS_SELECTOR, ".text-ds-grey-darker-1")
                        date_type = "Actual" 
                    except NoSuchElementException:
                        pass # Vẫn là Estimated
                    
                    if current_location: # Chỉ thêm sự kiện nếu đã xác định được vị trí
                        all_events.append({
                            "location": current_location, 
                            "description": description, 
                            "date": date_str, 
                            "type": date_type
                        })
                except Exception as e:
                    logger.warning("Bỏ qua một hàng sự kiện không thể phân tích: %s", e)

            # Vòng 2: Xử lý danh sách sự kiện để lấy ngày (logic giống COSCO)
            logger.info("Đã build xong danh sách %d sự kiện. Bắt đầu xử lý logic ngày...", len(all_events))
            for event in all_events:
                loc = event["location"]
                desc = event["description"]
                date_str = event["date"]
                date_type = event["type"]

                is_pol = pol.lower() in loc.lower()
                is_pod = pod.lower() in loc.lower()

                # --- Xử lý POL ---
                if is_pol:
                    # Ưu tiên "Vessel Departure" làm ATD/ETD
                    if "vessel departure" in desc:
                        if date_type == "Actual":
                            atd = date_str
                        else:
                            etd = date_str
                    # Fallback "Loaded on Vessel"
                    elif "loaded on vessel" in desc and not atd and not etd:
                         if date_type == "Actual":
                            atd = date_str
                         else:
                            etd = date_str

                # --- Xử lý POD ---
                elif is_pod:
                    if "vessel arrival" in desc:
                        if date_type == "Actual":
                            ata = date_str
                        else:
                            eta = date_str
                
                # --- Xử lý Transit (không phải POL, không phải POD) ---
                else:
                    if loc not in transit_port_list:
                        transit_port_list.append(loc)
                        logger.debug("Tìm thấy cảng transit: %s", loc)

                    # Logic tìm ATA/ETA Transit (lấy cái *đầu tiên*)
                    if "vessel arrival" in desc or "unloaded from vessel" in desc:
                        if date_type == "Actual" and not ata_transit_found:
                            ata_transit_found = date_str
                            logger.debug("Tìm thấy AtaTransit đầu tiên: %s", date_str)
                        elif date_type == "Estimated" and not ata_transit_found and not eta_transit_found:
                            eta_transit_found = date_str
                            logger.debug("Tìm thấy EtaTransit đầu tiên: %s", date_str)
                    
                    # Logic tìm ATD Transit (lấy cái *cuối cùng*)
                    if "vessel departure" in desc or "loaded on vessel" in desc:
                        if date_type == "Actual":
                            atd_transit_found = date_str # Sẽ bị ghi đè, lấy cái cuối
                            logger.debug("Cập nhật AtdTransit cuối cùng: %s", date_str)
                        
                        # Logic tìm ETD Transit (lấy cái gần nhất > hôm nay)
                        elif date_type == "Estimated":
                            try:
                                etd_date = datetime.strptime(date_str.split(' ')[0], '%Y-%m-%d').date()
                                if etd_date > today:
                                    future_etd_transits.append((etd_date, loc, date_str))
                                    logger.debug("Thêm ETD transit trong tương lai: %s (%s)", date_str, loc)
                            except (ValueError, IndexError):
                                logger.warning("Không thể parse ETD transit: %s", date_str)
            
            # Xử lý kết quả transit
            if future_etd_transits:
                future_etd_transits.sort() # Sắp xếp để lấy ngày gần nhất
                etd_transit_final = future_etd_transits[0][2]
                logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
            else:
                 logger.info("Không tìm thấy ETD transit nào trong tương lai.")

            atd_transit = atd_transit_found
            eta_transit = eta_transit_found
            ata_transit = ata_transit_found

            # === BƯỚC 4: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
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
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None