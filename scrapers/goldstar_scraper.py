import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class GoldstarScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Gold Star Line và chuẩn hóa kết quả
    theo template JSON yêu cầu.
    Đã được cập nhật để sử dụng logging, logic chuẩn hóa mới và xử lý transit.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD-Mon-YYYY' (ví dụ: '07-Oct-2025') sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Chuyển đổi từ định dạng '%d-%b-%Y'
            dt_obj = datetime.strptime(date_str, '%d-%b-%Y')
            # Format lại thành 'DD/MM/YYYY'
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            # Trả về chuỗi gốc hoặc None nếu không parse được
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return None # Trả về None để logic `or ""` hoạt động

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã tracking và trả về một dictionary JSON đã được chuẩn hóa.
        """
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            # 1. Xử lý cookie nếu có
            try:
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "rcc-confirm-button"))
                )
                self.driver.execute_script("arguments[0].click();", cookie_button)
                logger.info("Đã chấp nhận cookies.")
            except Exception:
                logger.info("Banner cookie không xuất hiện hoặc không thể xử lý.")

            # 2. Nhập mã và tìm kiếm (với logic tránh bot)
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "containerid")))
            search_input.clear()
            search_input.send_keys(tracking_number)
            
            time.sleep(1.5) # Chờ một chút

            # --- GIẢI PHÁP TRÁNH BOT ---
            # Click vào một element khác để làm mất focus khỏi ô input
            try:
                self.driver.find_element(By.CSS_SELECTOR, "body > main > section.inner-center.mobile-margin > div > div > div > div > div > div.col-lg-5.col-md-12.col-sm-12.pot-form.p-0 > div > h3").click()
                logger.info("Đã click ra ngoài để tránh bot.")
                time.sleep(0.5)
            except Exception as e_click:
                 logger.warning("Không click ra ngoài được, tiếp tục: %s", e_click)
            
            search_button = self.wait.until(EC.element_to_be_clickable((By.ID, "submitDetails")))
            self.driver.execute_script("arguments[0].click();", search_button)
            logger.info("Đang tìm kiếm mã: %s", tracking_number)

            # 3. Đợi kết quả và mở rộng tất cả chi tiết
            self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "trackShipmentResultCard")))
            logger.info("Trang kết quả đã tải.")
            time.sleep(2) # Chờ UI ổn định

            expand_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".accordion-item .arrowButton")
            logger.info("Tìm thấy %d container(s) để mở rộng.", len(expand_buttons))
            for i, button in enumerate(expand_buttons):
                try:
                    # Click bằng Javascript để đảm bảo
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(0.5)
                    self.driver.execute_script("arguments[0].click();", button)
                    logger.info("Đã mở rộng container #%d", i+1)
                    time.sleep(1) # Chờ nội dung expand
                except Exception as e:
                    logger.warning("Không thể nhấn nút mở rộng container #%d: %s", i+1, e)

            # 4. Trích xuất và chuẩn hóa dữ liệu
            logger.info("Bắt đầu trích xuất và chuẩn hóa dữ liệu.")
            normalized_data = self._extract_and_normalize_data(tracking_number)
            
            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/goldstar_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        try:
            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT ===
            # Lấy B/L No (chính xác hơn từ file HTML)
            bl_no_element = self.wait.until(EC.presence_of_element_located((By.XPATH, "//h3[span[text()='B/L No']]")))
            bl_number = bl_no_element.text.replace("B/L No", "").strip()
            booking_no = bl_number # Goldstar dùng B/L No làm mã chính
            booking_status = "" # Không có thông tin
            logger.info("BL Number (BookingNo): %s", bl_number)
            
            # Lấy POL, POD, ETD, ETA từ bảng chi tiết (chính xác hơn summary)
            pol = self.driver.find_element(By.XPATH, "//td[normalize-space(text())=\"Port of Loading (POL)\"]/parent::tr/td[contains(@class, 'font-bold')]").text.strip()
            pod = self.driver.find_element(By.XPATH, "//td[normalize-space(text())=\"Port of Discharge (POD)\"]/parent::tr/td[contains(@class, 'font-bold')]").text.strip()
            etd = self.driver.find_element(By.XPATH, "//td[normalize-space(text())='Sailing Date']/following-sibling::td").text.strip()
            eta = self.driver.find_element(By.XPATH, "//td[normalize-space(text())='Estimated Time of Arrival']/following-sibling::td").text.strip()
            
            logger.info("POL: %s | POD: %s", pol, pod)
            logger.info("ETD (Sailing Date): %s | ETA (Estimated Time of Arrival): %s", etd, eta)

            # === BƯỚC 2: THU THẬP TẤT CẢ SỰ KIỆN ===
            all_events = []
            container_items = self.driver.find_elements(By.CSS_SELECTOR, ".accordion-item")
            logger.info("Bắt đầu thu thập sự kiện từ %d container(s).", len(container_items))
            for item in container_items:
                events = self._extract_events_from_container(item)
                all_events.extend(events)
            logger.info("Đã thu thập tổng cộng %d sự kiện.", len(all_events))
            
            # === BƯỚC 3: TÌM ATD VÀ ATA TỪ SỰ KIỆN ===
            # Các sự kiện đã được sắp xếp từ MỚI NHẤT -> CŨ NHẤT
            actual_departure = self._find_event(all_events, "Container was loaded at Port of Loading", pol)
            actual_arrival = self._find_event(all_events, "Container was discharged at Port of Destination", pod)
            
            atd = actual_departure.get('date') if actual_departure else None
            ata = actual_arrival.get('date') if actual_arrival else None
            logger.info("ATD (từ sự kiện): %s | ATA (từ sự kiện): %s", atd, ata)

            # === BƯỚC 4: XỬ LÝ TRANSIT TỪ SỰ KIỆN ===
            # Logic: Tìm các sự kiện "discharged" và "loaded" KHÔNG phải ở POL hoặc POD.
            # Trang này chỉ có dữ liệu quá khứ, nên không thể tìm thấy ETDTransit hoặc ETATransit.
            logger.info("Bắt đầu xử lý thông tin transit...")
            transit_ports = []
            transit_discharge_events = [] # Dùng để tìm AtaTransit (sự kiện dỡ hàng ĐẦU TIÊN ở cảng transit)
            transit_load_events = []    # Dùng để tìm AtdTransit (sự kiện xếp hàng CUỐI CÙNG ở cảng transit)
            
            pol_lower = pol.lower()
            pod_lower = pod.lower()

            for event in all_events:
                location = event.get('location', '').strip()
                if not location:
                    continue
                    
                location_lower = location.lower()
                description_lower = event.get('description', '').lower()
                
                # Kiểm tra nếu cảng sự kiện không phải POL và POD
                if pol_lower not in location_lower and pod_lower not in location_lower:
                    if "discharged" in description_lower:
                        if location not in transit_ports: 
                            transit_ports.append(location)
                        transit_discharge_events.append(event)
                    elif "loaded" in description_lower:
                        if location not in transit_ports: 
                            transit_ports.append(location)
                        transit_load_events.append(event)
            
            logger.info("Tìm thấy các cảng transit: %s", ", ".join(transit_ports))

            # Vì sự kiện được sắp xếp MỚI -> CŨ:
            # AtaTransit (đến transit sớm nhất) = sự kiện "discharged" CUỐI CÙNG trong danh sách.
            # AtdTransit (rời transit muộn nhất) = sự kiện "loaded" ĐẦU TIÊN trong danh sách.
            ata_transit = transit_discharge_events[-1].get('date') if transit_discharge_events else None
            atd_transit = transit_load_events[0].get('date') if transit_load_events else None
            
            logger.info("AtaTransit (sớm nhất): %s | AtdTransit (muộn nhất): %s", ata_transit, atd_transit)
            
            # === BƯỚC 5: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no,
                BlNumber= bl_number,
                BookingStatus= booking_status, # ""
                Pol= pol,
                Pod= pod,
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= ", ".join(transit_ports) if transit_ports else "",
                EtdTransit= "", # Không thể lấy từ trang này
                AtdTransit= self._format_date(atd_transit) or "",
                EtaTransit= "", # Không thể lấy từ trang này
                AtaTransit= self._format_date(ata_transit) or ""
            )    
            
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            return shipment_data
            
        except Exception as e:
            logger.error("Lỗi khi trích xuất và chuẩn hóa dữ liệu cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None

    def _extract_events_from_container(self, container_item):
        """
        Trích xuất lịch sử sự kiện từ một accordion container.
        Các sự kiện được trả về theo thứ tự trên web (mới nhất -> cũ nhất).
        """
        events = []
        try:
            history_rows = container_item.find_elements(By.CSS_SELECTOR, ".accordion-body .grid-container")
            for row in history_rows:
                try:
                    # Lấy text và loại bỏ label "Last Activity\n", "Location\n", "Date\n"
                    description = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][2]").text.replace('Last Activity\n', '').strip()
                    location = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][3]").text.replace('Location\n', '').strip()
                    date_str = row.find_element(By.XPATH, ".//div[contains(@class, 'grid-item')][4]").text.replace('Date\n', '').strip()

                    if description and location and date_str:
                        events.append({
                            "date": date_str,
                            "description": description,
                            "location": location
                        })
                except NoSuchElementException:
                    continue # Bỏ qua hàng không hợp lệ
        except NoSuchElementException:
            logger.warning("Không tìm thấy bảng lịch sử cho một container.")
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        """
        Tìm sự kiện ĐẦU TIÊN (gần nhất) khớp với mô tả và địa điểm.
        (Vì danh sách sự kiện đã được sắp xếp MỚI -> CŨ)
        """
        if not events or not location_keyword:
            return {}
        
        normalized_loc_keyword = location_keyword.lower()
        desc_keyword_lower = description_keyword.lower()
        
        for event in events: # Duyệt từ mới -> cũ
            event_location = event.get("location", "").lower()
            event_desc = event.get("description", "").lower()
            
            # Chỉ cần "contains" là đủ
            if desc_keyword_lower in event_desc and normalized_loc_keyword in event_location:
                return event # Trả về sự kiện MỚI NHẤT khớp
        return {}