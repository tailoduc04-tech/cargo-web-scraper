import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import traceback
import time

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho module
logger = logging.getLogger(__name__)

class KmtcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web eKMTC (đã refactor),
    sử dụng logging, cấu trúc chuẩn và chuẩn hóa kết quả đầu ra.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY.MM.DD HH:mm' hoặc 'YYYY-MM-DD HH:mm'
        sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        
        date_part = date_str.split(' ')[0]
        
        try:
            # Thử định dạng 1: YYYY.MM.DD (Thường thấy trong bảng tóm tắt)
            dt_obj = datetime.strptime(date_part, '%Y.%m.%d')
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            try:
                # Thử định dạng 2: YYYY-MM-DD (Thường thấy trong timeline)
                dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
                return dt_obj.strftime('%d/%m/%Y')
            except ValueError:
                logger.warning("[KMTC Scraper] Không thể phân tích định dạng ngày: %s", date_str)
                return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính.
        Thực hiện tìm kiếm và trả về dữ liệu đã chuẩn hóa.
        """
        logger.info("[KMTC Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        try:
            logger.info("[KMTC Scraper] 1. Điều hướng đến URL...")
            self.driver.get(self.config['url'])
            # Tăng thời gian chờ cho nhất quán với COSCO
            self.wait = WebDriverWait(self.driver, 45)

            logger.info("[KMTC Scraper] 2. Điền thông tin vào form tìm kiếm...")
            search_input = self.wait.until(EC.presence_of_element_located((By.ID, "blNo")))
            search_input.clear()
            search_input.send_keys(tracking_number)

            search_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.button.blue.sh")))
            self.driver.execute_script("arguments[0].click();", search_button)
            logger.info("[KMTC Scraper] -> Đã nhấn nút tìm kiếm.")
            
            logger.info("[KMTC Scraper] 3. Chờ trang kết quả tải...")
            # Chờ div chứa timeline xuất hiện
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.location_detail_box")))
            logger.info(f"[KMTC Scraper] -> Trang kết quả cho '{tracking_number}' đã tải xong.")
            time.sleep(1) # Chờ một chút để JS có thể render xong

            logger.info("[KMTC Scraper] 4. Bắt đầu trích xuất và chuẩn hóa dữ liệu...")
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                logger.warning(f"[KMTC Scraper] Lỗi: Không thể trích xuất dữ liệu cho '{tracking_number}'.")
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            logger.info("[KMTC Scraper] 5. Trả về kết quả thành công.")
            return normalized_data, None

        except TimeoutException:
            logger.warning(f"[KMTC Scraper] Lỗi: TimeoutException xảy ra cho mã '{tracking_number}'.")
            try:
                # Kiểm tra xem có thông báo "No Data" không
                if self.driver.find_element(By.ID, "e-alert-message").is_displayed():
                    logger.warning(f"[KMTC Scraper] -> Phát hiện thông báo 'No Data'.")
                    return None, f"Không tìm thấy dữ liệu (No Data) cho mã '{tracking_number}' trên trang eKMTC."
            except NoSuchElementException:
                pass # Không tìm thấy alert, tiếp tục xử lý timeout
                
            logger.warning(f"[KMTC Scraper] -> Không tìm thấy kết quả, có thể mã không hợp lệ hoặc trang web chậm.")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/kmtc_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.info(f"  -> Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                logger.error(f"  -> Lỗi: Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            logger.error(f"[KMTC Scraper] Lỗi: Đã xảy ra lỗi không mong muốn cho '{tracking_number}'.", exc_info=True)
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ trang kết quả của eKMTC sang N8nTrackingInfo.
        """
        logger.info("[KMTC Scraper] --- Bắt đầu _extract_and_normalize_data ---")
        try:
            # --- 1. Trích xuất thông tin chung từ bảng tóm tắt ---
            summary_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl_col tbody")))
            
            bl_no_cell = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:first-child")
            bl_number = bl_no_cell.text.split('\n')[0].strip()
            booking_status = bl_no_cell.find_element(By.TAG_NAME, "span").text.strip()
            booking_no = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:nth-child(2)").text.strip()
            
            pol_raw = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:nth-child(6)").text.strip()
            pod_raw = summary_table.find_element(By.CSS_SELECTOR, "tr:first-child td:nth-child(7)").text.strip()

            pol = pol_raw.split('\n')[0]
            # Đây là ngày Estimated (dự kiến) từ bảng tóm tắt
            etd = pol_raw.split('\n')[1] if '\n' in pol_raw else None
            pod = pod_raw.split('(')[0].strip()
            # Đây là ngày Estimated (dự kiến) từ bảng tóm tắt
            eta = pod_raw.split('\n')[1] if '\n' in pod_raw else None
            
            logger.info(f"[KMTC Scraper] -> Thông tin tóm tắt: POL='{pol}', POD='{pod}', ETD='{etd}', ETA='{eta}'")

            # --- 2. Cập nhật timeline nếu có nhiều container ---
            # Logic cũ chỉ kiểm tra [0], logic mới tìm link <a> đầu tiên
            logger.info("[KMTC Scraper] -> Kiểm tra container links để cập nhật timeline...")
            first_container_link = None
            try:
                first_container_link = self.driver.find_element(By.CSS_SELECTOR, "a.cntrNo_area.link")
            except NoSuchElementException:
                logger.info("[KMTC Scraper] -> Không có container nào là link. Sử dụng timeline mặc định.")

            if first_container_link:
                container_no = first_container_link.text
                logger.info(f"  -> Container '{container_no}' là một link, thực hiện click...")
                self.driver.execute_script("arguments[0].click();", first_container_link)
                WebDriverWait(self.driver, 10).until(
                    EC.text_to_be_present_in_element((By.CSS_SELECTOR, ".location_detail_header .ship_num"), container_no)
                )
                time.sleep(0.5) # Chờ JS render
            else:
                logger.info("[KMTC Scraper] -> Sử dụng timeline của container mặc định.")

            # --- 3. Trích xuất lịch sử từ biểu đồ tiến trình ---
            all_events = self._extract_events_from_timeline()
            
            # --- 4. Tìm các ngày thực tế (Actual) và cảng trung chuyển từ lịch sử ---
            # Ngày Actual Departure (ATD) là sự kiện "Loading" tại POL
            departure_event = self._find_event(all_events, "Loading", pol)
            atd = departure_event.get('date') if departure_event else None
            
            # Ngày Actual Arrival (ATA) là sự kiện "Discharging" tại POD
            arrival_event = self._find_event(all_events, "Discharging", pod)
            ata = arrival_event.get('date') if arrival_event else None
            
            # Xử lý transit
            transit_port_list = []
            ata_transit = None
            
            # Trang KMTC chỉ cung cấp sự kiện "Transhipment", không có ETD/ATD/ETA transit
            transhipment_events = [e for e in all_events if "Transhipment" in e.get("description", "").lower()]
            
            if transhipment_events:
                logger.info(f"[KMTC Scraper] -> Tìm thấy {len(transhipment_events)} sự kiện transhipment.")
                # Lấy danh sách các cảng transit (loại bỏ trùng lặp)
                transit_port_list = list(set([e.get('location') for e in transhipment_events if e.get('location')]))
                # Lấy ngày ATA Transit (ngày thực tế đến cảng transit) đầu tiên
                ata_transit = transhipment_events[0].get('date')
            else:
                logger.info("[KMTC Scraper] -> Không tìm thấy sự kiện transhipment.")

            transit_port = ", ".join(transit_port_list)
            
            # Các trường không có trên KMTC
            etd_transit = ""
            atd_transit = ""
            eta_transit = ""
            
            # --- 5. Xây dựng đối tượng JSON cuối cùng ---
            normalized_data = N8nTrackingInfo(
                BookingNo= booking_no or tracking_number,
                BlNumber= bl_number or tracking_number,
                BookingStatus= booking_status or "",
                Pol= pol,
                Pod= pod,
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= transit_port or "",
                EtdTransit= etd_transit,
                AtdTransit= atd_transit,
                EtaTransit= eta_transit,
                AtaTransit= self._format_date(ata_transit) or ""
            )
            
            logger.info("[KMTC Scraper] --- Hoàn tất, đã chuẩn hóa dữ liệu ---")
            return normalized_data

        except Exception as e:
            logger.error(f"[KMTC Scraper] Lỗi trong quá trình trích xuất: {e}", exc_info=True)
            return None


    def _extract_events_from_timeline(self):
        """
        Trích xuất tất cả các sự kiện từ biểu đồ tiến trình 'Current Location'.
        (Đã refactor để sử dụng logging)
        """
        events = []
        try:
            timeline = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.location_detail")))
            all_event_items = timeline.find_elements(By.TAG_NAME, "li")
            
            for item in all_event_items:
                item_class = item.get_attribute("class")
                # Bỏ qua các sự kiện chưa xảy ra (inactive)
                if "inactive" in item_class or not item.is_displayed():
                    continue
                
                try:
                    sub_event = item.find_element(By.CSS_SELECTOR, ".ts_scroll div")
                    p_tags = sub_event.find_elements(By.TAG_NAME, "p")
                    if len(p_tags) < 2: continue

                    description = p_tags[0].text.replace('\n', ' ').strip()
                    datetime_raw = p_tags[1].text.replace('\n', ' ').strip()
                    main_event_text = item.find_element(By.CSS_SELECTOR, ".txt").text.lower()
                    location = None

                    if 'on board' in main_event_text:
                        # Sự kiện 'Loading' (On board) -> lấy location từ POL
                        location = self.driver.find_element(By.CSS_SELECTOR, "table.tbl_col tbody tr:first-child td:nth-child(6)").text.split('\n')[0].strip()
                        description = "Loading" # Chuẩn hóa tên sự kiện
                    elif 'discharging' in main_event_text:
                        # Sự kiện 'Discharging' -> lấy location từ POD
                        location = self.driver.find_element(By.CSS_SELECTOR, "table.tbl_col tbody tr:first-child td:nth-child(7)").text.split('(')[0].strip()
                    elif '(transhipped)' in main_event_text:
                        description = "Transhipment" # Chuẩn hóa tên sự kiện
                        # Location được lấy từ chính text của event
                        location_text = p_tags[0].text
                        location = location_text.replace('T/S', '').replace('\n', ' ').strip()

                    if datetime_raw: # Chỉ thêm các sự kiện đã có ngày
                        events.append({
                            "date": datetime_raw,
                            "description": description,
                            "location": location
                        })
                except (NoSuchElementException, IndexError) as e_item:
                    logger.warning(f"  -> Lỗi nhỏ khi xử lý một timeline event: {e_item}")

        except Exception as e:
             logger.error(f"  -> Lỗi nghiêm trọng khi trích xuất timeline: {e}", exc_info=True)
        
        logger.info(f"  -> Đã trích xuất được {len(events)} sự kiện thực tế từ timeline.")
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """
        Tìm một sự kiện cụ thể trong danh sách.
        (Giữ nguyên logic, không cần thay đổi)
        """
        if not events: return {}
        
        for event in reversed(events): # Tìm từ cuối về đầu để lấy sự kiện mới nhất
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            
            if location_keyword:
                loc_match = location_keyword.lower() in (event.get("location") or "").lower()
                if desc_match and loc_match:
                    return event
            elif desc_match:
                return event
        return {}