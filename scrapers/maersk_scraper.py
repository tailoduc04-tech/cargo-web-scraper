import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback
import logging
from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module này
logger = logging.getLogger(__name__)

class MaerskScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Maersk.
    Sử dụng phương pháp truy cập URL trực tiếp, logging,
    và logic transit nâng cao.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'DD Mon YYYY HH:MM' sang 'DD/MM/YYYY'.
        Ví dụ: '24 Oct 2025 09:00' -> '24/10/2025'
        """
        if not date_str:
            return None
        try:
            # Tách chuỗi để xử lý các định dạng có thể có
            clean_date_str = date_str.split('(')[0].strip()
            # Định dạng chính là 'DD Mon YYYY HH:MM'
            dt_obj = datetime.strptime(clean_date_str, '%d %b %Y %H:%M')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            # Thử định dạng không có giờ
            try:
                dt_obj = datetime.strptime(clean_date_str, '%d %b %Y')
                return dt_obj.strftime('%d/%m/%Y')
            except (ValueError, IndexError):
                logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
                return date_str # Trả về chuỗi gốc nếu không phân tích được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính, truy cập URL trực tiếp và trả về một dictionary JSON duy nhất.
        """
        logger.info("Bắt đầu scrape cho mã: %s", tracking_number)
        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            logger.info(f"Đang truy cập URL: {direct_url}")
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 45) # Tăng thời gian chờ

            # 1. Xử lý cookie nếu có
            try:
                allow_all_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'coi-banner__accept') and contains(., 'Allow all')]"))
                )
                self.driver.execute_script("arguments[0].click();", allow_all_button)
                self.wait.until(EC.invisibility_of_element_located((By.ID, "coiOverlay")))
                logger.info("Đã chấp nhận cookies.")
            except TimeoutException:
                logger.info("Banner cookie không xuất hiện hoặc đã được chấp nhận.")
            
            # 2. Chờ trang kết quả tải
            try:
                logger.info("Chờ trang kết quả tải...")
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))
                logger.info("Trang kết quả đã tải.")

                # 3. Trích xuất và chuẩn hóa dữ liệu
                normalized_data = self._extract_and_normalize_data(tracking_number)
                
                if not normalized_data:
                    logger.warning("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                    return None, f"Could not extract normalized data for '{tracking_number}'."

                logger.info("Hoàn tất scrape thành công cho mã: %s", tracking_number)
                return normalized_data, None

            except TimeoutException:
                # Kiểm tra xem có phải lỗi do tracking number sai không
                try:
                    error_script = "return document.querySelector('mc-input[data-test=\"track-input\"]').shadowRoot.querySelector('.mds-helper-text--negative').textContent"
                    error_message = self.driver.execute_script(error_script)
                    if error_message and "Incorrect format" in error_message:
                        logger.warning("Lỗi định dạng tracking number '%s': %s", tracking_number, error_message.strip())
                        return None, f"Không tìm thấy kết quả cho '{tracking_number}': {error_message.strip()}"
                except Exception:
                    pass # Bỏ qua nếu không tìm thấy lỗi cụ thể
                
                logger.error("Trang kết quả không tải kịp (Timeout) cho mã: %s", tracking_number)
                raise TimeoutException("Results page did not load.")

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/maersk_timeout_{tracking_number}_{timestamp}.png"
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
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu thành một dictionary duy nhất
        dựa trên logic hoạt động mới.
        """
        try:
            # 1. Trích xuất thông tin tóm tắt chung
            summary_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))
            
            try:
                pol = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-from-value']").text
                logger.info("Đã tìm thấy POL: %s", pol)
            except NoSuchElementException:
                pol = None
                logger.warning("Không tìm thấy POL cho mã: %s", tracking_number)
            
            try:
                pod = summary_element.find_element(By.CSS_SELECTOR, "dd[data-test='track-to-value']").text
                logger.info("Đã tìm thấy POD: %s", pod)
            except NoSuchElementException:
                pod = None
                logger.warning("Không tìm thấy POD cho mã: %s", tracking_number)
            
            # 2. Mở rộng và thu thập các sự kiện từ 5 container đầu tiên
            all_events = []
            containers = self.driver.find_elements(By.CSS_SELECTOR, "div.container--ocean")
            logger.info("Tìm thấy %d container. Sẽ xử lý tối đa 5 container đầu tiên.", len(containers))
            
            for container in containers[:5]: # Chỉ xử lý 5 container đầu tiên
                try:
                    container_name = container.find_element(By.CSS_SELECTOR, "span.mds-text--medium-bold").text
                    logger.debug("Đang xử lý container: %s", container_name)
                    toggle_button_host = container.find_element(By.CSS_SELECTOR, "mc-button[data-test='container-toggle-details']")
                    if toggle_button_host.get_attribute("aria-expanded") == 'false':
                        logger.debug("Mở rộng chi tiết cho container: %s", container_name)
                        button_to_click = self.driver.execute_script("return arguments[0].shadowRoot.querySelector('button')", toggle_button_host)
                        self.driver.execute_script("arguments[0].click();", button_to_click)
                        WebDriverWait(self.driver, 10).until(
                            lambda d: toggle_button_host.get_attribute("aria-expanded") == 'true'
                        )
                        time.sleep(0.5) # Chờ một chút để animation hoàn tất
                except (NoSuchElementException, TimeoutException) as toggle_e:
                    logger.warning("Không thể mở rộng hoặc không tìm thấy nút toggle cho container: %s. Lỗi: %s", container_name, toggle_e)
                    pass
                
                events = self._extract_events_from_container(container)
                all_events.extend(events)

            if not all_events:
                logger.warning("Không trích xuất được sự kiện nào từ container cho mã: %s", tracking_number)
                # Vẫn có thể trả về thông tin cơ bản nếu có
                
            # 3. Tìm các sự kiện quan trọng và cảng trung chuyển từ danh sách tổng hợp
            departure_event_actual = self._find_event(all_events, "Vessel departure", pol, event_type="ngay_thuc_te")
            departure_event_estimated = self._find_event(all_events, "Vessel departure", pol, event_type="ngay_du_kien")
            arrival_event_actual = self._find_event(all_events, "Vessel arrival", pod, event_type="ngay_thuc_te")
            arrival_event_estimated = self._find_event(all_events, "Vessel arrival", pod, event_type="ngay_du_kien")

            # 4. Logic transit mới (giống COSCO)
            transit_ports = []
            for event in all_events:
                location = event.get('location', '').strip() if event.get('location') else ''
                desc = event.get('description', '').lower()
                if location and pol not in location and pod not in location:
                    if "arrival" in desc or "departure" in desc:
                        if location not in transit_ports:
                            transit_ports.append(location)
            
            logger.info("Tìm thấy các cảng transit: %s", transit_ports)

            etd_transit_final, atd_transit, eta_transit, ata_transit = None, None, None, None
            future_etd_transits = []
            today = date.today()

            if transit_ports:
                first_transit_port = transit_ports[0]
                
                # Tìm AtaTransit (đầu tiên) hoặc EtaTransit (đầu tiên)
                ata_transit_event = self._find_event(all_events, "Vessel arrival", first_transit_port, event_type="ngay_thuc_te")
                if ata_transit_event:
                    ata_transit = ata_transit_event.get('date')
                    logger.debug("Tìm thấy AtaTransit đầu tiên: %s", ata_transit)
                else:
                    eta_transit_event = self._find_event(all_events, "Vessel arrival", first_transit_port, event_type="ngay_du_kien")
                    eta_transit = eta_transit_event.get('date')
                    logger.debug("Không thấy AtaTransit, tìm thấy EtaTransit đầu tiên: %s", eta_transit)

                # Tìm AtdTransit (cuối cùng) và EtdTransit (gần nhất > hôm nay)
                for port in transit_ports:
                    # AtdTransit: Luôn lấy cái cuối cùng
                    atd_event = self._find_event(all_events, "Vessel departure", port, event_type="ngay_thuc_te")
                    if atd_event:
                        atd_transit = atd_event.get('date') # Sẽ bị ghi đè, lấy cái cuối
                        logger.debug("Cập nhật AtdTransit cuối cùng: %s (tại %s)", atd_transit, port)

                    # EtdTransit: Lấy tất cả các ngày dự kiến trong tương lai
                    etd_events = [
                        e for e in all_events 
                        if "Vessel departure".lower() in e.get("description", "").lower() 
                        and port.lower() in (e.get("location") or "").lower() 
                        and e.get("type") == "ngay_du_kien"
                    ]
                    
                    for etd_event in etd_events:
                        temp_etd_transit_str = etd_event.get('date')
                        if temp_etd_transit_str:
                            clean_date_str = temp_etd_transit_str.split('(')[0].strip()
                            try:
                                etd_transit_date = datetime.strptime(clean_date_str, '%d %b %Y %H:%M').date()
                            except (ValueError, IndexError):
                                try:
                                    etd_transit_date = datetime.strptime(clean_date_str, '%d %b %Y').date()
                                except (ValueError, IndexError):
                                    logger.warning("Không thể parse ngày ETD transit: %s", clean_date_str)
                                    continue
                            
                            if etd_transit_date > today:
                                future_etd_transits.append((etd_transit_date, port, temp_etd_transit_str))
                                logger.debug("Thêm ETD transit trong tương lai: %s (%s)", temp_etd_transit_str, port)

            if future_etd_transits:
                future_etd_transits.sort() # Sắp xếp theo ngày
                etd_transit_final = future_etd_transits[0][2] # Lấy ngày (chuỗi) của event sớm nhất
                logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
            else:
                 logger.info("Không tìm thấy ETD transit nào trong tương lai.")

            # 5. Xây dựng đối tượng JSON cuối cùng
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= tracking_number,
                BookingStatus= "", # Không tìm thấy trên trang này
                Pol= pol or "",
                Pod= pod or "",
                Etd= self._format_date(departure_event_estimated.get('date')) if departure_event_estimated else "",
                Atd= self._format_date(departure_event_actual.get('date')) if departure_event_actual else "",
                Eta= self._format_date(arrival_event_estimated.get('date')) if arrival_event_estimated else "",
                Ata= self._format_date(arrival_event_actual.get('date')) if arrival_event_actual else "",
                TransitPort= ", ".join(transit_ports) if transit_ports else "",
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

    def _extract_events_from_container(self, container_element):
        """
        Trích xuất lịch sử sự kiện từ một khối container (Transport Plan).
        """
        events = []
        try:
            transport_plan = container_element.find_element(By.CSS_SELECTOR, ".transport-plan__list")
            list_items = transport_plan.find_elements(By.CSS_SELECTOR, "li.transport-plan__list__item")
            last_location = None
            for item in list_items:
                event_data = {}
                try:
                    # Vị trí
                    location_text = item.find_element(By.CSS_SELECTOR, "div.location").text
                    event_data['location'] = location_text.split('\n')[0].strip()
                    last_location = event_data['location']
                except NoSuchElementException: 
                    # Nếu không có location, dùng location của event trước đó
                    event_data['location'] = last_location
                
                # Chi tiết sự kiện
                milestone_div = item.find_element(By.CSS_SELECTOR, "div.milestone")
                milestone_lines = milestone_div.text.split('\n')
                
                event_data['description'] = milestone_lines[0] if len(milestone_lines) > 0 else None
                event_data['date'] = milestone_lines[1] if len(milestone_lines) > 1 else None
                
                # Loại sự kiện (Actual/Estimated)
                event_data['type'] = "ngay_du_kien" if "future" in item.get_attribute("class") else "ngay_thuc_te"
                events.append(event_data)
        except NoSuchElementException:
            logger.warning("Không tìm thấy transport plan cho một container.")
        
        logger.debug("Trích xuất được %d sự kiện từ container.", len(events))
        return events
    
    def _find_event(self, events, description_keyword, location_keyword, event_type=None):
        """
        Tìm một sự kiện cụ thể, có thể lọc theo loại (thực tế/dự kiến).
        Tìm kiếm ngược để lấy sự kiện cuối cùng (gần nhất)
        """
        if not location_keyword: 
            logger.debug("Bỏ qua _find_event vì location_keyword rỗng.")
            return {}
        
        for event in reversed(events):
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            event_location = event.get("location") or ""
            loc_match = location_keyword.lower() in event_location.lower()
            
            type_match = True
            if event_type:
                type_match = (event.get("type") == event_type)

            if desc_match and loc_match and type_match:
                logger.debug("Tìm thấy sự kiện khớp: desc='%s', loc='%s', type='%s'", description_keyword, location_keyword, event_type)
                return event
        
        logger.debug("Không tìm thấy sự kiện khớp: desc='%s', loc='%s', type='%s'", description_keyword, location_keyword, event_type)
        return {}