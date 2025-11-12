import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time # <--- Thêm import time
import traceback
import logging
from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module này
logger = logging.getLogger(__name__)

class MaerskScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Maersk.
    Sử dụng phương pháp truy cập URL trực tiếp
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
        t_total_start = time.time() # Tổng thời gian bắt đầu
        
        # Đặt timeout mặc định cho page
        self.page.set_default_timeout(45000) # 45 giây

        try:
            direct_url = f"{self.config['url']}{tracking_number}"
            logger.info(f"Đang truy cập URL: {direct_url}")
            t_nav_start = time.time()
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 45) # Tăng thời gian chờ
            # Log thời gian tải trang sẽ chính xác hơn khi chờ element đầu tiên
            # logger.info("-> (Thời gian) Tải trang: %.2fs", time.time() - t_nav_start)

            # 1. Xử lý cookie nếu có
            t_cookie_start = time.time()
            try:
                # Chờ element đầu tiên (nút cookie) sẽ bao gồm thời gian tải trang thực tế
                allow_all_button = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'coi-banner__accept') and contains(., 'Allow all')]"))
                )
                logger.info("-> (Thời gian) Tải trang và tìm nút cookie: %.2fs", time.time() - t_nav_start)
                self.driver.execute_script("arguments[0].click();", allow_all_button)
                self.wait.until(EC.invisibility_of_element_located((By.ID, "coiOverlay")))
                logger.info("Đã chấp nhận cookies. (Thời gian xử lý cookie: %.2fs)", time.time() - t_cookie_start)
            except TimeoutException:
                 # Nếu không có cookie, log thời gian tải trang ở đây
                 logger.info("-> (Thời gian) Tải trang (không có cookie): %.2fs", time.time() - t_nav_start)
                 logger.info("Banner cookie không xuất hiện hoặc đã được chấp nhận. (Thời gian kiểm tra: %.2fs)", time.time() - t_cookie_start)


            # 2. Chờ trang kết quả tải
            t_wait_result_start = time.time()
            try:
                logger.info("Chờ trang kết quả tải...")
                self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-test='search-summary-ocean']")))
                logger.info("Trang kết quả đã tải. (Thời gian chờ: %.2fs)", time.time() - t_wait_result_start)

                # 3. Trích xuất và chuẩn hóa dữ liệu
                t_extract_start = time.time()
                normalized_data = self._extract_and_normalize_data(tracking_number)
                logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)


                if not normalized_data:
                    logger.warning("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                    return None, f"Could not extract normalized data for '{tracking_number}'."

                t_total_end = time.time()
                logger.info("Hoàn tất scrape thành công cho mã: %s (Tổng thời gian: %.2fs)",
                             tracking_number, t_total_end - t_total_start)
                return normalized_data, None

            except TimeoutException:
                logger.error("Trang kết quả không tải kịp (Timeout) cho mã: %s (Thời gian chờ: %.2fs)",
                             tracking_number, time.time() - t_wait_result_start)
                # Kiểm tra xem có phải lỗi do tracking number sai không
                try:
                    error_script = "return document.querySelector('mc-input[data-test=\"track-input\"]').shadowRoot.querySelector('.mds-helper-text--negative').textContent"
                    error_message = self.driver.execute_script(error_script)
                    if error_message and "Incorrect format" in error_message:
                        logger.warning("Lỗi định dạng tracking number '%s': %s", tracking_number, error_message.strip())
                        return None, f"Không tìm thấy kết quả cho '{tracking_number}': {error_message.strip()}"
                except Exception:
                    pass # Bỏ qua nếu không tìm thấy lỗi cụ thể

                raise TimeoutException("Results page did not load.") # Ném lại lỗi để xử lý screenshot

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/maersk_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s (Tổng thời gian: %.2fs)",
                             tracking_number, screenshot_path, t_total_fail - t_total_start)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu thành một dictionary duy nhất
        Chỉ xử lý container đầu tiên.
        """
        try:
            # 1. Trích xuất thông tin tóm tắt chung
            logger.debug("Bắt đầu trích xuất thông tin tóm tắt...")
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

            # 2. Mở rộng và thu thập các sự kiện từ container ĐẦU TIÊN
            logger.debug("Bắt đầu xử lý container đầu tiên...")
            all_events = []
            containers = self.driver.find_elements(By.CSS_SELECTOR, "div.container--ocean")
            logger.info("Tìm thấy %d container. Sẽ chỉ xử lý container đầu tiên.", len(containers))

            # --- THAY ĐỔI: Chỉ xử lý container đầu tiên (nếu có) ---
            if containers:
                container = containers[0] # Chỉ lấy container đầu tiên
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
                    else:
                         logger.debug("Container %s đã được mở rộng sẵn.", container_name)

                    events = self._extract_events_from_container(container)
                    all_events.extend(events)

                except (NoSuchElementException, TimeoutException) as toggle_e:
                    logger.warning("Không thể mở rộng hoặc không tìm thấy nút toggle cho container đầu tiên: %s. Lỗi: %s", container_name, toggle_e)
                    pass # Vẫn tiếp tục xử lý các bước sau
            else:
                 logger.warning("Không tìm thấy container nào trên trang.")
            # --- KẾT THÚC THAY ĐỔI ---


            if not all_events:
                logger.warning("Không trích xuất được sự kiện nào từ container cho mã: %s", tracking_number)
            
            # 3. Tìm các sự kiện quan trọng
            logger.info("Tìm kiếm các sự kiện quan trọng và transit...")
            departure_event_actual = self._find_event(all_events, "Vessel departure", pol, event_type="ngay_thuc_te")
            if not departure_event_actual:
                departure_event_actual = self._find_event(all_events, "Feeder departure", pol, event_type="ngay_thuc_te")
            departure_event_estimated = self._find_event(all_events, "Vessel departure", pol, event_type="ngay_du_kien")
            if not departure_event_estimated:
                departure_event_estimated = self._find_event(all_events, "Feeder departure", pol, event_type="ngay_du_kien")
            arrival_event_actual = self._find_event(all_events, "Vessel arrival", pod, event_type="ngay_thuc_te")
            if not arrival_event_actual:
                arrival_event_actual = self._find_event(all_events, "Feeder arrival", pod, event_type="ngay_thuc_te")
            arrival_event_estimated = self._find_event(all_events, "Vessel arrival", pod, event_type="ngay_du_kien")
            if not arrival_event_estimated:
                arrival_event_estimated = self._find_event(all_events, "Feeder arrival", pod, event_type="ngay_du_kien")

            # 4. Logic transit
            transit_ports = []
            for event in all_events:
                location = event.get('location', '').strip() if event.get('location') else ''
                desc = event.get('description', '').lower()
                # Kiểm tra location khác POL và POD (phải kiểm tra cả hai không rỗng trước)
                is_not_pol = bool(pol and pol.strip() and pol.lower() not in location.lower())
                is_not_pod = bool(pod and pod.strip() and pod.lower() not in location.lower())

                if location and is_not_pol and is_not_pod:
                    if "arrival" in desc or "departure" in desc:
                        if location not in transit_ports:
                            transit_ports.append(location)
                            logger.debug("Tìm thấy cảng transit: %s", location)


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
                        if "vessel departure".lower() in e.get("description", "").lower() # So sánh case-insensitive
                        and port.lower() in (e.get("location") or "").lower() # So sánh case-insensitive
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
            logger.debug("Tạo đối tượng N8nTrackingInfo...")
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
        Trích xuất lịch sử sự kiện từ một khối container (Transport Plan). (Async)
        """
        events = []
        try:
            transport_plan = container_element.find_element(By.CSS_SELECTOR, ".transport-plan__list")
            list_items = transport_plan.find_elements(By.CSS_SELECTOR, "li.transport-plan__list__item")
            last_location = None
            logger.debug("--> Bắt đầu trích xuất %d event items từ container.", len(list_items))
            for item in list_items:
                event_data = {}
                try:
                    milestone_div = item.locator("div.milestone[data-test='milestone']").first

                    # Lấy description (span đầu tiên)
                    event_data['description'] = (await milestone_div.locator("span").first.text_content(timeout=1000)).strip()
                    
                    # Lấy date (span có data-test="milestone-date")
                    event_data['date'] = (await milestone_div.locator("span[data-test='milestone-date']").first.text_content(timeout=1000)).strip()
                    # ---------------------------------
                
                except Exception as e:
                    # Fallback nếu không tìm thấy cấu trúc span[data-test]
                    logger.warning(f"Không tìm thấy cấu trúc span[data-test='milestone-date'], thử fallback: {e}")
                     
                    milestone_lines = (await milestone_div.text_content(timeout=1000)).split('\n')
                    event_data['description'] = milestone_lines[0].strip() if len(milestone_lines) > 0 else None
                    event_data['date'] = milestone_lines[1].strip() if len(milestone_lines) > 1 else None


                # Loại sự kiện (Actual/Estimated)
                item_class = await item.get_attribute("class") or ""
                event_data['type'] = "ngay_du_kien" if "transport-plan__list__item transport-plan__list__item--future" in item_class else "ngay_thuc_te"
                
                events.append(event_data)
                logger.debug("---> Trích xuất event: %s", event_data)
        except NoSuchElementException:
            logger.warning("Không tìm thấy transport plan cho một container.")

        logger.debug("--> Kết thúc trích xuất %d sự kiện từ container.", len(events))
        return events

    def _find_event(self, events, description_keyword, location_keyword, event_type=None):
        """
        Tìm một sự kiện cụ thể, có thể lọc theo loại (thực tế/dự kiến).
        Tìm kiếm ngược để lấy sự kiện cuối cùng (gần nhất)
        """
        if not location_keyword:
            logger.debug("Bỏ qua _find_event vì location_keyword rỗng.")
            return {}

        logger.debug("-> _find_event: Tìm '%s' tại '%s', type '%s'", description_keyword, location_keyword, event_type)
        for event in reversed(events):
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            event_location = event.get("location") or ""
            # Kiểm tra location_keyword có nằm trong event_location không (case-insensitive)
            loc_match = location_keyword.lower() in event_location.lower()

            type_match = True
            if event_type:
                type_match = (event.get("type") == event_type)

            if desc_match and loc_match and type_match:
                logger.debug("--> Khớp: %s", event)
                return event

        logger.debug("--> Không khớp.")
        return {}