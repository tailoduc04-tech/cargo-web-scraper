import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import re

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

# Thiết lập logger cho module này
logger = logging.getLogger(__name__)

class InterasiaScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Interasia (đã cập nhật)
    và chuẩn hóa kết quả theo template JSON yêu cầu, sử dụng logging.
    """

    def _format_date(self, date_str):
        """Chuyển đổi chuỗi ngày từ 'YYYY/MM/DD HH:MM:SS' sang 'DD/MM/YYYY'."""
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Lấy phần ngày tháng năm, bỏ qua phần giờ
            date_part = date_str.split(" ")[0]
            dt_obj = datetime.strptime(date_part, '%Y/%m/%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho Interasia.
        Thực hiện tìm kiếm, click vào link chi tiết và trích xuất dữ liệu.
        """
        logger.info("Bắt đầu scrape cho mã: %s (Interasia)", tracking_number)
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 20)

            # --- 1. Thực hiện tìm kiếm ---
            logger.debug("Chờ ô tìm kiếm...")
            search_input = self.wait.until(EC.presence_of_element_located((By.NAME, "query")))
            search_input.clear()
            search_input.send_keys(tracking_number)
            self.driver.find_element(By.CSS_SELECTOR, "#containerSumbit").click()
            logger.info("Đã gửi yêu cầu tìm kiếm cho: %s", tracking_number)
            
            # --- 2. Lấy link chi tiết B/L và truy cập ---
            logger.debug("Chờ link chi tiết B/L từ trang kết quả...")
            try:
                # Chờ cho đến khi bảng kết quả ban đầu xuất hiện
                # Dựa trên cleaned_interasia_result1.html
                detail_link_element = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".m-table-group tbody tr:first-child td:first-child a[href*='/Service/ContainerDetail']"))
                )
                detail_link = detail_link_element.get_attribute('href')
                logger.info("Đã tìm thấy link chi tiết: %s", detail_link)
            except TimeoutException:
                 logger.warning("Không tìm thấy dữ liệu cho '%s' trên trang kết quả chính.", tracking_number)
                 return None, f"Không tìm thấy dữ liệu cho '{tracking_number}' trên trang kết quả chính."

            # --- 3. Scrape trang chi tiết và chuẩn hóa ---
            normalized_data = self._extract_and_normalize_data(detail_link, tracking_number)
            
            if not normalized_data:
                # Lỗi đã được log bên trong hàm _extract_and_normalize_data
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            # --- 4. Trả về kết quả ---
            logger.info("Hoàn tất scrape thành công cho mã: %s", tracking_number)
            return normalized_data, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/interasia_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("Timeout khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s", tracking_number, screenshot_path)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s", tracking_number, e, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, detail_url, tracking_number):
        """
        Scrape trang chi tiết B/L (từ detail_url), trích xuất và chuẩn hóa dữ liệu.
        Hàm này tương ứng với _scrape_and_normalize_details của file cũ.
        """
        logger.info("Bắt đầu trích xuất dữ liệu từ trang chi tiết: %s", detail_url)
        try:
            self.driver.get(detail_url)
            # Chờ phần tử chính của trang chi tiết (dựa trên cleaned_interasia_result2.html)
            main_group = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "main-group")))
            
            # 1. Trích xuất thông tin tóm tắt chung (từ bảng đầu tiên)
            logger.debug("Trích xuất thông tin tóm tắt (POL, POD, ETD, ETA)...")
            summary_table = main_group.find_element(By.CSS_SELECTOR, ".m-table-group")
            cells = summary_table.find_elements(By.CSS_SELECTOR, "tbody tr td")
            
            pol = cells[0].text.strip() if len(cells) > 0 else None
            pod = cells[1].text.strip() if len(cells) > 1 else None
            etd = cells[2].text.strip() if len(cells) > 2 else None
            eta = cells[3].text.strip() if len(cells) > 3 else None
            logger.info(f"Summary: POL={pol}, POD={pod}, ETD={etd}, ETA={eta}")

            # 2. Lặp qua từng container để tổng hợp tất cả sự kiện
            logger.debug("Tổng hợp tất cả sự kiện từ các container...")
            all_events = []
            # Selector này tìm các thẻ <div> chứa <p> có text 'Container No'
            container_blocks = main_group.find_elements(By.XPATH, "./div[.//p[contains(text(), 'Container No')]]")
            logger.info(f"Tìm thấy {len(container_blocks)} khối container.")
            
            for block in container_blocks:
                events = self._extract_events_from_container(block)
                all_events.extend(events)
            logger.info(f"Tổng cộng {len(all_events)} sự kiện đã được thu thập.")

            # 3. Tìm các sự kiện quan trọng (ATD, ATA) từ danh sách đã tổng hợp
            # Logic này giữ lại từ file cũ vì nó phù hợp với cấu trúc dữ liệu của Interasia
            logger.debug("Tìm kiếm ATD và ATA từ danh sách sự kiện...")
            actual_departure = self._find_event(all_events, "LOADED ON BOARD VESSEL", pol)
            actual_arrival = self._find_event(all_events, "DISCHARGED FROM VESSEL", pod)
            
            atd = actual_departure.get('date') if actual_departure else None
            ata = actual_arrival.get('date') if actual_arrival else None
            logger.info(f"Found: ATD={atd}, ATA={ata}")

            # 4. Xử lý logic Transit (tương tự file cũ, nhưng dọn dẹp và logging)
            # Khác với COSCO, Interasia không có bảng schedule,
            # nên ta phải suy luận từ danh sách sự kiện.
            logger.debug("Xử lý logic transit từ danh sách sự kiện...")
            transit_ports = []
            transit_arrival_events = [] # Sẽ chứa các sự kiện 'discharged' ở transit
            transit_departure_events = [] # Sẽ chứa các sự kiện 'loaded' ở transit

            for event in all_events:
                desc = event.get('description', '').lower()
                port_location = event.get('location')
                
                # Sự kiện transit thường chứa "transhipment", "transit" hoặc "tz:"
                is_transit_event = "transhipment" in desc or "transit" in desc or "tz:" in desc
                
                if is_transit_event and port_location:
                    # Chuẩn hóa tên cảng (ví dụ: "MYPKG\nPORT KLANG NORTH PORT" -> "PORT KLANG NORTH PORT")
                    simple_port_name = port_location.split('\n')[-1].strip()
                    
                    if simple_port_name and simple_port_name not in transit_ports:
                         transit_ports.append(simple_port_name)
                         logger.debug(f"Tìm thấy cảng transit mới: {simple_port_name}")

                    # Giả định "DISCHARGED" tại cảng lạ là sự kiện đến cảng trung chuyển
                    if "discharged" in desc:
                        transit_arrival_events.append(event)
                    # Giả định "LOADED" tại cảng lạ là sự kiện rời cảng trung chuyển
                    if "loaded" in desc:
                         transit_departure_events.append(event)

            # Sắp xếp các sự kiện transit theo ngày để đảm bảo chính xác
            # (Giả định 'date' có định dạng YYYY/MM/DD HH:MM:SS có thể so sánh)
            try:
                transit_arrival_events.sort(key=lambda x: x.get('date', ''))
                transit_departure_events.sort(key=lambda x: x.get('date', ''))
            except Exception:
                logger.warning("Không thể sắp xếp các sự kiện transit.")

            # Lấy AtaTransit (đầu tiên) và AtdTransit (cuối cùng)
            ata_transit = transit_arrival_events[0].get('date') if transit_arrival_events else None
            atd_transit = transit_departure_events[-1].get('date') if transit_departure_events else None
            
            logger.info(f"Transit: Ports={transit_ports}, AtaTransit={ata_transit}, AtdTransit={atd_transit}")
            
            # QUAN TRỌNG: Interasia (theo HTML mẫu) không cung cấp ETD/ETA cho cảng transit.
            # Chỉ có ETD/ETA tổng cho POL/POD.
            # Do đó, EtaTransit và EtdTransit sẽ là chuỗi rỗng.
            eta_transit = ""
            etd_transit = ""
            
            # 5. Xây dựng đối tượng JSON chuẩn hóa (đủ 14 trường)
            logger.debug("Xây dựng đối tượng N8nTrackingInfo...")
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= tracking_number,
                BookingStatus= "", # Không tìm thấy trường này trong HTML
                Pol= pol or "",
                Pod= pod or "",
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= ", ".join(transit_ports) if transit_ports else "",
                EtdTransit= etd_transit, # Sẽ là ""
                AtdTransit= self._format_date(atd_transit) or "",
                EtaTransit= eta_transit, # Sẽ là ""
                AtaTransit= self._format_date(ata_transit) or ""
            )
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            return shipment_data

        except Exception as e:
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None

    def _extract_events_from_container(self, container_block):
        """Trích xuất tất cả sự kiện từ một khối container được cung cấp."""
        events = []
        try:
            # Lấy số container để logging (hữu ích khi debug)
            try:
                container_no = container_block.find_element(By.CSS_SELECTOR, "p.title").text.replace("Container No |", "").strip()
                logger.debug(f"Trích xuất sự kiện cho container: {container_no}")
            except Exception:
                logger.debug("Trích xuất sự kiện từ một khối container...")

            # Tìm bảng sự kiện bên trong khối container này
            event_table = container_block.find_element(By.CLASS_NAME, "m-table-group")
            rows = event_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                # Dựa trên result2.html:
                # 0: Event Date, 1: Depot, 2: Port, 3: Event Description
                if len(cells) >= 4:
                    events.append({
                        "date": cells[0].text.strip(),        # Event Date
                        "description": cells[3].text.strip(), # Event Description
                        "location": cells[2].text.strip()     # Port
                    })
        except NoSuchElementException:
            logger.warning("Không tìm thấy bảng sự kiện (m-table-group) trong một khối container.")
            pass # Bỏ qua nếu không tìm thấy bảng sự kiện
        return events

    def _find_event(self, events, description_keyword, location_keyword):
        """
        Tìm một sự kiện cụ thể trong danh sách các sự kiện dựa trên
        từ khóa mô tả và từ khóa địa điểm.
        Trả về sự kiện (dict) đầu tiên khớp hoặc dictionary rỗng.
        """
        if not location_keyword:
            logger.debug(f"Không tìm thấy sự kiện '{description_keyword}' vì location_keyword rỗng.")
            return {}
        
        # Chuẩn hóa location_keyword, ví dụ: "IDJKT(JAKARTA)" -> "jakarta"
        match = re.search(r'\((.*?)\)', location_keyword)
        normalized_loc_keyword = match.group(1).strip().lower() if match else location_keyword.lower()
        logger.debug(f"Đang tìm sự kiện '{description_keyword}' tại location_keyword '{normalized_loc_keyword}'")

        for event in events:
            # event_location ví dụ: "MYPKG\nPORT KLANG NORTH PORT"
            event_location = event.get("location", "").lower()
            # event_description ví dụ: "TZ:待轉運\nLADEN OR EMPTY TRANSIT..."
            event_description = event.get("description", "").lower()
            
            desc_match = description_keyword.lower() in event_description
            # "jakarta" phải có trong "idjkt\njakarta"
            loc_match = normalized_loc_keyword in event_location

            if desc_match and loc_match:
                logger.debug(f"Đã tìm thấy sự kiện khớp: {event}")
                return event
        
        logger.debug(f"Không tìm thấy sự kiện khớp cho '{description_keyword}' tại '{normalized_loc_keyword}'.")
        return {}