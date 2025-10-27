import logging
import pandas as pd
from datetime import datetime, date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time # <--- Thêm import time
import traceback
import re # Import re for potential future use, though not strictly needed based on current HTML analysis

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class PilScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web PIL (Pacific International Lines)
    và chuẩn hóa kết quả đầu ra theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Hàm trợ giúp để chuyển đổi chuỗi ngày tháng từ 'DD-Mon-YYYY...' sang 'DD/MM/YYYY'.
        Đã điều chỉnh để xử lý cả chuỗi có giờ/phút/giây.
        """
        if not date_str or not isinstance(date_str, str):
            return "" # Trả về chuỗi rỗng nếu không có dữ liệu
        try:
            # Tách phần ngày ra khỏi chuỗi có thể có cả thời gian
            date_part = date_str.split(" ")[0]
            # Chuyển đổi từ định dạng 'DD-Mon-YYYY'
            dt_obj = datetime.strptime(date_part, '%d-%b-%Y')
            # Format lại thành 'DD/MM/YYYY'
            return dt_obj.strftime('%d/%m/%Y')
        except ValueError:
            logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
            return date_str # Trả về chuỗi gốc nếu không parse được

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu cho một mã B/L hoặc Booking từ trang web PIL.
        """
        logger.info("--- [PIL Scraper] Bắt đầu scrape cho mã: %s ---", tracking_number)
        t_total_start = time.time() # Tổng thời gian bắt đầu
        try:
            url = self.config['url'].replace('<BL_NUMBER>', tracking_number)
            logger.info("Đang truy cập URL: %s", url)
            t_nav_start = time.time()
            self.driver.get(url)
            self.wait = WebDriverWait(self.driver, 45)
            logger.info("Trang đã tải xong. (Thời gian tải trang: %.2fs)", time.time() - t_nav_start)
            t_wait_result_start = time.time()
            # Chờ bảng tóm tắt đầu tiên xuất hiện
            self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div#results")))
            # Log thời gian tải trang + chờ bảng
            logger.info("Thời gian chờ bảng: %.2fs", time.time() - t_wait_result_start)

            # 2. Trích xuất và chuẩn hóa dữ liệu
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data(tracking_number)
            logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)


            if not normalized_data:
                 # _extract_and_normalize_data đã log lỗi cụ thể
                 return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("Hoàn tất scrape thành công cho mã: %s (Tổng thời gian: %.2fs)",
                         tracking_number, t_total_end - t_total_start)
            return normalized_data, None

        except TimeoutException:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/pil_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.warning("TimeoutException khi scrape mã '%s'. Đã lưu ảnh chụp màn hình vào %s (Tổng thời gian: %.2fs)",
                             tracking_number, screenshot_path, t_total_fail - t_total_start)
            except Exception as ss_e:
                logger.error("Không thể lưu ảnh chụp màn hình khi bị timeout cho mã '%s': %s", tracking_number, ss_e)
            return None, f"Không tìm thấy kết quả hoặc trang tải quá lâu cho '{tracking_number}' (Timeout)."
        except Exception as e:
            t_total_fail = time.time()
            logger.error("Đã xảy ra lỗi không mong muốn khi scrape mã '%s': %s (Tổng thời gian: %.2fs)",
                         tracking_number, e, t_total_fail - t_total_start, exc_info=True)
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_summary_data(self):
        """Trích xuất dữ liệu từ bảng tóm tắt chính đầu tiên."""
        summary_data = {}
        try:
            logger.debug("Đang tìm bảng tóm tắt đầu tiên...")
            # Sử dụng XPath để chắc chắn lấy tbody của bảng đầu tiên trong div#results
            summary_tbody = self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='results']/div[@class='mypil-table']/table/tbody")))
            logger.debug("Đã tìm thấy tbody tóm tắt.")

            # Lấy POL (Port of Loading)
            location_cell = summary_tbody.find_element(By.CSS_SELECTOR, "td.location")
            location_text = location_cell.text
            lines = [line.strip() for line in location_text.split('\n') if line.strip()]
            summary_data['POL'] = lines[1].split(',')[0].strip() if len(lines) > 1 else ""
            logger.debug("Trích xuất POL: '%s'", summary_data.get('POL'))

            # Lấy POD (Port of Discharge) và ETA (Estimated Time of Arrival)
            next_location_cell = summary_tbody.find_element(By.CSS_SELECTOR, "td.next-location")
            next_location_text = next_location_cell.text
            lines = [line.strip() for line in next_location_text.split('\n') if line.strip()]
            summary_data['POD'] = lines[0].split(',')[0].strip() if len(lines) > 0 else ""
            summary_data['ETA'] = lines[-1] if lines else ""
            logger.debug("Trích xuất POD: '%s', ETA: '%s'", summary_data.get('POD'), summary_data.get('ETA'))

            # Lấy ETD (Estimated Time of Departure)
            arrival_delivery_cell = summary_tbody.find_element(By.CSS_SELECTOR, "td.arrival-delivery")
            arrival_delivery_text = arrival_delivery_cell.text
            lines = [line.strip() for line in arrival_delivery_text.split('\n') if line.strip()]
            summary_data['ETD'] = lines[-1] if lines else ""
            logger.debug("Trích xuất ETD: '%s'", summary_data.get('ETD'))

        except (NoSuchElementException, IndexError) as e:
            logger.error("Lỗi khi trích xuất dữ liệu tóm tắt: %s", e, exc_info=True)
        except Exception as e:
            logger.error("Lỗi không xác định khi trích xuất dữ liệu tóm tắt: %s", e, exc_info=True)

        return summary_data

    def _expand_all_container_details(self):
        """Tìm và nhấp vào nút 'Trace' của container đầu tiên để hiển thị lịch sử chi tiết."""
        logger.info("Bắt đầu mở rộng chi tiết container đầu tiên...")
        t_expand_start = time.time()
        try:
            # Tìm tất cả các tbody chính chứa thông tin container
            main_container_tbodys = self.driver.find_elements(By.XPATH, "//div[@id='results']//table//tbody[not(contains(@class, 'sub-info-table')) and .//b[@class='cont-numb']]")
            logger.info("Tìm thấy %d khối thông tin container. Sẽ chỉ xử lý container đầu tiên.", len(main_container_tbodys))

            if not main_container_tbodys:
                 logger.warning("Không tìm thấy container nào để mở rộng chi tiết.")
                 return

            # --- THAY ĐỔI: Chỉ xử lý tbody đầu tiên ---
            main_tbody = main_container_tbodys[0] # Chỉ lấy tbody đầu tiên
            try:
                button = main_tbody.find_element(By.CSS_SELECTOR, "a.trackinfo")
                container_no_element = main_tbody.find_element(By.CSS_SELECTOR, "b.cont-numb")
                container_no = container_no_element.text if container_no_element else "container đầu tiên"

                logger.debug("Đang nhấp vào nút Trace cho %s...", container_no)
                # Sử dụng Javascript click để tránh vấn đề element bị che khuất
                self.driver.execute_script("arguments[0].click();", button)

                # Chờ cho bảng lịch sử bên trong xuất hiện
                history_tbody_selector = (By.XPATH, f"//b[contains(text(), '{container_no}')]/ancestor::tbody/following-sibling::tbody[1]")
                logger.debug("Chờ tbody lịch sử của %s xuất hiện...", container_no)

                history_tbody = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located(history_tbody_selector)
                )

                # Chờ cho class 'hidden' bị xóa đi
                WebDriverWait(self.driver, 20).until(
                    lambda d: 'hidden' not in d.find_element(*history_tbody_selector).get_attribute('class')
                )
                logger.info("Đã mở rộng chi tiết cho %s.", container_no)
                # break # Không cần break vì đã chỉ xử lý item đầu

            except TimeoutException:
                logger.warning("Timeout khi chờ lịch sử %s xuất hiện.", container_no)
            except NoSuchElementException:
                logger.warning("Không tìm thấy nút Trace hoặc tbody lịch sử cho %s.", container_no)
            except Exception as e:
                logger.error("Lỗi khi nhấp và chờ nút trace cho %s: %s", container_no, e, exc_info=True)
            # --- KẾT THÚC THAY ĐỔI ---

            logger.info("Đã xử lý mở rộng chi tiết container đầu tiên. (Thời gian: %.2fs)", time.time() - t_expand_start)
        except Exception as e:
            logger.error("Lỗi nghiêm trọng khi cố gắng mở rộng chi tiết container: %s (Thời gian: %.2fs)", e, time.time() - t_expand_start, exc_info=True)


    def _gather_all_container_events(self):
        """Thu thập sự kiện từ tất cả các container đã được mở rộng."""
        all_events = []
        try:
            logger.debug("Bắt đầu thu thập sự kiện từ các bảng chi tiết...")
            # Lấy tất cả các tbody chứa lịch sử
            history_tbodys = self.driver.find_elements(By.XPATH, "//div[@id='results']//table//tbody[contains(@class, 'sub-info-table') and not(contains(@class, 'hidden'))]")
            logger.info("Tìm thấy %d bảng lịch sử chi tiết đã mở.", len(history_tbodys))

            if not history_tbodys:
                 logger.warning("Không tìm thấy bảng lịch sử chi tiết nào đã được mở.")
                 return all_events

            for history_tbody in history_tbodys:
                try:
                    main_tbody = history_tbody.find_element(By.XPATH, "./preceding-sibling::tbody[1]")
                    container_no = main_tbody.find_element(By.CSS_SELECTOR, "b.cont-numb").text
                    logger.debug("Đang xử lý sự kiện cho container: %s", container_no)
                    events = self._extract_container_events(history_tbody, container_no)
                    logger.debug("   -> Tìm thấy %d sự kiện cho container %s.", len(events), container_no)
                    all_events.extend(events)
                except NoSuchElementException:
                    logger.warning("Bỏ qua một tbody lịch sử không tìm thấy tbody chính tương ứng.")
                    continue
                except Exception as e:
                    logger.error("Lỗi khi xử lý sự kiện cho một container: %s", e, exc_info=True)
        except Exception as e:
            logger.error("Lỗi nghiêm trọng khi thu thập sự kiện container: %s", e, exc_info=True)

        return all_events


    def _extract_container_events(self, history_tbody, container_no):
        """Trích xuất lịch sử sự kiện cho một container cụ thể từ tbody của nó."""
        events = []
        try:
            rows = history_tbody.find_elements(By.TAG_NAME, "tr")
            logger.debug("   -> Phân tích %d hàng trong bảng lịch sử.", len(rows))
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 6: # Cần ít nhất 6 cột như trong HTML
                    event_date_time = cells[3].text.strip()
                    event_name = cells[4].text.strip()
                    event_location_full = cells[5].text.strip()
                    event_location = event_location_full.split(',')[0].strip()

                    # Chỉ lấy phần ngày
                    event_date = event_date_time.split(" ")[0]

                    events.append({
                        "container_no": container_no,
                        "date": event_date, # Chỉ lưu ngày DD-Mon-YYYY
                        "description": event_name,
                        "location": event_location,
                        "full_date_time": event_date_time # Lưu lại để sort nếu cần
                    })
                else:
                    logger.warning("   -> Bỏ qua một hàng không đủ cột trong bảng lịch sử của %s", container_no)
        except NoSuchElementException:
            logger.error("   -> Lỗi NoSuchElementException khi trích xuất sự kiện cho %s.", container_no, exc_info=True)
        except Exception as e:
             logger.error("   -> Lỗi không xác định khi trích xuất sự kiện cho %s: %s", container_no, e, exc_info=True)
        return events

    def _find_event(self, events, description_keyword, location_keyword=None, find_first=True):
        """
        Tìm một sự kiện cụ thể trong danh sách các sự kiện.
        find_first=True: trả về sự kiện đầu tiên khớp.
        find_first=False: trả về sự kiện cuối cùng khớp.
        """
        if not events:
            logger.debug("Danh sách sự kiện rỗng, không thể tìm.")
            return {}

        matching_events = []
        logger.debug("Bắt đầu tìm kiếm sự kiện với keyword '%s' tại '%s'. Tìm kiếm đầu tiên: %s", description_keyword, location_keyword, find_first)

        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = True # Mặc định là khớp nếu không có location_keyword
            if location_keyword:
                 event_location = event.get("location") or ""
                 # So sánh chặt chẽ hơn, đảm bảo khớp hoàn toàn (sau khi chuẩn hóa)
                 loc_match = location_keyword.strip().lower() == event_location.strip().lower()
                 # Nếu không khớp hoàn toàn, thử kiểm tra contains
                 if not loc_match:
                      loc_match = location_keyword.strip().lower() in event_location.strip().lower()


            if desc_match and loc_match:
                logger.debug("   -> Tìm thấy sự kiện khớp: %s", event)
                matching_events.append(event)

        if not matching_events:
            logger.debug("Không tìm thấy sự kiện nào khớp với '%s' tại '%s'.", description_keyword, location_keyword)
            return {}

        # Sắp xếp các sự kiện tìm được theo thời gian để lấy first/last một cách đáng tin cậy
        try:
             # Sử dụng full_date_time để sort chính xác
             matching_events.sort(key=lambda x: datetime.strptime(x.get("full_date_time", "01-Jan-1900 00:00:00"), '%d-%b-%Y %H:%M:%S'))
        except ValueError:
             logger.warning("Không thể sort sự kiện theo thời gian do lỗi định dạng.")
             # Nếu không sort được, vẫn trả về first/last dựa trên thứ tự tìm thấy

        if find_first:
            result = matching_events[0]
            logger.info("Trả về sự kiện ĐẦU TIÊN khớp: %s", result)
            return result
        else:
            result = matching_events[-1]
            logger.info("Trả về sự kiện CUỐI CÙNG khớp: %s", result)
            return result


    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất và chuẩn hóa dữ liệu từ trang kết quả của PIL.
        """
        try:
            # === BƯỚC 1: LẤY THÔNG TIN TÓM TẮT ===
            t_summary_start = time.time()
            logger.info("Bắt đầu trích xuất thông tin tóm tắt...")
            summary_data = self._extract_summary_data()
            pol = summary_data.get("POL")
            pod = summary_data.get("POD")
            etd = summary_data.get("ETD") # Dự kiến đi
            eta = summary_data.get("ETA") # Dự kiến đến
            logger.info("Thông tin tóm tắt: POL=%s, POD=%s, ETD=%s, ETA=%s", pol, pod, etd, eta)
            logger.debug("-> (Thời gian) Trích xuất tóm tắt: %.2fs", time.time() - t_summary_start)


            if not pol or not pod:
                 logger.error("Không thể trích xuất POL hoặc POD từ bảng tóm tắt.")
                 return None

            # Booking No và BL Number mặc định là tracking_number
            booking_no = tracking_number
            bl_number = tracking_number
            booking_status = "" # Không có Booking Status rõ ràng

            # === BƯỚC 2: MỞ RỘNG CHI TIẾT CONTAINER ĐẦU TIÊN ===
            self._expand_all_container_details() # Hàm này đã được sửa để chỉ xử lý container đầu tiên

            # === BƯỚC 3: THU THẬP TẤT CẢ SỰ KIỆN ===
            t_event_start = time.time()
            logger.info("Bắt đầu thu thập lịch sử chi tiết của các container...")
            all_events = self._gather_all_container_events()
            logger.info("Tổng cộng đã thu thập được %d sự kiện.", len(all_events))
            logger.debug("-> (Thời gian) Thu thập sự kiện: %.2fs", time.time() - t_event_start)


            # === BƯỚC 4: XÁC ĐỊNH NGÀY THỰC TẾ VÀ TRANSIT ===
            t_process_event_start = time.time()
            logger.info("Bắt đầu xác định các ngày quan trọng và cảng trung chuyển...")

            # ATD: Actual Time of Departure - Tìm sự kiện "Loading" cuối cùng tại POL
            actual_departure_event = self._find_event(all_events, "Vessel Loading", pol, find_first=False)
            atd = actual_departure_event.get("date") if actual_departure_event else ""

            # ATA: Actual Time of Arrival - Tìm sự kiện "Discharge" đầu tiên tại POD
            actual_arrival_event = self._find_event(all_events, "Vessel Discharge", pod, find_first=True)
            ata = actual_arrival_event.get("date") if actual_arrival_event else ""

            logger.info("Xác định ATD: %s, ATA: %s", atd, ata)

            # Xác định cảng trung chuyển (Transit Ports)
            transit_ports_dict = {} # Dùng dict để lưu cảng và thời gian đến/đi đầu tiên/cuối cùng
            for event in all_events:
                location = event.get('location', '')
                description = event.get('description', '').lower()
                event_dt_str = event.get('full_date_time')
                event_dt = None
                if event_dt_str:
                     try:
                          event_dt = datetime.strptime(event_dt_str, '%d-%b-%Y %H:%M:%S')
                     except ValueError:
                          logger.warning("Không thể parse datetime cho sự kiện transit: %s", event_dt_str)
                          continue # Bỏ qua nếu không parse được thời gian

                if location and pol and pod and \
                   location.strip().lower() != pol.strip().lower() and \
                   location.strip().lower() != pod.strip().lower():

                    if "discharge" in description:
                         if location not in transit_ports_dict:
                              transit_ports_dict[location] = {'arrival': None, 'departure': None}
                         # Lưu thời gian đến đầu tiên tại cảng transit này
                         if event_dt and (transit_ports_dict[location]['arrival'] is None or event_dt < transit_ports_dict[location]['arrival']):
                              transit_ports_dict[location]['arrival'] = event_dt
                              logger.debug("Cập nhật thời gian đến transit tại %s: %s", location, event_dt_str)

                    elif "loading" in description:
                         if location not in transit_ports_dict:
                              transit_ports_dict[location] = {'arrival': None, 'departure': None}
                         # Lưu thời gian đi cuối cùng tại cảng transit này
                         if event_dt and (transit_ports_dict[location]['departure'] is None or event_dt > transit_ports_dict[location]['departure']):
                              transit_ports_dict[location]['departure'] = event_dt
                              logger.debug("Cập nhật thời gian đi transit tại %s: %s", location, event_dt_str)

            # Sắp xếp các cảng transit theo thời gian đến (nếu có)
            sorted_transit_locations = sorted(transit_ports_dict.keys(),
                                             key=lambda loc: transit_ports_dict[loc]['arrival'] or datetime.max)

            transit_port_list_str = ", ".join(sorted_transit_locations) if sorted_transit_locations else ""
            logger.info("Các cảng trung chuyển theo thứ tự: %s", transit_port_list_str)

            # AtaTransit: Thời gian đến thực tế ở cảng transit đầu tiên
            ata_transit_dt = transit_ports_dict[sorted_transit_locations[0]]['arrival'] if sorted_transit_locations and transit_ports_dict[sorted_transit_locations[0]]['arrival'] else None
            ata_transit = ata_transit_dt.strftime('%d-%b-%Y') if ata_transit_dt else ""

            # AtdTransit: Thời gian đi thực tế từ cảng transit cuối cùng
            atd_transit_dt = transit_ports_dict[sorted_transit_locations[-1]]['departure'] if sorted_transit_locations and transit_ports_dict[sorted_transit_locations[-1]]['departure'] else None
            atd_transit = atd_transit_dt.strftime('%d-%b-%Y') if atd_transit_dt else ""

            logger.info("Xác định AtaTransit: %s, AtdTransit: %s", ata_transit, atd_transit)

            # EtaTransit và EtdTransit: Không có thông tin này trong file HTML mẫu
            eta_transit = ""
            etd_transit_final = ""
            logger.warning("Không tìm thấy thông tin EtaTransit và EtdTransit trên trang PIL.")
            logger.debug("-> (Thời gian) Xác định ngày và transit: %.2fs", time.time() - t_process_event_start)


            # === BƯỚC 5: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            t_normalize_start = time.time()
            logger.info("Bắt đầu chuẩn hóa dữ liệu đầu ra...")
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no.strip(),
                BlNumber= bl_number.strip(),
                BookingStatus= booking_status.strip(), # Sẽ là ""
                Pol= pol.strip(),
                Pod= pod.strip(),
                Etd= self._format_date(etd) or "",
                Atd= self._format_date(atd) or "",
                Eta= self._format_date(eta) or "",
                Ata= self._format_date(ata) or "",
                TransitPort= transit_port_list_str,
                EtdTransit= self._format_date(etd_transit_final) or "", # Sẽ là ""
                AtdTransit= self._format_date(atd_transit) or "",
                EtaTransit= self._format_date(eta_transit) or "", # Sẽ là ""
                AtaTransit= self._format_date(ata_transit) or ""
            )
            logger.info("Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu: %.2fs", time.time() - t_normalize_start)
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích xuất
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None