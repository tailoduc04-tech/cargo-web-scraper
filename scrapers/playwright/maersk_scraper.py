from datetime import datetime, date
from playwright.async_api import Page, TimeoutError, expect
import time
import logging
from ..playwright_scraper import PlaywrightScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module này
logger = logging.getLogger(__name__)

class MaerskScraper(PlaywrightScraper):
    """
    Triển khai logic scraping cụ thể cho trang Maersk.
    Sử dụng Playwright và playwright-stealth (Async).
    """
    def __init__(self, page: Page, config: dict):
        super().__init__(page=page, config=config)

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

    
    async def scrape(self, tracking_number):
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
            
            # 1. Tải trang (dùng await)
            await self.page.goto(direct_url, wait_until="domcontentloaded")
            logger.info("-> (Thời gian) Tải trang: %.2fs", time.time() - t_nav_start)
            
            # 3. Chờ trang kết quả tải
            t_wait_result_start = time.time()
            try:
                logger.info("Chờ trang kết quả tải...")
                
                await self.page.wait_for_selector("div[data-test='search-summary-ocean']", state="visible")
                logger.info("Trang kết quả đã tải. (Thời gian chờ: %.2fs)", time.time() - t_wait_result_start)

                # 4. Trích xuất và chuẩn hóa dữ liệu (dùng await)
                t_extract_start = time.time()
                normalized_data = await self._extract_and_normalize_data(tracking_number)
                logger.info("-> (Thời gian) Trích xuất dữ liệu: %.2fs", time.time() - t_extract_start)

                if not normalized_data:
                    logger.warning("Không thể trích xuất dữ liệu đã chuẩn hóa cho '%s'.", tracking_number)
                    return None, f"Could not extract normalized data for '{tracking_number}'."

                t_total_end = time.time()
                logger.info("Hoàn tất scrape thành công cho mã: %s (Tổng thời gian: %.2fs)",
                             tracking_number, t_total_end - t_total_start)
                return normalized_data, None

            except TimeoutError:
                logger.error("Trang kết quả không tải kịp (Timeout) cho mã: %s (Thời gian chờ: %.2fs)",
                             tracking_number, time.time() - t_wait_result_start)
                # Kiểm tra lỗi tracking number sai (pierce shadow DOM)
                try:
                    error_locator = self.page.locator("mc-input[data-test='track-input'] >> .mds-helper-text--negative")
                    
                    error_message = await error_locator.text_content(timeout=1000)
                    if error_message and "Incorrect format" in error_message:
                        logger.warning("Lỗi định dạng tracking number '%s': %s", tracking_number, error_message.strip())
                        return None, f"Không tìm thấy kết quả cho '{tracking_number}': {error_message.strip()}"
                except Exception:
                    pass
                raise TimeoutError("Results page did not load.")

        except TimeoutError:
            t_total_fail = time.time()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/maersk_timeout_{tracking_number}_{timestamp}.png"
            try:
                
                await self.page.screenshot(path=screenshot_path, full_page=True)
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

    
    async def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu thành một dictionary duy nhất
        Chỉ xử lý container đầu tiên.
        """
        try:
            # 1. Trích xuất thông tin tóm tắt chung
            logger.info("Bắt đầu trích xuất thông tin tóm tắt...")
            summary_element = self.page.locator("div[data-test='search-summary-ocean']").first

            try:
                
                pol = await summary_element.locator("dd[data-test='track-from-value']").text_content(timeout=5000)
                logger.info("Đã tìm thấy POL: %s", pol)
            except Exception:
                pol = None
                logger.warning("Không tìm thấy POL cho mã: %s", tracking_number)

            try:
                
                pod = await summary_element.locator("dd[data-test='track-to-value']").text_content(timeout=5000)
                logger.info("Đã tìm thấy POD: %s", pod)
            except Exception:
                pod = None
                logger.warning("Không tìm thấy POD cho mã: %s", tracking_number)

            # 2. Mở rộng và thu thập các sự kiện từ container ĐẦU TIÊN
            logger.info("Bắt đầu xử lý container đầu tiên...")
            all_events = []
            
            containers = await self.page.locator("div.container--ocean").all()
            logger.info("Tìm thấy %d container. Sẽ chỉ xử lý container đầu tiên.", len(containers))

            if containers:
                container = containers[0]
                container_name = ""
                try:
                    
                    container_name = await container.locator("span.mds-text--medium-bold").text_content(timeout=5000)
                    logger.info("Đang xử lý container: %s", container_name)
                    
                    events = await self._extract_events_from_container(container)
                    all_events.extend(events)

                except (TimeoutError, Exception) as toggle_e:
                    logger.warning("Không thể mở rộng hoặc không tìm thấy nút toggle cho container '%s'. Lỗi: %s", container_name, toggle_e)
                    pass # Vẫn tiếp tục xử lý
            else:
                 logger.warning("Không tìm thấy container nào trên trang.")

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
                is_not_pol = bool(pol and pol.strip() and pol.lower() not in location.lower())
                is_not_pod = bool(pod and pod.strip() and pod.lower() not in location.lower())

                if location and is_not_pol and is_not_pod:
                    if "arrival" in desc or "departure" in desc:
                        if location not in transit_ports:
                            transit_ports.append(location)
                            logger.info("Tìm thấy cảng transit: %s", location)

            logger.info("Tìm thấy các cảng transit: %s", transit_ports)
            etd_transit_final, atd_transit, eta_transit, ata_transit = None, None, None, None
            future_etd_transits = []
            today = date.today()

            if transit_ports:
                first_transit_port = transit_ports[0]
                ata_transit_event = self._find_event(all_events, "Vessel arrival", first_transit_port, event_type="ngay_thuc_te")
                if not ata_transit_event:
                    ata_transit_event = self._find_event(all_events, "Feeder arrival", first_transit_port, event_type="ngay_thuc_te")
                if ata_transit_event:
                    ata_transit = ata_transit_event.get('date')
                    logger.info("Tìm thấy AtaTransit đầu tiên: %s", ata_transit)
                else:
                    eta_transit_event = self._find_event(all_events, "Vessel arrival", first_transit_port, event_type="ngay_du_kien")
                    if not eta_transit_event:
                        eta_transit_event = self._find_event(all_events, "Feeder arrival", first_transit_port, event_type="ngay_du_kien")
                    eta_transit = eta_transit_event.get('date')
                    logger.info("Không thấy AtaTransit, tìm thấy EtaTransit đầu tiên: %s", eta_transit)

                for port in transit_ports:
                    atd_event = self._find_event(all_events, "Vessel departure", port, event_type="ngay_thuc_te")
                    if not atd_event:
                        atd_event = self._find_event(all_events, "Feeder departure", port, event_type="ngay_thuc_te")
                    if atd_event:
                        atd_transit = atd_event.get('date')
                        logger.info("Cập nhật AtdTransit cuối cùng: %s (tại %s)", atd_transit, port)
                    
                    etd_events = [
                        e for e in all_events
                        if ("vessel departure".lower() in e.get("description", "").lower() or "feeder departure".lower() in e.get("description", "").lower())
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
                                logger.info("Thêm ETD transit trong tương lai: %s (%s)", temp_etd_transit_str, port)

            if future_etd_transits:
                future_etd_transits.sort()
                etd_transit_final = future_etd_transits[0][2]
                logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
            else:
                 logger.info("Không tìm thấy ETD transit nào trong tương lai.")

            # 5. Xây dựng đối tượng JSON
            logger.info("Tạo đối tượng N8nTrackingInfo...")
            shipment_data = N8nTrackingInfo(
                BookingNo= tracking_number,
                BlNumber= tracking_number,
                BookingStatus= "",
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
            logger.error("Lỗi trong quá trình trích xuất chi tiết cho mã '%s': %s", tracking_number, e, exc_info=True)
            return None

    
    async def _extract_events_from_container(self, container_element):
        """
        Trích xuất lịch sử sự kiện từ một khối container (Transport Plan). (Async)
        """
        events = []
        try:
            transport_plan = container_element.locator(".transport-plan__list").first
            
            list_items = await transport_plan.locator("li.transport-plan__list__item").all()
            last_location = None
            logger.info("--> Bắt đầu trích xuất %d event items từ container.", len(list_items))
            
            for item in list_items:
                event_data = {}
                # Lấy tất cả các text node bên trong, làm sạch và join bằng ", "
                location_locator = item.locator("div.location[data-test='location-name'] strong")
                
                location_parts = [part.strip() for part in await location_locator.all_text_contents() if part.strip()]
                if location_parts:
                    event_data['location'] = ", ".join(location_parts)
                    last_location = event_data['location']
                    # -------------------------
                else:
                    event_data['location'] = last_location
                    logger.info("---> Sử dụng last_location: %s", last_location)

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
                logger.info("---> Trích xuất event: %s", event_data)
                
        except Exception as e: # Bắt lỗi chung nếu transport_plan không tìm thấy
            logger.warning(f"Không tìm thấy transport plan cho một container: {e}")

        logger.info("--> Kết thúc trích xuất %d sự kiện từ container.", len(events))
        return events

    def _find_event(self, events, description_keyword, location_keyword, event_type=None):
        """
        Tìm một sự kiện cụ thể, có thể lọc theo loại (thực tế/dự kiến).
        Tìm kiếm ngược để lấy sự kiện cuối cùng (gần nhất)
        """
        if not location_keyword:
            logger.info("Bỏ qua _find_event vì location_keyword rỗng.")
            return {}

        logger.info("-> _find_event: Tìm '%s' tại '%s', type '%s'", description_keyword, location_keyword, event_type)
        for event in reversed(events):
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            event_location = event.get("location") or ""
            loc_match = location_keyword.lower() in event_location.lower()
            type_match = True
            if event_type:
                type_match = (event.get("type") == event_type)

            if desc_match and loc_match and type_match:
                logger.info("--> Khớp: %s", event)
                return event

        logger.info("--> Không khớp.")
        return {}