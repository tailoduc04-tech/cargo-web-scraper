import logging
import requests
import json
import time
from datetime import datetime, date

from ..api_scraper import ApiScraper
from schemas import N8nTrackingInfo

# Lấy logger cho module
logger = logging.getLogger(__name__)

class OneScraper(ApiScraper):
    # Triển khai logic scraping cho Ocean Network Express (ONE) bằng API trực tiếp và chuẩn hóa kết quả theo định dạng yêu cầu.

    def __init__(self, driver, config):
        super().__init__(config=config)
        self.search_url = self.config.get('search_url', 'https://ecomm.one-line.com/api/v1/edh/containers/track-and-trace/search')
        self.events_url = self.config.get('events_url', 'https://ecomm.one-line.com/api/v1/edh/containers/track-and-trace/cop-events')
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://ecomm.one-line.com',
            'Referer': 'https://ecomm.one-line.com/one-ecom/manage-shipment/cargo-tracking', # Giả lập trang gốc
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            # Các headers khác có thể cần thiết, ví dụ Authorization hoặc Cookies
        })

    def _format_date(self, date_str):
        # Chuyển đổi chuỗi ngày từ API format 'YYYY-MM-DDTHH:MM:SS.sssZ' sang 'DD/MM/YYYY'. Trả về "" nếu lỗi.
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            # Lấy phần ngày tháng năm, bỏ qua phần giờ và timezone
            date_part = date_str.split('T')[0]
            if not date_part:
                return ""
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            logger.warning("[ONE API Scraper] Không thể phân tích định dạng ngày: %s", date_str)
            return "" # Trả về chuỗi rỗng nếu lỗi

    def scrape(self, tracking_number):
        # Phương thức scrape chính cho ONE bằng API. Thực hiện 2 request: lấy container no và lấy events.
        logger.info("[ONE API Scraper] Bắt đầu scrape cho mã: %s", tracking_number)
        t_total_start = time.time()
        container_no = None
        search_data = None
        events_data = None

        # --- Request 1: Lấy thông tin cơ bản và container number ---
        try:
            current_timestamp_ms = int(time.time() * 1000)
            payload_req1 = {
                "page": 1,
                "page_length": 10, # Lấy tối đa 10 container
                "filters": {
                    "search_text": tracking_number,
                    "search_type": "BKG_NO" # Giả định input luôn là Booking No
                },
                "timestamp": current_timestamp_ms
            }
            logger.info(f"[ONE API Scraper] Gửi POST request đến: {self.search_url}")
            t_req1_start = time.time()
            response_req1 = self.session.post(self.search_url, json=payload_req1, timeout=30)
            logger.info("-> (Thời gian) Gọi API Search: %.2fs", time.time() - t_req1_start)
            response_req1.raise_for_status()
            search_data = response_req1.json()

            # Trích xuất containerNo từ container đầu tiên
            if search_data.get("data") and isinstance(search_data["data"], list) and len(search_data["data"]) > 0:
                first_container_info = search_data["data"][0]
                container_no = first_container_info.get("containerNo")
                if container_no:
                    logger.info("[ONE API Scraper] Đã trích xuất containerNo: %s", container_no)
                else:
                    logger.warning("[ONE API Scraper] Không tìm thấy 'containerNo' trong dữ liệu container đầu tiên.")
                    # Vẫn tiếp tục thử lấy dữ liệu chung nếu có thể
            else:
                logger.warning("[ONE API Scraper] Response Search không chứa danh sách 'data' hợp lệ hoặc danh sách rỗng.")

        except requests.exceptions.Timeout:
            logger.error("[ONE API Scraper] Request Search bị timeout.")
            return None, f"Request timeout khi tìm kiếm thông tin ban đầu cho '{tracking_number}'."
        except requests.exceptions.HTTPError as e:
            logger.error(f"[ONE API Scraper] Lỗi HTTP (Search): {e.response.status_code} - {e.response.reason}. Response: {e.response.text}")
            return None, f"Lỗi HTTP {e.response.status_code} khi tìm kiếm '{tracking_number}'."
        except requests.exceptions.RequestException as e:
            logger.error(f"[ONE API Scraper] Lỗi Request (Search): {e}", exc_info=True)
            return None, f"Lỗi kết nối khi tìm kiếm '{tracking_number}': {e}"
        except json.JSONDecodeError:
            logger.error("[ONE API Scraper] Không thể parse JSON từ Response Search. Response text: %s", response_req1.text)
            return None, f"API Response (Search) không phải JSON hợp lệ cho '{tracking_number}'."
        except Exception as e:
            logger.error(f"[ONE API Scraper] Lỗi không xác định (Search): {e}", exc_info=True)
            return None, f"Lỗi không xác định khi tìm kiếm '{tracking_number}': {e}"

        # --- Request 2: Lấy thông tin Events (nếu có container_no) ---
        if container_no:
            try:
                params_req2 = {
                    "booking_no": tracking_number,
                    "container_no": container_no
                }
                logger.info(f"[ONE API Scraper] Gửi GET request đến: {self.events_url} với params: {params_req2}")
                t_req2_start = time.time()
                response_req2 = self.session.get(self.events_url, params=params_req2, timeout=30)
                logger.info("-> (Thời gian) Gọi API Events: %.2fs", time.time() - t_req2_start)
                response_req2.raise_for_status()
                events_data = response_req2.json()

            except requests.exceptions.Timeout:
                logger.warning("[ONE API Scraper] Request Events bị timeout. Sẽ chỉ xử lý dữ liệu từ request Search.")
                events_data = None # Đặt lại để xử lý bên dưới
            except requests.exceptions.HTTPError as e:
                logger.warning(f"[ONE API Scraper] Lỗi HTTP (Events): {e.response.status_code} - {e.response.reason}. Sẽ chỉ xử lý dữ liệu từ request Search. Response: {e.response.text}")
                events_data = None
            except requests.exceptions.RequestException as e:
                logger.warning(f"[ONE API Scraper] Lỗi Request (Events): {e}. Sẽ chỉ xử lý dữ liệu từ request Search.", exc_info=True)
                events_data = None
            except json.JSONDecodeError:
                logger.warning("[ONE API Scraper] Không thể parse JSON từ Response Events. Sẽ chỉ xử lý dữ liệu từ request Search. Response text: %s", response_req2.text)
                events_data = None
            except Exception as e:
                logger.warning(f"[ONE API Scraper] Lỗi không xác định (Events): {e}. Sẽ chỉ xử lý dữ liệu từ request Search.", exc_info=True)
                events_data = None
        else:
            logger.warning("[ONE API Scraper] Không có container_no, bỏ qua request lấy Events.")

        # --- BƯỚC 3: Trích xuất và chuẩn hóa ---
        if search_data:
            t_extract_start = time.time()
            normalized_data = self._extract_and_normalize_data_api(search_data, events_data, tracking_number)
            logger.info("-> (Thời gian) Trích xuất và chuẩn hóa: %.2fs", time.time() - t_extract_start)

            if not normalized_data:
                logger.warning(f"[ONE API Scraper] Không thể chuẩn hóa dữ liệu cho '{tracking_number}'.")
                return None, f"Không thể chuẩn hóa dữ liệu từ API cho '{tracking_number}'."

            t_total_end = time.time()
            logger.info("[ONE API Scraper] Hoàn tất scrape thành công. (Tổng thời gian: %.2fs)", t_total_end - t_total_start)
            return normalized_data, None
        else:
             # Trường hợp request 1 lỗi ngay từ đầu
             return None, f"Không lấy được dữ liệu ban đầu cho '{tracking_number}' từ API."


    def _extract_and_normalize_data_api(self, search_response, events_response, booking_no_input):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ response JSON của API ONE.
        """
        logger.info("[ONE API Scraper] --- Bắt đầu _extract_and_normalize_data_api ---")
        t_extract_detail_start = time.time()
        try:
            # --- Lấy thông tin từ search_response (Request 1) ---
            if not search_response or not search_response.get("data"):
                logger.error("[ONE API Scraper] Dữ liệu từ Search API không hợp lệ.")
                return None

            # Lấy thông tin từ container đầu tiên
            first_container_data = search_response["data"][0]
            bl_number = booking_no_input # ONE dùng Booking No làm key chính
            booking_no = booking_no_input
            pol_info = first_container_data.get("por", {}) # Dùng Port of Receipt làm POL
            pod_info = first_container_data.get("pod", {}) # Port of Discharge
            pol = pol_info.get("locationName", "")
            pod = pod_info.get("locationName", "")

            # Booking Status lấy từ latestEvent
            latest_event = first_container_data.get("latestEvent", {})
            booking_status = latest_event.get("eventName", "")

            logger.info(f"[ONE API Scraper] -> Thông tin cơ bản: BKG='{booking_no}', BL='{bl_number}', POL='{pol}', POD='{pod}', Status='{booking_status}'")
            logger.debug("-> (Thời gian) Trích xuất thông tin cơ bản: %.2fs", time.time() - t_extract_detail_start) # Log tạm thời gian ở đây

            # --- Xử lý sự kiện từ events_response (Request 2) nếu có ---
            t_event_start = time.time()
            etd, atd, eta, ata = "", "", "", ""
            etd_transit_final, atd_transit, transit_port_list, eta_transit, ata_transit = "", "", [], "", ""
            future_etd_transits = []
            today = date.today()

            all_events = []
            if events_response and events_response.get("data") and isinstance(events_response["data"], list):
                 all_events = events_response["data"]
                 logger.info("[ONE API Scraper] Tìm thấy %d sự kiện từ API Events.", len(all_events))
                 # Sắp xếp sự kiện theo thời gian tăng dần (cũ -> mới) dựa vào eventDate
                 all_events.sort(key=lambda x: x.get("eventDate", ""))
            else:
                 logger.warning("[ONE API Scraper] Không có dữ liệu sự kiện chi tiết từ API Events.")
                 # Thử lấy ETD/ATD, ETA/ATA từ Search API (cargoEvents) nếu không có events chi tiết
                 cargo_events = first_container_data.get("cargoEvents", [])
                 pol_code = pol_info.get("code")
                 pod_code = pod_info.get("code")

                 for event in cargo_events:
                      loc_code = event.get("locationName", "").split(',')[0].strip() # Lấy tên chính
                      event_trigger = event.get("trigger", "").upper()
                      event_date = event.get("localPortDate") or event.get("date")
                      event_id = event.get("matrixId", "") # E061: Depart, E089: Arrive, E105: Discharge

                      if pol in loc_code and event_id == "E061": # Depart POL
                           if event_trigger == "ACTUAL": atd = event_date
                           else: etd = event_date
                      elif pod in loc_code and (event_id == "E089" or event_id == "E105"): # Arrive/Discharge POD
                           if event_trigger == "ACTUAL": ata = event_date
                           else: eta = event_date
                 logger.warning("[ONE API Scraper] Đã thử lấy ETD/ATD/ETA/ATA từ Search API.")


            # Xử lý logic sự kiện nếu có all_events từ API Events
            if all_events:
                logger.debug("[ONE API Scraper] Bắt đầu xử lý logic sự kiện và transit từ API Events...")
                atd_transit_found = None
                ata_transit_found = None
                eta_transit_found = None

                for event in all_events:
                    loc_info = event.get("location", {})
                    loc_name = loc_info.get("locationName", "")
                    loc_name_lower = loc_name.lower()
                    desc = event.get("eventName", "").lower()
                    date_str = event.get("eventLocalPortDate") or event.get("eventDate") # Ưu tiên local time
                    date_type = event.get("triggerType", "").upper()

                    is_pol = pol.lower() in loc_name_lower
                    is_pod = pod.lower() in loc_name_lower

                    # --- Xử lý POL ---
                    if is_pol:
                        if "vessel departure" in desc or "loaded on vessel" in desc:
                            if date_type == "ACTUAL": atd = date_str
                            else: etd = date_str

                    # --- Xử lý POD ---
                    elif is_pod:
                        if "vessel arrival" in desc or "unloaded from vessel" in desc:
                            if date_type == "ACTUAL": ata = date_str
                            else: eta = date_str

                    # --- Xử lý Transit ---
                    else:
                        if loc_name and ( "vessel arrival" in desc or "unloaded from vessel" in desc or "vessel departure" in desc or "loaded on vessel" in desc):
                            if loc_name not in transit_port_list:
                                transit_port_list.append(loc_name)
                                logger.debug("Tìm thấy cảng transit: %s", loc_name)

                        if "vessel arrival" in desc or "unloaded from vessel" in desc:
                            if date_type == "ACTUAL" and not ata_transit_found:
                                ata_transit_found = date_str
                                logger.debug("Tìm thấy AtaTransit đầu tiên: %s", date_str)
                            elif date_type == "ESTIMATED" and not ata_transit_found and not eta_transit_found:
                                eta_transit_found = date_str
                                logger.debug("Tìm thấy EtaTransit đầu tiên: %s", date_str)

                        if "vessel departure" in desc or "loaded on vessel" in desc:
                            if date_type == "ACTUAL":
                                atd_transit_found = date_str # Lấy cái cuối cùng
                                logger.debug("Cập nhật AtdTransit cuối cùng: %s", date_str)
                            elif date_type == "ESTIMATED":
                                try:
                                    # Parse ngày YYYY-MM-DD từ chuỗi date_str
                                    etd_date = datetime.strptime((date_str or "").split('T')[0], '%Y-%m-%d').date()
                                    if etd_date > today:
                                        future_etd_transits.append((etd_date, loc_name, date_str))
                                        logger.debug("Thêm ETD transit trong tương lai: %s (%s)", date_str, loc_name)
                                except (ValueError, IndexError, AttributeError):
                                    logger.warning("Không thể parse ETD transit: %s", date_str)

                # Xử lý kết quả transit
                if future_etd_transits:
                    future_etd_transits.sort()
                    etd_transit_final = future_etd_transits[0][2] # Lấy chuỗi ngày
                    logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
                else:
                    logger.info("Không tìm thấy ETD transit nào trong tương lai.")

                atd_transit = atd_transit_found
                eta_transit = eta_transit_found
                ata_transit = ata_transit_found

            logger.debug("-> (Thời gian) Xử lý sự kiện và transit: %.2fs", time.time() - t_event_start)

            # === BƯỚC 4: TẠO ĐỐI TƯỢNG JSON CHUẨN HÓA ===
            t_normalize_start = time.time()
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no.strip(),
                BlNumber= bl_number.strip(),
                BookingStatus= booking_status.strip(),
                Pol= pol.strip() or "",
                Pod= pod.strip() or "",
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
            logger.info("[ONE API Scraper] Đã tạo đối tượng N8nTrackingInfo thành công.")
            logger.debug("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)
            logger.info("[ONE API Scraper] --- Hoàn tất _extract_and_normalize_data_api --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return shipment_data

        except Exception as e:
            # Log lỗi cụ thể khi trích \\\\\\//////xuất
            logger.error("[ONE API Scraper] Lỗi trong quá trình trích xuất chi tiết từ API cho mã '%s': %s", booking_no_input, e, exc_info=True)
            logger.info("[ONE API Scraper] --- Hoàn tất _extract_and_normalize_data_api (lỗi) --- (Tổng thời gian trích xuất: %.2fs)", time.time() - t_extract_detail_start)
            return None