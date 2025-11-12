import logging
import requests
import time
from datetime import datetime, date
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
from schemas import N8nTrackingInfo

logger = logging.getLogger(__name__)

class PilScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang web PIL (Pacific International Lines)
    bằng cách gọi API trực tiếp và chuẩn hóa kết quả đầu ra.
    Bao gồm gọi API lần 2 để lấy chi tiết container.
    """
    def __init__(self, driver, config):
        self.config = config
        self.get_n_url = "https://www.pilship.com/wp-content/themes/hello-theme-child-master/pil-api/common/get-n.php"
        self.track_url = "https://www.pilship.com/wp-content/themes/hello-theme-child-master/pil-api/trackntrace-containertnt.php"
        self.track_container_url = "https://www.pilship.com/wp-content/themes/hello-theme-child-master/pil-api/trackntrace-containertnt-trace.php?"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'www.pilship.com',
            'Sec-Ch-Ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        })
        self.timestamp = int(time.time() * 1000)


    def _format_date(self, date_str):
        """
        Chuyển đổi các định dạng ngày tháng ('DD-Mon-YYYY HH:MM:SS' hoặc 'DD-Mon-YYYY')
        sang 'DD/MM/YYYY'.
        Xử lý thêm dấu '*' ở đầu ngày Estimated.
        """
        if not date_str or not isinstance(date_str, str):
            return ""

        cleaned_date_str = date_str.strip().lstrip('*').strip() # Loại bỏ dấu * và khoảng trắng

        for fmt in ('%d-%b-%Y %H:%M:%S', '%d-%b-%Y'): # Thử cả 2 định dạng
            try:
                dt_obj = datetime.strptime(cleaned_date_str, fmt)
                return dt_obj.strftime('%d/%m/%Y')
            except ValueError:
                continue

        logger.warning("Không thể phân tích định dạng ngày: %s", date_str)
        return date_str # Trả về gốc nếu không parse được

    def _get_n_value(self, referer_url):
        """Lấy giá trị 'n' động từ API, sử dụng Referer được cung cấp."""
        t_get_n_start = time.time()
        try:
            current_timestamp_ms = self.timestamp
            params = {'timestamp': str(current_timestamp_ms)}
            self.session.headers.update({'Referer': referer_url}) # Cập nhật Referer

            logger.debug(f"Đang lấy 'n' từ: {self.get_n_url} với params: {params}")
            response = self.session.get(self.get_n_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            n_value = data.get('n')
            if n_value:
                 logger.info("Lấy được giá trị 'n' mới. (Thời gian: %.2fs)", time.time() - t_get_n_start)
                 return n_value
            else:
                 logger.error("Không tìm thấy key 'n' trong response từ get-n.php. Response: %s", data)
                 return None
        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error("Lỗi khi lấy giá trị 'n': %s (Thời gian: %.2fs)", e, time.time() - t_get_n_start, exc_info=True)
            return None

    def _get_container_details_html(self, tracking_number, container_no, referer_url):
        """Lấy HTML chi tiết sự kiện cho một container cụ thể."""
        logger.info(f"Đang lấy chi tiết cho container: {container_no}")
        # Lấy 'n' mới cho request này
        n_value = self._get_n_value(referer_url)
        if not n_value:
            logger.error(f"Không thể lấy 'n' cho chi tiết container {container_no}.")
            return None

        t_detail_start = time.time()
        try:
            current_timestamp_ms = self.timestamp
            params = {
                'module': 'TrackTraceJob',
                'reference_no': tracking_number,
                'cntr_no': container_no,
                'n': n_value,
                'timestamp': str(current_timestamp_ms)
            }
            # Referer đã được set trong _get_n_value
            logger.info(f"Đang gửi request chi tiết container: {self.track_container_url} với params: {params}")
            response = self.session.get(self.track_container_url, params=params, timeout=30)
            response.raise_for_status()
            logger.info("-> (Thời gian) Gọi API chi tiết container: %.2fs", time.time() - t_detail_start)

            data = response.json()
            if data.get("success") and "data" in data and isinstance(data["data"], str):
                return data["data"]
            else:
                logger.warning("API chi tiết container không trả về HTML hợp lệ. Response: %s", data)
                return None
        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error("Lỗi khi lấy chi tiết container %s: %s (Thời gian: %.2fs)", container_no, e, time.time() - t_detail_start, exc_info=True)
            return None

    def scrape(self, tracking_number):
        """
        Scrape dữ liệu bằng cách gọi API trực tiếp (bao gồm cả chi tiết container).
        """
        logger.info("--- [PIL API Scraper] Bắt đầu scrape cho mã: %s ---", tracking_number)
        t_total_start = time.time()
        referer_url = f'https://www.pilship.com/digital-solutions/?tab=customer&id=track-trace&label=containerTandT&module=TrackTraceJob&refNo={tracking_number}'

        # 1. Lấy giá trị 'n' đầu tiên
        n_value = self._get_n_value(referer_url)
        if not n_value:
            return None, "Không thể lấy token 'n' ban đầu."

        # 2. Gửi request tracking chính để lấy summary HTML
        summary_html = None
        soup_summary = None
        try:
            current_timestamp_ms = self.timestamp
            params = {
                'module': 'TrackTraceJob',
                'refNo': tracking_number,
                'n': n_value,
                'timestamp': str(current_timestamp_ms)
            }
            logger.info(f"Đang gửi request tracking chính đến: {self.track_url}")
            t_track_start = time.time()
            response = self.session.get(self.track_url, params=params, timeout=30)
            response.raise_for_status()
            logger.info("-> (Thời gian) Gọi API tracking chính: %.2fs", time.time() - t_track_start)

            data = response.json()
            if data.get("success") and "data" in data and isinstance(data["data"], str):
                summary_html = data["data"]
                soup_summary = BeautifulSoup(summary_html, 'lxml')
            else:
                logger.warning("API tracking chính không trả về dữ liệu HTML hợp lệ. Response: %s", data)
                error_message = data.get("message", "API tracking chính không trả về dữ liệu.")
                return None, f"Không tìm thấy kết quả cho '{tracking_number}': {error_message}"

        except (requests.exceptions.RequestException, ValueError) as e:
            logger.error("Lỗi khi gọi API tracking chính: %s", e, exc_info=True)
            return None, f"Lỗi kết nối khi tracking '{tracking_number}': {e}"
        except Exception as e:
             logger.error("Lỗi không mong muốn khi lấy summary HTML: %s", e, exc_info=True)
             return None, f"Lỗi không xác định khi lấy summary: {e}"

        if not soup_summary:
             return None, f"Không thể parse HTML tóm tắt cho '{tracking_number}'."

        # 3. Trích xuất thông tin cơ bản và container đầu tiên từ summary HTML
        t_extract_summary_start = time.time()
        basic_info = self._extract_summary_from_html(soup_summary)
        container_no = self._extract_first_container_no(soup_summary)
        logger.info("-> (Thời gian) Trích xuất summary HTML: %.2fs", time.time() - t_extract_summary_start)


        # 4. Lấy và xử lý chi tiết container (nếu tìm thấy container_no)
        all_events = []
        if container_no:
            detail_html_rows = self._get_container_details_html(tracking_number, container_no, referer_url)
            if detail_html_rows:
                 t_extract_events_start = time.time()
                 all_events = self._extract_events_from_detail_html(detail_html_rows)
                 logger.info("-> (Thời gian) Trích xuất events từ HTML chi tiết: %.2fs", time.time() - t_extract_events_start)

        # 5. Chuẩn hóa dữ liệu cuối cùng
        t_normalize_start = time.time()
        normalized_data = self._normalize_data(basic_info, all_events, tracking_number)
        logger.info("-> (Thời gian) Chuẩn hóa dữ liệu cuối cùng: %.2fs", time.time() - t_normalize_start)


        if not normalized_data:
            return None, f"Không thể chuẩn hóa dữ liệu cuối cùng cho '{tracking_number}'."

        t_total_end = time.time()
        logger.info("Hoàn tất scrape API thành công cho mã: %s (Tổng thời gian: %.2fs)",
                     tracking_number, t_total_end - t_total_start)
        return normalized_data, None


    def _extract_summary_from_html(self, soup):
        """Trích xuất dữ liệu tóm tắt từ bảng HTML đầu tiên."""
        summary_data = {'POL': '', 'POD': '', 'ETD': '', 'ETA': '', 'BookingNo': ''}
        try:
            # Lấy Booking Reference
            bkg_ref_p = soup.find('p', string=lambda text: text and 'Booking Reference:' in text)
            if bkg_ref_p and bkg_ref_p.find('b'):
                summary_data['BookingNo'] = bkg_ref_p.find('b').text.strip()

            summary_table = soup.find('div', class_='mypil-table').find('table')
            if not summary_table: return summary_data
            rows = summary_table.find_all('tr')
            if len(rows) < 2: return summary_data
            data_row = rows[1]
            cells = data_row.find_all('td')
            if len(cells) >= 4:
                arr_del_cell = cells[0]
                lines = [line.strip() for line in arr_del_cell.get_text(separator='\n').split('\n') if line.strip()]
                summary_data['ETD'] = lines[-1] if lines else ""
                loc_cell = cells[1]
                lines = [line.strip() for line in loc_cell.get_text(separator='\n').split('\n') if line.strip()]
                summary_data['POL'] = lines[1].split(',')[0].strip() if len(lines) > 1 else ""
                next_loc_cell = cells[3]
                lines = [line.strip() for line in next_loc_cell.get_text(separator='\n').split('\n') if line.strip()]
                summary_data['POD'] = lines[0].split(',')[0].strip() if len(lines) > 0 else ""
                summary_data['ETA'] = lines[-1] if lines else ""
        except Exception as e:
            logger.error("Lỗi khi trích xuất bảng tóm tắt HTML: %s", e, exc_info=True)
        return summary_data

    def _extract_first_container_no(self, soup):
         """Lấy số container đầu tiên từ bảng container."""
         try:
             container_table = soup.find('div', class_='mypil-table').find_next_sibling('div', class_='mypil-table')
             if container_table:
                 first_container_b = container_table.find('b', class_='cont-numb')
                 if first_container_b:
                     container_no = first_container_b.text.strip()
                     logger.info(f"Tìm thấy container đầu tiên: {container_no}")
                     return container_no
             logger.warning("Không tìm thấy số container trong HTML tóm tắt.")
             return None
         except Exception as e:
              logger.error("Lỗi khi tìm số container đầu tiên: %s", e, exc_info=True)
              return None

    def _extract_events_from_detail_html(self, html_rows_str):
        """Parse HTML các hàng sự kiện và trích xuất thông tin."""
        events = []
        try:
            # Cần tạo một soup mới chỉ với các hàng này, bao bọc bởi <table><tbody>
            soup_events = BeautifulSoup(f"<table><tbody>{html_rows_str}</tbody></table>", 'lxml')
            rows = soup_events.find_all('tr')
            logger.info(f"--> Phân tích {len(rows)} hàng sự kiện từ HTML chi tiết.")
            for row in rows:
                cells = row.find_all('td')
                # Bỏ qua header row (có thể kiểm tra bằng class hoặc nội dung)
                if not cells or 'mypil-tbody-no-top-border' in cells[0].get('class', []): continue

                if len(cells) >= 6: # Cần đủ 6 cột dữ liệu
                    event_date = cells[3].text.strip()
                    event_name = cells[4].text.strip()
                    event_location = cells[5].text.strip()
                    # Xác định type (Actual/Estimated) dựa vào dấu '*'
                    event_type = "Estimated" if event_date.startswith('*') else "Actual"

                    events.append({
                        "date": event_date,
                        "description": event_name,
                        "location": event_location,
                        "type": event_type # Thêm type để xử lý logic sau
                    })
                else:
                    logger.warning("--> Bỏ qua hàng sự kiện không đủ cột: %s", [c.text for c in cells])
        except Exception as e:
            logger.error("--> Lỗi khi parse HTML sự kiện chi tiết: %s", e, exc_info=True)
        return events

    def _find_event(self, events, description_keyword, location_keyword=None, event_type=None, find_first=False, find_last=False):
        """
        Tìm sự kiện trong danh sách (đã được sort nếu cần).
        event_type có thể là 'Actual' hoặc 'Estimated'.
        """
        matches = []
        if not events: return {}

        desc_keyword_lower = description_keyword.lower()
        loc_keyword_lower = location_keyword.lower().strip() if location_keyword else None

        logger.debug("--> _find_event: Tìm '%s' tại '%s', type '%s', first: %s, last: %s",
                     description_keyword, location_keyword, event_type, find_first, find_last)

        for event in events:
            desc_match = desc_keyword_lower in event.get("description", "").lower()
            loc_match = True
            if loc_keyword_lower:
                event_loc = (event.get("location") or "").lower().strip()
                loc_match = loc_keyword_lower in event_loc # Dùng 'in' để linh hoạt

            type_match = True
            if event_type:
                type_match = (event.get("type") == event_type)

            if desc_match and loc_match and type_match:
                matches.append(event)
                if find_first:
                    logger.debug("---> Khớp (first): %s", event)
                    return event # Trả về ngay khi tìm thấy đầu tiên

        if not matches:
             logger.debug("---> Không khớp.")
             return {}

        # Nếu không phải find_first, hoặc là find_last, trả về cái cuối
        logger.debug("---> Khớp (last/default): %s", matches[-1])
        return matches[-1]


    def _normalize_data(self, basic_info, all_events, tracking_number):
         """Chuẩn hóa dữ liệu từ thông tin cơ bản và danh sách sự kiện."""
         try:
            logger.info("Bắt đầu chuẩn hóa dữ liệu cuối cùng...")
            pol = basic_info.get("POL")
            pod = basic_info.get("POD")
            etd = basic_info.get("ETD") # Estimated từ summary
            eta = basic_info.get("ETA") # Estimated từ summary
            booking_no = basic_info.get("BookingNo") or tracking_number
            bl_number = booking_no # PIL dùng chung

            atd = ""
            ata = ""
            transit_ports = []
            ata_transit = ""
            atd_transit = ""
            eta_transit = ""
            etd_transit_final = ""
            future_etd_transits = []
            today = date.today()

            if all_events:
                # Sắp xếp sự kiện theo ngày để xử lý logic transit và first/last
                # Cần parse ngày trước khi sort
                for event in all_events:
                    cleaned_date_str = event.get('date', '').strip().lstrip('*').strip()
                    parsed_dt = None
                    for fmt in ('%d-%b-%Y %H:%M:%S', '%d-%b-%Y'):
                        try:
                            parsed_dt = datetime.strptime(cleaned_date_str, fmt)
                            break
                        except ValueError:
                            continue
                    event['parsed_datetime'] = parsed_dt or datetime.min # Gán datetime object hoặc min nếu lỗi

                sorted_events = sorted(all_events, key=lambda x: x['parsed_datetime'])

                # Tìm ATD (Actual Departure): Vessel Loading cuối cùng tại POL
                atd_event = self._find_event(sorted_events, "Vessel Loading", pol, event_type="Actual", find_last=True)
                atd = atd_event.get("date") if atd_event else ""

                # Tìm ATA (Actual Arrival): Vessel Discharge đầu tiên tại POD
                ata_event = self._find_event(sorted_events, "Vessel Discharge", pod, event_type="Actual", find_first=True)
                ata = ata_event.get("date") if ata_event else ""

                # Xử lý Transit
                logger.debug("Xử lý transit từ %d sự kiện...", len(sorted_events))
                processed_ports = set() # Tránh lặp lại cảng
                first_transit_arrival_event = None
                last_transit_departure_event = None

                for event in sorted_events:
                    loc = event.get("location")
                    desc = event.get("description", "").lower()
                    event_type = event.get("type")
                    event_date_str = event.get("date")

                    if not loc: continue
                    simple_loc = loc.split(',')[0].strip() # Lấy tên chính của cảng

                    is_pol = bool(pol and pol.lower() in loc.lower())
                    is_pod = bool(pod and pod.lower() in loc.lower())

                    if not is_pol and not is_pod: # Là cảng transit
                        if simple_loc not in processed_ports:
                             transit_ports.append(simple_loc)
                             processed_ports.add(simple_loc)
                             logger.debug("Tìm thấy cảng transit: %s", simple_loc)

                        # AtaTransit: Vessel Discharge đầu tiên (Actual) tại bất kỳ cảng transit nào
                        if "vessel discharge" in desc and event_type == "Actual":
                            if first_transit_arrival_event is None:
                                first_transit_arrival_event = event
                                logger.debug("Tìm thấy AtaTransit event đầu tiên: %s", event)

                        # AtdTransit: Vessel Loading cuối cùng (Actual) tại bất kỳ cảng transit nào
                        if "vessel loading" in desc and event_type == "Actual":
                            last_transit_departure_event = event # Ghi đè để lấy cái cuối cùng
                            logger.debug("Cập nhật AtdTransit event cuối cùng: %s", event)

                        # EtdTransit: Tìm các Vessel Loading (Estimated) trong tương lai
                        if "vessel loading" in desc and event_type == "Estimated" and event['parsed_datetime'] != datetime.min:
                             if event['parsed_datetime'].date() > today:
                                 future_etd_transits.append((event['parsed_datetime'].date(), simple_loc, event_date_str))
                                 logger.debug("Thêm ETD transit trong tương lai: %s (%s)", event_date_str, simple_loc)

                ata_transit = first_transit_arrival_event.get("date") if first_transit_arrival_event else ""
                atd_transit = last_transit_departure_event.get("date") if last_transit_departure_event else ""

                if future_etd_transits:
                    future_etd_transits.sort()
                    etd_transit_final = future_etd_transits[0][2]
                    logger.info("ETD transit gần nhất trong tương lai được chọn: %s", etd_transit_final)
                else:
                    logger.info("Không tìm thấy ETD transit nào trong tương lai.")

                # EtaTransit không có thông tin rõ ràng, để trống
                eta_transit = ""

            # --- Tạo đối tượng kết quả ---
            shipment_data = N8nTrackingInfo(
                BookingNo= booking_no.strip(),
                BlNumber= bl_number.strip(),
                BookingStatus= "", # Không có trạng thái tổng quát
                Pol= pol.strip() if pol else "",
                Pod= pod.strip() if pod else "",
                Etd= self._format_date(etd) or "", # ETD từ summary
                Atd= self._format_date(atd) or "", # ATD từ event
                Eta= self._format_date(eta) or "", # ETA từ summary
                Ata= self._format_date(ata) or "", # ATA từ event
                TransitPort= ", ".join(transit_ports) if transit_ports else "",
                EtdTransit= self._format_date(etd_transit_final) or "",
                AtdTransit= self._format_date(atd_transit) or "",
                EtaTransit= self._format_date(eta_transit) or "",
                AtaTransit= self._format_date(ata_transit) or ""
            )
            logger.info("Đã chuẩn hóa dữ liệu thành công.")
            return shipment_data

         except Exception as e:
             logger.error("Lỗi khi chuẩn hóa dữ liệu: %s", e, exc_info=True)
             return None