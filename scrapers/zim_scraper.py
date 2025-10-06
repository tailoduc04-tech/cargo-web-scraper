import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import traceback
from .base_scraper import BaseScraper

class ZimScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang ZIM.
    Sử dụng phương pháp truy cập URL trực tiếp và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        try:
            # Xây dựng URL trực tiếp với mã B/L
            direct_url = f"{self.config['url']}?consnumber={tracking_number}"
            print(f"Accessing direct URL: {direct_url}")
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 45) # Tăng thời gian chờ cho ZIM

            # Chờ cho đến khi phần kết quả chính có thể nhìn thấy
            self.wait.until(EC.visibility_of_element_located((By.ID, "tracking-results-container")))
            time.sleep(2) # Chờ thêm một chút để trang ổn định

            # Mở rộng tất cả các chi tiết của container
            try:
                expand_buttons = self.wait.until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".tracking-result-routing-details-toggle"))
                )
                for button in expand_buttons:
                    # Dùng JavaScript để click vì đôi khi nút bị che
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(1) # Chờ cho animation mở rộng hoàn tất
                print(f"Expanded details for {len(expand_buttons)} container(s).")
            except TimeoutException:
                print("No container details to expand or toggle buttons not found.")
                # Kiểm tra xem có thông báo lỗi không
                try:
                    error_msg = self.driver.find_element(By.CSS_SELECTOR, ".tracking-error-container .tracking-error-message").text
                    return None, f"Không tìm thấy kết quả cho '{tracking_number}': {error_msg}"
                except NoSuchElementException:
                    # Nếu không có nút expand và cũng không có lỗi, có thể dữ liệu hiển thị sẵn
                     pass


            # Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data()

            if not normalized_data:
                 return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            # Đóng gói kết quả vào DataFrame để nhất quán với các scraper khác
            results_df = pd.DataFrame(normalized_data)
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/zim_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            print(f"An unexpected error occurred for '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ các chi tiết container trên trang ZIM.
        """
        all_shipments = []

        # Lấy thông tin tóm tắt chung từ header
        try:
            pol = self.driver.find_element(By.CSS_SELECTOR, ".tracking-result-summary-pol .tracking-result-summary-location-name").text
            pod = self.driver.find_element(By.CSS_SELECTOR, ".tracking-result-summary-pod .tracking-result-summary-location-name").text
        except NoSuchElementException:
            pol, pod = None, None
            print("Warning: Could not find POL/POD in summary.")


        # Lặp qua từng khối container đã được mở rộng
        container_blocks = self.driver.find_elements(By.CSS_SELECTOR, ".tracking-result-container")

        for container_block in container_blocks:
            try:
                events = self._extract_events_from_container(container_block)
                if not events:
                    continue

                # Tìm các sự kiện quan trọng từ lịch sử
                departure_event = self._find_event(events, "Vessel departure from Port of Loading", pol)
                arrival_event = self._find_event(events, "Vessel arrival to Port of Discharge", pod) # ZIM dùng "to Port of Discharge"

                # Xác định các cảng trung chuyển
                transit_ports = []
                for event in events:
                    location = event.get('location', '').strip()
                    desc = event.get('description', '').lower()
                    if "transshipment" in desc and location:
                        if location not in transit_ports:
                            transit_ports.append(location)

                # Xây dựng đối tượng JSON chuẩn hóa
                shipment_data = {
                    "POL": pol,
                    "POD": pod,
                    "transit_port": ", ".join(transit_ports) if transit_ports else None,
                    "ngay_tau_di": {
                        "ngay_du_kien": None, # ZIM không hiển thị rõ ngày dự kiến đi
                        "ngay_thuc_te": departure_event.get('date') if departure_event else None
                    },
                    "ngay_tau_den": {
                        "ngay_du_kien": arrival_event.get('date') if arrival_event and arrival_event.get('type') == 'ngay_du_kien' else None,
                        "ngay_thuc_te": arrival_event.get('date') if arrival_event and arrival_event.get('type') == 'ngay_thuc_te' else None
                    },
                    "lich_su": events
                }
                all_shipments.append(shipment_data)

            except Exception as e:
                print(f"Could not process a container block: {e}")
                traceback.print_exc()

        return all_shipments

    def _extract_events_from_container(self, container_element):
        """Trích xuất tất cả các sự kiện từ một khối container."""
        events = []
        try:
            rows = container_element.find_elements(By.CSS_SELECTOR, ".tracking-result-routing-details-container tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 4:
                    continue

                date_text = cells[1].text.strip()
                activity_text = cells[2].text.strip()
                location_text = cells[3].text.strip()

                # ZIM thường hiển thị ngày thực tế cho các sự kiện đã qua và ngày dự kiến cho các sự kiện tương lai
                # Giả định dựa trên class của thẻ `tr` hoặc `td` nếu có, ở đây ta sẽ mặc định là thực tế
                # và sẽ cập nhật nếu tìm thấy ngày tương lai.
                event_type = "ngay_thuc_te"
                try:
                    # Thử kiểm tra xem ngày có phải trong tương lai không
                    # Lưu ý: Cách này không hoàn toàn chính xác nếu không có chỉ báo rõ ràng từ trang web
                    event_date = datetime.strptime(date_text.split('\n')[0], '%d-%b-%Y')
                    if event_date > datetime.now():
                        event_type = "ngay_du_kien"
                except (ValueError, IndexError):
                     pass # Bỏ qua nếu không parse được ngày

                events.append({
                    "date": date_text.replace('\n', ' '),
                    "type": event_type,
                    "description": activity_text,
                    "location": location_text
                })
        except NoSuchElementException:
            print("Event table not found for a container.")
        return events

    def _find_event(self, events, description_keyword, location_keyword=None):
        """Tìm một sự kiện cụ thể trong danh sách các sự kiện."""
        # Tìm kiếm chính xác trước
        for event in events:
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = True
            if location_keyword:
                loc_match = location_keyword.lower() in event.get("location", "").lower()

            if desc_match and loc_match:
                return event
        
        # Nếu không thấy, thử tìm kiếm rộng hơn (không cần location)
        for event in events:
            if description_keyword.lower() in event.get("description", "").lower():
                return event

        return {}