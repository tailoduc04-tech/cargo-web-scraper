import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper

class OneScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Ocean Network Express (ONE)
    và chuẩn hóa kết quả theo định dạng JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'YYYY-MM-DD HH:MM' sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Lấy phần ngày, bỏ qua phần giờ
            date_part = date_str.split(' ')[0]
            dt_obj = datetime.strptime(date_part, '%Y-%m-%d')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            # Trả về chuỗi gốc nếu không parse được
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scrape chính cho ONE. Truy cập URL, nhấn nút mở rộng,
        chờ dữ liệu động tải và trích xuất thông tin.
        """
        print(f"[ONE Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            # Xây dựng URL trực tiếp từ cấu hình và tracking number
            direct_url = f'{self.config['url']}{tracking_number}'
            print(f"[ONE Scraper] -> Đang truy cập URL: {direct_url}")
            self.driver.get(direct_url)
            self.wait = WebDriverWait(self.driver, 45)

            # 1. Chờ cho container chính của bảng kết quả xuất hiện
            print("[ONE Scraper] -> Chờ bảng kết quả chính tải...")
            self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.mb-20"))
            )
            time.sleep(2) # Chờ thêm để các thành phần JS khác ổn định

            # 2. Tìm và nhấn vào tất cả các nút mũi tên để mở rộng chi tiết
            print("[ONE Scraper] -> Tìm và nhấn các nút mở rộng chi tiết...")
            # Selector này nhắm vào icon mũi tên bên trong mỗi hàng
            expand_buttons = self.driver.find_elements(By.CSS_SELECTOR, "div.relative.cursor-pointer")
            if not expand_buttons:
                print("[ONE Scraper] Cảnh báo: Không tìm thấy nút mở rộng nào.")
            
            for i, button in enumerate(expand_buttons):
                try:
                    # Dùng JavaScript để click, đáng tin cậy hơn
                    self.driver.execute_script("arguments[0].click();", button)
                    print(f"[ONE Scraper] -> Đã nhấn nút mở rộng #{i+1}")
                    time.sleep(1) # Chờ một chút cho animation
                    break
                except Exception as e:
                    print(f"[ONE Scraper] Cảnh báo: Không thể nhấn nút mở rộng #{i+1}: {e}")

            # 3. Chờ cho phần thông tin chi tiết của container đầu tiên được hiển thị
            print("[ONE Scraper] -> Chờ dữ liệu chi tiết hiển thị...")
            self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.SailingInformation_sailing-table-wrap__PL8Px"))
            )
            print("[ONE Scraper] -> Dữ liệu chi tiết đã hiển thị.")

            # 4. Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data(tracking_number)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[ONE Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            print(f"[ONE Scraper] Lỗi: TimeoutException xảy ra cho mã '{tracking_number}'.")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/one_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"  -> Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"  -> Lỗi: Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            print(f"[ONE Scraper] Lỗi: Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, tracking_number):
        """
        Trích xuất dữ liệu từ các phần đã được tải trên trang và ánh xạ vào template JSON.
        Logic này được giữ nguyên vì nó hoạt động trên cấu trúc HTML chi tiết.
        """
        try:
            print("[ONE Scraper] --- Bắt đầu trích xuất và chuẩn hóa dữ liệu ---")
            
            # --- 1. Trích xuất từ bảng "Sailing Information" ---
            sailing_table = self.driver.find_element(By.ID, "sailing-table-wrap")
            
            pol = sailing_table.find_element(By.CSS_SELECTOR, "div.SailingTable_port-of-loading-td__nnGHt").text
            pod = sailing_table.find_element(By.CSS_SELECTOR, "div.SailingTable_port-of-discharge-td__kSfvf").text
            atd_raw = sailing_table.find_element(By.CSS_SELECTOR, "div.SailingTable_departure-date-td__ABo6E").text.replace('\n', ' ')
            eta_raw = sailing_table.find_element(By.CSS_SELECTOR, "div.SailingTable_arrival-time-td__hWCoE").text.replace('\n', ' ')

            # --- 2. Trích xuất từ lịch sử sự kiện ---
            all_events = []
            # Tìm trong khu vực chi tiết đã mở rộng
            detail_container = self.driver.find_element(By.ID, "shipment-detail-container-id")
            event_rows = detail_container.find_elements(By.CSS_SELECTOR, "tr.EventTable_table-row__yVKnA")
            
            for row in event_rows:
                try:
                    location = row.find_element(By.CSS_SELECTOR, "div.EventTable_country-name__fax8l").text
                    description = row.find_element(By.CSS_SELECTOR, ".EventTable_event-name-vessel-group__sDbkT > div").text
                    date_time = row.find_element(By.CSS_SELECTOR, ".EventTable_actual-estimate-schedule__dlQ1t").text.replace('\n', ' ')
                    
                    all_events.append({
                        "location": location.strip(),
                        "description": description.strip(),
                        "date": date_time.strip()
                    })
                except NoSuchElementException:
                    continue

            # Tìm các sự kiện quan trọng
            ata_event = self._find_event(all_events, "Vessel Arrival at Port of Discharge", pod)
            
            transit_ports = []
            ata_transit_event = None
            atd_transit_event = None
            
            for event in all_events:
                loc = event.get("location", "")
                desc = event.get("description", "").lower()
                is_pol = pol and pol.lower() in loc.lower()
                is_pod = pod and pod.lower() in loc.lower()

                if loc and not is_pol and not is_pod:
                    if "unloaded from vessel" in desc or "vessel arrival" in desc:
                        if loc not in transit_ports: transit_ports.append(loc)
                        if not ata_transit_event: 
                            ata_transit_event = event
                    
                    if "loaded on vessel" in desc:
                         if loc not in transit_ports: transit_ports.append(loc)
                         atd_transit_event = event

            # --- 3. Xây dựng đối tượng JSON cuối cùng ---
            shipment_data = {
                "BookingNo": tracking_number,
                "BlNumber": tracking_number,
                "BookingStatus": None,
                "Pol": pol,
                "Pod": pod,
                "Etd": None,
                "Atd": self._format_date(atd_raw),
                "Eta": self._format_date(eta_raw),
                "Ata": self._format_date(ata_event.get("date")) if ata_event else None,
                "TransitPort": ", ".join(transit_ports) if transit_ports else None,
                "EtdTransit": None,
                "AtdTrasit": self._format_date(atd_transit_event.get("date")) if atd_transit_event else None,
                "EtaTransit": None,
                "AtaTrasit": self._format_date(ata_transit_event.get("date")) if ata_transit_event else None
            }
            
            print("[ONE Scraper] --- Hoàn tất, đã chuẩn hóa dữ liệu ---")
            return shipment_data

        except Exception as e:
            print(f"[ONE Scraper] Lỗi trong quá trình trích xuất chi tiết: {e}")
            traceback.print_exc()
            return None

    def _find_event(self, events, description_keyword, location_keyword):
        """
        Tìm một sự kiện cụ thể trong danh sách, ưu tiên sự kiện gần nhất.
        """
        if not events or not location_keyword:
            return {}
        
        for event in reversed(events):
            desc_match = description_keyword.lower() in event.get("description", "").lower()
            loc_match = location_keyword.lower() in event.get("location", "").lower()

            if desc_match and loc_match:
                return event
        return {}