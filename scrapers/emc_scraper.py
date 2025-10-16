# scrapers/emc_scraper.py

import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper

class EmcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Evergreen (EMC)
    và chuẩn hóa kết quả theo template JSON yêu cầu.
    """

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày từ 'MON-DD-YYYY' (ví dụ: SEP-21-2025) sang 'DD/MM/YYYY'.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # Đảm bảo tháng viết hoa chữ cái đầu để parse
            date_str_title = date_str.strip().title()
            # Định dạng của EMC là %b-%d-%Y (ví dụ: Sep-21-2025)
            dt_obj = datetime.strptime(date_str_title, '%b-%d-%Y')
            return dt_obj.strftime('%d/%m/%Y')
        except (ValueError, IndexError):
            print(f"    [EMC Scraper] Cảnh báo: Không thể phân tích định dạng ngày: {date_str}")
            return date_str

    def scrape(self, tracking_number):
        """
        Phương thức scraping chính cho Evergreen.
        """
        print(f"[EMC Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        main_window = self.driver.current_window_handle
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)
            
            try:
                cookie_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btn_cookie_accept_all"))
                )
                cookie_button.click()
                print("[EMC Scraper] -> Đã chấp nhận cookies.")
                time.sleep(1) # Chờ cho banner biến mất
            except TimeoutException:
                print("[EMC Scraper] -> Banner cookie không xuất hiện hoặc đã được chấp nhận.")

            # --- 1. Thực hiện tìm kiếm ---
            print("[EMC Scraper] -> Điền thông tin tìm kiếm...")
            bl_radio = self.wait.until(EC.element_to_be_clickable((By.ID, "s_bl")))
            self.driver.execute_script("arguments[0].click();", bl_radio)

            search_input = self.driver.find_element(By.ID, "NO")
            search_input.clear()
            search_input.send_keys(tracking_number)

            submit_button = self.driver.find_element(By.CSS_SELECTOR, "#nav-quick > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(1) > td.ec-text-start > table > tbody > tr > td > div:nth-child(2) > input")
            submit_button.click()

            # --- 2. Chờ trang kết quả và trích xuất dữ liệu ---
            print("[EMC Scraper] -> Chờ trang kết quả tải...")
            self.wait.until(EC.visibility_of_element_located((By.XPATH, "//th[contains(text(), 'B/L No.')]")))
            print("[EMC Scraper] -> Trang kết quả đã tải. Bắt đầu trích xuất.")
            
            normalized_data = self._extract_and_normalize_data(tracking_number, main_window)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[EMC Scraper] -> Hoàn tất scrape thành công.")
            return normalized_data, None

        except TimeoutException:
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"
        finally:
            # Đảm bảo quay về cửa sổ chính
            if self.driver.current_window_handle != main_window:
                self.driver.close()
                self.driver.switch_to.window(main_window)


    def _extract_events_from_popup(self):
        """
        Trích xuất lịch sử di chuyển từ cửa sổ popup của container.
        """
        events = []
        try:
            # Chờ bảng trong popup xuất hiện
            event_table = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "table")))
            rows = event_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    events.append({
                        "date": cells[0].text.strip(),
                        "description": cells[1].text.strip(),
                        "location": cells[2].text.strip(),
                    })
        except (NoSuchElementException, TimeoutException):
            print("[EMC Scraper] Cảnh báo: Không tìm thấy bảng sự kiện trong popup.")
        return events

    def _find_event(self, events, desc_keyword, loc_keyword):
        """
        Tìm một sự kiện cụ thể trong danh sách, trả về sự kiện đầu tiên khớp.
        """
        if not events or not loc_keyword:
            return {}
        for event in events:
            desc_match = desc_keyword.lower() in event.get("description", "").lower()
            loc_match = loc_keyword.lower() in event.get("location", "").lower()
            if desc_match and loc_match:
                return event
        return {}

    def _extract_and_normalize_data(self, tracking_number, main_window):
        """
        Hàm chính để trích xuất dữ liệu từ trang kết quả và các popup.
        """
        try:
            # --- LẤY THÔNG TIN CƠ BẢN TỪ TRANG CHÍNH ---
            bl_number = self.driver.find_element(By.XPATH, "//th[contains(text(), 'B/L No.')]/following-sibling::td").text.strip()
            pol = self.driver.find_element(By.XPATH, "//th[contains(text(), 'Port of Loading')]/following-sibling::td").text.strip()
            pod = self.driver.find_element(By.XPATH, "//th[contains(text(), 'Port of Discharge')]/following-sibling::td").text.strip()
            etd_str = self.driver.find_element(By.XPATH, "//th[contains(text(), 'Estimated On Board Date')]/following-sibling::td").text.strip()
            eta_str = self.driver.find_element(By.XPATH, "//td[contains(., 'Estimated Date of Arrival at Destination')]/font").text.strip()

            # --- LẤY SỰ KIỆN TỪ TẤT CẢ CÁC CONTAINER ---
            all_events = []
            container_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'frmCntrMoveDetail')]")
            
            for link in container_links:
                link.click()
                # Chờ cửa sổ mới mở ra và chuyển sang nó
                self.wait.until(EC.number_of_windows_to_be(2))
                new_window = [window for window in self.driver.window_handles if window != main_window][0]
                self.driver.switch_to.window(new_window)

                events = self._extract_events_from_popup()
                all_events.extend(events)

                # Đóng cửa sổ popup và quay lại cửa sổ chính
                self.driver.close()
                self.driver.switch_to.window(main_window)
                time.sleep(0.5)

            # --- TÌM CÁC SỰ KIỆN QUAN TRỌNG ---
            atd_event = self._find_event(all_events, "Loaded", pol)
            ata_event = self._find_event(all_events, "Discharged", pod)

            # Tìm thông tin trung chuyển
            transit_ports = []
            ata_transit_event = None
            atd_transit_event = None
            for event in all_events:
                loc = event.get("location")
                desc = event.get("description", "").lower()
                if loc and pol not in loc and pod not in loc:
                    if "discharged" in desc:
                        if loc not in transit_ports: transit_ports.append(loc)
                        if not ata_transit_event: # Chỉ lấy sự kiện đầu tiên
                            ata_transit_event = event
                    elif "loaded on outbound vessel" in desc:
                        if loc not in transit_ports: transit_ports.append(loc)
                        atd_transit_event = event # Lấy sự kiện cuối cùng

            # --- TẠO JSON ĐẦU RA ---
            shipment_data = {
                "BookingNo": tracking_number,
                "BlNumber": bl_number,
                "BookingStatus": None,
                "Pol": pol,
                "Pod": pod,
                "Etd": self._format_date(etd_str),
                "Atd": self._format_date(atd_event.get("date")) if atd_event else None,
                "Eta": self._format_date(eta_str),
                "Ata": self._format_date(ata_event.get("date")) if ata_event else None,
                "TransitPort": ", ".join(transit_ports) if transit_ports else None,
                "EtdTransit": None, 
                "AtdTrasit": self._format_date(atd_transit_event.get("date")) if atd_transit_event else None,
                "EtaTransit": None,
                "AtaTrasit": self._format_date(ata_transit_event.get("date")) if ata_transit_event else None
            }
            return shipment_data
        except Exception as e:
            print(f"    [EMC Scraper] Lỗi trong quá trình trích xuất: {e}")
            traceback.print_exc()
            return None