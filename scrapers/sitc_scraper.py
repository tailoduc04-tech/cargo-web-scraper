import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback
import time

from .base_scraper import BaseScraper

class SitcScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang SITC.
    """

    def scrape(self, tracking_number):
        print(f"[SITC Scraper] Bắt đầu scrape cho mã: {tracking_number}")
        try:
            print("[SITC Scraper] 1. Điều hướng đến URL...")
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 30)

            print("[SITC Scraper] 2. Điền thông tin vào form tìm kiếm...")
            search_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "form.search-form input[placeholder='B/L No.']"))
            )
            search_input.clear()
            search_input.send_keys(tracking_number)
            print(f"-> Đã điền mã: {tracking_number}")

            search_button = self.driver.find_element(By.CSS_SELECTOR, "form.search-form button")
            self.driver.execute_script("arguments[0].click();", search_button)
            print("-> Đã nhấn nút tìm kiếm.")

            print("[SITC Scraper] 3. Chờ trang kết quả tải...")
            # Chờ một element đặc trưng của trang kết quả xuất hiện
            self.wait.until(
                EC.visibility_of_element_located((By.XPATH, "//h4[text()='Basic Information']"))
            )
            print(f"-> Trang kết quả cho '{tracking_number}' đã tải xong.")
            time.sleep(2)  # Chờ thêm để đảm bảo tất cả JS đã chạy xong

            print("[SITC Scraper] 4. Bắt đầu trích xuất và chuẩn hóa dữ liệu...")
            normalized_data = self._extract_and_normalize_data()

            if not normalized_data:
                print(f"[SITC Scraper] Lỗi: Không thể trích xuất dữ liệu cho '{tracking_number}'.")
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            print("[SITC Scraper] 5. Đóng gói và trả về kết quả thành công.")
            results_df = pd.DataFrame(normalized_data)
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            print(f"[SITC Scraper] Lỗi: TimeoutException xảy ra cho mã '{tracking_number}'.")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/sitc_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"  -> Đã lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"  -> Lỗi: Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' (Timeout)."
        except Exception as e:
            print(f"[SITC Scraper] Lỗi: Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Lỗi không xác định khi scrape '{tracking_number}': {e}"

    def _extract_and_normalize_data(self):
        """
        Trích xuất, xử lý và chuẩn hóa dữ liệu từ trang kết quả của SITC.
        """
        print("[SITC Scraper] --- Bắt đầu _extract_and_normalize_data ---")
        all_shipments = []
        try:
            # 1. Trích xuất thông tin cơ bản
            print("  -> Trích xuất 'Basic Information'...")
            basic_info_table = self.wait.until(EC.presence_of_element_located((By.XPATH, "//h4[text()='Basic Information']/following-sibling::div[contains(@class, 'el-table')]")))
            pol = basic_info_table.find_element(By.CSS_SELECTOR, "tbody tr td:nth-child(2)").text.strip()
            pod = basic_info_table.find_element(By.CSS_SELECTOR, "tbody tr td:nth-child(3)").text.strip()
            print(f"  -> Đã trích xuất POL: '{pol}', POD: '{pod}'")

            # 2. Trích xuất lịch trình và các sự kiện
            print("  -> Trích xuất 'Sailing Schedule Information'...")
            # Sửa lỗi XPath: h4 nằm trong 1 div, div đó là anh em với div chứa table
            schedule_table = self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[h4[contains(text(), 'Sailing Schedule')]]/following-sibling::div[contains(@class, 'el-table')]")))
            rows = schedule_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            print(f"  -> Tìm thấy {len(rows)} chặng trong lịch trình.")

            transit_ports = []
            lich_su = []
            
            for i, row in enumerate(rows):
                cells = row.find_elements(By.TAG_NAME, "td")
                leg_pol = cells[2].text.strip()
                schedule_etd = cells[3].text.strip()
                etd_atd = cells[4].text.strip()
                leg_pod = cells[5].text.strip()
                schedule_eta = cells[6].text.strip()
                eta_ata = cells[7].text.strip()

                lich_su.append({
                    "date": etd_atd if etd_atd else schedule_etd,
                    "type": "ngay_thuc_te" if "color: red" in cells[4].find_element(By.TAG_NAME, "span").get_attribute("style") else "ngay_du_kien",
                    "description": f"Vessel Departure from {leg_pol}",
                    "location": leg_pol
                })

                lich_su.append({
                    "date": eta_ata if eta_ata else schedule_eta,
                    "type": "ngay_thuc_te" if "color: red" in cells[7].find_element(By.TAG_NAME, "span").get_attribute("style") else "ngay_du_kien",
                    "description": f"Vessel Arrival at {leg_pod}",
                    "location": leg_pod
                })
                
                if i < len(rows) - 1:
                    transit_ports.append(leg_pod)
            
            print(f"  -> Đã xử lý {len(lich_su)} sự kiện lịch sử.")
            if transit_ports:
                print(f"  -> Cảng trung chuyển: {', '.join(transit_ports)}")

            # 3. Lấy thông tin ngày đi và ngày đến chính
            first_leg_cells = rows[0].find_elements(By.TAG_NAME, "td")
            last_leg_cells = rows[-1].find_elements(By.TAG_NAME, "td")

            ngay_tau_di_du_kien = first_leg_cells[3].text.strip()
            ngay_tau_di_thuc_te = first_leg_cells[4].text.strip() if "color: red" in first_leg_cells[4].find_element(By.TAG_NAME, "span").get_attribute("style") else None

            ngay_tau_den_du_kien = last_leg_cells[6].text.strip()
            ngay_tau_den_thuc_te = last_leg_cells[7].text.strip() if "color: red" in last_leg_cells[7].find_element(By.TAG_NAME, "span").get_attribute("style") else None
            
            print("  -> Đã xác định ngày đi/đến chính.")

            # 4. Xây dựng đối tượng JSON chuẩn hóa
            shipment_data = {
                "POL": pol,
                "POD": pod,
                "transit_port": ", ".join(list(dict.fromkeys(transit_ports))) if transit_ports else None,
                "ngay_tau_di": {
                    "ngay_du_kien": ngay_tau_di_du_kien,
                    "ngay_thuc_te": ngay_tau_di_thuc_te if ngay_tau_di_thuc_te else ngay_tau_di_du_kien # Fallback nếu không có ngày thực tế
                },
                "ngay_tau_den": {
                    "ngay_du_kien": ngay_tau_den_du_kien,
                    "ngay_thuc_te": ngay_tau_den_thuc_te # Sẽ là None nếu chưa đến
                },
                "lich_su": lich_su
            }
            all_shipments.append(shipment_data)
            print("[SITC Scraper] --- Hoàn tất _extract_and_normalize_data ---")

        except Exception as e:
            print(f"[SITC Scraper] Lỗi trong quá trình trích xuất dữ liệu: {e}")
            traceback.print_exc()
            return [] # Trả về list rỗng nếu có lỗi

        return all_shipments