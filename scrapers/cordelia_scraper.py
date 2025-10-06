import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import traceback

from .base_scraper import BaseScraper

class CordeliaScraper(BaseScraper):
    """
    Triển khai logic scraping cụ thể cho trang Cordelia Line và chuẩn hóa kết quả.
    """

    def scrape(self, tracking_number):
        try:
            # URL đã chứa sẵn phần query, ta chỉ cần nối mã B/L vào cuối
            url = f"{self.config['url']}{tracking_number}"
            self.driver.get(url)
            self.wait = WebDriverWait(self.driver, 30)

            # Đợi cho bảng kết quả được JavaScript tải và hiển thị
            # Trang web có một div id="loader" sẽ biến mất khi tải xong
            self.wait.until(EC.invisibility_of_element_located((By.ID, "loader")))
            
            # Sau đó, đợi bảng dữ liệu chính xuất hiện
            result_table = self.wait.until(
                EC.visibility_of_element_located((By.ID, "checkShedTable"))
            )
            print("Cordelia: Trang kết quả đã tải và bảng dữ liệu đã hiển thị.")

            # Trích xuất và chuẩn hóa dữ liệu
            normalized_data = self._extract_and_normalize_data(result_table)

            if not normalized_data:
                return None, f"Không thể trích xuất dữ liệu đã chuẩn hóa cho '{tracking_number}'."

            # Trả về kết quả theo định dạng chuẩn của dự án
            results_df = pd.DataFrame(normalized_data)
            results = {
                "tracking_info": results_df
            }
            return results, None

        except TimeoutException:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = f"output/cordelia_timeout_{tracking_number}_{timestamp}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Đang lưu ảnh chụp màn hình vào {screenshot_path}")
            except Exception as ss_e:
                print(f"Không thể lưu ảnh chụp màn hình: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}' hoặc trang web không phản hồi."
        except Exception as e:
            print(f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}")
            traceback.print_exc()
            return None, f"Đã xảy ra lỗi không mong muốn cho '{tracking_number}': {e}"

    def _extract_and_normalize_data(self, table):
        """
        Trích xuất và chuẩn hóa dữ liệu từ bảng kết quả.
        """
        all_shipments = []
        try:
            # Tìm tất cả các hàng (tr) trong phần thân (tbody) của bảng
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            if not rows:
                print("Cordelia: Không tìm thấy hàng dữ liệu nào trong bảng.")
                return []

            # Giả định mỗi hàng là một lô hàng (thường chỉ có một)
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 9:
                    continue

                # Ánh xạ dữ liệu từ các ô trong bảng sang các trường yêu cầu
                pol = cells[1].text.strip()
                pod = cells[5].text.strip()  # FPOD - Final Port of Discharge
                sob_date = cells[4].text.strip()  # SOB Date - Ngày tàu đi thực tế
                eta_fpod = cells[6].text.strip()  # ETA FPOD - Ngày tàu đến dự kiến

                # Trang web không cung cấp ngày đi dự kiến và ngày đến thực tế.
                # Cảng trung chuyển (transit port) không được liệt kê rõ ràng,
                # nhưng có thể suy ra nếu có nhiều chặng (leg).
                # Dựa trên JS, nếu POD ban đầu khác FPOD, thì đó là cảng trung chuyển.
                # Vì chúng ta không có dữ liệu gốc từ AJAX, ta sẽ tạm để trống.
                transit_port = None
                
                # Tạo cấu trúc dữ liệu chuẩn hóa
                shipment_data = {
                    "POL": pol,
                    "POD": pod,
                    "transit_port": transit_port,
                    "ngay_tau_di": {
                        "ngay_du_kien": None,
                        "ngay_thuc_te": sob_date if sob_date else None
                    },
                    "ngay_tau_den": {
                        "ngay_du_kien": eta_fpod if eta_fpod else None,
                        "ngay_thuc_te": None 
                    },
                    "lich_su": [] # Không có lịch sử chi tiết trên trang này
                }
                all_shipments.append(shipment_data)
                
            return all_shipments

        except Exception as e:
            print(f"    Cảnh báo: Không thể phân tích bảng kết quả của Cordelia: {e}")
            traceback.print_exc()
            return []