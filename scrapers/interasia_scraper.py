import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from .base_scraper import BaseScraper

class InterasiaScraper(BaseScraper):
    """Triển khai logic scraping cụ thể cho trang Interasia."""
    
    def scrape(self, tracking_number):
        try:
            self.driver.get(self.config['url'])
            self.wait = WebDriverWait(self.driver, 15)

            # --- Logic tìm kiếm chính ---
            search_input = self.wait.until(EC.presence_of_element_located((By.NAME, "query")))
            search_input.clear()
            search_input.send_keys(tracking_number)
            self.driver.find_element(By.CSS_SELECTOR, "#containerSumbit").click()
            
            # Chờ cho đến khi bảng kết quả xuất hiện
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "m-table-group")))
            
            main_results_table = self.driver.find_element(By.CLASS_NAME, "m-table-group")
            rows = main_results_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            if not rows or "no data" in rows[0].text.lower():
                return None, f"No data found for '{tracking_number}' on the main page."

            # --- Trích xuất dữ liệu từ bảng chính ---
            headers = ["B/L No", "Container No", "Event Date", "Depot", "Port", "Event Description", "Voyage", "Vessel Name", "Vessel Code", "B/L Detail URL", "Container History URL"]
            scraped_data = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = [cell.text.strip().replace('\n', ' ') for cell in cells]
                try: row_data.append(cells[0].find_element(By.TAG_NAME, 'a').get_attribute('href'))
                except (NoSuchElementException, IndexError): row_data.append(None)
                try: row_data.append(cells[1].find_element(By.TAG_NAME, 'a').get_attribute('href'))
                except (NoSuchElementException, IndexError): row_data.append(None)
                scraped_data.append(row_data)

            main_df = pd.DataFrame(scraped_data, columns=headers)

            # --- Dọn dẹp dữ liệu trong cột B/L No và Container No ---
            main_df['B/L No'] = main_df['B/L No'].str.split('(').str[0].str.strip()
            main_df['Container No'] = main_df['Container No'].str.split('(').str[0].str.strip()
            # ----------------------------------------------------------------

            # --- Scrape các trang chi tiết ---
            summaries, bl_details = self._scrape_all_bl_details(main_df)
            histories = self._scrape_all_container_histories(main_df)
            
            # --- Trả về theo định dạng chuẩn ---
            results = {
                "main_results": main_df,
                "bl_summaries": pd.DataFrame(summaries) if summaries else pd.DataFrame(),
                "bl_details": pd.DataFrame(bl_details) if bl_details else pd.DataFrame(),
                "container_histories": pd.DataFrame(histories) if histories else pd.DataFrame()
            }
            return results, None

        except TimeoutException:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = f"output/interasia_timeout_{tracking_number}_{timestamp}.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"Timeout occurred. Saving screenshot to {screenshot_path}")
            except Exception as ss_e:
                print(f"Could not save screenshot: {ss_e}")
            return None, f"Không tìm thấy kết quả cho '{tracking_number}'."
        except Exception as e:
            return None, f"Không tìm thấy kết quả cho '{tracking_number}': {e}"


    def _scrape_all_bl_details(self, main_df):
        summaries, all_container_events = [], []
        for url in main_df['B/L Detail URL'].dropna().unique():
            try:
                print(f"  Scraping B/L detail from: {url}")
                self.driver.get(url)
                main_group = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "main-group")))
                
                bl_info_text = main_group.find_element(By.CSS_SELECTOR, ".info-group .title").text
                bl_no = bl_info_text.split('|')[1].strip()

                summary_data = {"B/L No": bl_no}
                try:
                    summary_table = main_group.find_element(By.CSS_SELECTOR, ".m-table-group")
                    cells = summary_table.find_elements(By.CSS_SELECTOR, "tbody tr td")
                    if len(cells) == 4:
                        summary_data.update({
                            "Loading Port": cells[0].text.strip(), "Discharging Port": cells[1].text.strip(),
                            "Estimated Departure Date": cells[2].text.strip(), "Estimated Arrival Date": cells[3].text.strip()
                        })
                except NoSuchElementException:
                    print(f"    Warning: B/L summary table not found for {bl_no}.")
                summaries.append(summary_data)

                container_blocks = main_group.find_elements(By.XPATH, "./div[.//p[contains(text(), 'Container No')]]")
                for block in container_blocks:
                    container_no = block.find_element(By.CSS_SELECTOR, ".info-group .title").text.split('|')[1].strip()
                    event_table = block.find_element(By.CLASS_NAME, "m-table-group")
                    rows = event_table.find_elements(By.CSS_SELECTOR, "tbody tr")
                    for row in rows:
                        cells = [cell.text.strip().replace('\n', ' ') for cell in row.find_elements(By.TAG_NAME, "td")]
                        if len(cells) == 7:
                            all_container_events.append({
                                "B/L No": bl_no, "Container No": container_no, "Event Date": cells[0],
                                "Depot": cells[1], "Port": cells[2], "Event Description": cells[3],
                                "Voyage": cells[4], "Vessel Name": cells[5], "Vessel Code": cells[6]
                            })
            except Exception as e:
                print(f"    Warning: Failed to scrape B/L detail page {url}: {e}")
        return summaries, all_container_events

    def _scrape_all_container_histories(self, main_df):
        history_events = []
        for url in main_df['Container History URL'].dropna().unique():
            try:
                print(f"  Scraping container history from: {url}")
                self.driver.get(url)
                self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "main-group")))
                container_no = self.driver.find_element(By.CSS_SELECTOR, ".main-group .info-group .title").text.split('|')[1].strip()
                
                table = self.driver.find_element(By.CLASS_NAME, "m-table-group")
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                for row in rows:
                    cells = [cell.text.strip().replace('\n', ' ') for cell in row.find_elements(By.TAG_NAME, "td")]
                    if len(cells) == 7:
                        history_events.append({
                            "Container No": container_no, "Event Date": cells[0], "Depot": cells[1],
                            "Port": cells[2], "Event Description": cells[3], "Voyage": cells[4],
                            "Vessel Name": cells[5], "Vessel Code": cells[6]
                        })
            except Exception as e:
                print(f"    Warning: Failed to scrape container history page {url}: {e}")
        return history_events