import unittest
import pandas as pd
import sys
import os

# Đảm bảo Python có thể tìm thấy các module trong project của bạn
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import driver_setup
from scrapers.cma_cgm_scraper import CmaCgmScraper
from config import SCRAPER_CONFIGS

class TestCmaCgmScraperSelenium(unittest.TestCase):
    """
    Test scraper CMA CGM bằng cách chạy một trình duyệt thật với Selenium.
    """

    def setUp(self):
        """
        Thiết lập môi trường test trước mỗi bài test.
        Hàm này sẽ được gọi tự động.
        """
        # Khởi tạo WebDriver. 
        # Sử dụng hàm create_driver từ project của bạn để đảm bảo cấu hình nhất quán.
        # Bỏ qua proxy để test đơn giản hơn.
        self.driver = driver_setup.create_driver() 
        self.scraper_config = SCRAPER_CONFIGS['cma_cgm']
        self.scraper = CmaCgmScraper(self.driver, self.scraper_config)
        
    def test_scrape_live_site_with_booking_number(self):
        """
        Test chức năng scrape trên trang web thật với mã Booking Number.
        """
        tracking_number = "CHP0181971"
        print(f"\nBắt đầu test với tracking number: {tracking_number}")

        # Chạy scraper
        data, error = self.scraper.scrape(tracking_number)

        # 1. Kiểm tra không có lỗi trả về
        self.assertIsNone(error, f"Scraper đã trả về lỗi: {error}")
        print("-> Scraper không trả về lỗi.")

        # 2. Kiểm tra dữ liệu có được trả về hay không
        self.assertIsNotNone(data, "Scraper không trả về dữ liệu.")
        self.assertIn("summary", data)
        self.assertIn("history", data)
        print("-> Đã nhận được dữ liệu summary và history.")

        # 3. Kiểm tra dữ liệu trong DataFrame 'summary'
        summary_df = data['summary']
        self.assertIsInstance(summary_df, pd.DataFrame, "Dữ liệu summary không phải là DataFrame.")
        self.assertEqual(summary_df.shape[0], 1, "DataFrame summary nên chỉ có một dòng.")
        
        print("-> Bảng tóm tắt (summary):")
        print(summary_df.to_string())

        self.assertEqual(summary_df['Booking Reference'].iloc[0], tracking_number)
        self.assertEqual(summary_df['Container No'].iloc[0], 'BEAU6103981')
        self.assertEqual(summary_df['POL'].iloc[0], 'KHALIFA PORT, ABU DHABI (AE)')
        self.assertEqual(summary_df['POD'].iloc[0], 'QINGDAO (CN)')
        self.assertIsNotNone(summary_df['ETA'].iloc[0])
        print("-> Dữ liệu tóm tắt chính xác.")

        # 4. Kiểm tra dữ liệu trong DataFrame 'history'
        history_df = data['history']
        self.assertIsInstance(history_df, pd.DataFrame, "Dữ liệu history không phải là DataFrame.")
        self.assertGreater(history_df.shape[0], 1, "DataFrame history nên có nhiều hơn một sự kiện.")

        print("\n-> Bảng lịch sử (history):")
        print(history_df.to_string())
        
        # Kiểm tra một vài sự kiện quan trọng
        self.assertTrue('READY TO BE LOADED' in history_df['Event Description'].tolist())
        self.assertTrue('VESSEL DEPARTURE' in history_df['Event Description'].tolist())
        self.assertTrue('VESSEL ARRIVAL' in history_df['Event Description'].tolist())
        
        # Lấy dòng sự kiện 'Vessel Departure' và kiểm tra thông tin
        departure_event = history_df[history_df['Event Description'] == 'VESSEL DEPARTURE'].iloc[0]
        self.assertEqual(departure_event['Vessel Name'], 'CMA CGM ALASKA')
        self.assertIsNotNone(departure_event['Voyage'])
        print("-> Dữ liệu lịch sử chính xác.")
        print(f"-> Test cho {tracking_number} hoàn thành thành công!")


    def tearDown(self):
        """
        Dọn dẹp sau mỗi bài test.
        Hàm này sẽ được gọi tự động.
        """
        if self.driver:
            self.driver.quit()

if __name__ == '__main__':
    unittest.main()