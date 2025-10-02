import os
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env
load_dotenv()

# --- Cấu hình chung ---
MAX_RETRIES = 3
DELAY_BETWEEN_REQUESTS = (3, 7)
RETRY_DELAY_EXPONENT_BASE = 2

# --- Cấu hình Proxy (Đọc từ biến môi trường) ---
PROXY_USER = os.getenv("PROXY_USER_NAME")
PROXY_PASS = os.getenv("PROXY_PASSWORD")

# Tạo danh sách proxy chỉ khi có đủ thông tin
PROXY_LIST = []
if PROXY_USER and PROXY_PASS:
    ports = range(10001, 10008) # Từ 10001 đến 10007
    PROXY_LIST = [
        {"host": "dc.decodo.com", "port": str(port), "user": PROXY_USER, "password": PROXY_PASS}
        for port in ports
    ]
else:
    print("Proxy username or password not found in .env file. Running without proxy.")


# --- Cấu hình Scraper ---
SCRAPER_CONFIGS = {
    "interasia": {
        "url": "https://www.interasia.cc/Service/Form?servicetype=0",
        "output_files": {
            "main_results": "interasia_results.csv",
            "bl_summaries": "interasia_bl_summaries.csv",
            "bl_details": "interasia_bl_details.csv",
            "container_histories": "interasia_container_histories.csv"
        }
    },
    # CMA CGM gắt quá không lấy được data
    #"cma_cgm": {
    #    "url": "https://www.cma-cgm.com/ebusiness/tracking",
    #    "output_files": {
    #        "summary": "cma_cgm_summary.csv",
    #        "history": "cma_cgm_history.csv"
    #    }
    #},
    "maersk": {
        "url": "https://www.maersk.com/tracking/",
        "output_files": {
            "summary": "maersk_summary.csv",
            "history": "maersk_history.csv"
        }
    }
}