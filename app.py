import os
import random
from datetime import datetime
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import config
import driver_setup
import scrapers

app = FastAPI()

# Tạo thư mục output nếu chưa có
if not os.path.exists("output"):
    os.makedirs("output")

def run_scraping_task(scraper_name: str, tracking_number: str):
    """
    Logic scraping được tái cấu trúc để trả về dữ liệu thô và thông báo lỗi.
    Hàm này giờ sẽ ưu tiên trả về dictionary.
    (Giữ nguyên hàm này)
    """
    selected_proxy = None
    if config.PROXY_LIST:
        selected_proxy = random.choice(config.PROXY_LIST)
        print(f"Selected proxy for this session: {selected_proxy['host']}:{selected_proxy['port']}")

    driver = None
    try:
        driver = driver_setup.create_driver(selected_proxy)
        scraper_config = config.SCRAPER_CONFIGS.get(scraper_name, {})
        scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)

        data, error = scraper_instance.scrape(tracking_number)

        if error:
            print(f"Failed to scrape: {error}")
            return None, error

        if not data:
            return None, f"Scraping for '{tracking_number}' on {scraper_name} returned no data."

        print(f"Successfully scraped data for '{tracking_number}'.")

        # Hàm scrape giờ đã trả về dictionary trực tiếp
        if isinstance(data, dict):
            return data, None
        else:
             # Trường hợp hiếm hoi scraper cũ trả về dạng khác
             print(f"Warning: Scraper returned unexpected data type: {type(data)}")
             return None, "Scraper returned unexpected data format."


    finally:
        if driver:
            print("Closing browser.")
            driver.quit()

    return None, "An unknown error occurred during the scraping task."

# --- Endpoint để lấy danh sách services ---
@app.get("/api/v1/services")
async def get_available_services():
    """
    API endpoint để lấy danh sách tất cả các service_name khả dụng.
    """
    # Lấy danh sách các keys từ dictionary SCRAPERS trong module scrapers
    available_services = list(scrapers.SCRAPERS.keys())
    # Trả về danh sách dưới dạng JSON
    return JSONResponse(content={"services": available_services})

# --- Endpoint để thực hiện scrape web
@app.post("/api/v1/track")
async def track(bl_number: str = Form(...), service_name: str = Form(...)):
    """
    API endpoint để tra cứu mã vận đơn trên một dịch vụ cụ thể.
    """
    if service_name not in scrapers.SCRAPERS.keys():
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "message": f"service_name phải nằm trong danh sách sau: {list(scrapers.SCRAPERS.keys())}"
            }
        )

    data, error = run_scraping_task(service_name, bl_number)

    if error or not data:
        message = error or f"Không tìm thấy thông tin cho mã '{bl_number}' trên trang {service_name}."
        # Trả về 404 nếu không có dữ liệu, 500 nếu có lỗi khác
        status_code = 404 if "Không tìm thấy" in message or "returned no data" in message else 500
        return JSONResponse(
            status_code=status_code,
            content={"success": False, "message": message}
        )

    # Trả về kết quả thành công, bao gồm cả source và data
    return JSONResponse(
        content={
            "success": True,
            "message": f"Đã tìm thấy thông tin trên trang: {service_name}",
            "source": service_name,
            **data # Giải nén dictionary data vào response
        }
    )