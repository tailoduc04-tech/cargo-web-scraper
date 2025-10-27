import os
import random
from datetime import datetime
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import config
import driver_setup
import scrapers
from schemas import N8nTrackingInfo, Result
from typing import Tuple, Optional
import logging

STRICT_PAGES_LOAD = ["IAL", "MSK", "SEALEAD"]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

app = FastAPI()

# Tạo thư mục output nếu chưa có
if not os.path.exists("output"):
    os.makedirs("output")

def run_scraping_task(scraper_name: str, tracking_number: str) -> Tuple[Optional[N8nTrackingInfo], Optional[str]]:
    """
    Trả về dữ liệu thô và thông báo lỗi.
    """
    selected_proxy = None
    if config.PROXY_LIST:
        selected_proxy = random.choice(config.PROXY_LIST)
        print(f"Selected proxy for this session: {selected_proxy['host']}:{selected_proxy['port']}")

    driver = None
    try:
        page_load_strategy = 'normal' if scraper_name in STRICT_PAGES_LOAD else 'eager'
        driver = driver_setup.create_driver(selected_proxy, page_load_strategy=page_load_strategy)
        scraper_config = config.SCRAPER_CONFIGS.get(scraper_name, {})
        scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)

        data, error = scraper_instance.scrape(tracking_number)

        if isinstance(data, N8nTrackingInfo): # Kiểm tra kiểu trả về nếu cần
         return data, None
        elif data is None and error:
            return None, error
        elif data is None and not error:
            return None, f"Scraping for '{tracking_number}' on {scraper_name} returned no data."
        else:
            print(f"Warning: Scraper returned unexpected data type: {type(data)}")
            # Trả về lỗi nếu kiểu dữ liệu không đúng mong đợi
            return None, "Scraper returned unexpected data format."


    finally:
        if driver:
            print("Closing browser.")
            driver.quit()

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
# app.py
@app.post("/api/v1/track", response_model=Result) # Thêm response_model
async def track(bl_number: str = Form(...), service_name: str = Form(...)):
    if service_name not in scrapers.SCRAPERS.keys():
        # Trả về lỗi theo schema Result
        return Result(
            Error=True,
            Message=f"service_name phải nằm trong danh sách sau: {list(scrapers.SCRAPERS.keys())}",
            Status=400,
            MessageStatus="Bad Request"
        )

    data, error = run_scraping_task(service_name, bl_number)

    if error or not data:
        message = error or f"Không tìm thấy thông tin cho mã '{bl_number}' trên trang {service_name}."
        status_code = 404 if "Không tìm thấy" in message or "returned no data" in message else 500
        response_content = Result(
            Error=True,
            Message=message,
            Status=status_code,
            MessageStatus="Error"
        ).model_dump(exclude_none=True) # Dùng model_dump thay vì dict() trong Pydantic v2

        return JSONResponse(status_code=status_code, content=response_content)


    # Trả về thành công theo schema Result
    return Result(
        ResultData=data,
        Error=False,
        Message=f"Đã tìm thấy thông tin trên trang: {service_name}",
        Status=200,
        MessageStatus="Success"
    )