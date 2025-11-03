import os
import random
from datetime import datetime
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
import config
import driver_setup
import browser_setup
import scrapers
from scrapers import SCRAPER_STRATEGY
from schemas import N8nTrackingInfo, Result
from typing import Tuple, Optional
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

app = FastAPI()

# Tạo thư mục output nếu chưa có
if not os.path.exists("output"):
    os.makedirs("output")

# Đổi thành async def
async def run_scraping_task(scraper_name: str, tracking_number: str) -> Tuple[Optional[N8nTrackingInfo], Optional[str]]:
    """
    Trả về dữ liệu thô và thông báo lỗi.
    """
    selected_proxy = None
    if config.PROXY_LIST:
        selected_proxy = random.choice(config.PROXY_LIST)
        print(f"Selected proxy for this session: {selected_proxy['host']}:{selected_proxy['port']}")

    driver = None # Cho Selenium
    p, browser, page = None, None, None # Cho Playwright
    
    strategy = SCRAPER_STRATEGY.get(scraper_name)

    try:
        scraper_config = config.SCRAPER_CONFIGS.get(scraper_name, {})
        
        data = None
        error = None

        if strategy == "selenium":
            start_driver_time = time.time()
            print(f"[{scraper_name}] Strategy: Selenium. Initializing driver...")
            driver = driver_setup.create_driver(selected_proxy) # Sync
            print(f"Driver initialized in {time.time() - start_driver_time:.2f} seconds.")
            
            scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)
            data, error = scraper_instance.scrape(tracking_number) # Sync call

        elif strategy == "playwright":
            start_browser_time = time.time()
            print(f"[{scraper_name}] Strategy: Playwright. Initializing async browser...")
            p, browser = await browser_setup.create_playwright_context(selected_proxy) # Async
            
            if not browser:
                return None, "Failed to initialize Playwright browser"
                
            page = await browser_setup.create_page_context(browser) # Async
            if not page:
                 # Dọn dẹp nếu tạo page lỗi
                await browser.close()
                await p.stop()
                return None, "Failed to initialize Playwright page"

            print(f"Playwright browser/page initialized in {time.time() - start_browser_time:.2f} seconds.")
            
            scraper_instance = scrapers.get_scraper(scraper_name, page, scraper_config)
            data, error = await scraper_instance.scrape(tracking_number)

        elif strategy == "api":
            print(f"[{scraper_name}] Strategy: API. Skipping driver initialization.")
            scraper_instance = scrapers.get_scraper(scraper_name, None, scraper_config)
            data, error = scraper_instance.scrape(tracking_number)
            
        else:
            return None, f"Scraper strategy not defined for service: {scraper_name}"

        # Xử lý kết quả trả về
        if isinstance(data, N8nTrackingInfo):
         return data, None
        elif data is None and error:
            return None, error
        elif data is None and not error:
            return None, f"Scraping for '{tracking_number}' on {scraper_name} returned no data."
        else:
            print(f"Warning: Scraper returned unexpected data type: {type(data)}")
            return None, "Scraper returned unexpected data format."

    finally:
        # Dọn dẹp Selenium
        if driver:
            print("Closing Selenium driver.")
            driver.quit()
        
        # Dọn dẹp Playwright (async)
        if page:
            print("Closing Playwright page.")
            await page.close()
        if browser:
            print("Closing Playwright browser.")
            await browser.close()
        if p:
            print("Stopping Playwright.")
            await p.stop()

# --- Endpoint để lấy danh sách services ---
@app.get("/api/v1/services")
async def get_available_services():
    """
    API endpoint để lấy danh sách tất cả các service_name khả dụng.
    """
    available_services = list(scrapers.SCRAPERS.keys())
    return JSONResponse(content={"services": available_services})

# --- Endpoint để thực hiện scrape web ---
# Đổi thành async def
@app.post("/api/v1/track", response_model=Result)
async def track(bl_number: str = Form(...), service_name: str = Form(...)):
    if service_name not in scrapers.SCRAPERS.keys():
        return Result(
            Error=True,
            Message=f"service_name phải nằm trong danh sách sau: {list(scrapers.SCRAPERS.keys())}",
            Status=400,
            MessageStatus="Bad Request"
        )

    # Dùng await để gọi hàm async
    data, error = await run_scraping_task(service_name, bl_number)

    if error or not data:
        message = error or f"Không tìm thấy thông tin cho mã '{bl_number}' trên trang {service_name}."
        status_code = 404 if "Không tìm thấy" in message or "returned no data" in message else 500
        response_content = Result(
            Error=True,
            Message=message,
            Status=status_code,
            MessageStatus="Error"
        ).model_dump(exclude_none=True) 

        return JSONResponse(status_code=status_code, content=response_content)

    return Result(
        ResultData=data,
        Error=False,
        Message=f"Đã tìm thấy thông tin trên trang: {service_name}",
        Status=200,
        MessageStatus="Success"
    )