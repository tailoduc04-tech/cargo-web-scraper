import os
import random
import asyncio
from datetime import datetime
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

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
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

app = FastAPI()

#Cấu hình CORS
origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Tạo thư mục output nếu chưa có
if not os.path.exists("output"):
    os.makedirs("output")
    
def run_selenium_task_sync(scraper_name, tracking_number, scraper_config, proxy_info):
    """
    Hàm này chạy trọn vẹn vòng đời của 1 driver: Tạo -> Scrape -> Quit
    """
    driver = None
    try:
        print(f"[{scraper_name}] Thread: Starting driver...")
        driver = driver_setup.create_driver(proxy_info)
        
        scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)
        data, error = scraper_instance.scrape(tracking_number)
        return data, error
    except Exception as e:
        print(f"[{scraper_name}] Thread Error: {e}")
        return None, str(e)
    finally:
        if driver:
            print(f"[{scraper_name}] Thread: Closing driver.")
            try:
                driver.quit()
            except Exception:
                pass

# Đổi thành async def
async def run_scraping_task(scraper_name: str, tracking_number: str) -> Tuple[Optional[N8nTrackingInfo], Optional[str]]:
    """
    Trả về dữ liệu thô và thông báo lỗi.
    """
    selected_proxy = None
    if config.PROXY_LIST:
        selected_proxy = random.choice(config.PROXY_LIST)

    strategy = SCRAPER_STRATEGY.get(scraper_name)
    scraper_config = config.SCRAPER_CONFIGS.get(scraper_name, {})
    
    if strategy == "selenium":
        # [QUAN TRỌNG] Dùng asyncio.to_thread để không chặn FastAPI
        print(f"[{scraper_name}] Dispatching Selenium task to thread pool...")
        data, error = await asyncio.to_thread(
            run_selenium_task_sync, 
            scraper_name, 
            tracking_number, 
            scraper_config, 
            selected_proxy
        )
        return data, error

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
        
        try:
            scraper_instance = scrapers.get_scraper(scraper_name, page, scraper_config)
            data, error = await scraper_instance.scrape(tracking_number)
            return data, error
        finally:
            # CRITICAL: Always cleanup browser resources to prevent memory leaks
            print(f"[{scraper_name}] Cleaning up Playwright browser and context...")
            try:
                if page:
                    # Closing page also closes its context
                    context = page.context
                    await page.close()
                    if context:
                        await context.close()
            except Exception as e:
                print(f"[{scraper_name}] Error closing page/context: {e}")
            try:
                if browser:
                    await browser.close()
            except Exception as e:
                print(f"[{scraper_name}] Error closing browser: {e}")
            try:
                if p:
                    await p.stop()
            except Exception as e:
                print(f"[{scraper_name}] Error stopping playwright: {e}")

    elif strategy == "api":
        scraper_instance = scrapers.get_scraper(scraper_name, None, scraper_config)
        try:
            data, error = await asyncio.to_thread(scraper_instance.scrape, tracking_number)
            return data, error
        finally:
            # Close the requests session to prevent resource leaks
            if hasattr(scraper_instance, 'close'):
                try:
                    scraper_instance.close()
                    print(f"[{scraper_name}] API scraper session closed.")
                except Exception as e:
                    print(f"[{scraper_name}] Error closing API scraper: {e}")

    return None, f"Strategy not found: {scraper_name}"

# --- Endpoint để lấy danh sách services ---
@app.get("/api/v1/services")
async def get_available_services():
    """
    API endpoint để lấy danh sách tất cả các service_name khả dụng.
    """
    available_services = list(scrapers.SCRAPERS.keys())
    return JSONResponse(content={"services": available_services})

# --- Endpoint để thực hiện scrape web ---
@app.post("/api/v1/track", response_model=Result)
async def track(bl_number: str = Form(...), service_name: str = Form(...)):
    if service_name not in scrapers.SCRAPERS.keys():
        return Result(
            Error=True,
            Message=f"service_name phải nằm trong danh sách sau: {list(scrapers.SCRAPERS.keys())}",
            Status=400,
            MessageStatus="Bad Request"
        )

    data, error = await run_scraping_task(service_name, bl_number)

    if error or not data:
        message = error or f"Không tìm thấy thông tin cho mã '{bl_number}' trên trang {service_name}."
        status_code = 404 if "Không tìm thấy" in message or "returned no data" in message else 500
        response_content = Result(
            Error=True,
            Message=message,
            Status=status_code,
            MessageStatus="Error",
            Service=service_name
        ).model_dump(exclude_none=True) 

        return JSONResponse(status_code=status_code, content=response_content)

    return Result(
        ResultData=data,
        Error=False,
        Message=f"Đã tìm thấy thông tin trên trang: {service_name}",
        Status=200,
        MessageStatus="Success",
        Service=service_name
    )