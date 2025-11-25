import os
import random
import asyncio
from datetime import datetime
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from driver_pool import driver_pool

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code chạy khi App KHỞI ĐỘNG
    driver_pool.initialize() 
    yield
    # Code chạy khi App TẮT
    driver_pool.shutdown()

app = FastAPI(lifespan=lifespan)

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
    # Hàm này chạy toàn bộ vòng đời của một driver: Tạo -> Scrape -> Thoát
    driver = None
    try:
        print(f"[{scraper_name}] Đang lấy driver từ Pool...")
        # 1. Lấy driver từ Pool (Sẽ chờ nếu cả 4 driver đều đang bận)
        driver = driver_pool.get_driver()
        
        # 2. Scrape như bình thường
        scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)
        data, error = scraper_instance.scrape(tracking_number)
        return data, error
        
    except Exception as e:
        print(f"[{scraper_name}] Lỗi trong luồng Selenium: {e}")
        return None, str(e)
        
    finally:
        # 3. Trả driver về Pool
        if driver:
            print(f"[{scraper_name}] Đang trả driver về Pool.")
            driver_pool.return_driver(driver)

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
        # Dùng asyncio.to_thread để không chặn FastAPI
        print(f"[{scraper_name}] Đang chuyển tác vụ Selenium sang thread pool...")
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
        print(f"[{scraper_name}] Chiến lược: Playwright. Đang khởi tạo trình duyệt async...")
        p, browser = await browser_setup.create_playwright_context(selected_proxy)
        if not browser:
            return None, "Không khởi tạo được trình duyệt Playwright"
        page = await browser_setup.create_page_context(browser)
        if not page:
            # Dọn dẹp nếu tạo page lỗi
            await browser.close()
            await p.stop()
            return None, "Không khởi tạo được trang Playwright"
        print(f"Trình duyệt/trang Playwright khởi tạo sau {time.time() - start_browser_time:.2f} giây.")
        try:
            scraper_instance = scrapers.get_scraper(scraper_name, page, scraper_config)
            data, error = await scraper_instance.scrape(tracking_number)
            return data, error
        finally:
            print(f"[{scraper_name}] Đang dọn dẹp trình duyệt và context Playwright...")
            try:
                if page:
                    context = page.context
                    await page.close()
                    if context:
                        await context.close()
            except Exception as e:
                print(f"[{scraper_name}] Lỗi khi đóng page/context: {e}")
            try:
                if browser:
                    await browser.close()
            except Exception as e:
                print(f"[{scraper_name}] Lỗi khi đóng trình duyệt: {e}")
            try:
                if p:
                    await p.stop()
            except Exception as e:
                print(f"[{scraper_name}] Lỗi khi dừng playwright: {e}")

    elif strategy == "api":
        scraper_instance = scrapers.get_scraper(scraper_name, None, scraper_config)
        try:
            data, error = await asyncio.to_thread(scraper_instance.scrape, tracking_number)
            return data, error
        finally:
            if hasattr(scraper_instance, 'close'):
                try:
                    scraper_instance.close()
                    print(f"[{scraper_name}] Đã đóng session API scraper.")
                except Exception as e:
                    print(f"[{scraper_name}] Lỗi khi đóng API scraper: {e}")

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