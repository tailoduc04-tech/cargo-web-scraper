import os
import random
from datetime import datetime

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd

import config
import driver_setup
import scrapers

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

if not os.path.exists("output"):
    os.makedirs("output")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Endpoint để hiển thị trang chủ với form nhập liệu.
    """
    services = list(scrapers.SCRAPERS.keys())
    return templates.TemplateResponse("index.html", {"request": request, "services": services})

def run_scraping_task(scraper_name: str, tracking_number: str):
    """
    Logic scraping được tái cấu trúc để trả về dữ liệu thô và thông báo lỗi.
    """
    selected_proxy = None
    if config.PROXY_LIST:
        selected_proxy = random.choice(config.PROXY_LIST)
        print(f"Selected proxy for this session: {selected_proxy['host']}:{selected_proxy['port']}")

    driver = None
    try:
        driver = driver_setup.create_driver(selected_proxy)
        scraper_config = config.SCRAPER_CONFIGS[scraper_name]
        scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)

        data, error = scraper_instance.scrape(tracking_number)

        if error:
            print(f"Failed to scrape: {error}")
            return None, error
        else:
            print(f"Successfully scraped data for '{tracking_number}'.")
            # Chuyển đổi DataFrame thành dict có thể serialize được
            json_data = {}
            for key, df in data.items():
                if isinstance(df, pd.DataFrame):
                    json_data[key] = df.to_dict(orient='records')
            return json_data, None
    finally:
        if driver:
            print("Closing browser.")
            driver.quit()
    return None, "An unknown error occurred during the scraping task."

@app.post("/start-scrape")
async def start_scrape(request: Request, service: str = Form(...), bl_number: str = Form(...)):
    """
    Endpoint cho giao diện web, trả về dữ liệu scrape dưới dạng JSON.
    """
    data, error = run_scraping_task(service, bl_number)

    if error:
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": error}
        )

    if not data:
        return JSONResponse(
            status_code=404,
            content={"success": False, "message": "Scraping không trả về dữ liệu."}
        )

    return JSONResponse(
        content={
            "success": True,
            "data": data
        }
    )

@app.post("/api/v1/track-all")
async def track_all(request: Request, bl_number: str = Form(...)):
    """
    API endpoint để tự động tra cứu mã vận đơn trên tất cả các scraper.
    """
    all_scrapers = list(scrapers.SCRAPERS.keys())
    searched_scrapers = []

    for scraper_name in all_scrapers:
        searched_scrapers.append(scraper_name)
        data, error = run_scraping_task(scraper_name, bl_number)

        if data and not error:
            return JSONResponse(
                content={
                    "success": True,
                    "message": f"Đã tìm thấy thông tin trên trang: {scraper_name}",
                    "source": scraper_name,
                    "data": data
                }
            )

    return JSONResponse(
        status_code=404,
        content={
            "success": False,
            "message": f"Không tìm thấy thông tin cho mã '{bl_number}'.",
            "searched_on": searched_scrapers
        }
    )

@app.post("/api/v1/track")
async def track(request: Request, bl_number: str = Form(...), service_name: str = Form(...)):
    """
    API endpoint để tra cứu mã vận đơn trên một dịch vụ cụ thể.
    """
    if service_name not in scrapers.SCRAPERS.keys():
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "message": f"service_name phải nằm trong danh sách sau: {list(config.SCRAPER_CONFIGS.keys())}"
            }
        )

    data, error = run_scraping_task(service_name, bl_number)

    if error:
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": error}
        )
    
    if data:
        return JSONResponse(
            content={
                "success": True,
                "message": f"Đã tìm thấy thông tin trên trang: {service_name}",
                "source": service_name,
                "data": data
            }
        )

    return JSONResponse(
        status_code=404,
        content={
            "success": False,
            "message": f"Không tìm thấy thông tin cho mã '{bl_number}' trên trang {service_name}."
        }
    )