import os
import zipfile
import random
from datetime import datetime
import glob

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import config
import driver_setup
import file_handler
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
    Logic scraping được tái cấu trúc để trả về cả file đã lưu và thông báo lỗi.
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
            results_to_save = {key: [df] for key, df in data.items() if not df.empty}
            output_filenames = config.SCRAPER_CONFIGS[scraper_name].get('output_files', {})
            saved_files = file_handler.save_results(results_to_save, output_filenames)
            return saved_files, None
    finally:
        if driver:
            print("Closing browser.")
            driver.quit()
    return None, "An unknown error occurred during scraping task."


@app.post("/start-scrape")
async def start_scrape(request: Request, service: str = Form(...), bl_number: str = Form(...)):
    saved_files, error = run_scraping_task(service, bl_number)

    if not saved_files:
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": error or "Scraping thất bại hoặc không tìm thấy dữ liệu."}
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename_base = f"{service}_{bl_number}_results_{timestamp}.zip"
    zip_filepath = os.path.join("output", zip_filename_base)

    with zipfile.ZipFile(zip_filepath, 'w') as zipf:
        for file in saved_files:
            zipf.write(file, os.path.basename(file))
    
    for file in saved_files:
        os.remove(file)

    # Tạo URL tuyệt đối
    download_url = request.url_for('download_file', filename=zip_filename_base)

    return JSONResponse(
        content={
            "success": True,
            "download_url": str(download_url), # Chuyển URL thành chuỗi
            "filename": zip_filename_base
        }
    )

@app.post("/api/v1/track")
async def track_all(request: Request, bl_number: str = Form(...)):
    """
    API endpoint để tự động tra cứu mã vận đơn trên tất cả các scraper.
    """
    searched_scrapers = []
    all_scrapers = list(scrapers.SCRAPERS.keys())

    for scraper_name in all_scrapers:
        searched_scrapers.append(scraper_name)
        saved_files, error = run_scraping_task(scraper_name, bl_number)

        if saved_files:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_filename_base = f"{scraper_name}_{bl_number}_results_{timestamp}.zip"
            zip_filepath = os.path.join("output", zip_filename_base)

            with zipfile.ZipFile(zip_filepath, 'w') as zipf:
                for file in saved_files:
                    zipf.write(file, os.path.basename(file))
            
            for file in saved_files:
                os.remove(file)

            # Tạo URL tuyệt đối
            download_url = request.url_for('download_file', filename=zip_filename_base)

            return JSONResponse(
                content={
                    "success": True,
                    "message": f"Đã tìm thấy thông tin trên trang: {scraper_name}",
                    "download_url": str(download_url)
                }
            )

        if error and "Timeout" in error:
            search_pattern = os.path.join('output', f'{scraper_name}_timeout_{bl_number}_*.png')
            screenshot_files = glob.glob(search_pattern)
            if screenshot_files:
                latest_screenshot = max(screenshot_files, key=os.path.getctime)
                return FileResponse(latest_screenshot, media_type='image/png', filename=os.path.basename(latest_screenshot))
            else:
                return JSONResponse(
                    status_code=500,
                    content={
                        "success": False,
                        "message": f"Timeout khi tra cứu trên {scraper_name}, nhưng không tìm thấy screenshot.",
                        "error": error
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
    
@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join("output", filename)
    if os.path.exists(file_path):
        headers = {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
        return FileResponse(
            file_path, 
            media_type='application/zip', 
            filename=filename,
            headers=headers
        )
    return JSONResponse(status_code=404, content={"message": "Không tìm thấy file."})