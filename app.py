import os
import zipfile
import random

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
    Logic scraping được tái cấu trúc để chạy một tác vụ duy nhất.
    """
    selected_proxy = None
    if config.PROXY_LIST:
        selected_proxy = random.choice(config.PROXY_LIST)
        print(f"Selected proxy for this session: {selected_proxy['host']}:{selected_proxy['port']}")

    driver = driver_setup.create_driver(selected_proxy)
    saved_files = []
    try:
        scraper_config = config.SCRAPER_CONFIGS[scraper_name]
        scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)

        data, error = scraper_instance.scrape(tracking_number)

        if error:
            print(f"Failed to scrape: {error}")
            return None
        else:
            print(f"Successfully scraped data for '{tracking_number}'.")
            # Tạo một dictionary để lưu kết quả
            results_to_save = {key: [df] for key, df in data.items() if not df.empty}
            output_filenames = config.SCRAPER_CONFIGS[scraper_name].get('output_files', {})
            saved_files = file_handler.save_results(results_to_save, output_filenames)
            return saved_files
    finally:
        print("Closing browser.")
        driver.quit()
    return saved_files


@app.post("/start-scrape")
async def start_scrape(service: str = Form(...), bl_number: str = Form(...)):
    saved_files = run_scraping_task(service, bl_number)

    if not saved_files:
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "Scraping thất bại hoặc không tìm thấy dữ liệu."}
        )

    # Nén các file kết quả
    zip_filename_base = f"{service}_{bl_number}_results.zip"
    zip_filepath = os.path.join("output", zip_filename_base)

    with zipfile.ZipFile(zip_filepath, 'w') as zipf:
        for file in saved_files:
            zipf.write(file, os.path.basename(file))
    
    # Dọn dẹp các file csv sau khi nén
    for file in saved_files:
        os.remove(file)

    # Trả về JSON chứa đường dẫn tải file
    return JSONResponse(
        content={
            "success": True,
            "download_url": f"/download/{zip_filename_base}",
            "filename": zip_filename_base
        }
    )
    
@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join("output", filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type='application/zip', filename=filename)
    return JSONResponse(status_code=404, content={"message": "Không tìm thấy file."})