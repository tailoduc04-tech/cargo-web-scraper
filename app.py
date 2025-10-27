import os
import random
from datetime import datetime
from fastapi import FastAPI, Form, Request, Depends
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import config
import driver_setup
import scrapers
from schemas import N8nTrackingInfo, Result
from typing import Tuple, Optional
import logging
import asyncio
import httpx
import time

# --- Cấu hình Logging ---
# (Giữ nguyên cấu hình logging)
logging.basicConfig(
    level=logging.INFO, # Giữ INFO để thấy log của pool, có thể đổi thành CRITICAL nếu muốn tắt hẳn
    format='%(asctime)s - %(name)s [%(levelname)s] (%(threadName)s) %(message)s', # Thêm threadName
)
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# --- Hàm chờ Selenium Hub sẵn sàng ---
async def wait_for_selenium_hub(url: str, timeout: int = 60):
    """Chờ cho đến khi Selenium Hub sẵn sàng nhận kết nối."""
    logger.info(f"Đang chờ Selenium Hub tại {url} sẵn sàng...")
    start_time = time.time()
    async with httpx.AsyncClient() as client:
        while time.time() - start_time < timeout:
            try:
                # Ping vào status endpoint
                response = await client.get(f"{url}/wd/hub/status", timeout=5)
                # Kiểm tra status code và nội dung JSON (ready=true)
                if response.status_code == 200 and response.json().get("value", {}).get("ready"):
                    logger.info(f"Selenium Hub đã sẵn sàng sau {time.time() - start_time:.2f} giây.")
                    return True
                else:
                    logger.debug(f"Selenium Hub chưa sẵn sàng (Status: {response.status_code}, Ready: {response.json().get('value', {}).get('ready')}). Đang thử lại...")
            except (httpx.RequestError, ConnectionRefusedError) as e:
                logger.debug(f"Chưa kết nối được tới Selenium Hub: {e}. Đang thử lại...")
            except Exception as e:
                 logger.warning(f"Lỗi không xác định khi kiểm tra Selenium Hub: {e}. Đang thử lại...")

            await asyncio.sleep(2) # Chờ 2 giây trước khi thử lại

    logger.error(f"Selenium Hub không sẵn sàng sau {timeout} giây.")
    return False

# --- Quản lý Vòng đời Ứng dụng và Driver Pool ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Chờ Selenium Hub rồi mới khởi tạo driver pool
    logger.info("Ứng dụng FastAPI đang khởi động...")

    # --- THÊM BƯỚC CHỜ SELENIUM HUB ---
    selenium_hub_url = "http://selenium:4444"
    hub_ready = await wait_for_selenium_hub(selenium_hub_url)
    # ----------------------------------

    if hub_ready:
        selected_proxy = None
        if config.PROXY_LIST:
            selected_proxy = random.choice(config.PROXY_LIST)
            logger.info(f"Sử dụng proxy {selected_proxy['host']}:{selected_proxy['port']} để khởi tạo pool.")

        # Chỉ khởi tạo pool nếu Hub đã sẵn sàng
        await asyncio.to_thread(driver_setup.initialize_driver_pool, proxy_config=selected_proxy) # Chạy trong thread để không block
    else:
        logger.critical("Không thể kết nối tới Selenium Hub. Driver pool sẽ không được khởi tạo!")
        # Cậu có thể quyết định dừng ứng dụng ở đây nếu muốn
        # raise RuntimeError("Không thể khởi động ứng dụng do không kết nối được Selenium Hub.")

    yield
    # Shutdown: Đóng tất cả driver trong pool
    logger.info("Ứng dụng FastAPI đang tắt...")
    # Chạy shutdown trong thread để tránh lỗi nếu event loop đã đóng
    await asyncio.to_thread(driver_setup.shutdown_driver_pool)

app = FastAPI(lifespan=lifespan)

# Tạo thư mục output nếu chưa có
# (Giữ nguyên)

# --- Hàm chạy Scraper (Sử dụng Driver Pool) ---
# (Giữ nguyên hàm run_scraping_task)
async def run_scraping_task(scraper_name: str, tracking_number: str) -> Tuple[Optional[N8nTrackingInfo], Optional[str]]:
    """
    Lấy driver từ pool, chạy scraper và trả driver về pool.
    """
    driver = None
    task_start_time = time.time()
    logger.info(f"Bắt đầu task scrape cho {scraper_name} - {tracking_number}")
    try:
        # Lấy driver từ pool
        get_driver_start_time = time.time()
        driver = await asyncio.to_thread(driver_setup.get_driver) # Chạy get_driver trong thread riêng để tránh block event loop
        if not driver:
            logger.error(f"Không thể lấy driver từ pool cho task {scraper_name} - {tracking_number}.")
            return None, "Không có driver Selenium sẵn sàng, vui lòng thử lại sau."
        logger.info(f"-> (Thời gian) Lấy driver: {time.time() - get_driver_start_time:.2f}s")


        # Lấy cấu hình và chạy scraper
        scraper_config = config.SCRAPER_CONFIGS.get(scraper_name, {})
        scraper_instance = scrapers.get_scraper(scraper_name, driver, scraper_config)

        scrape_start_time = time.time()
        # Chạy hàm scrape đồng bộ trong thread riêng để không block FastAPI
        data, error = await asyncio.to_thread(scraper_instance.scrape, tracking_number)
        logger.info(f"-> (Thời gian) Chạy scraper: {time.time() - scrape_start_time:.2f}s")


        # Xử lý kết quả
        if isinstance(data, N8nTrackingInfo):
            return data, None
        elif data is None and error:
            return None, error
        elif data is None and not error:
            return None, f"Scraping for '{tracking_number}' on {scraper_name} returned no data."
        else:
            logger.warning(f"Warning: Scraper trả về kiểu dữ liệu không mong đợi: {type(data)}")
            return None, "Scraper returned unexpected data format."

    except Exception as e:
         # Log lỗi xảy ra trong quá trình scrape
         logger.error(f"Lỗi không mong muốn trong run_scraping_task cho {scraper_name} - {tracking_number}: {e}", exc_info=True)
         return None, f"Lỗi hệ thống khi đang scrape: {e}"
    finally:
        # Luôn trả driver về pool, ngay cả khi có lỗi
        if driver:
            return_driver_start_time = time.time()
            await asyncio.to_thread(driver_setup.return_driver, driver) # Chạy return_driver trong thread riêng
            logger.info(f"-> (Thời gian) Trả driver về pool: {time.time() - return_driver_start_time:.2f}s")
        logger.info(f"Hoàn thành task scrape cho {scraper_name} - {tracking_number}. (Tổng thời gian task: {time.time() - task_start_time:.2f}s)")


# --- Endpoint để lấy danh sách services ---
# (Giữ nguyên)
@app.get("/api/v1/services")
async def get_available_services():
    """
    API endpoint để lấy danh sách tất cả các service_name khả dụng.
    """
    available_services = list(scrapers.SCRAPERS.keys())
    return JSONResponse(content={"services": available_services})

# --- Endpoint để thực hiện scrape web ---
# (Giữ nguyên)
@app.post("/api/v1/track", response_model=Result)
async def track(bl_number: str = Form(...), service_name: str = Form(...)):
    if service_name not in scrapers.SCRAPERS.keys():
        return JSONResponse(
            status_code=400,
            content=Result(
                Error=True,
                Message=f"service_name phải nằm trong danh sách sau: {list(scrapers.SCRAPERS.keys())}",
                Status=400,
                MessageStatus="Bad Request"
            ).model_dump(exclude_none=True)
        )

    # Chạy task scrape (đã bao gồm lấy/trả driver)
    data, error = await run_scraping_task(service_name, bl_number)

    if error or not data:
        message = error or f"Không tìm thấy thông tin cho mã '{bl_number}' trên trang {service_name}."
        # Xác định status code phù hợp hơn
        if "Không tìm thấy" in message or "returned no data" in message:
            status_code = 404
            message_status = "Not Found"
        elif "Không có driver Selenium sẵn sàng" in message:
             status_code = 503 # Service Unavailable
             message_status = "Service Unavailable"
        else:
             status_code = 500 # Internal Server Error
             message_status = "Error"

        response_content = Result(
            Error=True,
            Message=message,
            Status=status_code,
            MessageStatus=message_status
        ).model_dump(exclude_none=True)

        return JSONResponse(status_code=status_code, content=response_content)


    # Trả về thành công
    return Result(
        ResultData=data,
        Error=False,
        Message=f"Đã tìm thấy thông tin trên trang: {service_name}",
        Status=200,
        MessageStatus="Success"
    )

# --- Endpoint kiểm tra trạng thái pool ---
# (Giữ nguyên)
@app.get("/api/v1/pool_status")
async def get_pool_status():
    """Kiểm tra số lượng driver đang có trong pool."""
    return {"available_drivers_in_pool": driver_setup.driver_pool.qsize(),
            "total_active_drivers": len(driver_setup.active_drivers),
            "pool_max_size": driver_setup.POOL_SIZE}