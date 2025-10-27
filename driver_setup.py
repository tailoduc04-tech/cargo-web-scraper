import os
import shutil
import zipfile
import atexit
import logging
from queue import Queue, Empty, Full
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

logger = logging.getLogger(__name__)

# --- Cấu hình Pool ---
POOL_SIZE = 3 # Số lượng instance Selenium muốn tạo sẵn
driver_pool = Queue(maxsize=POOL_SIZE)
active_drivers = [] # Lưu lại các driver đã tạo để đóng khi shutdown

def _create_new_driver(proxy_config=None):
    """
    Hàm nội bộ để tạo một instance WebDriver mới.
    (Giữ nguyên logic tạo driver như trước)
    """
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager' # Giữ nguyên tối ưu tải trang
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage') # Quan trọng khi chạy trong Docker
    options.add_argument('--disable-gpu')
    options.add_argument("--window-size=1920,1080")
    # Tùy chọn chạy headless (ẩn trình duyệt) - Bỏ comment nếu muốn chạy ẩn
    # options.add_argument("--headless")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')

    plugin_zip = None # Khởi tạo plugin_zip
    if proxy_config and all(key in proxy_config for key in ['host', 'port', 'user', 'password']):
        logger.info(f"Tạo driver với proxy: {proxy_config['host']}:{proxy_config['port']}")
        plugin_zip = _create_proxy_extension(proxy_config)
        options.add_extension(plugin_zip)
    else:
        logger.info("Tạo driver không có proxy.")

    try:
        driver = webdriver.Remote(
            command_executor='http://selenium:4444/wd/hub',
            options=options
        )
        # --- Chạy các script che giấu ---
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        # ...(Các script khác giữ nguyên)...
        driver.execute_script("""
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
                ],
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            window.chrome = {
                runtime: {},
            };
        """)
        logger.info(f"Đã tạo thành công driver mới: {driver.session_id}")
        return driver
    except WebDriverException as e:
        logger.error(f"Lỗi khi tạo driver mới: {e}", exc_info=True)
        return None
    finally:
        # Dọn dẹp file extension tạm
        if plugin_zip and os.path.exists(plugin_zip):
            try:
                os.remove(plugin_zip)
            except OSError as e:
                logger.warning(f"Không thể xóa file zip extension tạm: {e}")


def initialize_driver_pool(size=POOL_SIZE, proxy_config=None):
    """
    Khởi tạo pool với số lượng driver được chỉ định.
    Gọi hàm này khi ứng dụng FastAPI khởi động.
    """
    logger.info(f"Đang khởi tạo driver pool với kích thước: {size}...")
    count = 0
    for _ in range(size):
        driver = _create_new_driver(proxy_config)
        if driver:
            try:
                driver_pool.put_nowait(driver)
                active_drivers.append(driver) # Thêm vào danh sách để quản lý shutdown
                count += 1
                logger.info(f"Đã thêm driver {driver.session_id} vào pool ({count}/{size}).")
            except Full:
                logger.warning("Driver pool đã đầy, không thêm driver mới.")
                if driver: # Đóng driver thừa nếu pool đã đầy
                     try:
                         driver.quit()
                     except Exception: pass
                break
            except Exception as e:
                 logger.error(f"Lỗi khi thêm driver vào pool: {e}", exc_info=True)
                 if driver:
                     try:
                         driver.quit()
                     except Exception: pass
        else:
             logger.error("Không thể tạo driver mới để thêm vào pool.")
    logger.info(f"Khởi tạo driver pool hoàn tất. Số lượng driver trong pool: {driver_pool.qsize()}")

def get_driver():
    """
    Lấy một driver từ pool. Nếu pool rỗng, chờ hoặc tạo mới (tùy chọn).
    Hiện tại sẽ chờ lấy từ pool.
    """
    logger.debug(f"Đang chờ lấy driver từ pool (Hiện có: {driver_pool.qsize()})...")
    try:
        # Chờ tối đa 60 giây để lấy driver từ pool
        driver = driver_pool.get(block=True, timeout=60)
        logger.info(f"Đã lấy driver {driver.session_id} từ pool (Còn lại: {driver_pool.qsize()}).")
        # Kiểm tra xem driver còn hoạt động không
        try:
            # Lấy title là một cách nhẹ nhàng để kiểm tra kết nối
            _ = driver.title
            return driver
        except WebDriverException as e:
            logger.warning(f"Driver {driver.session_id} không hoạt động ({e}). Đang loại bỏ và thử tạo mới...")
            _close_driver_safely(driver) # Đóng driver lỗi
            # Cố gắng tạo một driver mới thay thế ngay lập tức
            import config # Import config ở đây để tránh circular dependency
            import random
            selected_proxy = random.choice(config.PROXY_LIST) if config.PROXY_LIST else None
            new_driver = _create_new_driver(selected_proxy)
            if new_driver:
                active_drivers.append(new_driver) # Thêm vào quản lý shutdown
                logger.info(f"Đã tạo driver mới {new_driver.session_id} để thay thế.")
                return new_driver # Trả về driver mới
            else:
                 logger.error("Không thể tạo driver mới thay thế driver lỗi.")
                 return None # Không có driver để trả về

    except Empty:
        logger.error("Timeout khi chờ lấy driver từ pool. Pool có thể đang quá tải hoặc tất cả driver đều lỗi.")
        return None # Hoặc có thể raise lỗi ở đây
    except Exception as e:
        logger.error(f"Lỗi không xác định khi lấy driver từ pool: {e}", exc_info=True)
        return None

def return_driver(driver):
    """
    Trả driver về pool sau khi sử dụng xong.
    """
    if driver:
        # Reset driver về trang trống để sẵn sàng cho lần dùng tiếp theo
        try:
            driver.get("about:blank")
            logger.debug(f"Đã reset driver {driver.session_id} về about:blank.")
        except WebDriverException as e:
            # Nếu driver bị lỗi (ví dụ: trình duyệt đã đóng), đóng nó và không trả về pool
            logger.warning(f"Driver {driver.session_id} bị lỗi khi reset ({e}). Đang đóng...")
            _close_driver_safely(driver)
            return

        try:
            driver_pool.put_nowait(driver)
            logger.info(f"Đã trả driver {driver.session_id} về pool (Hiện có: {driver_pool.qsize()}).")
        except Full:
            logger.warning(f"Pool đã đầy, không thể trả driver {driver.session_id}. Đang đóng driver này.")
            _close_driver_safely(driver)
        except Exception as e:
            logger.error(f"Lỗi khi trả driver {driver.session_id} về pool: {e}", exc_info=True)
            _close_driver_safely(driver) # Đóng driver nếu có lỗi khi trả

def _close_driver_safely(driver):
    """Hàm nội bộ để đóng driver và xóa khỏi active_drivers."""
    if driver in active_drivers:
        active_drivers.remove(driver)
    try:
        driver.quit()
        logger.info(f"Đã đóng driver {getattr(driver, 'session_id', 'unknown')}.")
    except Exception as e:
        logger.warning(f"Lỗi khi đóng driver {getattr(driver, 'session_id', 'unknown')}: {e}")

def shutdown_driver_pool():
    """
    Đóng tất cả các driver trong pool và danh sách active_drivers.
    Gọi hàm này khi ứng dụng FastAPI dừng.
    """
    logger.info("Đang đóng tất cả driver trong pool...")
    # Đóng driver còn trong pool
    while not driver_pool.empty():
        try:
            driver = driver_pool.get_nowait()
            _close_driver_safely(driver)
        except Empty:
            break
        except Exception as e:
             logger.error(f"Lỗi khi lấy driver từ pool để đóng: {e}", exc_info=True)

    # Đóng các driver đang được sử dụng (nếu có lỗi xảy ra và chưa được trả về)
    logger.info(f"Đang đóng {len(active_drivers)} active driver(s) còn lại...")
    drivers_to_close = list(active_drivers) # Tạo bản sao để tránh lỗi khi sửa list đang duyệt
    for driver in drivers_to_close:
        _close_driver_safely(driver)

    logger.info("Shutdown driver pool hoàn tất.")

# --- Hàm tạo proxy extension ---
def _create_proxy_extension(config):
    """
    Hàm nội bộ, tạo một file .zip extension để xác thực proxy.
    """
    manifest_json = """
    {
        "version": "1.0.0", "manifest_version": 2, "name": "Chrome Proxy",
        "permissions": ["proxy", "tabs", "unlimitedStorage", "storage", "<all_urls>", "webRequest", "webRequestBlocking"],
        "background": {"scripts": ["background.js"]}
    }
    """
    background_js = """
    var config = {
        mode: "fixed_servers",
        rules: {
          singleProxy: { scheme: "http", host: "%s", port: parseInt(%s) },
          bypassList: ["localhost"]
        }
    };
    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});
    function callbackFn(details) {
        return { authCredentials: { username: "%s", password: "%s" } };
    }
    chrome.webRequest.onAuthRequired.addListener(
        callbackFn, {urls: ["<all_urls>"]}, ['blocking']
    );
    """ % (config['host'], config['port'], config['user'], config['password'])

    plugin_dir = 'proxy_auth_plugin'
    plugin_zip = f'{plugin_dir}.zip' # Định nghĩa plugin_zip ở đây

    # Xóa thư mục/file cũ nếu tồn tại
    if os.path.exists(plugin_dir):
        shutil.rmtree(plugin_dir)
    if os.path.exists(plugin_zip):
         try:
            os.remove(plugin_zip)
         except OSError as e:
              logger.warning(f"Không thể xóa file zip cũ {plugin_zip}: {e}")

    os.makedirs(plugin_dir)

    try:
        with open(os.path.join(plugin_dir, "manifest.json"), 'w') as f:
            f.write(manifest_json)
        with open(os.path.join(plugin_dir, "background.js"), 'w') as f:
            f.write(background_js)

        with zipfile.ZipFile(plugin_zip, 'w', zipfile.ZIP_DEFLATED) as zp:
            for file in os.listdir(plugin_dir):
                zp.write(os.path.join(plugin_dir, file), file)
        logger.debug(f"Đã tạo file proxy extension: {plugin_zip}")
    except Exception as e:
         logger.error(f"Lỗi khi tạo proxy extension: {e}", exc_info=True)
         return None # Trả về None nếu có lỗi
    finally:
        # Luôn dọn dẹp thư mục tạm
        if os.path.exists(plugin_dir):
            shutil.rmtree(plugin_dir)

    return plugin_zip