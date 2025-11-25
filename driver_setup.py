import os
import shutil
import zipfile
import uuid
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# Logger để debug
logger = logging.getLogger(__name__)

def create_driver(proxy_config=None, page_load_strategy='eager'):
    options = Options()
    options.page_load_strategy = page_load_strategy
    
    options.add_argument("--headless=new")

    # --- Cấu hình cơ bản ---
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # [QUAN TRỌNG] Đã xóa dòng '--disable-dev-shm-usage' để tận dụng shm_size: 4g trong Docker
    # options.add_argument('--disable-dev-shm-usage') 
    
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument("--window-size=1920,1080")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')

    plugin_zip = None

    if proxy_config and all(key in proxy_config for key in ['host', 'port', 'user', 'password']):
        # [Cải tiến] Tạo extension với tên file unique để tránh xung đột
        plugin_zip = _create_proxy_extension(proxy_config)
        if plugin_zip:
            options.add_extension(plugin_zip)
    
    driver = None
    try:
        # Kết nối tới Selenium Hub
        service = Service(ChromeDriverManager().install())
        
        driver = webdriver.Chrome(service=service, options=options)

        # --- Chạy script che giấu Selenium ---
        # Nếu lệnh này timeout, driver sẽ được đóng ở block except bên dưới -> Hết Leak
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
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

        return driver

    except Exception as e:
        logger.error(f"Lỗi khởi tạo Driver: {e}")
        # [QUAN TRỌNG] Nếu setup thất bại, phải tắt ngay driver vừa mở để không bị treo (Zombie)
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        raise e

    finally:
        # Luôn dọn dẹp file zip tạm dù thành công hay thất bại
        if plugin_zip and os.path.exists(plugin_zip):
            try:
                os.remove(plugin_zip)
            except OSError:
                pass

def _create_proxy_extension(config):
    try:
        # Tạo ID ngẫu nhiên cho folder tạm
        unique_id = str(uuid.uuid4())
        plugin_dir = f'proxy_auth_plugin_{unique_id}'
        
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

        os.makedirs(plugin_dir, exist_ok=True)

        with open(os.path.join(plugin_dir, "manifest.json"), 'w') as f:
            f.write(manifest_json)
        with open(os.path.join(plugin_dir, "background.js"), 'w') as f:
            f.write(background_js)

        plugin_zip = f'{plugin_dir}.zip'
        with zipfile.ZipFile(plugin_zip, 'w', zipfile.ZIP_DEFLATED) as zp:
            for file in os.listdir(plugin_dir):
                zp.write(os.path.join(plugin_dir, file), file)
        
        # Xóa folder nguồn ngay sau khi zip xong
        shutil.rmtree(plugin_dir)
        
        return plugin_zip
    except Exception as e:
        logger.error(f"Lỗi tạo proxy extension: {e}")
        return None