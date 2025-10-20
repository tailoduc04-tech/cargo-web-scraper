import os
import shutil
import zipfile
from selenium import webdriver

def create_driver(proxy_config=None):
    """
    Khởi tạo driver bằng cách kết nối tới Selenium Grid/Hub đang chạy
    và sử dụng extension để xử lý xác thực proxy.
    """
    options = webdriver.ChromeOptions()

    # --- Các tùy chọn nâng cao để chống phát hiện ---
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument("--window-size=1920,1080")

    # User-agent
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')

    # Nếu có cấu hình proxy, tạo và thêm extension để xác thực
    if proxy_config and all(key in proxy_config for key in ['host', 'port', 'user', 'password']):
        print(f"Initializing remote driver with proxy: {proxy_config['host']}:{proxy_config['port']}")
        plugin_zip = _create_proxy_extension(proxy_config)
        options.add_extension(plugin_zip)
    else:
        print("Initializing remote driver without proxy.")

    # Kết nối tới Selenium Hub đang chạy tại địa chỉ 'http://selenium:4444'
    driver = webdriver.Remote(
        command_executor='http://selenium:4444/wd/hub',
        options=options
    )

    # Dọn dẹp file extension tạm sau khi đã sử dụng
    if 'plugin_zip' in locals() and os.path.exists(plugin_zip):
        os.remove(plugin_zip)

    # --- Chạy các script để che giấu dấu vết của Selenium ---
    # Script để ẩn 'navigator.webdriver'
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    # Script để giả mạo các thuộc tính khác mà bot hay thiếu
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
    if os.path.exists(plugin_dir):
        shutil.rmtree(plugin_dir)
    os.makedirs(plugin_dir)

    with open(os.path.join(plugin_dir, "manifest.json"), 'w') as f:
        f.write(manifest_json)
    with open(os.path.join(plugin_dir, "background.js"), 'w') as f:
        f.write(background_js)

    plugin_zip = f'{plugin_dir}.zip'
    with zipfile.ZipFile(plugin_zip, 'w', zipfile.ZIP_DEFLATED) as zp:
        for file in os.listdir(plugin_dir):
            zp.write(os.path.join(plugin_dir, file), file)
    shutil.rmtree(plugin_dir)
    return plugin_zip