import os
import shutil
import zipfile
from selenium import webdriver

def create_driver(proxy_config=None):
    """
    Khởi tạo và trả về một WebDriver Chrome đã được cấu hình.
    Hỗ trợ tích hợp proxy có xác thực một cách linh hoạt.

    Args:
        proxy_config (dict, optional): Dictionary chứa thông tin proxy 
                                       gồm 'host', 'port', 'user', 'password'. 
                                       Mặc định là None, nghĩa là không dùng proxy.

    Returns:
        selenium.webdriver.Chrome: Đối tượng driver đã được cấu hình.
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("--disable-blink-features=AutomationControlled")
    #options.add_argument("--headless")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Nếu có cấu hình proxy, tạo extension để xác thực
    if proxy_config and all(key in proxy_config for key in ['host', 'port', 'user', 'password']):
        print(f"Initializing driver with proxy: {proxy_config['host']}:{proxy_config['port']}")
        plugin_zip = _create_proxy_extension(proxy_config)
        options.add_extension(plugin_zip)
    else:
        print("Initializing driver without proxy.")

    driver = webdriver.Chrome(options=options)
    
    # Dọn dẹp file extension tạm sau khi đã sử dụng
    if 'plugin_zip' in locals() and os.path.exists(plugin_zip):
        os.remove(plugin_zip)

    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def _create_proxy_extension(config):
    """
    Hàm nội bộ, tạo một file .zip extension để xác thực proxy.
    Điều này giúp xử lý các cửa sổ yêu cầu username/password tự động.
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