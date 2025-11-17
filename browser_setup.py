import logging
from playwright.async_api import async_playwright, PlaywrightContextManager, Browser, Page
from playwright_stealth import Stealth
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
async def create_playwright_context(proxy_config: Optional[dict] = None) -> Tuple[Optional[PlaywrightContextManager], Optional[Browser]]:
    """
    Khởi tạo Playwright và một phiên trình duyệt (Browser) (Async).
    """
    p = None
    browser = None
    try:
        p = await async_playwright().start()
        
        browser_options = {
            "headless": True,
            "channel": "chrome",
            "args": [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1920,1080'
            ]
        }

        if proxy_config and all(key in proxy_config for key in ['host', 'port', 'user', 'password']):
            logger.info(f"Initializing Playwright with proxy: {proxy_config['host']}:{proxy_config['port']}")
            browser_options["proxy"] = {
                "server": f"http://{proxy_config['host']}:{proxy_config['port']}",
                "username": proxy_config['user'],
                "password": proxy_config['password']
            }
        else:
            logger.info("Initializing Playwright without proxy.")

        browser = await p.chromium.launch(**browser_options)
        return p, browser
        
    except Exception as e:
        logger.error(f"Không thể khởi tạo Playwright hoặc trình duyệt: {e}", exc_info=True)
        if browser:
            await browser.close()
        if p:
            await p.stop()
        return None, None

async def create_page_context(browser: Browser) -> Optional[Page]:
    """
    Tạo một BrowserContext và Page mới, áp dụng stealth (Async).
    """
    if not browser:
        return None
        
    context = None
    page = None
    try:
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
            ignore_https_errors=True,
            java_script_enabled=True
        )
        
        await context.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort())

        page = await context.new_page()
        
        logger.debug("Áp dụng 'stealth' cho page mới...")
        await Stealth().apply_stealth_async(page)
        
        return page
        
    except Exception as e:
        logger.error(f"Không thể tạo page context mới: {e}", exc_info=True)
        if page:
            await page.close()
        if context:
            await context.close()
        return None