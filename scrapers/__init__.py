from .api_scraper import ApiScraper
from .selenium_scraper import SeleniumScraper
from .playwright_scraper import PlaywrightScraper

# --- Import all scrapers from sub-packages ---
from .api.msc_scraper import MscScraper
from .api.cordelia_scraper import CordeliaScraper
from .api.pil_scraper import PilScraper
from .api.sinokor_scraper import SinokorScraper
from .api.unifeeder_scraper import UnifeederScraper
from .api.heungaline_scraper import HeungALineScraper
from .api.kmtc_scraper import KmtcScraper
from .api.sitc_scraper import SitcScraper
from .api.goldstar_scraper import GoldstarScraper
from .api.one_scraper import OneScraper
from .api.osl_scraper import OslScraper
from .api.pan_scraper import PanScraper
from .api.sealead_scraper import SealeadScraper
from .api.transliner_scraper import TranslinerScraper
from .api.yangming_scraper import YangmingScraper

from .selenium.interasia_scraper import InterasiaScraper
from .selenium.tailwind_scraper import TailwindScraper
from .selenium.cosco_scraper import CoscoScraper
from .selenium.emc_scraper import EmcScraper

from .playwright.maersk_scraper import MaerskScraper

SCRAPERS = {
    "IAL": InterasiaScraper,
    "MSK": MaerskScraper,
    "MSC": MscScraper,
    "CSL": CordeliaScraper,
    "PIL": PilScraper,
    "SNK": SinokorScraper,
    "UNIFEEDER": UnifeederScraper,
    "HEUNG-A": HeungALineScraper,
    "Tailwind": TailwindScraper,
    "KMTC": KmtcScraper,
    "SITC": SitcScraper,
    "GOLSTAR": GoldstarScraper,
    "YML": YangmingScraper,
    "ONE": OneScraper,
    "COSCO": CoscoScraper,
    "EMC": EmcScraper,
    "OSL": OslScraper,
    "PAN": PanScraper,
    "SEALEAD": SealeadScraper,
    "TRANSLINER": TranslinerScraper
}

# --- Strategy definition for the app ---
SCRAPER_STRATEGY = {
    # Selenium Scrapers
    "IAL": "selenium",
    "Tailwind": "selenium",
    "COSCO": "selenium",
    "EMC": "selenium",
    
    # Playwright Scrapers
    "MSK": "playwright",
    
    # API Scrapers
    "MSC": "api",
    "CSL": "api",
    "PIL": "api",
    "SNK": "api",
    "UNIFEEDER": "api",
    "HEUNG-A": "api",
    "KMTC": "api",
    "SITC": "api",
    "GOLSTAR": "api",
    "ONE": "api",
    "OSL": "api",
    "PAN": "api",
    "SEALEAD": "api",
    "TRANSLINER": "api",
    "YML": "api"
}

def get_scraper(name, driver_or_page, config):
    """
    Factory function để lấy một instance của scraper dựa vào tên.
    Cập nhật để xử lý các hàm khởi tạo khác nhau (driver, page, hoặc không gì cả).
    """
    scraper_class = SCRAPERS.get(name)
    if not scraper_class:
        raise ValueError(f"Không có scraper '{name}'.")

    strategy = SCRAPER_STRATEGY.get(name)

    if strategy == "selenium":
        # SeleniumScraper yêu cầu (driver, config)
        return scraper_class(driver=driver_or_page, config=config)
    elif strategy == "playwright":
        # PlaywrightScraper yêu cầu (page, config)
        return scraper_class(page=driver_or_page, config=config)
    elif strategy == "api":
        # ApiScraper chỉ yêu cầu (config)
        # Truyền driver=None để tương thích với lớp con (nếu nó ghi đè __init__)
        return scraper_class(driver=None, config=config) 
    else:
        raise ValueError(f"Chiến lược scraper không xác định cho: {name}")