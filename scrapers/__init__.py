from .api_scraper import ApiScraper
from .selenium_scraper import SeleniumScraper

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

from .selenium.interasia_scraper import InterasiaScraper
from .selenium.maersk_scraper import MaerskScraper
from .selenium.tailwind_scraper import TailwindScraper
from .selenium.yangming_scraper import YangmingScraper
from .selenium.cosco_scraper import CoscoScraper
from .selenium.emc_scraper import EmcScraper

# --- Central registry of all scrapers ---
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
    "MSK": "selenium",
    "Tailwind": "selenium",
    "YML": "selenium",
    "COSCO": "selenium",
    "EMC": "selenium",
    
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
    "TRANSLINER": "api"
}

def get_scraper(name, driver, config):
    """
    Factory function để lấy một instance của scraper dựa vào tên.
    """
    scraper_class = SCRAPERS.get(name)
    if not scraper_class:
        raise ValueError(f"Không có scraper '{name}'.")
    
    # The caller (app.py) will decide whether to pass a real driver
    # or None based on the strategy.
    return scraper_class(driver=driver, config=config)