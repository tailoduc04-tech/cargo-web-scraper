from .interasia_scraper import InterasiaScraper
#from .cma_cgm_scraper import CmaCgmScraper
from .maersk_scraper import MaerskScraper
from .msc_scraper import MscScraper
from .cordelia_scraper import CordeliaScraper
#from .zim_scraper import ZimScraper
from .pil_scraper import PilScraper
from .sinokor_scraper import SinokorScraper
from .unifeeder_scraper import UnifeederScraper
from .heungaline_scraper import HeungALineScraper
from .tailwind_scraper import TailwindScraper
#from .hmm_scraper import HmmScraper
from .kmtc_scraper import KmtcScraper

SCRAPERS = {
    "interasia": InterasiaScraper,
    #"cma_cgm": CmaCgmScraper,
    "maersk": MaerskScraper,
    "msc": MscScraper,
    "cordelia": CordeliaScraper,
    #"zim": ZimScraper,
    "pil": PilScraper,
    "sinokor": SinokorScraper,
    "unifeeder": UnifeederScraper,
    "heungaline": HeungALineScraper,
    "tailwind": TailwindScraper,
    #"hmm": HmmScraper,
    "kmtc": KmtcScraper
}

def get_scraper(name, driver, config):
    """
    Factory function để lấy một instance của scraper dựa vào tên.
    """
    scraper_class = SCRAPERS.get(name)
    if not scraper_class:
        raise ValueError(f"Không có scraper '{name}'.")
    return scraper_class(driver, config)