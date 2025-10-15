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
from .sitc_scraper import SitcScraper
from .goldstar_scraper import GoldstarScraper

SCRAPERS = {
    "IAL": InterasiaScraper,
    #"cma_cgm": CmaCgmScraper,
    "maersk": MaerskScraper,
    "msc": MscScraper,
    "CSL": CordeliaScraper,
    #"zim": ZimScraper,
    "PIL": PilScraper,
    "SNK": SinokorScraper,
    "UNIFEEDER": UnifeederScraper,
    "HEUNG-A": HeungALineScraper,
    "Tailwind": TailwindScraper,
    #"hmm": HmmScraper,
    "KMTC": KmtcScraper,
    "SITC": SitcScraper,
    "GOLSTAR": GoldstarScraper
}

def get_scraper(name, driver, config):
    """
    Factory function để lấy một instance của scraper dựa vào tên.
    """
    scraper_class = SCRAPERS.get(name)
    if not scraper_class:
        raise ValueError(f"Không có scraper '{name}'.")
    return scraper_class(driver, config)