from .interasia_scraper import InterasiaScraper
from .cma_cgm_scraper import CmaCgmScraper
from .maersk_scraper import MaerskScraper

SCRAPERS = {
    "interasia": InterasiaScraper,
    #"cma_cgm": CmaCgmScraper,
    "maersk": MaerskScraper
}

def get_scraper(name, driver, config):
    """
    Factory function để lấy một instance của scraper dựa vào tên.
    """
    scraper_class = SCRAPERS.get(name)
    if not scraper_class:
        raise ValueError(f"Không có scraper '{name}'.")
    return scraper_class(driver, config)