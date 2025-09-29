from .interasia_scraper import InterasiaScraper

SCRAPERS = {
    "interasia": InterasiaScraper
}

def get_scraper(name, driver, config):
    """
    Factory function để lấy một instance của scraper dựa vào tên.
    """
    scraper_class = SCRAPERS.get(name)
    if not scraper_class:
        raise ValueError(f"No scraper found for '{name}'. Check SCRAPERS dictionary in scrapers/__init__.py")
    return scraper_class(driver, config)