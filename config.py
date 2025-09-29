MAX_RETRIES = 3
DELAY_BETWEEN_REQUESTS = (3, 7)
RETRY_DELAY_EXPONENT_BASE = 2
PROXY_CONFIG = {
    "host": "dc.decodo.com", "port": "8000",
    "user": "sp8s7h0c08", "password": "JiRq_amsv5J95x7yOw"
}

SCRAPER_CONFIGS = {
    "interasia": {
        "url": "https://www.interasia.cc/e-service/cargotracking.html",
        "output_files": {
            "main_results": "interasia_results.csv",
            "bl_summaries": "interasia_bl_summaries.csv",
            "bl_details": "interasia_bl_details.csv",
            "container_histories": "interasia_container_histories.csv"
        }
    }
}