import os
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env
load_dotenv()

# --- Cấu hình chung ---
MAX_RETRIES = 3
DELAY_BETWEEN_REQUESTS = (3, 7)
RETRY_DELAY_EXPONENT_BASE = 2

# --- Cấu hình Proxy (Đọc từ biến môi trường) ---
PROXY_USER = os.getenv("PROXY_USER_NAME")
PROXY_PASS = os.getenv("PROXY_PASSWORD")

# Tạo danh sách proxy chỉ khi có đủ thông tin
PROXY_LIST = []
if PROXY_USER and PROXY_PASS:
    ports = range(10001, 10008) # Từ 10001 đến 10007
    PROXY_LIST = [
        {"host": "dc.decodo.com", "port": str(port), "user": PROXY_USER, "password": PROXY_PASS}
        for port in ports
    ]
else:
    print("Proxy username or password not found in .env file. Running without proxy.")


# --- Cấu hình Scraper ---
SCRAPER_CONFIGS = {
    "IAL": {
        "url": "https://www.interasia.cc/Service/Form?servicetype=0",
    },
    # CMA CGM gắt quá không lấy được data
    #"cma_cgm": {
    #    "url": "https://www.cma-cgm.com/ebusiness/tracking",
    #},
    "MSK": {
        "url": "https://www.maersk.com/tracking/",
    },
    "MSC": {
        "url": "https://www.msc.com/en/track-a-shipment",
    },
    "CSL": {
        "url": "https://cordelialine.com/bltracking/?blno=",
    },
    #"zim": {
    #    "url": "https://www.zim.com/tools/track-a-shipment",
    #    "output_files": {
    #        "tracking_info": "zim_tracking_info.csv"
    #    }
    #},
    "PIL": {
        "url": "https://www.pilship.com/digital-solutions/?tab=customer&id=track-trace&label=containerTandT&module=TrackTraceJob&refNo=<BL_NUMBER>",
    },
    "SNK": {
        "url": "https://ebiz.sinokor.co.kr/BLDetail?blno=",
    },
    "UNIFEEDER": {
        "url": "https://www.unifeeder.cargoes.com/tracking?ID=",
    },
    "HEUNG-A": {
        "url": "https://ebiz.heungaline.com/BLDetail?blno="
     },
    "Tailwind": {
        "url": "https://tailwind-shipping.com/en/home"
     },
    #"hmm": {
    #   "url": "https://www.hmm21.com/e-service/general/trackNTrace/TrackNTrace.do"
    #}
    "KMTC": {
        "url": "https://www.ekmtc.com/index.html#/cargo-tracking"
    },
    "SITC": {
        "url": "https://ebusiness.sitcline.com/#/topMenu/cargoTrack"
    },
    "GOLSTAR": {
        "url": "https://www.goldstarline.com/tools/track_shipment"
    },
    "YML": {
        "url": "https://e-solution.yangming.com/e-service/track_trace/track_trace_cargo_tracking.aspx"
    },
    "ONE": {
        "url": "https://ecomm.one-line.com/one-ecom/manage-shipment/cargo-tracking?trakNoParam="
    },
    "COSCO": {
        "url": "https://elines.coscoshipping.com/ebusiness/cargotracking"
    }
}