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
# if PROXY_USER and PROXY_PASS:
#     ports = range(10001, 10008) # Từ 10001 đến 10007
#     PROXY_LIST = [
#         {"host": "dc.decodo.com", "port": str(port), "user": PROXY_USER, "password": PROXY_PASS}
#         for port in ports
#     ]
# else:
#     print("Proxy username or password not found in .env file. Running without proxy.")


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
        "api_url": "https://www.msc.com/api/feature/tools/TrackingInfo",
    },
    "CSL": {
        "url": "https://cordelialine.com/bltracking/?blno=",
        "api_url": "https://erp.cordelialine.com/cordelia/app/bltracking/bltracingweb?blno={blno}",
    },
    "ZIM": {
        "url": "https://www.zim.com/tools/track-a-shipment",
        "api_url": "https://apigw.zim.com/digital/TrackShipment/v1/",
        "subscription_key": "9d63cf020a4c4708a7b0ebfe39578300"
    },
    "PIL": {
        "url": "https://www.pilship.com/digital-solutions/?tab=customer&id=track-trace&label=containerTandT&module=TrackTraceJob&refNo=<BL_NUMBER>",
        "get_n_url": "https://www.pilship.com/wp-content/themes/hello-theme-child-master/pil-api/common/get-n.php",
        "track_url": "https://www.pilship.com/wp-content/themes/hello-theme-child-master/pil-api/trackntrace-containertnt.php",
        "track_container_url": "https://www.pilship.com/wp-content/themes/hello-theme-child-master/pil-api/trackntrace-containertnt-trace.php",
    },
    "SNK": {
        "url": "https://ebiz.sinokor.co.kr/BLDetail?blno=",
    },
    "UNIFEEDER": {
        "url": "https://www.unifeeder.cargoes.com/tracking?ID=",
        "api_url": "https://api-fr.cargoes.com/track/avana",
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
        "url": "https://www.ekmtc.com/index.html#/cargo-tracking",
        "api_step1_url": "https://api.ekmtc.com/trans/trans/cargo-tracking/",
        "api_step2_url": "https://api.ekmtc.com/trans/trans/cargo-tracking/{bkgNo}/close-info",
    },
    "SITC": {
        "url": "https://ebusiness.sitcline.com/#/topMenu/cargoTrack",
        "base_url": "https://ebusiness.sitcline.com/",
        "api_url": "https://ebusiness.sitcline.com/api/equery/cargoTrack/searchTrack",
    },
    "GOLSTAR": {
        "url": "https://www.goldstarline.com/tools/track_shipment",
        "api_url": "https://www.goldstarline.com/api/cms",
    },
    "YML": {
        "url": "https://e-solution.yangming.com/e-service/track_trace/track_trace_cargo_tracking.aspx",
        "landing_url": "https://www.yangming.com/en/esolution/cargo_tracking",
        "api_url": "https://www.yangming.com/api/CargoTracking/GetTracking",
    },
    "ONE": {
        "url": "https://ecomm.one-line.com/one-ecom/manage-shipment/cargo-tracking?trakNoParam=",
        "search_url": "https://ecomm.one-line.com/api/v1/edh/containers/track-and-trace/search",
        "events_url": "https://ecomm.one-line.com/api/v1/edh/containers/track-and-trace/cop-events",
    },
    "COSCO": {
        "url": "https://elines.coscoshipping.com/ebusiness/cargotracking"
    },
    "EMC": {
        "url": "https://ct.shipmentlink.com/servlet/TDB1_CargoTracking.do"
    },
    "OSL": {
        "url": "https://star-liners.com/track-my-shipment/",
        "api_url": "https://star-liners.com/wp-admin/admin-ajax.php",
        "nonce": "23a2b8b108",
    },
    "PAN": {
        "url": "http://www.shippingline.org/track/?type=bill&container={BL_NUMBER}&line=pancont&track=Track+container",
        "api_url": "https://www.pancon.co.kr/pan/selectWeb212AR.pcl",
    },
    "SEALEAD": {
        "url": "https://www.sea-lead.com/track-shipment/",
    },
    "TRANSLINER": {
        "url": "https://translinergroup.track.tigris.systems/?ref=",
        "api_url": "https://translinergroup.track.tigris.systems/api/bookings/{booking_number}",
    }
}