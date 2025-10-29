import requests
import time
import statistics
from collections import defaultdict

# --- Dữ liệu đầu vào ---
tracking_data = {
    "COSCO": "6430435850",
    "CSL": "CSX25SHKSOK040022",
    "EMC": "237500510574",
    "GOLSTAR": "GOSUXNG1835417",
    "HEUNG-A": "HASLK01250703685",
    "IAL": "A49FA02393",
    "KMTC": "KMTCTAO8083417",
    "MSC": "EBKG14022741",
    "MSK": "259545107",
    "ONE": "RICFHB866600",
    "OSL": "NGBEWSL20JEA250088",
    "PAN": "PCLUKAN00418449",
    "PIL": "NGPX50385100",
    "SEALEAD": "SLSNBV06890",
    "SITC": "SITGTXCE557927",
    "SNK": "SNKO03K250801024",
    "Tailwind": "TSHGNGB25033529",
    "TRANSLINER": "TRLPKGKAT6016163",
    "UNIFEEDER": "TAOSOK25051819",
    "YML": "I492369242",
}

# --- Cấu hình ---
API_ENDPOINT = "http://localhost:8000/api/v1/track"
NUM_REQUESTS = 3

# --- Script chính ---
results = defaultdict(list)
average_times = {}

print("Bắt đầu gửi request...\n")

for service, bl_number in tracking_data.items():
    print(f"--- Đang test dịch vụ: {service} ---")
    response_times = []
    successful_requests = 0

    for i in range(NUM_REQUESTS):
        start_time = time.time()
        try:
            payload = {'service_name': service, 'bl_number': bl_number}
            # Sử dụng timeout để tránh chờ quá lâu
            response = requests.post(API_ENDPOINT, data=payload, timeout=120) # Timeout 120 giây
            end_time = time.time()
            duration = end_time - start_time

            if response.status_code == 200 or response.status_code == 404:
                response_times.append(duration)
                successful_requests += 1
                print(f"  Request {i+1}/{NUM_REQUESTS}: Thành công ({response.status_code}) - {duration:.2f} giây")
            else:
                print(f"  Request {i+1}/{NUM_REQUESTS}: Thất bại - Status code: {response.status_code} - {duration:.2f} giây")
                results[service].append(f"Request {i+1}: Failed (Status: {response.status_code})")

        except requests.exceptions.Timeout:
            end_time = time.time()
            duration = end_time - start_time
            print(f"  Request {i+1}/{NUM_REQUESTS}: Thất bại - Timeout sau {duration:.2f} giây")
            results[service].append(f"Request {i+1}: Failed (Timeout)")
        except requests.exceptions.RequestException as e:
            end_time = time.time()
            duration = end_time - start_time
            print(f"  Request {i+1}/{NUM_REQUESTS}: Thất bại - Lỗi kết nối: {e} - {duration:.2f} giây")
            results[service].append(f"Request {i+1}: Failed (Connection Error: {e})")
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            print(f"  Request {i+1}/{NUM_REQUESTS}: Thất bại - Lỗi không xác định: {e} - {duration:.2f} giây")
            results[service].append(f"Request {i+1}: Failed (Unknown Error: {e})")

        # Thêm một khoảng nghỉ nhỏ giữa các request để tránh quá tải
        time.sleep(1)

    # Tính thời gian trung bình nếu có request thành công
    if response_times:
        avg_time = statistics.mean(response_times)
        average_times[service] = avg_time
        print(f"  => Thời gian phản hồi trung bình ({successful_requests}/{NUM_REQUESTS} thành công): {avg_time:.2f} giây")
    else:
        average_times[service] = None
        print(f"  => Không có request thành công nào.")
    print("-" * (len(service) + 24)) # In đường phân cách
    print() # Thêm dòng trống

# --- In kết quả tổng hợp ---
print("\n--- KẾT QUẢ THỜI GIAN PHẢN HỒI TRUNG BÌNH ---")
# Sắp xếp theo thời gian trung bình tăng dần, đưa None về cuối
sorted_results = sorted(average_times.items(), key=lambda item: (item[1] is None, item[1]))

for service, avg_time in sorted_results:
    if avg_time is not None:
        print(f"- {service:<12}: {avg_time:.2f} giây")
    else:
        print(f"- {service:<12}: Không có request thành công")

print("\n--- Chi tiết lỗi (nếu có) ---")
error_count = 0
for service, errors in results.items():
    if errors:
        error_count += 1
        print(f"\n# {service}:")
        for error in errors:
            print(f"  - {error}")

if error_count == 0:
    print("Không có lỗi nào xảy ra trong quá trình test.")

print("\nHoàn tất.")