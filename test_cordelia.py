import os
import re
import requests
import json
from pathlib import Path

bl_number = "CSX25SHKSOK040022"
api_url = f"https://erp.cordelialine.com/cordelia/app/bltracking/bltracingweb?blno={bl_number}"

# Thử thêm một số header phổ biến mà các AJAX request hay dùng
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept': 'application/json, text/javascript, */*; q=0.01'
    # Có thể cần thêm 'Referer': 'https://cordelialine.com/' (trang gốc)
}

try:
    print(f"Đang gọi API: {api_url}")
    response = requests.get(api_url, headers=headers, timeout=30) # Thêm timeout
    response.raise_for_status() # Kiểm tra lỗi HTTP (4xx, 5xx)

    # Thử parse JSON
    data = response.json()
    print("Thành công! Dữ liệu trả về:")
    print(json.dumps(data, indent=2)) # In JSON cho đẹp

    # --- Save the raw JSON response to a file in the output/ directory ---
    try:
        out_dir = Path("output")
        out_dir.mkdir(parents=True, exist_ok=True)

        # sanitize bl_number for filename
        safe_bl = re.sub(r'[^A-Za-z0-9_.-]', '_', bl_number)
        out_path = out_dir / f"cordelia_{safe_bl}.json"

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Đã lưu phản hồi JSON vào: {out_path}")
    except Exception as e:
        print(f"Không thể lưu file JSON: {e}")

    # --- TODO: Xử lý dữ liệu JSON này để map vào N8nTrackingInfo ---
    # Cậu cần xem cấu trúc JSON trả về (từ print ở trên)
    # và viết code để lấy các trường tương ứng (blNo, pol, sobDate,...)
    # Ví dụ (cần điều chỉnh dựa trên JSON thực tế):
    if data and data.get("searchList"):
        item = data["searchList"][0] # Giả sử chỉ lấy item đầu tiên
        # normalized_data = N8nTrackingInfo(
        #     BlNumber=item.get("blNo"),
        #     Pol=item.get("pol"),
        #     Atd=self._format_date(item.get("sobDate")), # SOB Date là ATD
        #     Pod=item.get("webFpod"), # Final POD
        #     Eta=self._format_date(item.get("flEta") or item.get("slEta") or item.get("tlEta") or item.get("frleta")), # Cần logic phức tạp để chọn đúng ETA FPOD
        #     BookingStatus=item.get("containerStatusDescription"),
        #     # Các trường transit không có trực tiếp, cần suy luận nếu có
        #     # ... (thêm các trường khác)
        # )
        # print("\nDữ liệu đã chuẩn hóa (ví dụ):")
        # print(normalized_data)
        pass # Placeholder cho code xử lý JSON


except requests.exceptions.RequestException as e:
    print(f"Lỗi khi gọi API: {e}")
except json.JSONDecodeError:
    print("Lỗi: Không thể parse JSON từ phản hồi.")
    print("Nội dung phản hồi:", response.text)
except Exception as e:
    print(f"Lỗi không xác định: {e}")