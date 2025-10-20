# Cargo Web Scraper API

Đây là một ứng dụng web API sử dụng FastAPI và Selenium để cào dữ liệu tracking container từ các trang web của hãng tàu khác nhau.

## Yêu cầu

* **Docker:**
* **Docker Compose:**

## Cài đặt và Chạy

1.  **Cấu hình Biến Môi trường:**
    * Tạo một file tên là `.env` trong thư mục gốc.
    * Thêm thông tin proxy vào file `.env`:
        ```dotenv
        PROXY_USER_NAME="<your_proxy_username>"
        PROXY_PASSWORD="<your_proxy_password>"
        ```
    * Nếu không có thông tin proxy hoặc không muốn dùng, ứng dụng vẫn sẽ chạy nhưng không qua proxy.

2.  **Build và Chạy Docker Containers:**
    * Mở terminal hoặc command prompt trong thư mục gốc của project (`cargo-web-scraper`).
    * Chạy lệnh sau:
        ```bash
        docker-compose up --build -d
        ```
        * `--build`: Build lại image nếu có thay đổi trong `Dockerfile.app` hoặc code.
        * `-d`: Chạy container ở chế độ detached (chạy nền).

3.  **Kiểm tra Trạng thái:**
    * Đợi một lát để các container khởi động. Có thể kiểm tra trạng thái bằng lệnh:
        ```bash
        docker-compose ps
        ```
    * Cần 2 container (`cargo_scraper_app` và `selenium_chrome`) đang chạy (`running` hoặc `up`).

## Sử dụng API

Sau khi các container đã chạy, API sẽ sẵn sàng tại địa chỉ `http://localhost:8000`.

* **Giao diện Web UI (Swagger):** Mở trình duyệt và truy cập `http://localhost:8000/docs` để xem tài liệu API và thử nghiệm endpoint.
* **Endpoint chính:**
    * `GET /api/v1/services`: Lấy danh sách các hãng tàu khả dụng
    * `POST /api/v1/track`: Tìm kiếm thông tin tracking trên một hãng tàu cụ thể.
        * **Form Data:**
            * `bl_number`: (Bắt buộc) Mã vận đơn hoặc mã booking cần tra cứu.
            * `service_name`: (Bắt buộc) Tên viết tắt của hãng tàu (ví dụ: "MSK", "PIL", "COSCO", "SNK",...).

**Ví dụ sử dụng `curl`:**

* Tìm thông tin các hãng tàu khả dụng:
    ```bash
    curl http://localhost:8000/api/v1/services
    ```
* Tìm thông tin cho mã `YOUR_BL_NUMBER` trên hãng tàu Maersk (`MSK`):
    ```bash
    curl -X POST -F "bl_number=YOUR_BL_NUMBER" -F "service_name=MSK" http://localhost:8000/api/v1/track
    ```