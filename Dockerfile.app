# Sử dụng image Python 3.11 làm base image
FROM python:3.11-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Sao chép và cài đặt các thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps chrome

# Sao chép toàn bộ mã nguồn ứng dụng
COPY . .

# Mở cổng cho ứng dụng FastAPI
EXPOSE 8000

# Lệnh để chạy ứng dụng
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]