FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Mặc định: phát luồng rating vào Kafka. Dữ liệu được mount vào /app/data.
CMD ["python", "stream_producer.py"]
