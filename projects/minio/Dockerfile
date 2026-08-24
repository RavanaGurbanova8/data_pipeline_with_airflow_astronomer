FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY create_bucket.py .

CMD ["python", "create_bucket.py"]