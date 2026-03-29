FROM python:3.10-slim

WORKDIR /app

# Copy requirements first
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion code
COPY ingestion/main.py .

# Copy SQL folder
COPY sql ./sql

CMD exec gunicorn --bind :8080 --workers 1 --threads 8 main:app
