FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium && playwright install-deps chromium

COPY . .

RUN mkdir -p /app/downloads /app/certs /app/browser_profiles

CMD gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 1 --timeout 300 webapp:app
