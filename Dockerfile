FROM python:3.12-slim

# Playwright dependencies (system libs for Chromium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnspr4 libnss3 libasound2t64 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libgbm1 libgtk-3-0 libpango-1.0-0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

COPY . .

# Entrypoint overridden per job via --command flag
CMD ["python", "scrape_event_urls.py"]
