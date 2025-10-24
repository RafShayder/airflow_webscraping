FROM python:3.13-slim

# Evitar pyc y forzar logs inmediatos
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dependencias de sistema requeridas por Chrome y Selenium
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    shared-mime-info \
    unzip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Instalar Chrome for Testing y Chromedriver emparejados
RUN set -eux; \
    CHROME_VERSION="$(curl -sSL https://storage.googleapis.com/chrome-for-testing-public/LATEST_RELEASE_STABLE)"; \
    wget -q -O /tmp/chrome.zip "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip"; \
    wget -q -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chromedriver-linux64.zip"; \
    unzip /tmp/chrome.zip -d /opt; \
    unzip /tmp/chromedriver.zip -d /opt; \
    mv /opt/chrome-linux64 /opt/chrome; \
    mv /opt/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver; \
    ln -s /opt/chrome/chrome /usr/local/bin/google-chrome; \
    chmod +x /usr/local/bin/chromedriver /usr/local/bin/google-chrome; \
    rm -rf /tmp/chrome.zip /tmp/chromedriver.zip /opt/chromedriver-linux64

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY . .

# Directorio para descargas
RUN mkdir -p /app/temp

# Variables por defecto
ENV PYTHONPATH=/app \
    DOWNLOAD_PATH=/app/temp \
    HEADLESS=true

# Mantener contenedor disponible para ejecuciones manuales
CMD ["tail", "-f", "/dev/null"]
