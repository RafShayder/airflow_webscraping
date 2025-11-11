# === Base: Airflow oficial ===
FROM apache/airflow:3.1.0-python3.12

USER root

# === 1. Instalar utilitarios base y dependencias de Chrome ===
RUN apt-get update && apt-get install -y --no-install-recommends \
    vim wget unzip fonts-liberation \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libatspi2.0-0 libcairo2 libcups2 libdbus-1-3 \
    libdrm2 libgbm1 libglib2.0-0 libgtk-3-0 \
    libnspr4 libnss3 libpango-1.0-0 \
    libudev1 libvulkan1 libx11-6 libxcb1 \
    libxcomposite1 libxdamage1 libxext6 libxfixes3 \
    libxkbcommon0 libxrandr2 xdg-utils \
    libcurl4 libexpat1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# === 2. Instalar Chrome y ChromeDriver ===
# Asegúrate de tener estos archivos en la misma carpeta del Dockerfile
COPY chrome_140_amd64.deb /tmp/
RUN dpkg -i /tmp/chrome_140_amd64.deb || apt-get install -f -y && rm /tmp/chrome_140_amd64.deb

COPY chromedriver-linux64.zip /tmp/
RUN unzip /tmp/chromedriver-linux64.zip -d /tmp/ && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver*

# === 3. Instalar dependencias Python (Airflow + scraping) ===
USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# === 4. Configurar PATH y PYTHONPATH ===
ENV PATH="/usr/local/bin:${PATH}"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/proyectos"

# === 5. Crear directorio de proyectos ===
USER root
RUN mkdir -p /opt/airflow/proyectos && chown -R airflow:root /opt/airflow/proyectos
USER airflow

# === 6. Directorio de trabajo por defecto ===
WORKDIR /opt/airflow