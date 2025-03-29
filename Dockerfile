FROM python:3.9-slim

# Install basic dependencies
RUN apt-get update && apt-get install -y \
    wget gnupg curl unzip git \
    fonts-liberation libasound2 libatk-bridge2.0-0 \
    libatk1.0-0 libatspi2.0-0 libcups2 libdbus-1-3 \
    libdrm2 libgbm1 libgtk-3-0 libnspr4 libnss3 \
    libwayland-client0 libxcomposite1 libxdamage1 \
    libxfixes3 libxkbcommon0 libxrandr2 xdg-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome stable (minimal install)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver using direct download from Chrome for Testing
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1-3) \
    && echo "Chrome version: $CHROME_VERSION" \
    && mkdir -p /tmp/chromedriver \
    && cd /tmp/chromedriver \
    && wget -q https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_STABLE \
    && CFT_VERSION=$(cat LATEST_RELEASE_STABLE) \
    && echo "Using Chrome for Testing version: $CFT_VERSION" \
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/$CFT_VERSION/linux64/chromedriver-linux64.zip" \
    && unzip chromedriver-linux64.zip \
    && mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/chromedriver \
    && echo "ChromeDriver installed at: $(which chromedriver)" \
    && chromedriver --version || echo "ChromeDriver version check failed, but continuing"

# Create app directory 
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Add download directory with proper permissions
RUN mkdir -p /tmp/downloads && chmod 777 /tmp/downloads

# Add shm-size environment (this helps with Chrome stability)
ENV SHM_SIZE=2g
ENV NODE_OPTIONS="--max-old-space-size=2048"
ENV CHROME_OPTIONS="--headless=new --no-sandbox --disable-dev-shm-usage --disable-gpu --single-process --disable-extensions"
ENV PYTHONUNBUFFERED=1

# Run the script
CMD ["python", "src/simple_export.py"] 