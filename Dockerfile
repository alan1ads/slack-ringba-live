FROM python:3.11-slim

# Install Chrome and dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y \
    google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver with a specific version that we know works
RUN mkdir -p /tmp/chromedriver \
    && cd /tmp/chromedriver \
    && CHROMEDRIVER_VERSION="113.0.5672.63" \
    && echo "Using Chrome Driver version: $CHROMEDRIVER_VERSION" \
    && wget -q https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip \
    && mv chromedriver /usr/local/bin/chromedriver \
    && ln -sf /usr/local/bin/chromedriver /usr/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && chmod +x /usr/bin/chromedriver \
    && cd / \
    && rm -rf /tmp/chromedriver \
    && echo "ChromeDriver installed at: " \
    && which chromedriver \
    && chromedriver --version

# Set up working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the app code
COPY . .

# Create necessary directories
RUN mkdir -p data screenshots

# Set environment variables
ENV USE_HEADLESS=true
ENV PYTHONUNBUFFERED=true
ENV CHROME_OPTIONS="--headless=new --no-sandbox --disable-dev-shm-usage --disable-gpu --disable-software-rasterizer --disable-extensions"
ENV PORT=8080
ENV PATH="/usr/local/bin:/usr/bin:${PATH}"

# Expose the port for the web service
EXPOSE 8080

# Run the web service
CMD ["python", "src/web_service.py"] 