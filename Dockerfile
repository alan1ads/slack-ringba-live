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

# Get Chrome version and install matching ChromeDriver
RUN google-chrome --version | grep -oE "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+" > /tmp/chrome_version.txt \
    && CHROME_VERSION=$(cat /tmp/chrome_version.txt) \
    && CHROME_MAJOR_VERSION=$(echo $CHROME_VERSION | cut -d. -f1) \
    && echo "Chrome version: $CHROME_VERSION (Major: $CHROME_MAJOR_VERSION)" \
    && mkdir -p /tmp/chromedriver \
    && cd /tmp/chromedriver \
    && wget -q -O chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/$CHROME_VERSION/linux64/chromedriver-linux64.zip" \
    && unzip chromedriver.zip \
    && mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && ln -sf /usr/local/bin/chromedriver /usr/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && chmod +x /usr/bin/chromedriver \
    && cd / \
    && rm -rf /tmp/chromedriver \
    && echo "ChromeDriver installed at: $(which chromedriver)" \
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
# Simpler Chrome options for greater stability
ENV CHROME_OPTIONS="--headless=new --no-sandbox --disable-dev-shm-usage --disable-gpu"
ENV PORT=8080
ENV PATH="/usr/local/bin:/usr/bin:${PATH}"
# Increase memory allocation
ENV NODE_OPTIONS="--max-old-space-size=8192"
# Add shared memory size for Chrome
ENV CHROME_SHIM_ARGS="--shm-size=2gb"
ENV PYTHONIOENCODING=UTF-8

# Expose the port for the web service
EXPOSE 8080

# Run the web service
CMD ["python", "src/web_service.py"] 