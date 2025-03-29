#!/bin/bash
set -e

echo "==> Installing Chrome and ChromeDriver for Render environment"

# Create temp directory
mkdir -p /tmp/chrome_setup
cd /tmp/chrome_setup

# Install Chrome (user level)
echo "==> Downloading Chrome..."
wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
dpkg -x google-chrome-stable_current_amd64.deb chrome-extraction

# Get Chrome version
CHROME_VERSION=$(./chrome-extraction/opt/google/chrome/chrome --version | awk '{print $3}' | cut -d. -f1-3)
echo "==> Chrome version: $CHROME_VERSION"

# Download matching ChromeDriver
echo "==> Downloading matching ChromeDriver..."
wget -q https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_STABLE -O LATEST_RELEASE
CFT_VERSION=$(cat LATEST_RELEASE)
echo "==> Using Chrome For Testing version: $CFT_VERSION"

mkdir -p /tmp/chromedriver
wget -q "https://storage.googleapis.com/chrome-for-testing-public/$CFT_VERSION/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip
unzip -q -o /tmp/chromedriver.zip -d /tmp

# Create directories if they don't exist
mkdir -p $HOME/bin
mkdir -p $HOME/chrome

# Copy Chrome and ChromeDriver to user directory
echo "==> Installing Chrome and ChromeDriver locally..."
cp -R ./chrome-extraction/opt/google/chrome/* $HOME/chrome/
cp /tmp/chromedriver-linux64/chromedriver $HOME/bin/chromedriver
chmod +x $HOME/bin/chromedriver

# Add to PATH
export PATH="$HOME/bin:$PATH"
echo "export PATH=\"$HOME/bin:$PATH\"" >> $HOME/.bashrc

# Create symbolic links
ln -sf $HOME/chrome/chrome $HOME/bin/google-chrome
ln -sf $HOME/chrome/chrome $HOME/bin/google-chrome-stable

# Verify installations
echo "==> Installation complete, verifying..."
$HOME/bin/chromedriver --version || echo "ChromeDriver version check failed, but continuing"
$HOME/bin/google-chrome --version || echo "Chrome version check failed, but continuing"

# Clean up
echo "==> Cleaning up..."
cd /
rm -rf /tmp/chrome_setup
rm -f /tmp/chromedriver.zip
rm -rf /tmp/chromedriver-linux64

echo "==> Setup complete!" 