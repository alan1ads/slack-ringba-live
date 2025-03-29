#!/bin/bash
set -e

echo "==> Starting Render.com bootstrap script"

# Create working directory
mkdir -p $HOME/setup
cd $HOME/setup

# Setup Chrome in user directory
echo "==> Setting up Chrome in user home directory"
wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
mkdir -p chrome_extract
dpkg -x google-chrome-stable_current_amd64.deb chrome_extract

# Setup ChromeDriver
echo "==> Setting up ChromeDriver in user home directory"
mkdir -p $HOME/bin $HOME/chrome
cp -r chrome_extract/opt/google/chrome/* $HOME/chrome/

# Make chrome executable
chmod +x $HOME/chrome/chrome
ln -sf $HOME/chrome/chrome $HOME/bin/google-chrome
ln -sf $HOME/chrome/chrome $HOME/bin/google-chrome-stable

# Download matching ChromeDriver
echo "==> Downloading matching ChromeDriver"
CHROME_VERSION=$($HOME/bin/google-chrome --version | awk '{print $3}' | cut -d. -f1)
wget -q "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION" -O chromedriver_version
CHROMEDRIVER_VERSION=$(cat chromedriver_version)
echo "ChromeDriver version: $CHROMEDRIVER_VERSION"

wget -q "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"
unzip -q chromedriver_linux64.zip
mv chromedriver $HOME/bin/chromedriver
chmod +x $HOME/bin/chromedriver

# Check installations
echo "==> Checking installations"
export PATH="$HOME/bin:$PATH"
echo "Chrome version: $($HOME/bin/google-chrome --version || echo 'Not found')"
echo "ChromeDriver version: $($HOME/bin/chromedriver --version || echo 'Not found')"

# Install pip requirements
echo "==> Installing Python requirements"
pip install selenium==4.18.1 requests==2.31.0 pandas==2.1.4 python-dotenv==1.0.1 schedule==1.2.1 beautifulsoup4==4.12.2 lxml==4.9.3 html5lib==1.1 numpy==1.23.5 pytz==2023.3 flask==2.3.2

# Cleanup
echo "==> Cleaning up"
cd $HOME
rm -rf $HOME/setup

echo "==> Bootstrap complete!" 