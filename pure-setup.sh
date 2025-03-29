#!/bin/bash
# No dependencies or system changes script for Render.com
set -e

echo "Setting up Chrome in user directory..."
cd $HOME
mkdir -p chrome-setup chrome bin downloads

# Get Chrome
cd chrome-setup
echo "Downloading Chrome..."
wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
mkdir -p extract
dpkg -x google-chrome-stable_current_amd64.deb extract
cp -r extract/opt/google/chrome/* $HOME/chrome/

# Make executable
chmod +x $HOME/chrome/chrome
ln -sf $HOME/chrome/chrome $HOME/bin/google-chrome

# Get Chrome version for matching chromedriver 
CHROME_VERSION=$($HOME/bin/google-chrome --version | grep -oE "[0-9]+" | head -1)
echo "Chrome version: $CHROME_VERSION"

# Get ChromeDriver
echo "Getting ChromeDriver..."
wget -q "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION" -O version
DRIVER_VERSION=$(cat version)
echo "ChromeDriver version: $DRIVER_VERSION"
wget -q "https://chromedriver.storage.googleapis.com/$DRIVER_VERSION/chromedriver_linux64.zip"
unzip -q chromedriver_linux64.zip
mv chromedriver $HOME/bin/
chmod +x $HOME/bin/chromedriver

# Add to path for this session
export PATH="$HOME/bin:$PATH"
echo 'export PATH="$HOME/bin:$PATH"' >> $HOME/.profile

# Check installations
echo "Installations:"
$HOME/bin/google-chrome --version || echo "Chrome not working"
$HOME/bin/chromedriver --version || echo "ChromeDriver not working"

# Clean up
cd $HOME
rm -rf chrome-setup

# Create requirements file
cat > requirements.txt << EOF
selenium==4.18.1
requests==2.31.0
pandas==2.1.4
python-dotenv==1.0.1
schedule==1.2.1
beautifulsoup4==4.12.2
lxml==4.9.3
html5lib==1.1
numpy==1.23.5
pytz==2023.3
flask==2.3.2
EOF

echo "Setup complete!" 