#!/usr/bin/env python3
"""
Chrome setup module - automatically installs Chrome and ChromeDriver
in the user's home directory when imported.
"""

import os
import shutil
import subprocess
import sys
import urllib.request
import zipfile
import logging

logger = logging.getLogger(__name__)

def setup_chrome_and_driver():
    """Set up Chrome and ChromeDriver in the user's home directory"""
    try:
        home_dir = os.path.expanduser("~")
        bin_dir = os.path.join(home_dir, "bin")
        chrome_dir = os.path.join(home_dir, "chrome")
        temp_dir = os.path.join(home_dir, "chrome_setup")
        
        # Create necessary directories
        os.makedirs(bin_dir, exist_ok=True)
        os.makedirs(chrome_dir, exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)
        
        # Check if Chrome is already installed
        chrome_executable = os.path.join(bin_dir, "google-chrome")
        chrome_driver_executable = os.path.join(bin_dir, "chromedriver")
        
        if os.path.exists(chrome_executable) and os.path.exists(chrome_driver_executable):
            logger.info("Chrome and ChromeDriver already installed")
            return True
            
        logger.info("Installing Chrome and ChromeDriver...")
        
        # Change to temp directory
        original_dir = os.getcwd()
        os.chdir(temp_dir)
        
        # Download Chrome
        chrome_url = "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb"
        chrome_deb = os.path.join(temp_dir, "chrome.deb")
        
        logger.info(f"Downloading Chrome from {chrome_url}")
        urllib.request.urlretrieve(chrome_url, chrome_deb)
        
        # Extract deb file
        extract_dir = os.path.join(temp_dir, "chrome_extract")
        os.makedirs(extract_dir, exist_ok=True)
        
        logger.info("Extracting Chrome package")
        subprocess.run(["dpkg", "-x", chrome_deb, extract_dir], check=True)
        
        # Copy Chrome to home directory
        chrome_binary_source = os.path.join(extract_dir, "opt", "google", "chrome")
        
        logger.info(f"Copying Chrome to {chrome_dir}")
        if os.path.exists(chrome_binary_source):
            for item in os.listdir(chrome_binary_source):
                s = os.path.join(chrome_binary_source, item)
                d = os.path.join(chrome_dir, item)
                if os.path.isdir(s):
                    shutil.copytree(s, d, dirs_exist_ok=True)
                else:
                    shutil.copy2(s, d)
        
        # Create symbolic link
        chrome_binary = os.path.join(chrome_dir, "chrome")
        if os.path.exists(chrome_binary):
            os.chmod(chrome_binary, 0o755)  # Make executable
            if not os.path.exists(chrome_executable):
                os.symlink(chrome_binary, chrome_executable)
        
        # Get Chrome version
        logger.info("Getting Chrome version")
        chrome_version_output = subprocess.check_output([chrome_executable, "--version"], stderr=subprocess.STDOUT)
        chrome_version = chrome_version_output.decode().strip().split()[-1].split(".")[0]
        
        logger.info(f"Chrome version: {chrome_version}")
        
        # Download matching ChromeDriver
        chromedriver_url = f"https://chromedriver.storage.googleapis.com/LATEST_RELEASE_{chrome_version}"
        logger.info(f"Getting ChromeDriver version from {chromedriver_url}")
        
        with urllib.request.urlopen(chromedriver_url) as response:
            chromedriver_version = response.read().decode().strip()
            
        logger.info(f"ChromeDriver version: {chromedriver_version}")
        
        # Download ChromeDriver
        driver_url = f"https://chromedriver.storage.googleapis.com/{chromedriver_version}/chromedriver_linux64.zip"
        driver_zip = os.path.join(temp_dir, "chromedriver.zip")
        
        logger.info(f"Downloading ChromeDriver from {driver_url}")
        urllib.request.urlretrieve(driver_url, driver_zip)
        
        # Extract ChromeDriver
        logger.info("Extracting ChromeDriver")
        with zipfile.ZipFile(driver_zip, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Move ChromeDriver to bin directory
        driver_path = os.path.join(temp_dir, "chromedriver")
        if os.path.exists(driver_path):
            os.chmod(driver_path, 0o755)  # Make executable
            shutil.move(driver_path, chrome_driver_executable)
        
        # Clean up
        os.chdir(original_dir)
        
        # Verify installation
        logger.info("Verifying installation")
        try:
            chrome_check = subprocess.check_output([chrome_executable, "--version"], stderr=subprocess.STDOUT)
            logger.info(f"Chrome installation verified: {chrome_check.decode().strip()}")
            
            driver_check = subprocess.check_output([chrome_driver_executable, "--version"], stderr=subprocess.STDOUT)
            logger.info(f"ChromeDriver installation verified: {driver_check.decode().strip()}")
            
            return True
        except Exception as e:
            logger.error(f"Verification failed: {str(e)}")
            return False
    
    except Exception as e:
        logger.error(f"Error setting up Chrome: {str(e)}")
        return False

# Run setup when this module is imported
try:
    setup_chrome_and_driver()
except Exception as e:
    logger.error(f"Failed to set up Chrome: {str(e)}") 