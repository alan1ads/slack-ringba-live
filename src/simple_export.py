#!/usr/bin/env python3
"""
Simplified browser-based CSV export for Ringba

This script handles automatic login and focuses on navigating to call logs,
setting the date range, and exporting the CSV.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import threading
import signal
import pickle
import requests
import base64
import csv

# Load environment variables
load_dotenv()

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Get configuration from environment variables
RINGBA_USERNAME = os.getenv('RINGBA_USERNAME')
RINGBA_PASSWORD = os.getenv('RINGBA_PASSWORD')
RINGBA_API_TOKEN = os.getenv('RINGBA_API_TOKEN')
RINGBA_ACCOUNT_ID = os.getenv('RINGBA_ACCOUNT_ID')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
USE_HEADLESS = os.getenv('USE_HEADLESS', 'true').lower() == 'true'
RPC_THRESHOLD = float(os.getenv('RPC_THRESHOLD', '12.0'))
MORNING_CHECK_TIME = os.getenv('MORNING_CHECK_TIME', '11:00')
MIDDAY_CHECK_TIME = os.getenv('MIDDAY_CHECK_TIME', '14:00')
AFTERNOON_CHECK_TIME = os.getenv('AFTERNOON_CHECK_TIME', '16:30')

# Create screenshots directory if it doesn't exist
screenshots_dir = "screenshots"
if not os.path.exists(screenshots_dir):
    os.makedirs(screenshots_dir)

def take_screenshot(browser, name):
    """Take a screenshot for debugging purposes"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Make sure the screenshots directory exists
        if not os.path.exists(screenshots_dir):
            os.makedirs(screenshots_dir)
            
        filename = f"{screenshots_dir}/{timestamp}_{name}.png"
        
        # Try different methods to take a screenshot
        try:
            # Standard method
            browser.save_screenshot(filename)
        except Exception as first_error:
            try:
                # Try to get screenshot via execute_script
                screenshot = browser.execute_script("""
                    var canvas = document.createElement('canvas');
                    var context = canvas.getContext('2d');
                    var width = window.innerWidth;
                    var height = window.innerHeight;
                    canvas.width = width;
                    canvas.height = height;
                    context.drawWindow(window, 0, 0, width, height, 'rgb(255,255,255)');
                    return canvas.toDataURL('image/png');
                """)
                
                # Save the Base64 image
                with open(filename, 'wb') as f:
                    f.write(base64.b64decode(screenshot.split(',')[1]))
                    
            except Exception as second_error:
                logger.error(f"Failed to take screenshot via alternative method: {str(second_error)}")
                return
                
        logger.info(f"Screenshot saved: {filename}")
    except Exception as e:
        logger.error(f"Failed to take screenshot: {str(e)}")
        # Don't raise the exception, just log it

def debug_environment():
    """Print debugging information about the environment"""
    try:
        logger.info("=== Environment Debug Information ===")
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"PATH: {os.environ.get('PATH', 'Not set')}")
        
        # Check for ChromeDriver in common locations
        common_paths = [
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
            "/snap/bin/chromedriver",
            os.path.join(os.getcwd(), "chromedriver")
        ]
        
        for path in common_paths:
            logger.info(f"Checking ChromeDriver at {path}: {os.path.exists(path)}")
        
        # Try to run chromedriver --version
        try:
            import subprocess
            version_cmd = subprocess.run(['chromedriver', '--version'], capture_output=True, text=True)
            logger.info(f"ChromeDriver version: {version_cmd.stdout.strip()}")
        except Exception as e:
            logger.warning(f"Failed to get ChromeDriver version: {str(e)}")
        
        # Check Chrome version
        try:
            chrome_cmd = subprocess.run(['google-chrome', '--version'], capture_output=True, text=True)
            logger.info(f"Chrome version: {chrome_cmd.stdout.strip()}")
        except Exception as e:
            logger.warning(f"Failed to get Chrome version: {str(e)}")
        
        logger.info("=== End Environment Debug ===")
    except Exception as e:
        logger.error(f"Error during environment debugging: {str(e)}")

def setup_browser():
    """Set up the Chrome browser with absolute minimum resources for container environments"""
    try:
        # Debug environment variables 
        logger.info(f"Chrome options from env: {os.getenv('CHROME_OPTIONS', 'Not set')}")
        
        # Create Chrome options with MINIMAL configuration
        chrome_options = webdriver.ChromeOptions()
        
        # Get $HOME/bin path
        home_bin = os.path.join(os.environ.get('HOME', ''), 'bin')
        chrome_path = os.path.join(os.environ.get('HOME', ''), 'chrome', 'chrome')
        
        # Check if we're in Render environment with local Chrome install
        if os.path.exists(chrome_path):
            logger.info(f"Using local Chrome installation: {chrome_path}")
            chrome_options.binary_location = chrome_path
        
        # Essential options only - stripped down to bare minimum
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Add critical memory-saving options
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer") 
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-features=site-per-process")
        chrome_options.add_argument("--js-flags=--expose-gc")
        chrome_options.add_argument("--single-process")  # Use single process to reduce memory
        chrome_options.add_argument("--window-size=800,600")  # Smaller window size
        
        # Set page load strategy to minimize resource usage
        chrome_options.page_load_strategy = 'eager'
        
        # In container environments, use /tmp which is guaranteed to be writable
        download_dir = "/tmp"
        os.makedirs(download_dir, exist_ok=True)
        logger.info(f"Using /tmp as download directory for container compatibility")
        
        # Set very explicit download settings
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "profile.default_content_settings.popups": 0,
            "browser.helperApps.neverAsk.saveToDisk": "application/csv,text/csv"
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Set the global download directory as an environment variable
        os.environ["DOWNLOAD_DIR"] = download_dir
        
        # Log the final Chrome options
        logger.info(f"Setting up Chrome with minimal options: {chrome_options.arguments}")
        
        # Create the browser - check for chromedriver in $HOME/bin first
        if os.path.exists(os.path.join(home_bin, 'chromedriver')):
            logger.info(f"Using local ChromeDriver: {os.path.join(home_bin, 'chromedriver')}")
            service = Service(executable_path=os.path.join(home_bin, 'chromedriver'))
            browser = webdriver.Chrome(service=service, options=chrome_options)
        else:
            # Fall back to system ChromeDriver
            logger.info("Using system ChromeDriver")
            browser = webdriver.Chrome(options=chrome_options)
        
        # Set shorter timeouts
        browser.set_page_load_timeout(60)
        browser.implicitly_wait(10)
        
        # Add a memory management hook - periodically call JS garbage collection
        browser.execute_script("window.gc = function() { if (window.gc) window.gc(); };")
        
        logger.info("Chrome browser set up successfully with minimal configuration")
        return browser
    except Exception as e:
        logger.error(f"Failed to set up browser: {str(e)}")
        return None

def login_to_ringba(browser):
    """Login to Ringba dashboard"""
    try:
        # Navigate to login page
        logger.info("Navigating to Ringba login page...")
        browser.get("https://app.ringba.com/#/login")
        time.sleep(10)  # Wait longer for page to load
        
        # Take screenshot for debugging
        take_screenshot(browser, "before_login")
        
        # Wait for login form to be present
        logger.info("Waiting for login form...")
        wait = WebDriverWait(browser, 30)
        
        # Try different approaches to find the username field
        username_input = None
        username_selectors = [
            (By.ID, "mat-input-0"),
            (By.NAME, "username"),
            (By.CSS_SELECTOR, "input[type='email']"),
            (By.CSS_SELECTOR, "input[formcontrolname='username']"),
            (By.CSS_SELECTOR, "input.username"),
            (By.XPATH, "//input[@placeholder='Username' or @placeholder='Email']")
        ]
        
        for selector_type, selector in username_selectors:
            try:
                logger.info(f"Trying to find username field with {selector_type}={selector}")
                username_input = wait.until(EC.presence_of_element_located((selector_type, selector)))
                logger.info(f"Found username field with {selector_type}={selector}")
                break
            except:
                continue
        
        if not username_input:
            logger.error("Could not find username field")
            take_screenshot(browser, "username_not_found")
            return False
        
        # Try different approaches to find the password field
        password_input = None
        password_selectors = [
            (By.ID, "mat-input-1"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']"),
            (By.CSS_SELECTOR, "input[formcontrolname='password']"),
            (By.XPATH, "//input[@placeholder='Password']")
        ]
        
        for selector_type, selector in password_selectors:
            try:
                logger.info(f"Trying to find password field with {selector_type}={selector}")
                password_input = wait.until(EC.presence_of_element_located((selector_type, selector)))
                logger.info(f"Found password field with {selector_type}={selector}")
                break
            except:
                continue
        
        if not password_input:
            logger.error("Could not find password field")
            take_screenshot(browser, "password_not_found")
            return False
        
        # Enter credentials
        logger.info("Entering credentials...")
        username_input.clear()
        username_input.send_keys(RINGBA_USERNAME)
        password_input.clear()
        password_input.send_keys(RINGBA_PASSWORD)
        
        # Take screenshot before clicking login
        take_screenshot(browser, "credentials_entered")
        
        # Try to find and click the login button
        login_button = None
        button_selectors = [
            (By.XPATH, "//button[@type='submit']"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.XPATH, "//button[contains(text(), 'Login') or contains(text(), 'Sign In')]"),
            (By.CSS_SELECTOR, ".login-button"),
            (By.CSS_SELECTOR, "button.mat-button")
        ]
        
        for selector_type, selector in button_selectors:
            try:
                logger.info(f"Trying to find login button with {selector_type}={selector}")
                login_button = wait.until(EC.element_to_be_clickable((selector_type, selector)))
                logger.info(f"Found login button with {selector_type}={selector}")
                break
            except:
                continue
        
        if login_button:
            logger.info("Clicking login button...")
            login_button.click()
        else:
            # Try submitting the form by pressing Enter on the password field
            logger.info("No login button found, trying to submit by pressing Enter")
            password_input.send_keys(Keys.RETURN)
        
        # Wait for login to complete
        logger.info("Waiting for login to complete...")
        wait.until(EC.url_contains("dashboard"))
        
        # Take screenshot after login
        take_screenshot(browser, "after_login")
        
        logger.info("Successfully logged in to Ringba")
        return True
    except Exception as e:
        logger.error(f"Failed to login to Ringba: {str(e)}")
        take_screenshot(browser, "login_error")
        return False

def navigate_to_call_logs(browser):
    """Navigate to Call Logs page with minimal resource usage"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Clear memory before navigation
            browser.execute_script("if(window.gc) window.gc();")
            
            # Directly navigate to the Call Logs URL with a simpler approach
            logger.info(f"Navigating to call logs (attempt {attempt+1}/{max_retries})...")
            
            # First go to a lighter page to ensure stability
            browser.get("https://app.ringba.com/#/dashboard")
            time.sleep(5)  # Give page time to stabilize
            
            # Force garbage collection
            browser.execute_script("if(window.gc) window.gc();")
            
            # Now navigate to the call logs with reduced elements
            browser.get("https://app.ringba.com/#/dashboard/call-logs/report/summary?limit=50")
            
            # Wait for page to load with a simpler check
            logger.info("Waiting for call logs page to load...")
            time.sleep(10)
            
            # Take screenshot for debugging
            take_screenshot(browser, f"call_logs_page_attempt_{attempt+1}")
            
            # Simple URL check instead of complex DOM verification
            if "call-logs" in browser.current_url:
                logger.info("Successfully navigated to call logs page")
                return True
            else:
                logger.error(f"Failed to navigate to call logs page. Current URL: {browser.current_url}")
                if attempt < max_retries - 1:
                    logger.info("Retrying navigation...")
                    time.sleep(5)
                else:
                    logger.error("Max retries reached, giving up on navigation")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to navigate to call logs page (attempt {attempt+1}): {str(e)}")
            if attempt < max_retries - 1:
                logger.info("Retrying navigation...")
                time.sleep(5)
            else:
                logger.error("Max retries reached, giving up on navigation")
                return False
                
    # If we get here, all attempts failed
    return False

def set_date_range(browser, start_date, end_date):
    """Set the date range for call logs"""
    # Skip setting date range as requested by user
    logger.info("Skipping date range modification as requested")
    take_screenshot(browser, "skipping_date_range")
    return True

def click_export_csv(browser):
    """Click the Export CSV button and capture the downloaded file in container environments"""
    try:
        # Navigate directly to the summary page with export option
        logger.info("Navigating directly to call summary report...")
        try:
            browser.get("https://app.ringba.com/#/dashboard/call-logs/report/summary")
            logger.info("Waiting for summary page to load...")
            time.sleep(20)  # Give page more time to load
        except Exception as e:
            logger.error(f"Failed to navigate to summary page: {str(e)}")
            take_screenshot(browser, "navigation_failed")
        
        # Take screenshot to see page state
        take_screenshot(browser, "before_export_attempt")
        
        # NEW: Add JavaScript that intercepts blob URLs and download events 
        logger.info("Setting up blob URL interception...")
        browser.execute_script("""
            // Store blob URLs and download data
            window.blobUrls = [];
            window.downloadData = null;
            
            // Override createObjectURL to capture blob URLs
            const originalCreateObjectURL = URL.createObjectURL;
            URL.createObjectURL = function(object) {
                const url = originalCreateObjectURL(object);
                console.log('Captured blob URL:', url);
                
                // Store the blob and URL
                window.blobUrls.push({
                    url: url,
                    blob: object,
                    timestamp: Date.now()
                });
                
                // If it's a CSV, try to read it
                if (object instanceof Blob && 
                    (object.type === 'text/csv' || 
                     object.type === 'application/csv' || 
                     object.type === 'application/vnd.ms-excel' ||
                     object.type === '')) {
                    
                    console.log('Found potential CSV blob:', object.type, object.size);
                    
                    // Read the blob
                    const reader = new FileReader();
                    reader.onload = function() {
                        const content = reader.result;
                        console.log('Read blob content, length:', content.length);
                        
                        // Simple CSV check
                        if (content.includes(',') && 
                            (content.includes('\\n') || content.includes('\\r'))) {
                            
                            window.downloadData = {
                                content: content,
                                timestamp: Date.now(),
                                type: object.type || 'text/csv'
                            };
                            console.log('Saved CSV content from blob');
                        }
                    };
                    reader.readAsText(object);
                }
                
                return url;
            };
            
            // Monitor anchor downloads
            document.addEventListener('click', function(e) {
                let target = e.target;
                
                // Look for download links
                while (target && target !== document) {
                    if (target.tagName === 'A' && target.href && target.href.startsWith('blob:')) {
                        console.log('Intercepted blob download click:', target.href);
                        
                        // Find the matching blob URL
                        const blobInfo = window.blobUrls.find(b => b.url === target.href);
                        if (blobInfo && blobInfo.blob) {
                            // Read the blob
                            const reader = new FileReader();
                            reader.onload = function() {
                                const content = reader.result;
                                console.log('Read blob content from click, length:', content.length);
                                
                                // Store the download data
                                window.downloadData = {
                                    content: content,
                                    timestamp: Date.now(),
                                    type: blobInfo.blob.type || 'text/csv'
                                };
                                console.log('Saved CSV content from click');
                            };
                            reader.readAsText(blobInfo.blob);
                        }
                    }
                    target = target.parentElement;
                }
            }, true);
        """)
        
        # Find all possible download directories (keep existing code)
        download_dir = "/tmp"
        possible_download_dirs = [
            "/tmp", 
            "/tmp/downloads",
            "/downloads",
            "/home/chrome/downloads",
            os.environ.get("HOME", "") + "/Downloads" if os.environ.get("HOME") else "",
            os.path.join(os.getcwd(), "downloads"),
            os.path.join(os.getcwd(), "tmp")
        ]
        
        # Filter out empty paths and ensure directories exist
        possible_download_dirs = [d for d in possible_download_dirs if d]
        for d in possible_download_dirs:
            os.makedirs(d, exist_ok=True)
            
        logger.info(f"Checking for downloads in these directories: {possible_download_dirs}")
        
        # Check all existing CSV files before download attempt
        existing_csv_files = {}
        for d in possible_download_dirs:
            try:
                csv_files = [f for f in os.listdir(d) if f.endswith('.csv')]
                for f in csv_files:
                    full_path = os.path.join(d, f)
                    existing_csv_files[full_path] = os.path.getmtime(full_path)
                logger.info(f"Found {len(csv_files)} existing CSV files in {d}")
            except Exception as e:
                logger.warning(f"Error checking directory {d}: {str(e)}")
        
        # Setup download tracking without modifying userAgent
        logger.info("Setting up download tracking...")
        browser.execute_script("""
            // Track download activity
            window.downloadActivity = {
                clicked: false,
                buttonElements: [],
                timestamp: null,
                message: null
            };
            
            // Monitor download-related events
            document.addEventListener('click', function(e) {
                let target = e.target;
                
                // Check if clicked element or its parent is a button/link containing "export" or "csv"
                while (target && target !== document) {
                    if (target.tagName === 'BUTTON' || target.tagName === 'A' || 
                        target.getAttribute('role') === 'button') {
                        
                        // Check if text content contains export/csv
                        if (target.textContent && 
                            (target.textContent.toLowerCase().includes('export') || 
                             target.textContent.toLowerCase().includes('csv'))) {
                            
                            console.log('Export button clicked:', target);
                            window.downloadActivity.clicked = true;
                            window.downloadActivity.buttonElements.push({
                                tagName: target.tagName,
                                id: target.id,
                                className: target.className,
                                textContent: target.textContent,
                                time: new Date().toISOString()
                            });
                            window.downloadActivity.timestamp = Date.now();
                            window.downloadActivity.message = 'Export button clicked at ' + new Date().toISOString();
                        }
                    }
                    target = target.parentElement;
                }
            }, true);
            
            // Intercept potential download triggers
            const originalOpen = XMLHttpRequest.prototype.open;
            XMLHttpRequest.prototype.open = function() {
                this._method = arguments[0];
                this._url = arguments[1];
                
                // Check if this might be related to a download
                if (this._url && (
                    this._url.includes('export') || 
                    this._url.includes('download') ||
                    this._url.includes('csv')
                )) {
                    console.log('Potential download XHR:', this._method, this._url);
                    
                    if (!window.downloadActivity.message) {
                        window.downloadActivity.message = 'Download XHR detected: ' + this._url;
                    }
                }
                
                return originalOpen.apply(this, arguments);
            };
        """)
        
        # Try direct API approach for getting call logs
        # Use CDP protocol to monitor network requests and intercept the API response
        logger.info("Setting up network interception for API monitoring...")
        browser.execute_cdp_cmd('Network.enable', {})
        browser.execute_cdp_cmd('Network.setRequestInterception', {'patterns': [{'urlPattern': '**/ringba/call-logs*', 'resourceType': 'XHR', 'interceptionStage': 'HeadersReceived'}]})
        
        # Create a variable to store API data
        browser.execute_script("""
            window.apiResponses = [];
            window.callLogsData = null;
        """)
        
        # Track network responses for call logs API
        browser.execute_cdp_cmd('Network.requestIntercepted', lambda params: browser.execute_cdp_cmd('Network.getResponseBodyForInterception', 
            {'interceptionId': params['interceptionId']}).then(
                lambda body: browser.execute_script("""
                    try {
                        const data = JSON.parse(arguments[0]);
                        console.log('Intercepted API response with ' + (data.length || 0) + ' items');
                        window.apiResponses.push({
                            timestamp: Date.now(),
                            data: data
                        });
                        window.callLogsData = data;
                    } catch(e) {
                        console.error('Failed to parse API data:', e);
                    }
                """, body['body'])
            ))
        
        # Click the export button
        export_clicked = False
        
        logger.info("Looking for and clicking the EXPORT CSV button...")
        try:
            # Try JavaScript approach first
            result = browser.execute_script("""
                // Find EXPORT button - more comprehensive approach
                function findExportButton() {
                    // Save all candidates we find
                    const candidates = [];
                    
                    // 1. Look for elements with "export" or "csv" in text 
                    document.querySelectorAll('button, a, span, div').forEach(el => {
                        if (el.textContent && (
                            el.textContent.toLowerCase().includes('export') || 
                            el.textContent.toLowerCase().includes('csv')
                        )) {
                            candidates.push({
                                element: el,
                                score: 10,  // Base score
                                text: el.textContent.trim()
                            });
                        }
                    });
                    
                    // 2. Check for elements with id/class containing export
                    document.querySelectorAll('[id*="export" i], [class*="export" i], [data-test*="export" i]').forEach(el => {
                        // Add if not already in candidates
                        if (!candidates.some(c => c.element === el)) {
                            candidates.push({
                                element: el,
                                score: 5,
                                text: el.textContent.trim()
                            });
                        } else {
                            // Increase score of existing candidate
                            const existing = candidates.find(c => c.element === el);
                            existing.score += 5;
                        }
                    });
                    
                    // Score adjustments based on various factors
                    candidates.forEach(c => {
                        // Exact match for "EXPORT CSV" gets highest score
                        if (c.text.toLowerCase() === 'export csv') {
                            c.score += 20;
                        }
                        
                        // Button elements get a bonus
                        if (c.element.tagName === 'BUTTON') {
                            c.score += 5;
                        }
                        
                        // Visible elements get a bonus
                        const style = window.getComputedStyle(c.element);
                        if (style.display !== 'none' && style.visibility !== 'hidden' && 
                            c.element.offsetWidth > 0 && c.element.offsetHeight > 0) {
                            c.score += 10;
                        }
                    });
                    
                    // Sort by score (highest first)
                    candidates.sort((a, b) => b.score - a.score);
                    
                    console.log('Found ' + candidates.length + ' export button candidates');
                    candidates.forEach((c, i) => {
                        console.log(`Candidate ${i}: ${c.element.tagName} "${c.text}" - Score: ${c.score}`);
                    });
                    
                    return candidates.length > 0 ? candidates[0].element : null;
                }
                
                // Find and click the export button
                const exportButton = findExportButton();
                if (exportButton) {
                    console.log('Clicking export button:', exportButton.outerHTML);
                    exportButton.click();
                    return true;
                }
                
                return false;
            """)
            
            if result:
                logger.info("Successfully clicked export button with JavaScript")
                export_clicked = True
            else:
                # Fall back to Selenium approach
                logger.warning("JavaScript approach failed, trying Selenium...")
                
                # Try different locators
                locators = [
                    (By.XPATH, "//button[contains(text(), 'Export')]"),
                    (By.XPATH, "//button[contains(text(), 'CSV')]"),
                    (By.XPATH, "//span[contains(text(), 'Export')]"),
                    (By.CSS_SELECTOR, "[id*='export']"),
                    (By.CSS_SELECTOR, "[class*='export']"),
                    (By.CSS_SELECTOR, "[title*='Export']"),
                ]
                
                for locator_type, locator_value in locators:
                    try:
                        element = browser.find_element(locator_type, locator_value)
                        logger.info(f"Found export button with locator: {locator_value}")
                        element.click()
                        export_clicked = True
                        break
                    except Exception as e:
                        logger.warning(f"Could not find/click with locator {locator_value}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error clicking export button: {str(e)}")
        
        # Take screenshot after clicking export
        take_screenshot(browser, "after_export_attempt")
        
        if not export_clicked:
            logger.error("Could not click export button with any method")
            return None
        
        # Wait for download to complete
        logger.info("Waiting for download to complete...")
        wait_time = 60  # 1 minute wait maximum
        start_time = time.time()
        
        # First try to get data directly from the intercepted blob 
        while time.time() - start_time < wait_time:
            # NEW: Check for blob data first
            download_data = browser.execute_script("return window.downloadData;")
            if download_data and download_data.get('content'):
                logger.info("Found download data directly from blob interception!")
                
                # Save the content to a file
                file_path = os.path.join("/tmp", f"blob_download_{int(time.time())}.csv")
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(download_data['content'])
                
                logger.info(f"Saved blob data to {file_path}")
                
                # Verify it's a valid CSV
                try:
                    df = pd.read_csv(file_path)
                    logger.info(f"Successfully read CSV from blob with {len(df)} rows and {len(df.columns)} columns")
                    return file_path
                except Exception as e:
                    logger.warning(f"Blob data is not a valid CSV: {str(e)}")
            
            # NEW: Check for intercepted API data
            api_data = browser.execute_script("return window.callLogsData;")
            if api_data:
                logger.info("Found call logs data from API interception!")
                
                # Convert API data to DataFrame and save as CSV
                try:
                    # Save API data to file
                    file_path = os.path.join("/tmp", f"api_data_{int(time.time())}.csv")
                    
                    # Convert JSON array to string to save
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(str(api_data))
                    
                    logger.info(f"Saved raw API data to {file_path}")
                    
                    # Try to convert to DataFrame and save as CSV
                    api_file_path = os.path.join("/tmp", f"api_data_processed_{int(time.time())}.csv")
                    
                    # Convert API data to DataFrame
                    df = pd.DataFrame(api_data)
                    df.to_csv(api_file_path, index=False)
                    
                    logger.info(f"Successfully converted API data to CSV with {len(df)} rows")
                    return api_file_path
                except Exception as e:
                    logger.warning(f"Failed to process API data: {str(e)}")
            
            # Check if there are any new CSV files in the download directories
            new_files = []
            
            # Check all possible download directories for new files
            for d in possible_download_dirs:
                try:
                    # Get all CSV files in this directory
                    curr_files = [os.path.join(d, f) for f in os.listdir(d) if f.endswith('.csv')]
                    
                    # Check which ones are new
                    for file_path in curr_files:
                        if file_path not in existing_csv_files:
                            # New file found
                            new_files.append(file_path)
                        elif os.path.getmtime(file_path) > existing_csv_files.get(file_path, 0):
                            # File was modified since we started
                            new_files.append(file_path)
                except Exception as e:
                    logger.warning(f"Error checking directory {d}: {str(e)}")
            
            if new_files:
                # Sort by modification time (newest first)
                new_files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
                newest_file = new_files[0]
                
                logger.info(f"Found new CSV file: {newest_file}")
                
                # Verify it's a valid CSV
                try:
                    df = pd.read_csv(newest_file)
                    logger.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
                    return newest_file
                except Exception as e:
                    logger.warning(f"File {newest_file} is not a valid CSV: {str(e)}")
            
            # Check download activity from browser
            download_activity = browser.execute_script("return window.downloadActivity || {};")
            if download_activity:
                logger.info(f"Download activity: {download_activity}")
            
            # Log progress every 10 seconds
            elapsed = int(time.time() - start_time)
            if elapsed % 10 == 0:
                logger.info(f"Still waiting for download... ({elapsed}s elapsed)")
            
            time.sleep(3)
        
        logger.warning("No new CSV files found after waiting")
        
        # NEW: Try to extract data directly from the table on the page
        logger.info("No downloads found. Attempting to extract data directly from the page...")
        table_data = browser.execute_script("""
            // Find any data tables on the page
            const tables = document.querySelectorAll('table');
            console.log(`Found ${tables.length} tables on page`);
            
            // Function to extract data from table
            function extractFromTable(table) {
                const rows = [];
                const headers = [];
                
                // Get headers
                const headerRow = table.querySelector('thead tr') || table.querySelector('tr');
                if (headerRow) {
                    const headerCells = headerRow.querySelectorAll('th, td');
                    for (const cell of headerCells) {
                        headers.push(cell.textContent.trim());
                    }
                }
                
                // Get data rows - start from second row if we used the first for headers
                const dataRows = table.querySelectorAll('tbody tr, tr');
                const startIdx = (table.querySelector('thead')) ? 0 : 1;
                
                for (let i = startIdx; i < dataRows.length; i++) {
                    const row = dataRows[i];
                    const cells = row.querySelectorAll('td');
                    
                    if (cells.length === 0) continue;
                    
                    const rowData = {};
                    for (let j = 0; j < Math.min(cells.length, headers.length); j++) {
                        rowData[headers[j] || `Column${j+1}`] = cells[j].textContent.trim();
                    }
                    
                    if (Object.keys(rowData).length > 0) {
                        rows.push(rowData);
                    }
                }
                
                return { headers, rows };
            }
            
            // Extract from all tables and find the one with most rows
            let bestTable = null;
            let maxRows = 0;
            
            for (const table of tables) {
                const data = extractFromTable(table);
                if (data.rows.length > maxRows) {
                    maxRows = data.rows.length;
                    bestTable = data;
                }
            }
            
            if (bestTable && bestTable.rows.length > 0) {
                return bestTable;
            }
            
            // If no tables found, check for grid components
            const grids = document.querySelectorAll('[role="grid"], .ag-root, .data-grid');
            console.log(`Found ${grids.length} grid components`);
            
            if (grids.length > 0) {
                // Extract from first grid (assuming it's the main one)
                const grid = grids[0];
                const headers = [];
                const rows = [];
                
                // Find headers
                const headerCells = grid.querySelectorAll('[role="columnheader"], .ag-header-cell');
                for (const cell of headerCells) {
                    headers.push(cell.textContent.trim());
                }
                
                // Find rows
                const rowElements = grid.querySelectorAll('[role="row"], .ag-row');
                for (const row of rowElements) {
                    const cells = row.querySelectorAll('[role="cell"], .ag-cell');
                    
                    if (cells.length === 0) continue;
                    
                    const rowData = {};
                    for (let j = 0; j < Math.min(cells.length, headers.length || cells.length); j++) {
                        rowData[headers[j] || `Column${j+1}`] = cells[j].textContent.trim();
                    }
                    
                    if (Object.keys(rowData).length > 0) {
                        rows.push(rowData);
                    }
                }
                
                if (rows.length > 0) {
                    return { headers, rows };
                }
            }
            
            return null;
        """)
        
        if table_data and table_data.get('rows') and len(table_data.get('rows')) > 0:
            logger.info(f"Successfully extracted {len(table_data['rows'])} rows directly from page")
            
            # Save to CSV
            download_dir = "/tmp"
            os.makedirs(download_dir, exist_ok=True)
            file_path = os.path.join(download_dir, f"page_extract_{int(time.time())}.csv")
            
            # Convert to DataFrame and save
            df = pd.DataFrame(table_data['rows'])
            df.to_csv(file_path, index=False)
            
            logger.info(f"Saved extracted data to {file_path}")
            return file_path
        
        # Try the Ringba API as fallback
        logger.info("Trying Ringba API as fallback...")
        try:
            api_token = os.getenv('RINGBA_API_TOKEN')
            account_id = os.getenv('RINGBA_ACCOUNT_ID')
            
            if api_token and account_id:
                # Get today's date range for the API query
                today = datetime.now()
                start_date = today.strftime('%Y-%m-%d')
                end_date = today.strftime('%Y-%m-%d')
                
                # Use Ringba API to get call data
                headers = {
                    'Authorization': f'Bearer {api_token}',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
                
                # Try the call logs API endpoint
                call_logs_url = f"https://api.ringba.com/v2/ringba/accounts/{account_id}/call-logs"
                params = {
                    'startDate': start_date,
                    'endDate': end_date,
                    'format': 'csv'
                }
                
                logger.info(f"Making API request to: {call_logs_url}")
                response = requests.get(call_logs_url, headers=headers, params=params)
                
                if response.status_code == 200:
                    logger.info("Successfully retrieved data from Ringba API")
                    
                    # Save response content to a CSV file
                    download_dir = "/tmp"
                    os.makedirs(download_dir, exist_ok=True)
                    file_path = os.path.join(download_dir, f"ringba_api_{int(time.time())}.csv")
                    
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    
                    logger.info(f"Saved API response to {file_path}")
                    
                    # Verify it's a valid CSV
                    try:
                        df = pd.read_csv(file_path)
                        logger.info(f"API CSV has {len(df)} rows and {len(df.columns)} columns")
                        return file_path
                    except Exception as e:
                        logger.warning(f"API response not a valid CSV: {str(e)}")
                else:
                    logger.warning(f"API call failed with status {response.status_code}: {response.text}")
            else:
                logger.warning("Missing API token or account ID, can't use API fallback")
        except Exception as e:
            logger.error(f"Error using Ringba API: {str(e)}")
            
        # If all methods fail, give up
        logger.error("All methods to export CSV have failed")
        return None
        
    except Exception as e:
        logger.error(f"Error in click_export_csv: {str(e)}")
        take_screenshot(browser, "export_error")
        return None

def process_csv_file(file_path):
    """Process the CSV file to extract targets with low RPC"""
    if not file_path or not os.path.exists(file_path):
        logger.error(f"CSV file not found at path: {file_path}")
        return None
        
    logger.info(f"Processing CSV file: {file_path}")
    
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Log the columns for debugging
        logger.info(f"CSV columns: {', '.join(df.columns)}")
        
        # Look for relevant columns
        target_col = None
        rpc_col = None
        
        # Try different potential column names for target
        for col in ['Target Name', 'Target', 'TargetName', 'Campaign']:
            if col in df.columns:
                target_col = col
                break
                
        # Try different potential column names for RPC
        for col in ['RPC', 'Avg. Revenue per Call', 'Revenue Per Call', 'Revenue per Call', 'RPCall']:
            if col in df.columns:
                rpc_col = col
                break
        
        if not target_col:
            logger.error("Could not find Target column in CSV")
            return None
            
        if not rpc_col:
            logger.error("Could not find RPC column in CSV")
            return None
            
        logger.info(f"Using columns: Target={target_col}, RPC={rpc_col}")
        
        # Convert RPC column to numeric, handling currency symbols and commas
        df[rpc_col] = df[rpc_col].replace('[\$,]', '', regex=True).astype(float)
        
        # Get threshold from environment variable or use default
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 12.0))
        logger.info(f"Using RPC threshold of ${rpc_threshold}")
        
        # Filter for targets below the threshold
        low_rpc_targets = df[df[rpc_col] < rpc_threshold][[target_col, rpc_col]].copy()
        
        # Sort by RPC (ascending)
        low_rpc_targets = low_rpc_targets.sort_values(by=rpc_col)
        
        # Log the results
        if not low_rpc_targets.empty:
            logger.info(f"Found {len(low_rpc_targets)} targets below the RPC threshold:")
            for index, row in low_rpc_targets.iterrows():
                logger.info(f"  {row[target_col]}: ${row[rpc_col]:.2f}")
            
            return {
                'targets': low_rpc_targets.to_dict('records'),
                'target_col': target_col,
                'rpc_col': rpc_col,
                'threshold': rpc_threshold
            }
        else:
            logger.info("No targets found below the RPC threshold")
            return None
            
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        return None

def check_time_range(current_time, target_time, window_minutes=30):
    """Check if current time is within window_minutes of target time"""
    try:
        # Parse times
        current_hour, current_minute = map(int, current_time.split(':'))
        target_hour, target_minute = map(int, target_time.split(':'))
        
        # Convert to minutes
        current_minutes = current_hour * 60 + current_minute
        target_minutes = target_hour * 60 + target_minute
        
        # Check if within window
        return abs(current_minutes - target_minutes) <= window_minutes
    except Exception as e:
        logger.error(f"Error checking time range: {str(e)}")
        return False

def send_to_slack(data, run_type):
    """Send notification to Slack with targets below RPC threshold"""
    if not data or 'targets' not in data or not data['targets']:
        logger.warning("No data to send to Slack")
        return False
        
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        logger.error("SLACK_WEBHOOK_URL environment variable not set")
        return False
        
    try:
        # Extract data
        targets = data['targets']
        target_col = data['target_col']
        rpc_col = data['rpc_col']
        threshold = data['threshold']
        
        # Create message
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{run_type} Report: Targets Below ${threshold} RPC"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{len(targets)} targets* found below the RPC threshold"
                    }
                },
                {
                    "type": "divider"
                }
            ]
        }
        
        # Add targets to message in chunks (to avoid message size limits)
        target_texts = []
        for target in targets:
            target_name = target[target_col]
            target_rpc = target[rpc_col]
            target_texts.append(f"• *{target_name}*: ${target_rpc:.2f}")
            
        # Split into chunks of 20 targets
        chunk_size = 20
        for i in range(0, len(target_texts), chunk_size):
            chunk = target_texts[i:i+chunk_size]
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(chunk)
                }
            })
            
        # Send to Slack
        response = requests.post(webhook_url, json=message)
        
        if response.status_code == 200:
            logger.info(f"Successfully sent {run_type} report to Slack")
            return True
        else:
            logger.error(f"Failed to send to Slack: {response.status_code} {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending to Slack: {str(e)}")
        return False

def save_morning_results(targets_df, target_col, rpc_col):
    """Save morning results for comparison with afternoon run"""
    try:
        # Create a copy to avoid any reference issues
        df_copy = targets_df.copy()
        
        # Create a dictionary with all the data we need
        morning_data = {
            'targets_df': df_copy,
            'target_col': target_col,
            'rpc_col': rpc_col,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save as pickle for complete data preservation
        with open('morning_results.pkl', 'wb') as f:
            pickle.dump(morning_data, f)
        
        logger.info("Morning results saved successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to save morning results: {str(e)}")
        return False

def load_morning_results():
    """Load morning results for afternoon comparison"""
    try:
        # Check if the file exists
        if not os.path.exists('morning_results.pkl'):
            logger.warning("Morning results file does not exist")
            return None
        
        # Load the saved data
        with open('morning_results.pkl', 'rb') as f:
            morning_data = pickle.load(f)
        
        # Verify the data has the expected structure
        required_keys = ['targets_df', 'target_col', 'rpc_col']
        if not all(key in morning_data for key in required_keys):
            logger.warning("Morning results file has invalid format")
            return None
        
        # Check if the data is from today
        timestamp = datetime.fromisoformat(morning_data['timestamp'])
        today = datetime.now().date()
        
        if timestamp.date() != today:
            logger.warning(f"Morning results are from {timestamp.date()}, not from today ({today})")
            return None
        
        logger.info("Morning results loaded successfully")
        return morning_data
    except Exception as e:
        logger.error(f"Failed to load morning results: {str(e)}")
        return None

def save_midday_results(targets_df, target_col, rpc_col):
    """Save midday results for comparison with afternoon run"""
    try:
        # Create a copy to avoid any reference issues
        df_copy = targets_df.copy()
        
        # Create a dictionary with all the data we need
        midday_data = {
            'targets_df': df_copy,
            'target_col': target_col,
            'rpc_col': rpc_col,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save as pickle for complete data preservation
        with open('midday_results.pkl', 'wb') as f:
            pickle.dump(midday_data, f)
        
        logger.info("Midday results saved successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to save midday results: {str(e)}")
        return False

def load_midday_results():
    """Load midday results for afternoon comparison"""
    try:
        # Check if the file exists
        if not os.path.exists('midday_results.pkl'):
            logger.warning("Midday results file does not exist")
            return None
        
        # Load the saved data
        with open('midday_results.pkl', 'rb') as f:
            midday_data = pickle.load(f)
        
        # Verify the data has the expected structure
        required_keys = ['targets_df', 'target_col', 'rpc_col']
        if not all(key in midday_data for key in required_keys):
            logger.warning("Midday results file has invalid format")
            return None
        
        # Check if the data is from today
        timestamp = datetime.fromisoformat(midday_data['timestamp'])
        today = datetime.now().date()
        
        if timestamp.date() != today:
            logger.warning(f"Midday results are from {timestamp.date()}, not from today ({today})")
            return None
        
        logger.info("Midday results loaded successfully")
        return midday_data
    except Exception as e:
        logger.error(f"Failed to load midday results: {str(e)}")
        return None

def compare_and_send_midday_results(targets_df, target_col, rpc_col):
    """Compare midday results with morning run and send notification"""
    try:
        # Try to load morning results
        morning_data = load_morning_results()
        
        if not morning_data:
            logger.warning("No morning results available for comparison")
            # Just send regular results without comparison
            return send_results_to_slack(targets_df, target_col, rpc_col, run_label='midday')
        
        # Get threshold from environment variable or use default
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 12.0))
        
        # Get the morning data components
        morning_df = morning_data['targets_df']
        morning_target_col = morning_data['target_col']
        morning_rpc_col = morning_data['rpc_col']
        
        # Make sure both DataFrames have the same columns
        if not target_col in targets_df.columns or not rpc_col in targets_df.columns:
            logger.error(f"Current data missing required columns: {target_col}, {rpc_col}")
            return send_results_to_slack(targets_df, target_col, rpc_col, run_label='midday')
        
        if not morning_target_col in morning_df.columns or not morning_rpc_col in morning_df.columns:
            logger.error(f"Morning data missing required columns: {morning_target_col}, {morning_rpc_col}")
            return send_results_to_slack(targets_df, target_col, rpc_col, run_label='midday')
        
        # Rename columns to avoid confusion
        targets_df = targets_df.rename(columns={rpc_col: 'midday_rpc'})
        morning_df = morning_df.rename(columns={morning_rpc_col: 'morning_rpc'})
        
        # Merge the dataframes on target column
        merged_df = pd.merge(morning_df, targets_df, how='outer', left_on=morning_target_col, right_on=target_col)
        
        # Fill NaN values
        merged_df['morning_rpc'] = merged_df['morning_rpc'].fillna(0)
        merged_df['midday_rpc'] = merged_df['midday_rpc'].fillna(0)
        
        # Use original target column if available, otherwise morning target column
        merged_df[target_col] = merged_df[target_col].fillna(merged_df[morning_target_col])
        
        # Find targets that went below the threshold since morning
        went_below_threshold = merged_df[
            (merged_df['morning_rpc'] >= rpc_threshold) & 
            (merged_df['midday_rpc'] < rpc_threshold)
        ]
        
        # Current targets BELOW threshold in midday run
        current_below_threshold = merged_df[merged_df['midday_rpc'] < rpc_threshold]
        
        # Save midday results for afternoon comparison
        save_midday_results(targets_df, target_col, 'midday_rpc')
        
        # Send midday results with comparison
        return send_midday_comparison_to_slack(
            targets_df=current_below_threshold, 
            went_below_df=went_below_threshold, 
            target_col=target_col, 
            rpc_col='midday_rpc', 
            morning_rpc_col='morning_rpc'
        )
    except Exception as e:
        logger.error(f"Error comparing midday results: {str(e)}")
        # Fall back to sending regular results
        return send_results_to_slack(targets_df, target_col, rpc_col, run_label='midday')

def send_midday_comparison_to_slack(targets_df, went_below_df, target_col, rpc_col, morning_rpc_col):
    """Send midday comparison results to Slack"""
    import requests
    
    try:
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            logger.error("Slack webhook URL not configured, skipping notification")
            return False
        
        # Get RPC threshold for context
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', '12.0'))
        
        # Format the date for display
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Create the message with comparison
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"Ringba RPC Report - {today} *MIDDAY RUN (2 PM ET)*",
                        "emoji": True
                    }
                }
            ]
        }
        
        # Targets that fell below threshold since morning
        if not went_below_df.empty:
            below_list = "*Targets that FELL BELOW ${:.2f} RPC since morning:* 📉\n".format(rpc_threshold)
            for _, row in went_below_df.iterrows():
                target_name = row[target_col] if not pd.isna(row[target_col]) else "Total RPC (including the ones below $12)"
                morning_rpc = row[morning_rpc_col]
                midday_rpc = row[rpc_col]
                change = midday_rpc - morning_rpc
                change_pct = (change / morning_rpc) * 100 if morning_rpc > 0 else 0
                
                below_list += f"• *{target_name}*: {morning_rpc:.2f} → {midday_rpc:.2f} ({change_pct:.1f}%)\n"
            
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": below_list
                }
            })
        else:
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*No targets fell below threshold since morning* 🎉"
                }
            })
        
        # Add a divider to separate the sections
        message["blocks"].append({
            "type": "divider"
        })
        
        # Current targets BELOW threshold
        if not targets_df.empty:
            current_list = f"*Current Targets Below ${rpc_threshold:.2f} RPC:* 📉\n"
            for _, row in targets_df.iterrows():
                target_name = row[target_col] if not pd.isna(row[target_col]) else "Total RPC (including the ones below $12)"
                rpc_value = row[rpc_col]
                current_list += f"• *{target_name}*: RPC = {rpc_value:.2f}\n"
            
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": current_list
                }
            })
        else:
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*No targets currently below ${rpc_threshold:.2f} RPC* 🎉"
                }
            })
        
        # Add footer
        message["blocks"].append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Data from Ringba for {today}"
                }
            ]
        })
        
        # Send the message
        response = requests.post(
            webhook_url,
            json=message,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            logger.info("Midday comparison results sent to Slack successfully")
            return True
        else:
            logger.error(f"Failed to send midday comparison results to Slack: {response.status_code} {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending midday comparison results to Slack: {str(e)}")
        return False

def compare_and_send_afternoon_results(targets_df, target_col, rpc_col, run_label='afternoon'):
    """Compare afternoon results with midday run and send notification"""
    try:
        # First try to load midday results - this is the priority for afternoon comparison
        midday_data = load_midday_results()
        
        if midday_data:
            logger.info("Using midday results for afternoon comparison")
            
            # Get threshold from environment variable or use default
            rpc_threshold = float(os.getenv('RPC_THRESHOLD', 12.0))
            
            # Get the midday data components
            midday_df = midday_data['targets_df']
            midday_target_col = midday_data['target_col']
            midday_rpc_col = midday_data['rpc_col']
            
            # Make sure both DataFrames have the same columns
            if not target_col in targets_df.columns or not rpc_col in targets_df.columns:
                logger.error(f"Current data missing required columns: {target_col}, {rpc_col}")
                return send_results_to_slack(targets_df, target_col, rpc_col, run_label=run_label)
            
            if not midday_target_col in midday_df.columns or not midday_rpc_col in midday_df.columns:
                logger.error(f"Midday data missing required columns: {midday_target_col}, {midday_rpc_col}")
                return send_results_to_slack(targets_df, target_col, rpc_col, run_label=run_label)
            
            # Rename columns to avoid confusion
            targets_df = targets_df.rename(columns={rpc_col: 'afternoon_rpc'})
            midday_df = midday_df.rename(columns={midday_rpc_col: 'midday_rpc'})
            
            # Merge the dataframes on target column
            merged_df = pd.merge(midday_df, targets_df, how='outer', left_on=midday_target_col, right_on=target_col)
            
            # Fill NaN values
            merged_df['midday_rpc'] = merged_df['midday_rpc'].fillna(0)
            merged_df['afternoon_rpc'] = merged_df['afternoon_rpc'].fillna(0)
            
            # Use original target column if available, otherwise midday target column
            merged_df[target_col] = merged_df[target_col].fillna(merged_df[midday_target_col])
            
            # Find targets that went below the threshold since midday
            went_below_threshold = merged_df[
                (merged_df['midday_rpc'] >= rpc_threshold) & 
                (merged_df['afternoon_rpc'] < rpc_threshold)
            ]
            
            # Current targets BELOW threshold in afternoon run
            current_below_threshold = merged_df[merged_df['afternoon_rpc'] < rpc_threshold]
            
            # Send afternoon results with comparison to midday
            return send_afternoon_comparison_to_slack(
                targets_df=current_below_threshold, 
                went_below_df=went_below_threshold, 
                previous_run_name="midday",
                target_col=target_col, 
                rpc_col='afternoon_rpc', 
                previous_rpc_col='midday_rpc'
            )
        
        # If no midday results, fall back to morning comparison
        logger.warning("No midday results available, falling back to morning comparison")
        morning_data = load_morning_results()
        
        if not morning_data:
            logger.warning("No morning results available for comparison either")
            # Just send regular results without comparison
            return send_results_to_slack(targets_df, target_col, rpc_col, run_label=run_label)
        
        # Get threshold from environment variable or use default
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 12.0))
        
        # Get the morning data components
        morning_df = morning_data['targets_df']
        morning_target_col = morning_data['target_col']
        morning_rpc_col = morning_data['rpc_col']
        
        # Make sure both DataFrames have the same columns
        if not target_col in targets_df.columns or not rpc_col in targets_df.columns:
            logger.error(f"Current data missing required columns: {target_col}, {rpc_col}")
            return send_results_to_slack(targets_df, target_col, rpc_col, run_label=run_label)
        
        if not morning_target_col in morning_df.columns or not morning_rpc_col in morning_df.columns:
            logger.error(f"Morning data missing required columns: {morning_target_col}, {morning_rpc_col}")
            return send_results_to_slack(targets_df, target_col, rpc_col, run_label=run_label)
        
        # Rename columns to avoid confusion
        targets_df = targets_df.rename(columns={rpc_col: 'afternoon_rpc'})
        morning_df = morning_df.rename(columns={morning_rpc_col: 'morning_rpc'})
        
        # Merge the dataframes on target column
        merged_df = pd.merge(morning_df, targets_df, how='outer', left_on=morning_target_col, right_on=target_col)
        
        # Fill NaN values
        merged_df['morning_rpc'] = merged_df['morning_rpc'].fillna(0)
        merged_df['afternoon_rpc'] = merged_df['afternoon_rpc'].fillna(0)
        
        # Use original target column if available, otherwise morning target column
        merged_df[target_col] = merged_df[target_col].fillna(merged_df[morning_target_col])
        
        # Find targets that went below the threshold since morning
        went_below_threshold = merged_df[
            (merged_df['morning_rpc'] >= rpc_threshold) & 
            (merged_df['afternoon_rpc'] < rpc_threshold)
        ]
        
        # Current targets BELOW threshold in afternoon run
        current_below_threshold = merged_df[merged_df['afternoon_rpc'] < rpc_threshold]
        
        # Send afternoon results with comparison to morning
        return send_afternoon_comparison_to_slack(
            targets_df=current_below_threshold, 
            went_below_df=went_below_threshold, 
            previous_run_name="morning",
            target_col=target_col, 
            rpc_col='afternoon_rpc', 
            previous_rpc_col='morning_rpc'
        )
    except Exception as e:
        logger.error(f"Error comparing afternoon results: {str(e)}")
        # Fall back to sending regular results
        return send_results_to_slack(targets_df, target_col, rpc_col, run_label=run_label)

def send_afternoon_comparison_to_slack(targets_df, went_below_df, previous_run_name, target_col, rpc_col, previous_rpc_col):
    """Send afternoon comparison results to Slack"""
    import requests
    
    try:
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            logger.error("Slack webhook URL not configured, skipping notification")
            return False
        
        # Get RPC threshold for context
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', '12.0'))
        
        # Format the date for display
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Create the message with comparison
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"Ringba RPC Report - {today} *AFTERNOON RUN (4:30 PM ET)*",
                        "emoji": True
                    }
                }
            ]
        }
        
        # Targets that fell below threshold since previous run
        if not went_below_df.empty:
            below_list = f"*Targets that FELL BELOW ${rpc_threshold:.2f} RPC since {previous_run_name} run:* 📉\n"
            for _, row in went_below_df.iterrows():
                target_name = row[target_col] if not pd.isna(row[target_col]) else "Total RPC (including the ones below $12)"
                previous_rpc = row[previous_rpc_col]
                afternoon_rpc = row[rpc_col]
                change = afternoon_rpc - previous_rpc
                change_pct = (change / previous_rpc) * 100 if previous_rpc > 0 else 0
                
                below_list += f"• *{target_name}*: {previous_rpc:.2f} → {afternoon_rpc:.2f} ({change_pct:.1f}%)\n"
            
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": below_list
                }
            })
        else:
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*No targets fell below threshold since {previous_run_name} run* 🎉"
                }
            })
        
        # Add a divider to separate the sections
        message["blocks"].append({
            "type": "divider"
        })
        
        # Current targets BELOW threshold
        if not targets_df.empty:
            current_list = f"*Current Targets Below ${rpc_threshold:.2f} RPC:* 📉\n"
            for _, row in targets_df.iterrows():
                target_name = row[target_col] if not pd.isna(row[target_col]) else "Total RPC (including the ones below $12)"
                rpc_value = row[rpc_col]
                current_list += f"• *{target_name}*: RPC = {rpc_value:.2f}\n"
            
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": current_list
                }
            })
        else:
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*No targets currently below ${rpc_threshold:.2f} RPC* 🎉"
                }
            })
        
        # Add footer
        message["blocks"].append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Data from Ringba for {today}"
                }
            ]
        })
        
        # Send the message
        response = requests.post(
            webhook_url,
            json=message,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            logger.info("Afternoon comparison results sent to Slack successfully")
            return True
        else:
            logger.error(f"Failed to send afternoon comparison results to Slack: {response.status_code} {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending afternoon comparison results to Slack: {str(e)}")
        return False

def send_results_to_slack(message, results=None, error=False, screenshot_path=None):
    """Send results to Slack webhook"""
    try:
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            logger.error("No Slack webhook URL found in environment variables")
            return False
            
        logger.info(f"Sending {'error' if error else 'results'} to Slack")
        
        # Construct the Slack message payload
        payload = {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{'⚠️ ERROR' if error else '📊 RPC RESULTS'}*\n{message}"
                    }
                }
            ]
        }
        
        # Add results table if results are provided
        if results is not None and not error and isinstance(results, pd.DataFrame) and not results.empty:
            # Format the results for Slack
            targets_text = ""
            
            # Format the targets data
            for _, row in results.iterrows():
                target_name = row.get('Target Name', 'Unknown')
                rpc = float(row.get('RPC', 0.0))
                calls = int(row.get('Calls', 0))
                revenue = float(row.get('Revenue', 0.0))
                
                targets_text += f"• *{target_name}*\n"
                targets_text += f"  - RPC: ${rpc:.2f}\n"
                targets_text += f"  - Calls: {calls}\n"
                targets_text += f"  - Revenue: ${revenue:.2f}\n\n"
            
            if targets_text:
                payload["blocks"].append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": targets_text
                    }
                })
            
        # Add screenshot if provided
        if screenshot_path and os.path.exists(screenshot_path):
            logger.info(f"Attaching screenshot: {screenshot_path}")
            
            # Upload the screenshot to Slack
            with open(screenshot_path, 'rb') as img:
                response = requests.post(
                    'https://slack.com/api/files.upload',
                    data={
                        'channels': os.getenv('SLACK_CHANNEL', ''),
                        'title': 'Screenshot',
                        'filename': os.path.basename(screenshot_path),
                        'initial_comment': 'Screenshot attached'
                    },
                    files={'file': img},
                    headers={'Authorization': f'Bearer {os.getenv("SLACK_BOT_TOKEN", "")}'} 
                )
                
                if response.status_code != 200 or not response.json().get('ok', False):
                    logger.warning(f"Failed to upload screenshot: {response.json()}")
        
        # Send the message
        response = requests.post(webhook_url, json=payload)
        
        if response.status_code != 200:
            logger.error(f"Failed to send message to Slack: {response.status_code} {response.text}")
            return False
            
        logger.info("Sent notification to Slack")
        return True
        
    except Exception as e:
        logger.error(f"Error sending to Slack: {str(e)}")
        return False

def export_csv():
    """Export CSV from Ringba and notify Slack for any targets with RPC below threshold"""
    browser = None
    try:
        # Determine the current time in Eastern time
        eastern_tz = pytz.timezone('America/New_York')
        now = datetime.now(eastern_tz)
        
        # Format for checking times
        current_time_str = now.strftime('%H:%M')
        
        # Determine which type of run this is based on time
        if check_time_range(current_time_str, MORNING_CHECK_TIME):
            logger.info("Processing morning run (11 AM ET)")
            run_type = "Morning"
        elif check_time_range(current_time_str, MIDDAY_CHECK_TIME):
            logger.info("Processing midday run (2 PM ET)")
            run_type = "Midday"
        elif check_time_range(current_time_str, AFTERNOON_CHECK_TIME):
            logger.info("Processing afternoon run (4:30 PM ET)")
            run_type = "Afternoon"
        else:
            run_type = "Manual"
            logger.info(f"Processing manual run at {current_time_str} ET")
            
        # Start the browser
        browser = setup_browser()
        if not browser:
            logger.error("Failed to set up browser")
            return False

        # Login to Ringba
        if not login_to_ringba(browser):
            logger.error("Failed to login to Ringba")
            return False

        # Navigate to Call Logs
        if not navigate_to_call_logs(browser):
            logger.error("Failed to navigate to Call Logs")
            return False

        # Click Export CSV
        max_retries = 3
        csv_file_path = None
        
        for attempt in range(max_retries):
            logger.info(f"Export attempt {attempt+1}/{max_retries}")
            csv_file_path = click_export_csv(browser)
            
            if csv_file_path:
                logger.info(f"Successfully exported CSV file: {csv_file_path}")
                break
            elif attempt < max_retries - 1:
                logger.warning(f"Export attempt {attempt+1} failed, retrying...")
                time.sleep(5)  # Wait before retry
            else:
                logger.error("All export attempts failed")
        
        if not csv_file_path:
            logger.error("Failed to export CSV file after all attempts")
            # Send notification to Slack about the failure
            try:
                error_message = {
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"{run_type} Run Failed - Could Not Export CSV"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "The script was unable to download the CSV file from Ringba. Please check the logs and Ringba account."
                            }
                        }
                    ]
                }
                
                webhook_url = os.getenv('SLACK_WEBHOOK_URL')
                if webhook_url:
                    requests.post(webhook_url, json=error_message)
                    logger.info("Sent failure notification to Slack")
            except Exception as err:
                logger.error(f"Failed to send failure notification: {str(err)}")
                
            return False

        # Process the CSV file
        low_rpc_targets = process_csv_file(csv_file_path)
        if not low_rpc_targets:
            logger.info("No low RPC targets found")
            # Send a notification to Slack that no targets were found
            try:
                no_targets_message = {
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"{run_type} Report - No Targets Below Threshold"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "🎉 Great news! No targets were found below the RPC threshold."
                            }
                        }
                    ]
                }
                
                webhook_url = os.getenv('SLACK_WEBHOOK_URL')
                if webhook_url:
                    requests.post(webhook_url, json=no_targets_message)
                    logger.info("Sent 'no targets' notification to Slack")
            except Exception as err:
                logger.error(f"Failed to send 'no targets' notification: {str(err)}")
                
            return True

        # Send to Slack
        if send_to_slack(low_rpc_targets, run_type):
            logger.info(f"Successfully sent {run_type} report to Slack")
            return True
        else:
            logger.error("Failed to send report to Slack")
            return False
    except Exception as e:
        logger.error(f"Failed to export CSV: {str(e)}")
        return False
    finally:
        if browser:
            browser.quit()

def perform_test_run():
    """Perform a test run when the service is first deployed"""
    logger.info("Performing initial test run for web service deployment...")
    try:
        # Run the export with no parameters (uses environment variables)
        result = export_csv()
        
        if result:
            logger.info("Test run completed successfully")
            return True
        else:
            logger.error("Test run failed")
            return False
    except Exception as e:
        logger.error(f"Error during test run: {str(e)}")
        return False

if __name__ == "__main__":
    # Check if we're running as a web service (PORT environment variable is set)
    is_web_service = bool(os.getenv('PORT'))
    
    if is_web_service:
        logger.info("Running as web service, performing initial test run...")
        perform_test_run()
        logger.info("Test run complete, service will now wait for scheduled runs")
        
        # Start Flask app for web service
        from flask import Flask
        app = Flask(__name__)
        
        @app.route('/')
        def home():
            return "Ringba Export Service is running. Scheduled runs at 11 AM, 2 PM, and 4:30 PM ET."
        
        # Get port from environment variable
        port = int(os.getenv('PORT', 8080))
        app.run(host='0.0.0.0', port=port)
    else:
        # Regular command-line execution
        logger.info("Running as command-line tool")
        
        # Set a global timeout for the entire process
        def timeout_handler():
            logger.error("Global timeout reached, forcing script termination")
            # Force terminate the process
            os.kill(os.getpid(), signal.SIGTERM)
        
        # Set 10 minute timeout for the entire process
        global_timeout = int(os.getenv("GLOBAL_TIMEOUT_MINUTES", "10")) * 60
        timer = threading.Timer(global_timeout, timeout_handler)
        timer.daemon = True
        timer.start()
        
        try:
            # Run export
            export_csv()
        except Exception as e:
            logger.error(f"Unhandled exception in main process: {str(e)}")
        finally:
            # Cancel timer if script completes normally
            timer.cancel()
            logger.info("Script execution complete") 
