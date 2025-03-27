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
    """Set up the Chrome browser with appropriate options"""
    try:
        # Debug environment variables 
        logger.info(f"Chrome options from env: {os.getenv('CHROME_OPTIONS', 'Not set')}")
        
        # Create Chrome options
        chrome_options = webdriver.ChromeOptions()
        
        # Core stability options - always include these regardless of env vars
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Additional stability options
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        
        # Get additional Chrome options from environment variable
        chrome_options_str = os.getenv('CHROME_OPTIONS', '')
        if chrome_options_str:
            logger.info(f"Adding additional Chrome options from environment: {chrome_options_str}")
            for option in chrome_options_str.split():
                if option.strip() and not any(opt in option for opt in ["--headless", "--no-sandbox", "--disable-dev-shm-usage"]):
                    chrome_options.add_argument(option.strip())
        
        # Set page load strategy to eager (don't wait for all resources)
        chrome_options.page_load_strategy = 'eager'
            
        # Set download directory
        download_dir = os.path.abspath(os.getcwd())
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Disable automation flags
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        # Log the final Chrome options
        logger.info(f"Setting up Chrome with options: {chrome_options.arguments}")
        
        # Create the browser
        browser = webdriver.Chrome(options=chrome_options)
        
        # Set timeout
        browser.set_page_load_timeout(90)  # Longer timeout for page loads
        browser.implicitly_wait(15)        # Longer implicit wait
        
        # Set window size
        browser.set_window_size(1920, 1080)
        
        logger.info("Chrome browser set up successfully")
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
    """Navigate to Call Logs page"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Directly navigate to the Call Logs URL
            logger.info(f"Navigating to call logs report (attempt {attempt+1}/{max_retries})...")
            browser.get("https://app.ringba.com/#/dashboard/call-logs/report/summary")
            
            # Wait for page to load
            logger.info("Waiting for call logs page to load...")
            time.sleep(15)
            
            # Take screenshot
            take_screenshot(browser, f"call_logs_page_attempt_{attempt+1}")
            
            # Use WebDriverWait to look for elements that indicate page is loaded
            wait = WebDriverWait(browser, 30)
            
            # Try different selectors to confirm the page is loaded
            selectors = [
                (By.CSS_SELECTOR, ".reporting-container"),
                (By.CSS_SELECTOR, ".dashboard-container"),
                (By.CSS_SELECTOR, "table.mat-table"),
                (By.CSS_SELECTOR, ".mat-elevation-z8"),
                (By.XPATH, "//h1[contains(text(), 'Call Logs')]"),
                (By.XPATH, "//button[contains(text(), 'Export')]")
            ]
            
            page_loaded = False
            for selector_type, selector in selectors:
                try:
                    logger.info(f"Looking for element with {selector_type}={selector}")
                    wait.until(EC.presence_of_element_located((selector_type, selector)))
                    logger.info(f"Found element with {selector_type}={selector} - page appears to be loaded")
                    page_loaded = True
                    break
                except Exception as e:
                    logger.warning(f"Element {selector_type}={selector} not found: {str(e)}")
            
            # If we find any indicator that the page loaded successfully
            if page_loaded:
                logger.info("Successfully navigated to call logs page")
                return True
                
            # Even if indicators aren't found, check if we have the right URL
            if "call-logs" in browser.current_url:
                logger.info("URL contains 'call-logs' - assuming navigation was successful")
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
    """Click the Export CSV button and download the file"""
    try:
        # Navigate directly to the summary page with export option
        logger.info("Navigating directly to call summary report...")
        try:
            browser.get("https://app.ringba.com/#/dashboard/call-logs/report/summary")
            logger.info("Waiting for summary page to load...")
            time.sleep(15)  # Give page more time to load
        except Exception as e:
            logger.error(f"Failed to navigate to summary page: {str(e)}")
            take_screenshot(browser, "navigation_failed")
        
        # Take screenshot to see page state
        take_screenshot(browser, "before_export_attempt")
            
        # Try multiple approaches to find and click the export button
        logger.info("Trying multiple approaches to find and click EXPORT CSV button...")
        
        # Approach 1: Use JavaScript to find and click the button by text content
        try:
            logger.info("Approach 1: Using JavaScript to find button by text content")
            result = browser.execute_script("""
                // Find all buttons/spans that contain 'export' or 'csv'
                var buttons = [];
                document.querySelectorAll('button, span, a, div').forEach(function(el) {
                    if (el.textContent && 
                        (el.textContent.toLowerCase().includes('export') || 
                         el.textContent.toLowerCase().includes('csv'))) {
                        buttons.push(el);
                    }
                });
                
                // Log what we found for debugging
                console.log("Found " + buttons.length + " potential export buttons");
                buttons.forEach(function(b, i) { 
                    console.log(i + ": " + b.textContent.trim() + " - " + b.tagName); 
                });
                
                // If found any, click the first one
                if (buttons.length > 0) {
                    buttons[0].click();
                    return true;
                }
                
                return false;
            """)
            
            if result:
                logger.info("Successfully clicked export button via JavaScript")
                time.sleep(15)  # Wait for download to start
            else:
                logger.warning("Could not find export button via JavaScript")
                take_screenshot(browser, "js_approach_failed")
        except Exception as e:
            logger.error(f"Error with JavaScript approach: {str(e)}")
            take_screenshot(browser, "js_approach_error")
        
        # Approach 2: Try with explicit WebDriverWait and CSS selectors
        if not result:
            try:
                logger.info("Approach 2: Using WebDriverWait with various CSS selectors")
                selectors = [
                    "button.export-button",
                    ".export-csv",
                    "button.mat-button:has-text('EXPORT CSV')",
                    "button:contains('EXPORT')",
                    "button[title*='Export']",
                    "a.export-link"
                ]
                
                for selector in selectors:
                    try:
                        logger.info(f"Trying selector: {selector}")
                        export_button = WebDriverWait(browser, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        logger.info(f"Found button with selector: {selector}")
                        export_button.click()
                        logger.info("Clicked the export button")
                        result = True
                        time.sleep(15)  # Wait for download to start
                        break
                    except Exception as e:
                        logger.warning(f"Selector {selector} failed: {str(e)}")
                        continue
            except Exception as e:
                logger.error(f"Error with explicit selectors approach: {str(e)}")
                take_screenshot(browser, "explicit_selectors_error")
        
        # Approach 3: Try with XPath to find buttons containing text
        if not result:
            try:
                logger.info("Approach 3: Using XPath to find buttons by text content")
                xpaths = [
                    "//button[contains(., 'EXPORT')]",
                    "//button[contains(., 'Export')]",
                    "//button[contains(., 'CSV')]",
                    "//a[contains(., 'Export')]",
                    "//span[contains(., 'Export')]"
                ]
                
                for xpath in xpaths:
                    try:
                        logger.info(f"Trying XPath: {xpath}")
                        export_button = WebDriverWait(browser, 5).until(
                            EC.element_to_be_clickable((By.XPATH, xpath))
                        )
                        logger.info(f"Found button with XPath: {xpath}")
                        export_button.click()
                        logger.info("Clicked the export button")
                        result = True
                        time.sleep(15)  # Wait for download to start
                        break
                    except Exception as e:
                        logger.warning(f"XPath {xpath} failed: {str(e)}")
                        continue
            except Exception as e:
                logger.error(f"Error with XPath approach: {str(e)}")
                take_screenshot(browser, "xpath_approach_error")
        
        # Take screenshot after export attempt
        take_screenshot(browser, "after_export_attempt")
        
        # Wait for download to complete, regardless of whether we know we clicked successfully
        logger.info("Waiting for download to complete...")
        time.sleep(45)  # Wait longer to ensure download completes
        
        # Look for any CSV files
        download_dir = os.path.abspath(os.getcwd())
        csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
        
        if csv_files:
            # Sort by creation time
            csv_files.sort(key=lambda x: os.path.getctime(os.path.join(download_dir, x)), reverse=True)
            latest_csv = csv_files[0]
            logger.info(f"Found latest CSV file: {latest_csv}")
            
            # Check if it's a newly created file (within last 5 minutes)
            file_path = os.path.join(download_dir, latest_csv)
            file_creation_time = os.path.getctime(file_path)
            current_time = time.time()
            
            if current_time - file_creation_time < 300:  # 5 minutes
                logger.info(f"Using newly created CSV file: {latest_csv}")
                return file_path
            else:
                logger.warning(f"CSV file {latest_csv} is not recent (created {(current_time - file_creation_time)/60:.1f} minutes ago)")
                
        # Return the most recent CSV file even if it's old
        if csv_files:
            latest_csv = csv_files[0]
            logger.info(f"Using existing CSV file as fallback: {latest_csv}")
            return os.path.join(download_dir, latest_csv)
            
        logger.warning("No CSV files found after waiting")
        return None
            
    except Exception as e:
        logger.error(f"Failed to export CSV: {str(e)}")
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
        csv_file_path = click_export_csv(browser)
        if not csv_file_path:
            logger.error("Failed to export CSV file")
            return False
            
        logger.info(f"Exported CSV file: {csv_file_path}")

        # Process the CSV file
        low_rpc_targets = process_csv_file(csv_file_path)
        if not low_rpc_targets:
            logger.info("No low RPC targets found")
            return True

        # Send to Slack
        send_to_slack(low_rpc_targets, run_type)
        
        return True
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
