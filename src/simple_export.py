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

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('simple_export.log')
    ]
)
logger = logging.getLogger('simple_export')

# Create screenshots directory if it doesn't exist
screenshots_dir = "screenshots"
if not os.path.exists(screenshots_dir):
    os.makedirs(screenshots_dir)

def take_screenshot(browser, name):
    """Take a screenshot for debugging purposes"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{screenshots_dir}/{timestamp}_{name}.png"
        browser.save_screenshot(filename)
        logger.info(f"Screenshot saved: {filename}")
    except Exception as e:
        logger.error(f"Failed to take screenshot: {str(e)}")

def setup_browser():
    """Set up and configure the browser for automation"""
    
    # Create a new Chrome browser instance
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-infobars")
    options.add_argument("--mute-audio")
    
    # Use headless mode based on environment variable (for render.com deployment)
    use_headless = os.getenv("USE_HEADLESS", "false").lower() == "true"
    if use_headless:
        logger.info("Running in headless mode")
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
    else:
        logger.info("Running in visible mode")
    
    # Disable automation flags to avoid detection
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # On render.com, specify the Chrome binary location
    # For render.com's specific environment
    chrome_location = '/usr/bin/google-chrome-stable'
    if os.path.exists(chrome_location):
        options.binary_location = chrome_location
    
    # Set up download directory to current folder
    download_dir = os.path.abspath(os.getcwd())
    
    # For render.com, ensure the download directory exists and is writable
    download_dir = os.getenv("DOWNLOAD_DIR", download_dir)
    os.makedirs(download_dir, exist_ok=True)
    
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "credentials_enable_service": True,
        "profile.password_manager_enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    # Add retry logic for browser initialization
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Try to use service but handle if chromedriver isn't available
            try:
                browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            except Exception as chrome_error:
                logger.warning(f"Error with ChromeDriverManager: {str(chrome_error)}")
                # Try fallback without service
                browser = webdriver.Chrome(options=options)
            
            logger.info("Browser set up successfully")
            return browser
        except Exception as e:
            retry_count += 1
            logger.error(f"Failed to set up browser (attempt {retry_count}/{max_retries}): {str(e)}")
            if retry_count >= max_retries:
                logger.error("Max retries reached for browser setup, giving up")
                return None
            time.sleep(5)  # Wait before retrying

def login_to_ringba(browser, username=None, password=None):
    """Login to Ringba with credentials"""
    try:
        # Get credentials from environment variables if not provided
        if not username:
            username = os.getenv("RINGBA_USERNAME")
        if not password:
            password = os.getenv("RINGBA_PASSWORD")
            
        if not username or not password:
            logger.error("Missing username or password. Set RINGBA_USERNAME and RINGBA_PASSWORD environment variables.")
            return False
            
        # Navigate to login page
        logger.info("Navigating to Ringba login page")
        browser.get("https://app.ringba.com/#/login")
        
        # Take screenshot before login
        take_screenshot(browser, "before_login")
        
        # Wait for login page elements with increased timeout
        logger.info("Waiting for login page to load...")
        
        # Try different selectors for the login form
        selectors = [
            (By.ID, "username"),
            (By.NAME, "username"),
            (By.CSS_SELECTOR, "input[type='email']"),
            (By.CSS_SELECTOR, "input[type='text']")
        ]
        
        username_field = None
        for selector_type, selector_value in selectors:
            try:
                WebDriverWait(browser, 10).until(
                    EC.presence_of_element_located((selector_type, selector_value))
                )
                username_field = browser.find_element(selector_type, selector_value)
                logger.info(f"Found username field with selector: {selector_type}={selector_value}")
                break
            except:
                continue
        
        if not username_field:
            logger.error("Could not find username field")
            take_screenshot(browser, "username_field_not_found")
            return False
            
        # Clear and enter username
        logger.info(f"Entering username: {username}")
        username_field.clear()
        username_field.send_keys(username)
        time.sleep(1)
        
        # Find password field - it's usually the next input
        password_selectors = [
            (By.ID, "password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']")
        ]
        
        password_field = None
        for selector_type, selector_value in password_selectors:
            try:
                WebDriverWait(browser, 10).until(
                    EC.presence_of_element_located((selector_type, selector_value))
                )
                password_field = browser.find_element(selector_type, selector_value)
                logger.info(f"Found password field with selector: {selector_type}={selector_value}")
                break
            except:
                continue
                
        if not password_field:
            logger.error("Could not find password field")
            take_screenshot(browser, "password_field_not_found")
            return False
        
        # Clear and enter password
        logger.info("Entering password...")
        password_field.clear()
        password_field.send_keys(password)
        time.sleep(1)
        
        # Take screenshot before clicking login
        take_screenshot(browser, "before_clicking_login")
        
        # Try different methods to find and click login button
        login_button = None
        login_selectors = [
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, ".login-btn"),
            (By.CSS_SELECTOR, "button.btn-primary"),
            (By.XPATH, "//button[contains(text(), 'Login')]"),
            (By.XPATH, "//button[contains(text(), 'Sign In')]")
        ]
        
        for selector_type, selector_value in login_selectors:
            try:
                WebDriverWait(browser, 10).until(
                    EC.element_to_be_clickable((selector_type, selector_value))
                )
                login_button = browser.find_element(selector_type, selector_value)
                logger.info(f"Found login button with selector: {selector_type}={selector_value}")
                break
            except:
                continue
                
        if not login_button:
            logger.warning("Could not find login button with conventional selectors")
            # Try submitting the form by pressing Enter on the password field
            logger.info("Trying to submit by pressing Enter on password field")
            password_field.send_keys(Keys.RETURN)
        else:
            logger.info("Clicking login button...")
            login_button.click()
        
        # Take screenshot after clicking login
        time.sleep(3)
        take_screenshot(browser, "after_clicking_login")
        
        # Wait for dashboard to load with longer timeout
        logger.info("Waiting for dashboard to load...")
        
        # Wait for indicators of successful login
        dashboard_selectors = [
            (By.ID, "main-content"),
            (By.CSS_SELECTOR, ".page-content"),
            (By.CSS_SELECTOR, ".navbar-nav")
        ]
        
        dashboard_loaded = False
        for selector_type, selector_value in dashboard_selectors:
            try:
                WebDriverWait(browser, 90).until(
                    EC.presence_of_element_located((selector_type, selector_value))
                )
                logger.info(f"Dashboard detected with selector: {selector_type}={selector_value}")
                dashboard_loaded = True
                break
            except:
                continue
                
        if not dashboard_loaded:
            logger.error("Dashboard did not load after login")
            take_screenshot(browser, "dashboard_not_loaded")
            return False
        
        # Explicitly wait to ensure page is fully loaded
        time.sleep(5)
        take_screenshot(browser, "after_login_success")
        
        logger.info("Successfully logged in to Ringba")
        return True
    except Exception as e:
        logger.error(f"Failed to log in to Ringba: {str(e)}")
        take_screenshot(browser, "login_exception")
        return False

def navigate_to_call_logs(browser):
    """Navigate directly to the call logs page"""
    try:
        call_logs_url = "https://app.ringba.com/#/dashboard/call-logs/report/new"
        logger.info(f"Navigating to call logs page: {call_logs_url}")
        browser.get(call_logs_url)
        
        # Take screenshot after navigation
        time.sleep(3)
        take_screenshot(browser, "after_navigation_to_call_logs")
        
        # Wait for the page to load with multiple possible selectors
        logger.info("Waiting for call logs page to load...")
        
        call_logs_selectors = [
            (By.CSS_SELECTOR, ".reporting-call-logs-data"),
            (By.CSS_SELECTOR, ".reporting-main-container--call-logs"),
            (By.CSS_SELECTOR, ".call-log-vue-container"),
            (By.CSS_SELECTOR, ".reporting-call-logs-header"),
            (By.XPATH, "//h1[text()='Summary']"), # Try to find the Summary header
            (By.CSS_SELECTOR, ".btn.export-csv") # Try to find the Export CSV button itself
        ]
        
        page_loaded = False
        for selector_type, selector_value in call_logs_selectors:
            try:
                WebDriverWait(browser, 120).until(
                    EC.presence_of_element_located((selector_type, selector_value))
                )
                logger.info(f"Call logs page detected with selector: {selector_type}={selector_value}")
                page_loaded = True
                break
            except:
                continue
                
        if not page_loaded:
            logger.error("Call logs page did not load")
            take_screenshot(browser, "call_logs_page_not_loaded")
            return False
        
        # Give additional time for page to fully render
        time.sleep(10)
        take_screenshot(browser, "call_logs_page_loaded")
        
        logger.info("Successfully navigated to call logs page")
        return True
    except Exception as e:
        logger.error(f"Failed to navigate to call logs page: {str(e)}")
        take_screenshot(browser, "navigation_exception")
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
        # Use the exact selector provided by the user from inspection
        logger.info("Looking for Export CSV button using exact selector...")
        export_button_selector = (By.CSS_SELECTOR, "button.btn.btn-sm.m-r-15.export-summary-btn")
        
        try:
            logger.info("Waiting for Export CSV button to be clickable...")
            WebDriverWait(browser, 60).until(
                EC.element_to_be_clickable(export_button_selector)
            )
            export_button = browser.find_element(*export_button_selector)
            logger.info("Export CSV button found with exact selector")
        except Exception as e:
            logger.warning(f"Could not find export button with exact selector: {str(e)}")
            logger.info("Falling back to alternative selectors...")
            
            # Try multiple selectors as fallback
            export_button_selectors = [
                (By.XPATH, "//button[text()='Export CSV']"),
                (By.XPATH, "//button[contains(text(), 'Export CSV')]"),
                (By.CSS_SELECTOR, ".export-summary-btn"),
                (By.CSS_SELECTOR, ".export-csv")
            ]
            
            export_button = None
            for selector_type, selector_value in export_button_selectors:
                try:
                    WebDriverWait(browser, 30).until(
                        EC.element_to_be_clickable((selector_type, selector_value))
                    )
                    export_button = browser.find_element(selector_type, selector_value)
                    logger.info(f"Export CSV button found with fallback selector: {selector_type}={selector_value}")
                    break
                except:
                    continue
            
            if not export_button:
                logger.error("Export CSV button not found with any selector")
                take_screenshot(browser, "export_button_not_found")
                return False
        
        # Take screenshot before clicking
        take_screenshot(browser, "before_clicking_export")
        
        # Click the export button
        logger.info("Clicking Export CSV button...")
        try:
            # Try regular click
            export_button.click()
        except:
            # If regular click fails, try JavaScript click
            logger.info("Regular click failed, trying JavaScript click")
            browser.execute_script("arguments[0].click();", export_button)
        
        # Take screenshot after clicking
        time.sleep(3)
        take_screenshot(browser, "after_clicking_export")
        
        # Wait for a fixed amount of time instead of trying to detect the file
        # Since the file might be downloaded but detection is failing
        logger.info("Waiting for download to complete (fixed 30 second wait)...")
        time.sleep(30)
        
        # Just check if any new CSV files exist after waiting
        download_dir = os.path.abspath(os.getcwd())
        csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
        if csv_files:
            logger.info(f"Found CSV files in directory: {csv_files}")
            logger.info("Assuming download completed successfully")
            return True
        else:
            logger.warning("No CSV files found after waiting")
            return False
            
    except Exception as e:
        logger.error(f"Failed to export CSV: {str(e)}")
        take_screenshot(browser, "export_exception")
        return False

def process_csv_file(file_path):
    """Process the downloaded CSV file to find targets with RPC above threshold"""
    logger.info(f"Processing CSV file: {file_path}")
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        logger.info(f"CSV file loaded with {len(df)} rows")
        logger.info(f"Available columns: {df.columns.tolist()}")
        
        # Possible column names for RPC and Campaign/Target
        possible_rpc_columns = ['RPC', 'rpc', 'Revenue Per Call', 'revenue_per_call']
        possible_target_columns = ['Campaign', 'Campaign Name', 'Target', 'target', 'campaign', 'campaign_name']
        
        # Find actual column names
        rpc_col = next((col for col in possible_rpc_columns if col in df.columns), None)
        target_col = next((col for col in possible_target_columns if col in df.columns), None)
        
        if not rpc_col:
            logger.error("RPC column not found in CSV")
            return False
            
        if not target_col:
            logger.error("Target/Campaign column not found in CSV")
            return False
            
        logger.info(f"Using columns: RPC={rpc_col}, Target/Campaign={target_col}")
        
        # Get threshold from environment variable or use default
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 10.0))
        logger.info(f"Using RPC threshold: {rpc_threshold}")
        
        # Ensure RPC column is numeric
        df[rpc_col] = pd.to_numeric(df[rpc_col], errors='coerce')
        df = df.dropna(subset=[rpc_col])
        
        # Find targets that meet the RPC threshold
        targets = df[df[rpc_col] >= rpc_threshold]
        logger.info(f"Found {len(targets)} targets meeting RPC threshold")
        
        # Log the targets
        for index, row in targets.iterrows():
            logger.info(f"Target: {row[target_col]}, RPC: {row[rpc_col]:.2f}")
        
        # Save processed results
        results_path = os.path.join(os.path.dirname(file_path), "processed_results.csv")
        targets.to_csv(results_path, index=False)
        logger.info(f"Saved processed results to {results_path}")
        
        # Return the targets dataframe and column names for further processing
        return targets, target_col, rpc_col
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        return None

def send_results_to_slack(targets_df, target_col, rpc_col):
    """Send the processing results to Slack webhook"""
    import requests
    import json
    
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not slack_webhook_url:
        logger.warning("No Slack webhook URL provided, skipping notification")
        return False
    
    try:
        logger.info("Sending results to Slack")
        
        # Format the results
        if len(targets_df) == 0:
            message = {"text": "📊 *Ringba Target Report*\n\nNo targets meeting the RPC threshold were found."}
        else:
            # Replace 'nan' with 'Total RPC (including the ones below $10)'
            targets_df = targets_df.copy()
            if target_col in targets_df.columns:
                targets_df[target_col] = targets_df[target_col].fillna("Total RPC (including the ones below $10)")
            
            # Get threshold from environment variable or use default
            rpc_threshold = float(os.getenv('RPC_THRESHOLD', 10.0))
            
            # Create a more readable message
            message_parts = []
            message_parts.append(f"📊 *Ringba Target Report* - Showing Targets with RPC ≥ ${rpc_threshold:.2f}")
            message_parts.append("")
            message_parts.append("```")
            
            # Use a more compact format to fit more data
            # First determine the max length of target names for proper formatting
            max_target_len = targets_df[target_col].astype(str).map(len).max()
            max_target_len = max(max_target_len, 15)  # At least 15 chars
            
            # Create the header
            message_parts.append(f"{'Target/Campaign':<{max_target_len}} | {'RPC':>8}")
            message_parts.append("-" * (max_target_len + 12))
            
            # Add each row
            for index, row in targets_df.iterrows():
                target = str(row[target_col])
                message_parts.append(
                    f"{target:<{max_target_len}} | {row[rpc_col]:>8.2f}"
                )
            
            message_parts.append("```")
            
            message = {
                "text": "\n".join(message_parts)
            }
        
        # Send to Slack
        response = requests.post(
            slack_webhook_url,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info("Results sent to Slack successfully")
            return True
        else:
            logger.error(f"Failed to send to Slack. Status code: {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending to Slack: {str(e)}")
        return False

def save_morning_results(targets_df, target_col, rpc_col):
    """Save the morning results for comparison with afternoon run"""
    try:
        # Create a directory for storing comparison data if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # Get today's date for the filename
        today = datetime.now().strftime('%Y-%m-%d')
        morning_file = os.path.join(data_dir, f"morning_results_{today}.csv")
        
        # Save the dataframe with only target and RPC columns
        save_df = targets_df[[target_col, rpc_col]].copy()
        save_df.to_csv(morning_file, index=False)
        
        logger.info(f"Morning results saved to {morning_file}")
        return True
    except Exception as e:
        logger.error(f"Error saving morning results: {str(e)}")
        return False

def load_morning_results():
    """Load the morning results for comparison with afternoon run"""
    try:
        # Get today's date for the filename
        data_dir = "data"
        today = datetime.now().strftime('%Y-%m-%d')
        morning_file = os.path.join(data_dir, f"morning_results_{today}.csv")
        
        if not os.path.exists(morning_file):
            logger.warning(f"Morning results file not found: {morning_file}")
            return None
        
        # Load the dataframe
        morning_df = pd.read_csv(morning_file)
        logger.info(f"Loaded morning results from {morning_file} with {len(morning_df)} rows")
        
        return morning_df
    except Exception as e:
        logger.error(f"Error loading morning results: {str(e)}")
        return None

def compare_and_send_afternoon_results(targets_df, target_col, rpc_col):
    """Compare afternoon results with morning and send notification"""
    try:
        # Load morning results
        morning_df = load_morning_results()
        
        if morning_df is None:
            logger.warning("No morning results available for comparison")
            # Just send regular results without comparison
            return send_results_to_slack(targets_df, target_col, rpc_col)
        
        # Get threshold from environment variable or use default
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 10.0))
        
        # Merge the dataframes to compare
        # Use suffixes to identify morning vs afternoon values
        merged_df = pd.merge(
            morning_df, 
            targets_df, 
            on=target_col, 
            how='outer',
            suffixes=('_morning', '_afternoon')
        )
        
        # Fill NaN with 0 for any missing values (targets that weren't in one of the datasets)
        morning_rpc_col = f"{rpc_col}_morning"
        afternoon_rpc_col = f"{rpc_col}_afternoon"
        
        # Replace NaN with 0 for calculation purposes
        merged_df[morning_rpc_col] = merged_df[morning_rpc_col].fillna(0)
        merged_df[afternoon_rpc_col] = merged_df[afternoon_rpc_col].fillna(0)
        
        # Find which targets crossed the threshold
        # 1. Targets that went below threshold (were >= threshold in morning, now < threshold)
        went_below = merged_df[
            (merged_df[morning_rpc_col] >= rpc_threshold) & 
            (merged_df[afternoon_rpc_col] < rpc_threshold)
        ]
        
        # 2. Targets that went above threshold (were < threshold in morning, now >= threshold)
        went_above = merged_df[
            (merged_df[morning_rpc_col] < rpc_threshold) & 
            (merged_df[afternoon_rpc_col] >= rpc_threshold)
        ]
        
        # Send to Slack with comparison information
        send_afternoon_comparison_to_slack(
            targets_df, 
            went_below, 
            went_above, 
            target_col, 
            rpc_col, 
            morning_rpc_col, 
            afternoon_rpc_col
        )
        
        return True
    except Exception as e:
        logger.error(f"Error comparing results: {str(e)}")
        # Fall back to sending regular results
        return send_results_to_slack(targets_df, target_col, rpc_col)

def send_afternoon_comparison_to_slack(targets_df, went_below_df, went_above_df, target_col, rpc_col, morning_rpc_col, afternoon_rpc_col):
    """Send afternoon comparison results to Slack"""
    import requests
    import json
    
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not slack_webhook_url:
        logger.warning("No Slack webhook URL provided, skipping notification")
        return False
    
    try:
        logger.info("Sending afternoon comparison to Slack")
        
        # Get threshold
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 10.0))
        
        # Replace 'nan' with 'Total RPC (including the ones below $10)'
        targets_df = targets_df.copy()
        if target_col in targets_df.columns:
            targets_df[target_col] = targets_df[target_col].fillna("Total RPC (including the ones below $10)")
        
        # First determine the max length of target names for proper formatting
        max_target_len = targets_df[target_col].astype(str).map(len).max()
        max_target_len = max(max_target_len, 15)  # At least 15 chars
        
        # Create a more detailed afternoon message
        message_parts = []
        
        # Main header
        message_parts.append(f"📊 *Afternoon Ringba Target Report* - Showing Targets with RPC ≥ ${rpc_threshold:.2f}")
        message_parts.append("")
        
        # Current targets above threshold
        message_parts.append("*Current Targets Above Threshold:*")
        message_parts.append("```")
        message_parts.append(f"{'Target/Campaign':<{max_target_len}} | {'RPC':>8}")
        message_parts.append("-" * (max_target_len + 12))
        
        for index, row in targets_df.iterrows():
            target = str(row[target_col])
            message_parts.append(
                f"{target:<{max_target_len}} | {row[rpc_col]:>8.2f}"
            )
        
        message_parts.append("```")
        
        # Show targets that went below threshold since morning
        if not went_below_df.empty:
            message_parts.append("*🔻 Targets That Went BELOW Threshold Since Morning:*")
            message_parts.append("```")
            message_parts.append(f"{'Target/Campaign':<{max_target_len}} | {'Morning':>8} | {'Afternoon':>8} | {'Change':>8}")
            message_parts.append("-" * (max_target_len + 37))
            
            for index, row in went_below_df.iterrows():
                target = str(row[target_col])
                if pd.isna(target):
                    target = "Total RPC (including the ones below $10)"
                morning = row[morning_rpc_col]
                afternoon = row[afternoon_rpc_col]
                change = afternoon - morning
                
                message_parts.append(
                    f"{target:<{max_target_len}} | {morning:>8.2f} | {afternoon:>8.2f} | {change:>+8.2f}"
                )
            
            message_parts.append("```")
        
        # Show targets that went above threshold since morning
        if not went_above_df.empty:
            message_parts.append("*🔺 Targets That Went ABOVE Threshold Since Morning:*")
            message_parts.append("```")
            message_parts.append(f"{'Target/Campaign':<{max_target_len}} | {'Morning':>8} | {'Afternoon':>8} | {'Change':>8}")
            message_parts.append("-" * (max_target_len + 37))
            
            for index, row in went_above_df.iterrows():
                target = str(row[target_col])
                if pd.isna(target):
                    target = "Total RPC (including the ones below $10)"
                morning = row[morning_rpc_col]
                afternoon = row[afternoon_rpc_col]
                change = afternoon - morning
                
                message_parts.append(
                    f"{target:<{max_target_len}} | {morning:>8.2f} | {afternoon:>8.2f} | {change:>+8.2f}"
                )
            
            message_parts.append("```")
        
        # Send the message
        message = {
            "text": "\n".join(message_parts)
        }
        
        response = requests.post(
            slack_webhook_url,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info("Afternoon comparison sent to Slack successfully")
            return True
        else:
            logger.error(f"Failed to send to Slack. Status code: {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending afternoon comparison to Slack: {str(e)}")
        return False

def export_csv(username=None, password=None, start_date=None, end_date=None):
    """Main function to export CSV data"""
    # Set default dates if not provided
    if not start_date:
        # Default to TODAY instead of yesterday
        today = datetime.now().strftime('%Y-%m-%d')
        start_date = today
    
    if not end_date:
        end_date = start_date
    
    logger.info(f"Starting CSV export for period {start_date} to {end_date}")
    
    # Determine if this is a morning or afternoon run based on current time in EST
    eastern = pytz.timezone('US/Eastern')
    current_time_est = datetime.now(pytz.utc).astimezone(eastern)
    logger.info(f"Current time in EST: {current_time_est.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Get the configured times for morning and afternoon runs
    morning_time_str = os.getenv('MORNING_CHECK_TIME', '10:00')
    afternoon_time_str = os.getenv('AFTERNOON_CHECK_TIME', '15:00')
    
    # Parse the time strings
    morning_hour, morning_minute = map(int, morning_time_str.split(':'))
    afternoon_hour, afternoon_minute = map(int, afternoon_time_str.split(':'))
    
    # Create datetime objects for morning and afternoon runs
    morning_time = current_time_est.replace(
        hour=morning_hour, 
        minute=morning_minute, 
        second=0, 
        microsecond=0
    )
    
    afternoon_time = current_time_est.replace(
        hour=afternoon_hour, 
        minute=afternoon_minute, 
        second=0, 
        microsecond=0
    )
    
    # Determine if this is a morning or afternoon run
    # Allow for some flexibility (runs within 2 hours of scheduled time)
    time_window = timedelta(hours=2)
    
    is_morning_run = abs(current_time_est - morning_time) < time_window
    is_afternoon_run = abs(current_time_est - afternoon_time) < time_window
    
    if is_morning_run:
        logger.info(f"This is a morning run (scheduled for {morning_time_str} EST)")
    elif is_afternoon_run:
        logger.info(f"This is an afternoon run (scheduled for {afternoon_time_str} EST)")
    else:
        logger.info("This is neither a morning nor afternoon scheduled run")
    
    browser = setup_browser()
    if not browser:
        return False
    
    try:
        # Login to Ringba
        if not login_to_ringba(browser, username, password):
            logger.error("Login failed. Aborting export.")
            return False
        
        # Navigate to call logs
        if not navigate_to_call_logs(browser):
            return False
        
        # Set date range - even if this fails, we'll continue to try clicking Export CSV
        set_date_range(browser, start_date, end_date)
        
        # Click Export CSV
        if not click_export_csv(browser):
            return False
        
        logger.info("CSV export process completed")
        
        # Find the most recently downloaded CSV file
        download_dir = os.path.abspath(os.getcwd())
        all_files = os.listdir(download_dir)
        csv_files = [f for f in all_files if f.endswith('.csv')]
        
        if csv_files:
            # Sort by creation time, newest first
            csv_files.sort(key=lambda x: os.path.getctime(os.path.join(download_dir, x)), reverse=True)
            latest_csv = csv_files[0]
            csv_path = os.path.join(download_dir, latest_csv)
            
            logger.info(f"Processing the downloaded CSV file: {csv_path}")
            result = process_csv_file(csv_path)
            
            if result:
                # Unpack the targets dataframe and column names
                targets_df, target_col, rpc_col = result
                
                # Morning run: save results for afternoon comparison
                if is_morning_run:
                    logger.info("Saving morning results for afternoon comparison")
                    save_morning_results(targets_df, target_col, rpc_col)
                    
                    # Send morning notification
                    send_results_to_slack(targets_df, target_col, rpc_col)
                
                # Afternoon run: compare with morning results
                elif is_afternoon_run:
                    logger.info("Comparing afternoon results with morning run")
                    compare_and_send_afternoon_results(targets_df, target_col, rpc_col)
                
                # Neither morning nor afternoon: just send regular notification
                else:
                    logger.info("Sending regular notification (not a scheduled run)")
                    send_results_to_slack(targets_df, target_col, rpc_col)
            else:
                logger.error("Failed to process CSV file")
        else:
            logger.warning("No CSV files found to process")
        
        return True
    except Exception as e:
        logger.error(f"Error during export: {str(e)}")
        take_screenshot(browser, "export_process_exception")
        return False
    finally:
        # For render.com deployment, don't wait for user input
        # Just close the browser automatically
        browser.quit()
        logger.info("Browser closed automatically")

if __name__ == "__main__":
    # Check arguments
    if len(sys.argv) < 3:
        # No command line arguments provided, use environment variables
        logger.info("No command line credentials provided, using .env credentials")
        username = None  # Will use environment variables in login_to_ringba
        password = None  # Will use environment variables in login_to_ringba
        start_date = None  # Will use default date
        end_date = None  # Will use default date
    else:
        # Get credentials from command line
        username = sys.argv[1]
        password = sys.argv[2]
        
        # Get date range
        start_date = sys.argv[3] if len(sys.argv) > 3 else None
        end_date = sys.argv[4] if len(sys.argv) > 4 else start_date
    
    # Run export
    export_csv(username, password, start_date, end_date) 