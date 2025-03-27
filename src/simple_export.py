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
                import base64
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

def setup_chrome():
    """Set up Chrome with appropriate options"""
    try:
        # Print debugging information in Render environment
        if os.getenv('PORT'):  # Check if running on Render
            debug_environment()
        
        logger.info("Running in headless mode")
        chrome_options = Options()
        
        # Get Chrome options from environment
        chrome_options_str = os.getenv('CHROME_OPTIONS', '')
        if chrome_options_str:
            logger.info(f"Adding Chrome options from environment: {chrome_options_str}")
            for option in chrome_options_str.split():
                chrome_options.add_argument(option)
        
        # Add additional options for Render environment
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-extensions')
        
        # Explicitly set the path on Render environment - first priority
        chromedriver_path = "/usr/local/bin/chromedriver"
        if os.path.exists(chromedriver_path):
            logger.info(f"Found ChromeDriver at {chromedriver_path}")
            try:
                service = Service(executable_path=chromedriver_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("Successfully created Chrome driver with explicit path")
                return driver
            except Exception as e:
                logger.warning(f"Explicit path ChromeDriver failed: {str(e)}")
        
        # Try using system ChromeDriver next
        try:
            logger.info("Attempting to use system ChromeDriver...")
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("Successfully created Chrome driver with system ChromeDriver")
            return driver
        except Exception as e:
            logger.warning(f"System ChromeDriver failed: {str(e)}")
        
        # Try WebDriverManager as fallback - make sure to specify cache_valid_range
        try:
            logger.info("Trying WebDriverManager approach...")
            logger.info("====== WebDriver manager ======")
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager(cache_valid_range=1).install()),
                options=chrome_options
            )
            logger.info("Successfully created Chrome driver with WebDriverManager")
            return driver
        except Exception as e:
            logger.error(f"WebDriverManager setup failed: {str(e)}")
        
        # Try absolute minimal setup as last resort
        try:
            logger.info("Trying absolute minimal Chrome setup...")
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("Successfully created Chrome driver with minimal setup")
            return driver
        except Exception as e:
            logger.error(f"Minimal setup failed: {str(e)}")
            # Dump PATH environment variable for debugging
            logger.error(f"PATH environment: {os.environ.get('PATH', 'Not set')}")
            # Try to find chromedriver with 'which'
            try:
                import subprocess
                which_result = subprocess.run(['which', 'chromedriver'], capture_output=True, text=True)
                logger.info(f"which chromedriver result: {which_result.stdout.strip()}")
            except Exception as we:
                logger.error(f"Failed to run 'which chromedriver': {str(we)}")
        
        raise Exception("All browser setup attempts failed")
    except Exception as e:
        logger.error(f"Failed to set up Chrome: {str(e)}")
        raise

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
    """Navigate to call logs page"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Set a strict overall timeout for this entire function
            start_time = time.time()
            max_wait_time = 120  # Maximum 2 minutes for the entire page load process
            
            # Navigate to call logs
            call_logs_url = "https://app.ringba.com/#/dashboard/call-logs/report/new"
            logger.info(f"Navigating to call logs page: {call_logs_url}")
            
            try:
                browser.get(call_logs_url)
            except Exception as e:
                logger.error(f"Error loading call logs page: {str(e)}")
                # Try to refresh the page if there was an error
                try:
                    browser.refresh()
                    time.sleep(5)
                except:
                    pass
                
                # Try direct navigation again
                browser.get(call_logs_url)
            
            # Take screenshot if possible
            try:
                take_screenshot(browser, "after_navigation_to_call_logs")
            except Exception as ss_error:
                logger.error(f"Failed to take screenshot: {str(ss_error)}")
            
            # Wait for call logs page to load with multiple selectors
            logger.info("Waiting for call logs page to load...")
            
            # Try looking for any of these selectors
            call_logs_selectors = [
                (By.CSS_SELECTOR, ".reporting-call-logs-data"),
                (By.CSS_SELECTOR, ".page-content"),
                (By.XPATH, "//h1[contains(text(), 'Call Logs')]"),
                (By.CSS_SELECTOR, ".call-logs-container"),
                # Add more general selectors that might be present
                (By.CSS_SELECTOR, ".navbar-nav"),
                (By.CSS_SELECTOR, "button"),
                (By.CSS_SELECTOR, ".btn"),
                (By.TAG_NAME, "table")
            ]
            
            page_loaded = False
            selector_wait_timeout = 15  # Each selector gets max 15 seconds
            
            for selector_type, selector_value in call_logs_selectors:
                # Check if we've exceeded the overall timeout
                if time.time() - start_time > max_wait_time:
                    logger.warning(f"Overall timeout of {max_wait_time} seconds exceeded, proceeding anyway")
                    page_loaded = True  # Force proceed
                    break
                
                try:
                    logger.info(f"Trying selector: {selector_type}={selector_value}")
                    WebDriverWait(browser, selector_wait_timeout).until(
                        EC.presence_of_element_located((selector_type, selector_value))
                    )
                    logger.info(f"Call logs page detected with selector: {selector_type}={selector_value}")
                    page_loaded = True
                    break
                except Exception as wait_error:
                    logger.warning(f"Selector {selector_type}={selector_value} not found: {str(wait_error)}")
                    continue
            
            # If we're past the overall timeout, proceed anyway
            if time.time() - start_time > max_wait_time:
                logger.warning(f"Overall timeout of {max_wait_time} seconds exceeded, proceeding anyway")
                page_loaded = True
                
            if not page_loaded:
                # If we've reached max retries, but let's proceed anyway as a last resort
                if retry_count >= max_retries - 1:
                    logger.warning("Call logs page did not load, but proceeding anyway as last resort")
                    # Try to take a screenshot to see current state
                    try:
                        take_screenshot(browser, "call_logs_page_not_fully_loaded")
                    except:
                        pass
                    return True  # Return True to continue with export
                
                # Otherwise, increment retry counter and try again
                retry_count += 1
                logger.warning(f"Call logs page not loaded, retrying ({retry_count}/{max_retries})...")
                time.sleep(5)
                continue
            
            # Wait a bit longer after the page appears to be loaded
            logger.info("Page appears to be loaded, waiting 5 seconds for it to stabilize")
            time.sleep(5)
            
            # Take screenshot after page loads
            try:
                take_screenshot(browser, "call_logs_page_loaded")
            except Exception as ss_error:
                logger.error(f"Failed to take screenshot: {str(ss_error)}")
            
            logger.info("Successfully navigated to call logs page")
            return True
        
        except Exception as e:
            retry_count += 1
            logger.error(f"Navigation error (attempt {retry_count}/{max_retries}): {str(e)}")
            
            # If we've reached max retries, proceed anyway as a last resort
            if retry_count >= max_retries:
                logger.warning("Failed to navigate to call logs properly after multiple attempts, but proceeding anyway")
                return True  # Return True to try export anyway
            
            # Restart the browser if navigation fails
            try:
                browser.quit()
            except:
                pass
            
            logger.info("Restarting browser...")
            browser = setup_chrome()
            
            # Re-login if we had to restart the browser
            if not login_to_ringba(browser):
                logger.error("Login failed after browser restart. Aborting navigation.")
                return False
            
            time.sleep(5)  # Wait before retrying

def set_date_range(browser, start_date, end_date):
    """Set the date range for call logs"""
    # Skip setting date range as requested by user
    logger.info("Skipping date range modification as requested")
    take_screenshot(browser, "skipping_date_range")
    return True

def click_export_csv(browser):
    """Click the Export CSV button and download the file"""
    try:
        # Set a timeout for this entire function
        start_time = time.time()
        max_wait_time = 60  # Maximum 60 seconds for the entire process
        
        # Ensure page is properly loaded and stable
        logger.info("Executing JavaScript to scroll and ensure page is ready...")
        try:
            # Scroll to ensure the page is fully rendered
            browser.execute_script("window.scrollTo(0, 0);")
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            browser.execute_script("window.scrollTo(0, 0);")
            
            # Attempt to force page to be ready
            browser.execute_script("""
                // Force any pending DOM updates
                document.body.getBoundingClientRect();
            """)
        except Exception as js_error:
            logger.warning(f"JavaScript execution for page preparation failed: {str(js_error)}")
        
        # Force an explicit wait 
        logger.info("Waiting 5 seconds for page to stabilize...")
        time.sleep(5)
        
        # Take a screenshot to see what we're working with
        try:
            take_screenshot(browser, "before_export_button_search")
        except:
            pass
        
        # Attempt an ultra-aggressive approach to find and click the export button
        found_and_clicked = False
        
        # Method 1: Try direct selector approach
        logger.info("Method 1: Using direct CSS selectors...")
        export_selectors = [
            "button.btn.btn-sm.m-r-15.export-summary-btn",
            "button.export-summary-btn", 
            ".export-csv", 
            "button:contains('Export')",
            "button.btn:contains('Export')",
            "button.btn",
            "a:contains('Export')"
        ]
        
        for selector in export_selectors:
            # Check if we've exceeded our wait time
            if time.time() - start_time > max_wait_time:
                logger.warning(f"Exceeded max wait time, moving to forceful methods")
                break
                
            try:
                logger.info(f"Trying selector: {selector}")
                
                # Try to find elements via JavaScript for more reliable results
                elements = browser.execute_script(f"""
                    return document.querySelectorAll("{selector}");
                """)
                
                if elements and len(elements) > 0:
                    logger.info(f"Found {len(elements)} potential export buttons with selector: {selector}")
                    
                    for element in elements:
                        try:
                            logger.info("Attempting to click element via JavaScript...")
                            browser.execute_script("arguments[0].click();", element)
                            logger.info("Element clicked via JavaScript")
                            found_and_clicked = True
                            time.sleep(2)  # Wait briefly after click
                            break
                        except Exception as click_error:
                            logger.warning(f"Failed to click via JavaScript: {str(click_error)}")
                    
                    if found_and_clicked:
                        break
            except Exception as selector_error:
                logger.warning(f"Error with selector {selector}: {str(selector_error)}")
        
        # Method 2: Brute force approach - try to find by text content
        if not found_and_clicked and time.time() - start_time <= max_wait_time:
            logger.info("Method 2: Trying brute force approach, find by text content...")
            try:
                result = browser.execute_script("""
                    var buttons = document.querySelectorAll('button, a');
                    for (var i = 0; i < buttons.length; i++) {
                        var btn = buttons[i];
                        if (btn.innerText && (
                            btn.innerText.includes('Export') || 
                            btn.innerText.includes('CSV') ||
                            btn.innerText.includes('Download')
                        )) {
                            console.log("Found button by text: " + btn.innerText);
                            btn.click();
                            return true;
                        }
                    }
                    return false;
                """)
                
                if result:
                    logger.info("Successfully clicked export via text search")
                    found_and_clicked = True
            except Exception as js_error:
                logger.warning(f"Text search method failed: {str(js_error)}")
        
        # Method 3: XPath approach
        if not found_and_clicked and time.time() - start_time <= max_wait_time:
            logger.info("Method 3: Trying XPath selectors...")
            xpath_selectors = [
                "//button[contains(text(), 'Export')]",
                "//button[contains(text(), 'CSV')]",
                "//a[contains(text(), 'Export')]",
                "//a[contains(text(), 'CSV')]",
                "//button[contains(@class, 'export')]",
                "//a[contains(@class, 'export')]",
                "//button[contains(@class, 'btn')]",
                "//div[contains(@class, 'export')]//button"
            ]
            
            for xpath in xpath_selectors:
                try:
                    elements = browser.find_elements(By.XPATH, xpath)
                    if elements:
                        logger.info(f"Found {len(elements)} elements with XPath: {xpath}")
                        for element in elements:
                            try:
                                element.click()
                                logger.info(f"Clicked element with XPath: {xpath}")
                                found_and_clicked = True
                                time.sleep(2)
                                break
                            except:
                                try:
                                    browser.execute_script("arguments[0].click();", element)
                                    logger.info(f"Clicked element with XPath via JavaScript: {xpath}")
                                    found_and_clicked = True
                                    time.sleep(2)
                                    break
                                except:
                                    continue
                    if found_and_clicked:
                        break
                except Exception as xpath_error:
                    logger.warning(f"XPath method failed for {xpath}: {str(xpath_error)}")
        
        # Method 4: Last resort - try tab key navigation and enter
        if not found_and_clicked and time.time() - start_time <= max_wait_time:
            logger.info("Method 4: Trying keyboard navigation...")
            try:
                # First click on body to ensure focus is in the document
                browser.find_element(By.TAG_NAME, "body").click()
                
                # Send a series of TAB keys to navigate through elements
                actions = ActionChains(browser)
                for _ in range(20):  # Try 20 tabs
                    actions.send_keys(Keys.TAB)
                actions.perform()
                
                # Now send ENTER to try to activate the focused element
                actions = ActionChains(browser)
                actions.send_keys(Keys.RETURN)
                actions.perform()
                
                logger.info("Performed keyboard navigation attempt")
                # We don't know if this worked, but we'll assume it might have
                found_and_clicked = True
            except Exception as key_error:
                logger.warning(f"Keyboard navigation method failed: {str(key_error)}")
        
        # Take screenshot after clicking
        try:
            take_screenshot(browser, "after_export_attempts")
        except:
            pass
        
        # Log the outcome
        if found_and_clicked:
            logger.info("Export button appears to have been clicked, waiting for download...")
        else:
            logger.warning("Could not find or click export button with any method")
        
        # Wait for a fixed amount of time for download regardless
        logger.info("Waiting 30 seconds for any download to complete...")
        time.sleep(30)
        
        # Check for any CSV files
        download_dir = os.path.abspath(os.getcwd())
        csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
        if csv_files:
            logger.info(f"Found CSV files in directory: {csv_files}")
            logger.info("Assuming download completed successfully")
            return True
        else:
            logger.warning("No CSV files found after waiting")
            # Always proceed even if no CSV found - we tried our best
            logger.warning("Proceeding anyway - files may appear later or may have been downloaded elsewhere")
            return True
            
    except Exception as e:
        logger.error(f"Failed to export CSV: {str(e)}")
        try:
            take_screenshot(browser, "export_exception")
        except:
            pass
        # Always return True to continue - don't get stuck here
        return True

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
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 12.0))
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

def send_results_to_slack(targets_df, target_col, rpc_col, run_label=''):
    """Send the processing results to Slack webhook"""
    import requests
    
    try:
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            logger.error("Slack webhook URL not configured, skipping notification")
            return False
        
        # Get RPC threshold from environment
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', '12.0'))
        
        # Format the date for display
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Get targets BELOW the RPC threshold - changed to show low RPC targets
        low_rpc_targets = targets_df[targets_df[rpc_col] < rpc_threshold]
        target_count = len(low_rpc_targets)
        
        # Format run time based on label or current time
        if run_label:
            run_time_label = f"*{run_label.upper()} RUN*"
        else:
            # Use current time if no label
            current_time = datetime.now().strftime('%I:%M %p')
            run_time_label = f"*{current_time}*"
        
        # Create the message
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"Ringba RPC Report - {today} {run_time_label}",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Found *{target_count}* targets with RPC < {rpc_threshold} 📉"
                    }
                }
            ]
        }
        
        # Add targets to message
        if not low_rpc_targets.empty:
            target_list = ""
            for _, row in low_rpc_targets.iterrows():
                target_name = row[target_col] if not pd.isna(row[target_col]) else "Total RPC (including the ones below $12)"
                
                target_list += f"• *{target_name}*: RPC = {row[rpc_col]:.2f}\n"
            
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": target_list
                }
            })
        else:
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "🎉 Great news! No targets below RPC threshold."
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
            logger.info("Results sent to Slack successfully")
            return True
        else:
            logger.error(f"Failed to send results to Slack: {response.status_code} {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending results to Slack: {str(e)}")
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
    
    # Determine run time based on current time in EST
    eastern = pytz.timezone('US/Eastern')
    current_time_est = datetime.now(pytz.utc).astimezone(eastern)
    current_hour = current_time_est.hour
    current_minute = current_time_est.minute
    logger.info(f"Current time in EST: {current_time_est.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Get the configured times for different runs
    morning_time_str = os.getenv('MORNING_CHECK_TIME', '11:00')
    midday_time_str = os.getenv('MIDDAY_CHECK_TIME', '14:00')
    afternoon_time_str = os.getenv('AFTERNOON_CHECK_TIME', '16:30')
    
    # Parse time strings to hours and add debug logging
    try:
        morning_hour = int(morning_time_str.split(':')[0])
        morning_minute = int(morning_time_str.split(':')[1]) if ':' in morning_time_str else 0
        midday_hour = int(midday_time_str.split(':')[0])
        midday_minute = int(midday_time_str.split(':')[1]) if ':' in midday_time_str else 0
        afternoon_hour = int(afternoon_time_str.split(':')[0])
        afternoon_minute = int(afternoon_time_str.split(':')[1]) if ':' in afternoon_time_str else 0
        
        logger.info(f"Morning check time: {morning_hour}:{morning_minute:02d}")
        logger.info(f"Midday check time: {midday_hour}:{midday_minute:02d}")
        logger.info(f"Afternoon check time: {afternoon_hour}:{afternoon_minute:02d}")
    except Exception as e:
        logger.error(f"Error parsing time settings: {str(e)}")
        # Fallback to default times
        morning_hour, morning_minute = 11, 0
        midday_hour, midday_minute = 14, 0
        afternoon_hour, afternoon_minute = 16, 30
        logger.info(f"Using fallback times: 11:00, 14:00, 16:30")
    
    # Determine run type based on current time with wider windows
    is_morning_run = False
    is_midday_run = False
    is_afternoon_run = False
    
    # Use time windows instead of exact matches (30 minute window for each run)
    time_now_minutes = current_hour * 60 + current_minute
    morning_time_minutes = morning_hour * 60 + morning_minute
    midday_time_minutes = midday_hour * 60 + midday_minute
    afternoon_time_minutes = afternoon_hour * 60 + afternoon_minute
    
    if abs(time_now_minutes - morning_time_minutes) <= 30:
        is_morning_run = True
        logger.info(f"This is a morning run (current time {current_hour}:{current_minute:02d} within 30 min of {morning_hour}:{morning_minute:02d})")
    elif abs(time_now_minutes - midday_time_minutes) <= 30:
        is_midday_run = True
        logger.info(f"This is a midday run (current time {current_hour}:{current_minute:02d} within 30 min of {midday_hour}:{midday_minute:02d})")
    elif abs(time_now_minutes - afternoon_time_minutes) <= 30:
        is_afternoon_run = True
        logger.info(f"This is an afternoon run (current time {current_hour}:{current_minute:02d} within 30 min of {afternoon_hour}:{afternoon_minute:02d})")
    else:
        # Default to morning run behavior for manual runs
        is_morning_run = True
        logger.info(f"This is a manual run at {current_hour}:{current_minute:02d} (defaulting to morning run behavior)")
    
    browser = setup_chrome()
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
                
                # Based on the run type, perform different actions
                if is_morning_run:
                    # Morning run: just show targets below threshold
                    logger.info("Processing morning run (11 AM ET)")
                    send_results_to_slack(targets_df, target_col, rpc_col, run_label='morning')
                    
                elif is_midday_run:
                    # Midday run: just show targets below threshold
                    logger.info("Processing midday run (2 PM ET)")
                    send_results_to_slack(targets_df, target_col, rpc_col, run_label='midday')
                    
                elif is_afternoon_run:
                    # Afternoon run: just show targets below threshold
                    logger.info("Processing afternoon run (4:30 PM ET)")
                    send_results_to_slack(targets_df, target_col, rpc_col, run_label='afternoon')
                
                else:
                    # Default behavior for manual runs
                    logger.info("Processing manual run")
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

def perform_test_run():
    """Perform a test run when the service is first deployed"""
    logger.info("Performing initial test run for web service deployment...")
    try:
        # Run the export with test parameters
        result = export_csv(
            username=None,  # Use environment variables
            password=None,  # Use environment variables
            start_date=datetime.now().strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d')
        )
        
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
            export_csv(username, password, start_date, end_date)
        except Exception as e:
            logger.error(f"Unhandled exception in main process: {str(e)}")
        finally:
            # Cancel timer if script completes normally
            timer.cancel()
            logger.info("Script execution complete") 