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
# Import Chrome setup module to ensure Chrome is installed
try:
    from src import setup_chrome
except ImportError:
    try:
        import setup_chrome
    except ImportError:
        print("Warning: Could not import setup_chrome module")

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
import schedule

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
        # Find Chrome and ChromeDriver in PATH or HOME directory
        chrome_path = None
        chromedriver_path = None
        
        # Check HOME/bin first (Render.com setup)
        home_bin = os.path.join(os.environ.get('HOME', ''), 'bin')
        home_chrome = os.path.join(os.environ.get('HOME', ''), 'chrome', 'chrome')
        
        if os.path.exists(os.path.join(home_bin, 'google-chrome')):
            chrome_path = os.path.join(home_bin, 'google-chrome')
            logger.info(f"Using Chrome from HOME/bin: {chrome_path}")
        elif os.path.exists(home_chrome):
            chrome_path = home_chrome
            logger.info(f"Using Chrome from HOME/chrome: {chrome_path}")
            
        if os.path.exists(os.path.join(home_bin, 'chromedriver')):
            chromedriver_path = os.path.join(home_bin, 'chromedriver')
            logger.info(f"Using ChromeDriver from HOME/bin: {chromedriver_path}")
            
        # Create Chrome options with MINIMAL configuration
        chrome_options = webdriver.ChromeOptions()
        
        # Set Chrome binary location if found
        if chrome_path:
            chrome_options.binary_location = chrome_path
            
        # Essential options only - stripped down to bare minimum
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        
        # Use smaller window and memory footprint
        chrome_options.add_argument("--window-size=800,600")
        chrome_options.add_argument("--single-process")
        
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
        logger.info(f"Setting up Chrome with options: {chrome_options.arguments}")
        
        # Create the browser
        if chromedriver_path:
            logger.info(f"Using ChromeDriver path: {chromedriver_path}")
            service = Service(executable_path=chromedriver_path)
            browser = webdriver.Chrome(service=service, options=chrome_options)
        else:
            logger.info("Using system ChromeDriver")
            browser = webdriver.Chrome(options=chrome_options)
        
        # Set timeouts
        browser.set_page_load_timeout(60)
        browser.implicitly_wait(10)
        
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
    """Directly scrape the table data from the page instead of exporting CSV"""
    try:
        # Navigate directly to the summary page
        logger.info("Navigating directly to call summary report...")
        try:
            browser.get("https://app.ringba.com/#/dashboard/call-logs/report/summary")
            logger.info("Waiting for summary page to load...")
            time.sleep(20)  # Give page more time to load
        except Exception as e:
            logger.error(f"Failed to navigate to summary page: {str(e)}")
            take_screenshot(browser, "navigation_failed")
        
        # Take screenshot to see page state
        take_screenshot(browser, "before_table_extraction")
        
        # NEW ENHANCED VERSION: Directly extract from the table visible in the screenshot
        logger.info("Extracting data directly from the visible Ringba summary table...")
        table_data = browser.execute_script("""
            console.log('Starting enhanced Ringba table extraction...');
            
            // APPROACH 1: Target the specific summary table at the bottom of the page
            function extractRingbaSummaryTable() {
                console.log('Attempting to extract from Ringba summary table');
                
                // First look for the table headers we can see in the screenshot
                // Based on the visible headers in the screenshot: Campaign, Publisher, Target, Buyer, etc.
                const TARGET_COLUMNS = ['Campaign', 'Publisher', 'Target', 'Buyer', 'Dialed #', 'Number Pool', 
                                      'Date', 'Duplicate', 'Tags', 'RPC', 'Revenue', 'Payout'];
                
                // Check if we can find the header row
                const headerElements = Array.from(document.querySelectorAll('th, [role="columnheader"]'));
                console.log(`Found ${headerElements.length} potential header elements`);
                
                // Find header elements that match our target columns
                const foundHeaders = headerElements.filter(el => {
                    const text = el.textContent.trim().replace(/▼|▲|↓|↑/g, '').trim().toLowerCase();
                    return TARGET_COLUMNS.some(col => col.toLowerCase() === text);
                });
                
                console.log(`Found ${foundHeaders.length} matching header elements`);
                
                if (foundHeaders.length > 0) {
                    // Find the table containing these headers
                    const tableElement = foundHeaders[0].closest('table, [role="grid"], [role="table"]');
                    
                    if (tableElement) {
                        console.log('Found table containing target headers');
                        
                        // Extract the header texts
                        const headerRow = tableElement.querySelector('thead tr, [role="row"]') || 
                                          tableElement.querySelector('tr:first-child');
                        
                        const headerCells = headerRow ? 
                            headerRow.querySelectorAll('th, [role="columnheader"]') : 
                            foundHeaders;
                        
                        const headers = Array.from(headerCells).map(cell => 
                            cell.textContent.trim().replace(/▼|▲|↓|↑/g, '').trim());
                        
                        console.log('Extracted headers:', headers);
                        
                        // Find all data rows
                        const dataRows = tableElement.querySelectorAll('tbody tr, [role="row"]:not([role="columnheader"])');
                        console.log(`Found ${dataRows.length} data rows`);
                        
                        // Extract data from rows
                        const rows = [];
                        dataRows.forEach(row => {
                            // Skip header rows
                            if (row.querySelector('th') || row.closest('thead')) return;
                            
                            const cells = row.querySelectorAll('td, [role="cell"]');
                            if (cells.length === 0) return;
                            
                            const rowData = {};
                            for (let i = 0; i < Math.min(cells.length, headers.length); i++) {
                                rowData[headers[i]] = cells[i].textContent.trim();
                            }
                            
                            // Only include rows with meaningful data (not empty, not header repeats)
                            const hasData = Object.values(rowData).some(val => 
                                val && !headers.includes(val) && val !== 'Target' && val !== 'RPC');
                                
                            if (hasData) {
                                rows.push(rowData);
                            }
                        });
                        
                        console.log(`Extracted ${rows.length} data rows with content`);
                        return { headers, rows };
                    }
                }
                
                return null;
            }
            
            // APPROACH 2: Target the specific section visible in the screenshot (Summary section)
            function extractFromSummarySection() {
                console.log('Looking for Summary section...');
                
                // Look for the Summary heading or section
                const summarySection = document.querySelector('.summary, #summary, [data-test="summary"]');
                const summaryHeading = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'))
                    .find(el => el.textContent.trim() === 'Summary');
                
                const summaryContext = summarySection || 
                                      (summaryHeading && summaryHeading.parentElement) || 
                                      document.querySelector('[id*="summary"], [class*="summary"]');
                
                if (summaryContext) {
                    console.log('Found Summary section, looking for table within it');
                    
                    // Find table inside summary section
                    const tableElement = summaryContext.querySelector('table, [role="grid"], [role="table"]');
                    
                    if (tableElement) {
                        console.log('Found table in Summary section');
                        
                        // Extract headers
                        const headers = [];
                        const headerElements = tableElement.querySelectorAll('th, [role="columnheader"]');
                        
                        headerElements.forEach(el => {
                            headers.push(el.textContent.trim().replace(/▼|▲|↓|↑/g, '').trim());
                        });
                        
                        console.log('Found headers:', headers);
                        
                        // Extract data rows
                        const rows = [];
                        const dataRows = tableElement.querySelectorAll('tbody tr, [role="row"]:not([role="columnheader"])');
                        
                        dataRows.forEach(row => {
                            const cells = row.querySelectorAll('td, [role="cell"]');
                            if (cells.length === 0) return;
                            
                            const rowData = {};
                            for (let i = 0; i < Math.min(cells.length, headers.length); i++) {
                                rowData[headers[i]] = cells[i].textContent.trim();
                            }
                            
                            rows.push(rowData);
                        });
                        
                        console.log(`Extracted ${rows.length} rows from Summary section table`);
                        return { headers, rows };
                    }
                }
                
                return null;
            }
            
            // APPROACH 3: Search for any table containing the key columns (Target, RPC)
            function findTableWithTargetAndRPC() {
                console.log('Searching for any table with Target and RPC columns...');
                
                const tables = document.querySelectorAll('table, [role="grid"], [role="table"]');
                console.log(`Found ${tables.length} potential tables on page`);
                
                // Process each table
                for (const table of tables) {
                    // Get header elements
                    const headerElements = table.querySelectorAll('th, [role="columnheader"]');
                    if (headerElements.length === 0) continue;
                    
                    // Extract header texts
                    const headers = Array.from(headerElements).map(el => 
                        el.textContent.trim().replace(/▼|▲|↓|↑/g, '').trim());
                    
                    // Check if this table has both Target and RPC columns
                    const hasTarget = headers.some(h => h === 'Target' || h === 'target');
                    const hasRPC = headers.some(h => h === 'RPC' || h === 'rpc' || h.includes('Revenue'));
                    
                    if (hasTarget && hasRPC) {
                        console.log('Found table with both Target and RPC columns');
                        
                        // Extract data rows
                        const rows = [];
                        const dataRows = table.querySelectorAll('tbody tr, [role="row"]:not([role="columnheader"])');
                        
                        dataRows.forEach(row => {
                            const cells = row.querySelectorAll('td, [role="cell"]');
                            if (cells.length === 0) return;
                            
                            const rowData = {};
                            for (let i = 0; i < Math.min(cells.length, headers.length); i++) {
                                rowData[headers[i]] = cells[i].textContent.trim();
                            }
                            
                            rows.push(rowData);
                        });
                        
                        console.log(`Extracted ${rows.length} rows from table with Target and RPC`);
                        return { headers, rows };
                    }
                }
                
                return null;
            }
            
            // APPROACH 4: Position-based extraction for complex Angular/React tables
            function extractByPosition() {
                console.log('Attempting position-based extraction for complex tables...');
                
                // First try to find the Target and RPC column headers
                const columnHeaders = Array.from(document.querySelectorAll('div, span, th, [role="columnheader"]'))
                    .filter(el => {
                        const text = el.textContent.trim().toLowerCase();
                        return text === 'target' || text === 'rpc' || text === 'campaign' || 
                               text === 'revenue' || text === 'publisher';
                    });
                
                if (columnHeaders.length >= 2) {
                    console.log(`Found ${columnHeaders.length} column headers including Target/RPC`);
                    
                    // Get the positions of these headers
                    const headerPositions = columnHeaders.map(el => {
                        const rect = el.getBoundingClientRect();
                        return {
                            element: el,
                            text: el.textContent.trim(),
                            x: rect.left,
                            y: rect.top,
                            width: rect.width,
                            bottom: rect.bottom
                        };
                    });
                    
                    // Sort headers by Y position to group headers in the same row
                    const headersByRow = {};
                    headerPositions.forEach(pos => {
                        // Round Y position to group headers in the same row
                        const rowY = Math.round(pos.y / 5) * 5;
                        if (!headersByRow[rowY]) {
                            headersByRow[rowY] = [];
                        }
                        headersByRow[rowY].push(pos);
                    });
                    
                    // Use the row with the most column headers
                    let bestHeaderRow = null;
                    let maxHeaders = 0;
                    
                    Object.entries(headersByRow).forEach(([rowY, headers]) => {
                        if (headers.length > maxHeaders) {
                            maxHeaders = headers.length;
                            bestHeaderRow = headers;
                        }
                    });
                    
                    if (bestHeaderRow) {
                        // Sort headers by X position (left to right)
                        bestHeaderRow.sort((a, b) => a.x - b.x);
                        
                        // Extract header names
                        const headers = bestHeaderRow.map(h => h.text);
                        console.log('Found headers by position:', headers);
                        
                        // Get the Y position below the header row where data starts
                        const dataStartY = Math.max(...bestHeaderRow.map(h => h.bottom)) + 5;
                        
                        // Find all text elements that could be cell data
                        const allCellTexts = Array.from(document.querySelectorAll('div, span'))
                            .filter(el => {
                                // Skip elements with children (containers)
                                if (el.children.length > 0) return false;
                                
                                // Must have text content
                                if (!el.textContent.trim()) return false;
                                
                                // Get position
                                const rect = el.getBoundingClientRect();
                                
                                // Must be below the headers
                                return rect.top >= dataStartY;
                            })
                            .map(el => {
                                const rect = el.getBoundingClientRect();
                                return {
                                    element: el,
                                    text: el.textContent.trim(),
                                    x: rect.left,
                                    y: rect.top
                                };
                            });
                        
                        // Group cells by row position
                        const cellsByRow = {};
                        allCellTexts.forEach(cell => {
                            // Round Y position to group cells in the same row
                            const rowY = Math.round(cell.y / 5) * 5;
                            if (!cellsByRow[rowY]) {
                                cellsByRow[rowY] = [];
                            }
                            cellsByRow[rowY].push(cell);
                        });
                        
                        // Process each row to create data records
                        const rows = [];
                        Object.entries(cellsByRow).forEach(([rowY, cells]) => {
                            // Skip if this might be the header row
                            if (Math.abs(parseInt(rowY) - Math.round(bestHeaderRow[0].y / 5) * 5) < 10) {
                                return;
                            }
                            
                            // Sort cells by X position
                            cells.sort((a, b) => a.x - b.x);
                            
                            // Map cells to headers based on X position
                            const rowData = {};
                            bestHeaderRow.forEach((header, index) => {
                                // Find the cell that best matches this header's X position
                                const nearestCells = cells.filter(cell => 
                                    Math.abs(cell.x - header.x) < header.width * 0.8);
                                
                                if (nearestCells.length > 0) {
                                    // Use the nearest cell by X position
                                    nearestCells.sort((a, b) => 
                                        Math.abs(a.x - header.x) - Math.abs(b.x - header.x));
                                    
                                    rowData[header.text] = nearestCells[0].text;
                                }
                            });
                            
                            // Only include rows with reasonable data
                            if (Object.keys(rowData).length >= 2) {
                                rows.push(rowData);
                            }
                        });
                        
                        console.log(`Extracted ${rows.length} rows using position-based approach`);
                        return { headers, rows };
                    }
                }
                
                return null;
            }
            
            // APPROACH 5: Direct DOM scraping for dollar values and labels
            function extractDollarValuesAndLabels() {
                console.log('Extracting dollar values and their labels directly...');
                
                // Find all elements with $ sign that might be RPC values
                const dollarElements = Array.from(document.querySelectorAll('*'))
                    .filter(el => {
                        // Skip containers
                        if (el.children.length > 0) return false;
                        
                        const text = el.textContent.trim();
                        // Must start with $ and look like a currency value
                        return text.startsWith('$') && /\\$\\d+(\\.\\d+)?/.test(text);
                    });
                
                console.log(`Found ${dollarElements.length} dollar value elements`);
                
                // Find the nearest label for each dollar value
                const results = [];
                dollarElements.forEach(dollarEl => {
                    const dollarRect = dollarEl.getBoundingClientRect();
                    const dollarValue = dollarEl.textContent.trim();
                    
                    // Find all elements that could be labels
                    const potentialLabels = Array.from(document.querySelectorAll('*'))
                        .filter(el => {
                            // Skip containers
                            if (el.children.length > 0) return false;
                            
                            // Skip dollars
                            if (el.textContent.trim().startsWith('$')) return false;
                            
                            // Must have text content
                            const text = el.textContent.trim();
                            if (!text || text.length < 2) return false;
                            
                            // Position relative to dollar value
                            const rect = el.getBoundingClientRect();
                            
                            // Either same row (to the left) or row above
                            const sameRow = Math.abs(rect.top - dollarRect.top) < 10 && rect.left < dollarRect.left;
                            const rowAbove = dollarRect.top - rect.bottom < 30 && dollarRect.top - rect.bottom > 5 &&
                                          Math.abs(rect.left - dollarRect.left) < 50;
                            
                            return sameRow || rowAbove;
                        });
                    
                    if (potentialLabels.length > 0) {
                        // Sort by distance (prefer same row, then closest)
                        potentialLabels.sort((a, b) => {
                            const aRect = a.getBoundingClientRect();
                            const bRect = b.getBoundingClientRect();
                            
                            // Same row has priority
                            const aOnSameRow = Math.abs(aRect.top - dollarRect.top) < 10;
                            const bOnSameRow = Math.abs(bRect.top - dollarRect.top) < 10;
                            
                            if (aOnSameRow && !bOnSameRow) return -1;
                            if (!aOnSameRow && bOnSameRow) return 1;
                            
                            // Both on same row - compare horizontal distance
                            if (aOnSameRow && bOnSameRow) {
                                return (dollarRect.left - aRect.right) - (dollarRect.left - bRect.right);
                            }
                            
                            // Both on different rows - compare vertical then horizontal distance
                            const aVertDist = dollarRect.top - aRect.bottom;
                            const bVertDist = dollarRect.top - bRect.bottom;
                            
                            if (Math.abs(aVertDist - bVertDist) > 10) {
                                return aVertDist - bVertDist;
                            }
                            
                            return Math.abs(aRect.left - dollarRect.left) - Math.abs(bRect.left - dollarRect.left);
                        });
                        
                        const bestLabel = potentialLabels[0];
                        results.push({
                            Target: bestLabel.textContent.trim(),
                            RPC: dollarValue
                        });
                    }
                });
                
                console.log(`Constructed ${results.length} Target/RPC pairs`);
                return { 
                    headers: ['Target', 'RPC'],
                    rows: results 
                };
            }
            
            // Try each approach in order
            let result = extractRingbaSummaryTable();
            
            if (!result || !result.rows || result.rows.length === 0) {
                console.log('First approach failed, trying Summary section approach...');
                result = extractFromSummarySection();
            }
            
            if (!result || !result.rows || result.rows.length === 0) {
                console.log('Second approach failed, searching for Target/RPC table...');
                result = findTableWithTargetAndRPC();
            }
            
            if (!result || !result.rows || result.rows.length === 0) {
                console.log('Third approach failed, trying position-based extraction...');
                result = extractByPosition();
            }
            
            if (!result || !result.rows || result.rows.length === 0) {
                console.log('Fourth approach failed, extracting dollar values directly...');
                result = extractDollarValuesAndLabels();
            }
            
            // Extract relevant columns only (Target and RPC)
            if (result && result.rows && result.rows.length > 0) {
                // Check which columns are available
                const sampleRow = result.rows[0];
                
                let targetColumn = null;
                let rpcColumn = null;
                
                // Find Target column
                if ('Target' in sampleRow) targetColumn = 'Target';
                else if ('target' in sampleRow) targetColumn = 'target';
                else {
                    // Look for columns containing "target" (case insensitive)
                    for (const col in sampleRow) {
                        if (col.toLowerCase().includes('target')) {
                            targetColumn = col;
                            break;
                        }
                    }
                }
                
                // Find RPC column
                if ('RPC' in sampleRow) rpcColumn = 'RPC';
                else if ('rpc' in sampleRow) rpcColumn = 'rpc';
                else {
                    // Look for columns with $ values
                    for (const col in sampleRow) {
                        if (typeof sampleRow[col] === 'string' && sampleRow[col].includes('$')) {
                            rpcColumn = col;
                            break;
                        }
                    }
                    
                    // Look for columns containing "rpc" or "revenue" (case insensitive)
                    if (!rpcColumn) {
                        for (const col in sampleRow) {
                            if (col.toLowerCase().includes('rpc') || col.toLowerCase().includes('revenue')) {
                                rpcColumn = col;
                                break;
                            }
                        }
                    }
                }
                
                // If we found both columns, extract only those
                if (targetColumn && rpcColumn) {
                    console.log(`Extracting from columns: Target=${targetColumn}, RPC=${rpcColumn}`);
                    
                    const simplifiedRows = result.rows.map(row => ({
                        Target: row[targetColumn],
                        RPC: row[rpcColumn]
                    }));
                    
                    return {
                        headers: ['Target', 'RPC'],
                        rows: simplifiedRows
                    };
                }
            }
            
            return result || { headers: [], rows: [] };
        """)
        
        # Check if we got data from the scraping
        if table_data and 'rows' in table_data and table_data['rows']:
            logger.info(f"Successfully extracted {len(table_data['rows'])} rows directly from page")
            
            # Log the headers we found
            if 'headers' in table_data:
                logger.info(f"Extracted headers: {table_data['headers']}")
            
            # Convert to DataFrame
            df = pd.DataFrame(table_data['rows'])
            
            # Save to CSV file
            file_path = os.path.join("/tmp", f"table_extract_{int(time.time())}.csv")
            df.to_csv(file_path, index=False)
            
            logger.info(f"Saved extracted table data to {file_path}")
            return file_path
        else:
            # Take a screenshot to see what's on the page
            take_screenshot(browser, "table_extraction_failed")
            logger.warning("No data found in tables on the page")
            
            # Last resort: Try to scrape any text that looks like Target and RPC data
            logger.info("Trying last resort extraction of any Target/RPC-like data...")
            
            target_rpc_data = browser.execute_script("""
                // Find all elements that might contain RPC values (dollar amounts)
                const dollarElements = Array.from(document.querySelectorAll('*'))
                    .filter(el => {
                        if (el.children.length > 0) return false;
                        const text = el.textContent.trim();
                        return text.includes('$') && text.length < 20;
                    });
                
                console.log(`Found ${dollarElements.length} potential dollar amount elements`);
                
                // Function to find nearest text element that could be a target name
                function findNearestText(element) {
                    const rect = element.getBoundingClientRect();
                    
                    // Look for elements to the left or above
                    const candidates = Array.from(document.querySelectorAll('*'))
                        .filter(el => {
                            if (el.children.length > 0) return false;
                            const elRect = el.getBoundingClientRect();
                            const text = el.textContent.trim();
                            
                            // Skip if it's a dollar amount itself
                            if (text.includes('$')) return false;
                            
                            // Skip if text is too short or too long
                            if (text.length < 2 || text.length > 50) return false;
                            
                            // Check if it's to the left of the dollar amount (same row)
                            const sameRow = Math.abs(elRect.y - rect.y) < 20 && elRect.x < rect.x;
                            
                            // Or check if it's in the row above and aligned
                            const rowAbove = (rect.y - elRect.y) > 20 && (rect.y - elRect.y) < 60 && 
                                             Math.abs(elRect.x - rect.x) < 100;
                            
                            return sameRow || rowAbove;
                        });
                    
                    if (candidates.length === 0) return null;
                    
                    // Sort by horizontal distance (for same row) or by vertical distance (for row above)
                    candidates.sort((a, b) => {
                        const aRect = a.getBoundingClientRect();
                        const bRect = b.getBoundingClientRect();
                        
                        // Same row - sort by x distance
                        if (Math.abs(aRect.y - rect.y) < 20 && Math.abs(bRect.y - rect.y) < 20) {
                            return (rect.x - aRect.x) - (rect.x - bRect.x);
                        }
                        
                        // Different rows - sort by y distance
                        return (rect.y - aRect.y) - (rect.y - bRect.y);
                    });
                    
                    return candidates[0];
                }
                
                // Extract RPC and corresponding Target names
                const results = [];
                dollarElements.forEach(element => {
                    const rpcText = element.textContent.trim();
                    
                    // Verify this looks like an RPC value
                    if (!/\\$\\d+(\\.\\d+)?/.test(rpcText)) return;
                    
                    const targetElement = findNearestText(element);
                    if (targetElement) {
                        results.push({
                            Target: targetElement.textContent.trim(),
                            RPC: rpcText
                        });
                    }
                });
                
                return results;
            """)
            
            if target_rpc_data and len(target_rpc_data) > 0:
                logger.info(f"Found {len(target_rpc_data)} potential Target/RPC pairs using last resort method")
                
                # Convert to DataFrame
                df = pd.DataFrame(target_rpc_data)
                
                # Save to CSV file
                file_path = os.path.join("/tmp", f"text_extract_{int(time.time())}.csv")
                df.to_csv(file_path, index=False)
                
                logger.info(f"Saved extracted text data to {file_path}")
                return file_path
            
            # If all extraction methods fail
            logger.error("All extraction methods failed to find data")
            return None
            
    except Exception as e:
        logger.error(f"Error extracting table data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        take_screenshot(browser, "extraction_error")
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
        
        # Log the columns and first few rows for debugging
        logger.info(f"CSV columns: {', '.join(df.columns)}")
        logger.info(f"Found {len(df)} rows in the CSV file")
        
        # Show sample of the data
        if not df.empty:
            sample_rows = min(2, len(df))
            sample_df = df.head(sample_rows)
            for i, row in sample_df.iterrows():
                logger.info(f"Sample row {i+1}: {dict(row)}")
        
        # Look for relevant columns
        target_col = None
        rpc_col = None
        
        # Try different potential column names for target
        for col in ['Target', 'Target Name', 'TargetName', 'Campaign', 'Target Campaign']:
            if col in df.columns:
                target_col = col
                break
                
        # If Target column not found, check for columns matching the word "Target"
        if not target_col:
            target_cols = [col for col in df.columns if 'target' in col.lower()]
            if target_cols:
                target_col = target_cols[0]
                logger.info(f"Using column with 'target' in name: {target_col}")
                
        # Try different potential column names for RPC
        for col in ['RPC', 'Avg. Revenue per Call', 'Revenue Per Call', 'Revenue per Call', 'RPCall', 'Revenue']:
            if col in df.columns:
                rpc_col = col
                break
                
        # If RPC column not found, look for columns with $ values
        if not rpc_col:
            # Check each column for dollar sign patterns
            for col in df.columns:
                # Sample the column to see if it contains dollar amounts
                sample_values = df[col].astype(str).str.contains('\$').sum()
                if sample_values > 0:
                    rpc_col = col
                    logger.info(f"Using column with $ values as RPC: {rpc_col}")
                    break
                    
        # SPECIAL CASE: If we're using direct extraction method and columns aren't found
        # Handle the generic Column1, Column2, etc. naming convention
        if not target_col and any(col.startswith('Column') for col in df.columns):
            logger.info("Using direct extraction column mapping")
            
            # Try to identify which column might be Target based on values
            # Look for columns containing values like "Target", "Live", "Completed"
            for col in df.columns:
                values = df[col].astype(str).str.lower()
                if values.str.contains('target|live|completed|ivr').any():
                    target_col = col
                    logger.info(f"Identified Target column as {col} based on values")
                    break
                    
            # If we still don't have Target column, use Column3 which matches position in the table
            if not target_col and 'Column3' in df.columns:
                target_col = 'Column3'  # Typically third column is Target in Ringba UI
                logger.info("Using Column3 as Target column based on position")
                
            # Try to identify RPC column by looking for $ values
            if not rpc_col:
                for col in df.columns:
                    sample = df[col].astype(str)
                    if sample.str.contains('\$').any():
                        rpc_col = col
                        logger.info(f"Identified RPC column as {col} based on $ values")
                        break
                        
            # If still no RPC column, Column10 is often RPC in Ringba UI
            if not rpc_col and 'Column10' in df.columns:
                rpc_col = 'Column10'
                logger.info("Using Column10 as RPC column based on position")
                
        # If we couldn't find the target column at all, log and try to continue
        if not target_col:
            # As a last resort, pick a column that looks like it has names
            for col in df.columns:
                # Check if column has string values that could be target names
                if df[col].dtype == 'object' and df[col].astype(str).str.len().mean() > 3:
                    target_col = col
                    logger.info(f"Using {col} as Target column based on string content")
                    break
                    
            if not target_col and len(df.columns) >= 3:
                target_col = df.columns[2]  # Use the third column by position
                logger.info(f"Last resort: Using {target_col} as Target column")
            elif not target_col and len(df.columns) > 0:
                target_col = df.columns[0]  # Use the first column as a last resort
                logger.info(f"Absolute last resort: Using {target_col} as Target column")
            else:
                logger.error("Could not find Target column in CSV")
                return None
                
        # Same logic for RPC column
        if not rpc_col:
            # As a last resort, pick a numeric column that could be RPC
            for col in df.columns:
                try:
                    # Check if column can be converted to numeric
                    test_numeric = pd.to_numeric(df[col], errors='coerce')
                    if test_numeric.notna().sum() > 0:
                        rpc_col = col
                        logger.info(f"Using {col} as RPC column based on numeric content")
                        break
                except:
                    continue
                    
            if not rpc_col and len(df.columns) >= 10:
                rpc_col = df.columns[9]  # Use the tenth column by position (typical for RPC)
                logger.info(f"Last resort: Using {rpc_col} as RPC column")
            elif not rpc_col and len(df.columns) > 1:
                rpc_col = df.columns[1]  # Use the second column as a last resort
                logger.info(f"Absolute last resort: Using {rpc_col} as RPC column")
            else:
                logger.error("Could not find RPC column in CSV")
                return None
            
        logger.info(f"Using columns: Target={target_col}, RPC={rpc_col}")
        
        # Print sample of RPC values to debug
        if not df.empty:
            logger.info(f"Sample RPC values: {df[rpc_col].head(3).tolist()}")
        
        # Convert RPC column to numeric, handling currency symbols and commas
        try:
            # First try a straightforward approach
            df[rpc_col] = df[rpc_col].replace('[\$,]', '', regex=True).astype(float)
        except:
            # If that fails, try more careful conversion
            try:
                # Convert to string first
                df[rpc_col] = df[rpc_col].astype(str)
                # Remove currency symbols and commas
                df[rpc_col] = df[rpc_col].str.replace('[\$,£€]', '', regex=True)
                # Convert to float, treating errors as NaN
                df[rpc_col] = pd.to_numeric(df[rpc_col], errors='coerce')
                # Log NaN counts to debug conversion issues
                nan_count = df[rpc_col].isna().sum()
                if nan_count > 0:
                    logger.warning(f"Found {nan_count} NaN values in RPC column after conversion")
                # Drop rows where conversion failed
                df = df.dropna(subset=[rpc_col])
                logger.info(f"Converted {rpc_col} to numeric with {len(df)} valid rows")
            except Exception as e:
                logger.error(f"Could not convert RPC column to numeric: {str(e)}")
                return None
        
        # Print converted RPC values
        if not df.empty:
            logger.info(f"Converted RPC values: {df[rpc_col].head(3).tolist()}")
            logger.info(f"RPC column min: {df[rpc_col].min()}, max: {df[rpc_col].max()}, mean: {df[rpc_col].mean()}")
        
        # Get threshold from environment variable or use default
        rpc_threshold = float(os.getenv('RPC_THRESHOLD', 12.0))
        logger.info(f"Using RPC threshold of ${rpc_threshold}")
        
        # Filter for targets below the threshold
        low_rpc_targets = df[df[rpc_col] < rpc_threshold][[target_col, rpc_col]].copy()
        
        # Log threshold comparison
        logger.info(f"Found {len(low_rpc_targets)} targets with RPC < ${rpc_threshold}")
        logger.info(f"Found {len(df[df[rpc_col] >= rpc_threshold])} targets with RPC >= ${rpc_threshold}")
        
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
            logger.info("No low RPC targets found")
            return None
            
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
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

def main():
    """Main function that runs the export process based on schedule or command-line arguments"""
    try:
        # Get run_label from environment variable or command line argument
        run_label = os.getenv('RUN_LABEL', '').lower() or (sys.argv[1].lower() if len(sys.argv) > 1 else '')
        
        # Set up times for scheduled checks
        morning_check_time = os.getenv('MORNING_CHECK_TIME', '11:00')
        midday_check_time = os.getenv('MIDDAY_CHECK_TIME', '14:00')
        afternoon_check_time = os.getenv('AFTERNOON_CHECK_TIME', '16:30')
        
        logger.info(f"Scheduled check times: Morning={morning_check_time}, Midday={midday_check_time}, Afternoon={afternoon_check_time}")
        
        # If command-line argument or env var specifies a specific run, do it immediately
        if run_label in ['morning', 'midday', 'afternoon']:
            logger.info(f"Running {run_label} check immediately based on label")
            export_csv()
            logger.info("Script execution complete")
            return
            
        # If running in Render.com cron job with manual runs, just run once and exit
        if os.environ.get('RENDER') and (os.environ.get('RENDER_SERVICE_TYPE') == 'cron'):
            logger.info("Running in Render.com cron job mode")
            export_csv()
            logger.info("Script execution complete")
            return
        
        # If running in Render.com Background Worker, do immediate check
        if os.environ.get('RENDER') and (os.environ.get('RENDER_SERVICE_TYPE') == 'worker'):
            logger.info("Running in Render.com Background Worker mode - doing immediate check")
            export_csv()
            logger.info("Initial check complete. Will exit to let Render handle scheduling.")
            return
            
        # For scheduled operation, set up schedule
        logger.info("Setting up scheduled checks...")
        
        # Schedule the checks
        schedule.every().monday.at(morning_check_time).do(export_csv).tag('ringba')
        schedule.every().tuesday.at(morning_check_time).do(export_csv).tag('ringba')
        schedule.every().wednesday.at(morning_check_time).do(export_csv).tag('ringba')
        schedule.every().thursday.at(morning_check_time).do(export_csv).tag('ringba')
        schedule.every().friday.at(morning_check_time).do(export_csv).tag('ringba')
        
        schedule.every().monday.at(midday_check_time).do(export_csv).tag('ringba')
        schedule.every().tuesday.at(midday_check_time).do(export_csv).tag('ringba')
        schedule.every().wednesday.at(midday_check_time).do(export_csv).tag('ringba')
        schedule.every().thursday.at(midday_check_time).do(export_csv).tag('ringba')
        schedule.every().friday.at(midday_check_time).do(export_csv).tag('ringba')
        
        schedule.every().monday.at(afternoon_check_time).do(export_csv).tag('ringba')
        schedule.every().tuesday.at(afternoon_check_time).do(export_csv).tag('ringba')
        schedule.every().wednesday.at(afternoon_check_time).do(export_csv).tag('ringba')
        schedule.every().thursday.at(afternoon_check_time).do(export_csv).tag('ringba')
        schedule.every().friday.at(afternoon_check_time).do(export_csv).tag('ringba')
        
        logger.info("Scheduler set up. Waiting for scheduled times...")
        
        heartbeat_counter = 0
        while True:
            schedule.run_pending()
            
            # Add heartbeat every minute to prevent Render.com timeout
            heartbeat_counter += 1
            if heartbeat_counter >= 60:
                now = datetime.now()
                next_run = schedule.next_run()
                if next_run:
                    time_until_next = next_run - now
                    logger.info(f"Heartbeat: Still alive. Next check in {time_until_next}")
                else:
                    logger.info(f"Heartbeat: Still alive. No scheduled checks pending.")
                heartbeat_counter = 0
                
            time.sleep(1)  # Check every second
            
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        send_results_to_slack(f"Error in main function: {str(e)}", error=True)
    finally:
        logger.info("Script execution complete")

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
            main()
        except Exception as e:
            logger.error(f"Unhandled exception in main process: {str(e)}")
        finally:
            # Cancel timer if script completes normally
            timer.cancel()
            logger.info("Script execution complete") 
