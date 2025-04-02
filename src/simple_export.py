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
        
        # APPROACH 1: First try to extract data directly from the table - this is more reliable in container environments
        logger.info("First extracting table data directly from page (most reliable method)...")
        table_data = browser.execute_script("""
            // Find all tables and grid components in the page
            const tables = document.querySelectorAll('table');
            console.log(`Found ${tables.length} tables on page`);
            
            const gridElements = document.querySelectorAll('[role="grid"], [role="table"], .grid, .table, .data-grid');
            console.log(`Found ${gridElements.length} grid elements on page`);
            
            // Find row elements that might contain data
            const rowElements = document.querySelectorAll('[role="row"], tr, .row');
            console.log(`Found ${rowElements.length} row elements on page`);
            
            // Function to extract data from standard table 
            function extractFromTable(table) {
                const headers = [];
                const rows = [];
                
                // Special handling for Ringba table with filter buttons for column headers
                // First check if this might be the Ringba reporting table by looking for specific elements
                const filterButtons = table.querySelectorAll('button[class*="filter"], th button, th .mat-sort-header-container');
                const isRingbaTable = filterButtons.length > 0 || 
                                      table.closest('.report-view, .call-logs, .summary-report') !== null;
                
                if (isRingbaTable) {
                    console.log('Detected potential Ringba reporting table');
                    
                    // These are the expected column names in the Ringba reporting table based on the screenshot
                    const expectedHeaders = ['Campaign', 'Publisher', 'Target', 'Buyer', 'Dialed #', 
                                         'Number Pool', 'Date', 'Duplicate', 'Tags', 'IVR Handle Time',
                                         'RPC', 'Revenue', 'Payout'];
                    
                    // Try to find header row with filter buttons or column headers
                    const headerRow = table.querySelector('thead tr') || 
                                      table.querySelector('tr:first-child') ||
                                      document.querySelector('.mat-header-row, [role="row"]:first-child');
                    
                    if (headerRow) {
                        // Get all header cells
                        const headerCells = headerRow.querySelectorAll('th, td, .mat-header-cell, [role="columnheader"]');
                        
                        headerCells.forEach(cell => {
                            // Get text content of the cell, including any nested elements
                            let headerText = cell.textContent.trim();
                            
                            // Check if header contains a button (common in sortable Ringba tables)
                            const buttonText = cell.querySelector('button, .mat-sort-header-container')?.textContent.trim();
                            if (buttonText) {
                                headerText = buttonText;
                            }
                            
                            // Clean up the header text
                            headerText = headerText.replace(/▼|▲|↓|↑/g, '').trim();
                            
                            // Match against expected headers if text is ambiguous or empty
                            if (!headerText || headerText === '↕' || headerText.length < 2) {
                                // Try to infer header from column index and position
                                const index = headers.length;
                                if (index < expectedHeaders.length) {
                                    headerText = expectedHeaders[index];
                                    console.log(`Inferred header ${headerText} based on position`);
                                } else {
                                    headerText = `Column${index+1}`;
                                }
                            }
                            
                            headers.push(headerText);
                        });
                        
                        console.log('Extracted headers:', headers);
                    } else {
                        console.log('No header row found in Ringba table, using expected headers');
                        // If we can't find header cells, use the expected headers
                        expectedHeaders.forEach(header => headers.push(header));
                    }
                } else {
                    // Standard header extraction for non-Ringba tables
                    const headerCells = table.querySelectorAll('th') || table.querySelectorAll('tr:first-child td');
                    headerCells.forEach(cell => headers.push(cell.textContent.trim()));
                }
                
                // Get data rows
                const dataRows = table.querySelectorAll('tbody tr') || 
                                 table.querySelectorAll('tr:not(:first-child)') || 
                                 document.querySelectorAll('.mat-row, [role="row"]:not(:first-child)');
                
                dataRows.forEach(row => {
                    const rowData = {};
                    const cells = row.querySelectorAll('td, .mat-cell, [role="cell"]');
                    
                    // Skip header rows or rows with no cells
                    if (cells.length === 0 || row.closest('thead')) return;
                    
                    for (let i = 0; i < Math.min(cells.length, headers.length || cells.length); i++) {
                        const cellText = cells[i].textContent.trim();
                        const headerName = headers[i] || `Column${i+1}`;
                        rowData[headerName] = cellText;
                    }
                    
                    if (Object.keys(rowData).length > 0) {
                        rows.push(rowData);
                    }
                });
                
                return { headers, rows };
            }
            
            // Function to extract from Angular/React grid components 
            function extractFromGrid(grid) {
                const headers = [];
                const rows = [];
                
                // Try to get headers from different possible structures
                const headerCells = grid.querySelectorAll('[role="columnheader"], .header-cell, .column-header, th');
                
                // These are the expected column names in the Ringba reporting table
                const expectedHeaders = ['Campaign', 'Publisher', 'Target', 'Buyer', 'Dialed #', 
                                     'Number Pool', 'Date', 'Duplicate', 'Tags', 'IVR Handle Time',
                                     'RPC', 'Revenue', 'Payout'];
                                     
                // Check if this is likely the Ringba reporting grid
                const isRingbaGrid = grid.closest('.report-view, .call-logs, .summary-report') !== null || 
                                    headerCells.length >= 6; // Ringba tables typically have many columns
                
                if (headerCells.length > 0) {
                    headerCells.forEach(cell => {
                        let headerText = cell.textContent.trim();
                        
                        // Clean up the header text
                        headerText = headerText.replace(/▼|▲|↓|↑/g, '').trim();
                        
                        headers.push(headerText);
                    });
                } else if (isRingbaGrid) {
                    // If we can't find header cells but this looks like a Ringba grid, use the expected headers
                    console.log('Using expected Ringba headers for grid');
                    expectedHeaders.forEach(header => headers.push(header));
                }
                
                // Try to get rows from different possible structures  
                const dataRows = grid.querySelectorAll('[role="row"], .row, .data-row');
                dataRows.forEach(row => {
                    // Skip if this is likely a header row
                    if (row.getAttribute('role') === 'columnheader' || 
                        row.classList.contains('header-row') || 
                        row.parentElement.tagName === 'THEAD') {
                        return;
                    }
                    
                    const rowData = {};
                    const cells = row.querySelectorAll('[role="cell"], .cell, .data-cell, td');
                    
                    for (let i = 0; i < Math.min(cells.length, headers.length || cells.length); i++) {
                        const headerName = headers[i] || `Column${i+1}`;
                        // Use expected header if available for this position
                        const mappedHeader = isRingbaGrid && i < expectedHeaders.length ? 
                                            expectedHeaders[i] : headerName;
                        rowData[mappedHeader] = cells[i].textContent.trim();
                    }
                    
                    if (Object.keys(rowData).length > 0) {
                        rows.push(rowData);
                    }
                });
                
                return { headers, rows };
            }
            
            // Extract data from all tables and grids and find the best one
            let bestData = null;
            let maxRows = 0;
            
            // CUSTOM EXTRACTION FOR RINGBA TABLE - UPDATED FOR SPECIFIC RINGBA SUMMARY TABLE LAYOUT
            // First try a targeted approach specifically for the Ringba reporting table with focus on data rows
            try {
                console.log('Attempting targeted extraction for Ringba summary table');
                
                // Check if we can see the table headers first
                const tableHeaders = document.querySelectorAll('th, [role="columnheader"]');
                console.log(`Found ${tableHeaders.length} table headers`);
                
                // Extract header texts for debug
                const headerTexts = [];
                tableHeaders.forEach(th => headerTexts.push(th.textContent.trim()));
                console.log('Header texts:', headerTexts.join(', '));
                
                // Expected column names based on screenshot
                const expectedHeaders = ['Campaign', 'Publisher', 'Target', 'Buyer', 'Dialed #', 
                                    'Number Pool', 'Date', 'Duplicate', 'Tags', 'RPC', 'Revenue', 'Payout'];
                
                // Use the headers we found or fall back to expected headers
                const headers = headerTexts.length >= 3 ? headerTexts : expectedHeaders;
                
                // First try direct approach - find all cells on the page with data
                // This extracts even if we can't identify the table structure
                console.log("Direct cell extraction approach");
                
                // Find ALL div/span elements that might contain cell data
                const allTexts = Array.from(document.querySelectorAll('td, [role="cell"], div, span'))
                    .filter(el => {
                        // Only leaf nodes (no children elements)
                        if (el.children.length > 0) return false;
                        
                        // Has text content
                        if (!el.textContent.trim()) return false;
                        
                        // Not a button or link
                        if (el.tagName === 'BUTTON' || el.tagName === 'A') return false;
                        
                        // Not too short (likely not a cell value)
                        if (el.textContent.trim().length < 2) return false;
                        
                        return true;
                    });
                
                console.log(`Found ${allTexts.length} text elements that might be cells`);
                
                // Group texts by their vertical position to identify rows
                const rows = [];
                const rowsByPosition = {};
                
                allTexts.forEach(el => {
                    // Get position
                    const rect = el.getBoundingClientRect();
                    // Use a ~10px range for grouping by vertical position
                    const rowPosition = Math.floor(rect.top / 10) * 10;
                    
                    // Initialize row array if needed
                    if (!rowsByPosition[rowPosition]) {
                        rowsByPosition[rowPosition] = [];
                    }
                    
                    // Add to row group
                    rowsByPosition[rowPosition].push({
                        text: el.textContent.trim(),
                        x: rect.left,
                        element: el
                    });
                });
                
                // Sort cells within each row by x position (left to right)
                Object.keys(rowsByPosition).forEach(rowPos => {
                    const rowCells = rowsByPosition[rowPos].sort((a, b) => a.x - b.x);
                    
                    // Skip rows with too few cells
                    if (rowCells.length < 3) return;
                    
                    // Create row data object
                    const rowData = {};
                    for (let i = 0; i < Math.min(rowCells.length, headers.length); i++) {
                        rowData[headers[i]] = rowCells[i].text;
                    }
                    
                    // Only add rows that have key data fields
                    if (rowData.Target && rowData.RPC) {
                        rows.push(rowData);
                    }
                });
                
                console.log(`Extracted ${rows.length} rows by direct cell extraction`);
                
                // If direct cell approach found rows, use them
                if (rows.length > 0) {
                    console.log("Using rows from direct cell extraction");
                    bestData = {
                        headers: headers,
                        rows: rows
                    };
                    maxRows = rows.length;
                } else {
                    // Traditional approach - find the table element
                    const mainTable = document.querySelector('table, [role="grid"]');
                    
                    if (mainTable) {
                        console.log('Found main table element:', mainTable.tagName);
                        
                        // Get all rows
                        const tableRows = mainTable.querySelectorAll('tr, [role="row"]');
                        console.log(`Found ${tableRows.length} rows in main table`);
                        
                        // Extract rows data
                        const extractedRows = [];
                        
                        Array.from(tableRows).forEach((row, idx) => {
                            // Skip header row(s)
                            if (row.querySelector('th') || idx === 0) return;
                            
                            // Get all cells
                            const cells = row.querySelectorAll('td, [role="cell"]');
                            if (cells.length < 3) return;
                            
                            // Create row data object
                            const rowData = {};
                            for (let i = 0; i < Math.min(cells.length, headers.length); i++) {
                                rowData[headers[i]] = cells[i].textContent.trim();
                            }
                            
                            // Only add if we have data in required columns
                            if (Object.keys(rowData).length >= 3) {
                                extractedRows.push(rowData);
                            }
                        });
                        
                        console.log(`Extracted ${extractedRows.length} rows from main table`);
                        
                        if (extractedRows.length > 0) {
                            bestData = {
                                headers: headers,
                                rows: extractedRows
                            };
                            maxRows = extractedRows.length;
                        }
                    }
                }
            } catch (e) {
                console.error('Error in targeted Ringba extraction:', e);
            }
            
            // If targeted approach didn't work, fall back to other methods
            if (!bestData || bestData.rows.length === 0) {
                // Try fallback methods here...
                // The original code for table and grid extraction follows...
                
                // Try tables first
                tables.forEach(table => {
                    const data = extractFromTable(table);
                    if (data.rows.length > maxRows) {
                        maxRows = data.rows.length;
                        bestData = data;
                    }
                });
                
                // Then try grid components
                gridElements.forEach(grid => {
                    const data = extractFromGrid(grid);
                    if (data.rows.length > maxRows) {
                        maxRows = data.rows.length;
                        bestData = data;
                    }
                });
            }
            
            // ...rest of the scanning code for rows...
            
            // Add debugging to show which columns were found
            if (bestData && bestData.headers) {
                console.log('Found headers:', bestData.headers.join(', '));
            }
            
            // Return the best data found
            return bestData;
        """)
        
        if table_data and table_data.get('rows') and len(table_data.get('rows')) > 0:
            logger.info(f"Successfully extracted table data directly: {len(table_data['rows'])} rows")
            
            # Save to CSV
            download_dir = "/tmp"
            os.makedirs(download_dir, exist_ok=True)
            file_path = os.path.join(download_dir, f"direct_extract_{int(time.time())}.csv")
            
            # Convert to DataFrame and save
            df = pd.DataFrame(table_data['rows'])
            df.to_csv(file_path, index=False)
            
            logger.info(f"Saved directly extracted data to {file_path}")
            return file_path
        else:
            logger.warning("Could not extract table data directly, will try button click")
        
        # APPROACH 2: Try to click the export button and intercept the download
        # Enhanced JavaScript that intercepts blob URLs and download events 
        logger.info("Setting up enhanced blob URL interception...")
        browser.execute_script("""
            // Store blob URLs and download data
            window.blobUrls = [];
            window.downloadData = null;
            window.csvContent = null;
            
            // Debug logging helper
            function logDownloadInfo(message) {
                console.log('[Download Debug] ' + message);
                // Also store in a global array for access from Selenium
                if (!window.downloadLogs) window.downloadLogs = [];
                window.downloadLogs.push({
                    time: new Date().toISOString(),
                    message: message
                });
            }
            
            logDownloadInfo('Setting up blob interception');
            
            // Track all blobs
            const originalCreateObjectURL = URL.createObjectURL;
            URL.createObjectURL = function(object) {
                const url = originalCreateObjectURL(object);
                logDownloadInfo('Blob URL created: ' + url + ' (type: ' + (object.type || 'unknown') + ', size: ' + object.size + ')');
                
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
                    
                    logDownloadInfo('Found potential CSV blob: ' + object.type + ', ' + object.size);
                    
                    // Read the blob
                    const reader = new FileReader();
                    reader.onload = function() {
                        const content = reader.result;
                        logDownloadInfo('Read blob content, length: ' + content.length);
                        
                        // Simple CSV check - look for commas and newlines
                        if (content.includes(',') && 
                            (content.includes('\\n') || content.includes('\\r'))) {
                            
                            window.csvContent = content;
                            logDownloadInfo('Saved CSV content from blob of size: ' + content.length);
                            
                            // Store download data in format expected by the Python code
                            window.downloadData = {
                                content: content,
                                timestamp: Date.now(),
                                type: object.type || 'text/csv'
                            };
                        }
                    };
                    reader.readAsText(object);
                }
                
                return url;
            };
            
            // Monitor all XMLHttpRequests to catch CSV responses
            const originalXhrOpen = XMLHttpRequest.prototype.open;
            const originalXhrSend = XMLHttpRequest.prototype.send;
            
            XMLHttpRequest.prototype.open = function(method, url) {
                this._method = method;
                this._url = url;
                
                // Check if this might be related to a download or export
                if (typeof url === 'string' && (
                    url.includes('export') || 
                    url.includes('download') ||
                    url.includes('csv') ||
                    url.includes('call-logs')
                )) {
                    logDownloadInfo('Monitoring XHR: ' + method + ' ' + url);
                    this._isMonitored = true;
                }
                
                return originalXhrOpen.apply(this, arguments);
            };
            
            XMLHttpRequest.prototype.send = function() {
                if (this._isMonitored) {
                    const xhr = this;
                    
                    // Store the original onreadystatechange
                    const originalOnReadyStateChange = xhr.onreadystatechange;
                    
                    xhr.onreadystatechange = function() {
                        if (xhr.readyState === 4) {
                            // Request completed
                            if (xhr.status >= 200 && xhr.status < 300) {
                                // Success
                                logDownloadInfo('XHR completed: ' + xhr._url);
                                
                                // Check if response looks like CSV
                                const contentType = xhr.getResponseHeader('Content-Type');
                                if (contentType && (
                                    contentType.includes('csv') || 
                                    contentType.includes('text/plain') ||
                                    contentType.includes('octet-stream')
                                )) {
                                    logDownloadInfo('Found CSV response in XHR: ' + contentType);
                                    
                                    // Save the CSV content
                                    window.csvContent = xhr.responseText;
                                    window.downloadData = {
                                        content: xhr.responseText,
                                        timestamp: Date.now(),
                                        type: contentType
                                    };
                                }
                                else if (xhr._url.includes('call-logs')) {
                                    logDownloadInfo('Found call logs response, checking content');
                                    
                                    // Try to detect if content is CSV
                                    try {
                                        const text = xhr.responseText;
                                        if (text && text.includes(',') && 
                                            (text.includes('\\n') || text.includes('\\r'))) {
                                            
                                            logDownloadInfo('Content appears to be CSV, length: ' + text.length);
                                            window.csvContent = text;
                                            window.downloadData = {
                                                content: text,
                                                timestamp: Date.now(),
                                                type: 'text/csv'
                                            };
                                        }
                                    } catch (e) {
                                        logDownloadInfo('Error checking XHR content: ' + e);
                                    }
                                }
                            }
                        }
                        
                        // Call the original onreadystatechange
                        if (originalOnReadyStateChange) {
                            originalOnReadyStateChange.apply(xhr, arguments);
                        }
                    };
                }
                
                return originalXhrSend.apply(this, arguments);
            };
            
            // Monitor all fetch requests too
            const originalFetch = window.fetch;
            window.fetch = function(input, init) {
                // Determine URL
                const url = (typeof input === 'string') ? input : input.url;
                
                if (url && (
                    url.includes('export') || 
                    url.includes('download') ||
                    url.includes('csv') ||
                    url.includes('call-logs')
                )) {
                    logDownloadInfo('Monitoring fetch: ' + url);
                    
                    // Return a Promise that wraps the original fetch
                    return originalFetch.apply(this, arguments)
                        .then(response => {
                            // Clone the response so we can read it twice
                            const clone = response.clone();
                            
                            // Check content type
                            const contentType = clone.headers.get('Content-Type');
                            if (contentType && (
                                contentType.includes('csv') || 
                                contentType.includes('text/plain') ||
                                contentType.includes('octet-stream')
                            )) {
                                logDownloadInfo('Found CSV in fetch response: ' + contentType);
                                
                                // Read and store the content
                                clone.text().then(text => {
                                    window.csvContent = text;
                                    window.downloadData = {
                                        content: text,
                                        timestamp: Date.now(),
                                        type: contentType
                                    };
                                });
                            }
                            
                            // Return the original response
                            return response;
                        });
                }
                
                // Not a monitored URL, just pass through
                return originalFetch.apply(this, arguments);
            };
            
            // Monitor download clicks
            document.addEventListener('click', function(e) {
                let target = e.target;
                
                // Check if clicked element or its parent is a button/link containing "export" or "csv"
                while (target && target !== document) {
                    // Check for common download indicators
                    const isDownloadElem = 
                        (target.tagName === 'A' && target.href && target.href.startsWith('blob:')) ||
                        (target.tagName === 'BUTTON' && 
                          (target.textContent.toLowerCase().includes('export') || 
                           target.textContent.toLowerCase().includes('csv')));
                    
                    if (isDownloadElem) {
                        logDownloadInfo('Export/download element clicked: ' + target.tagName + 
                                        ' text: ' + target.textContent.trim());
                        
                        // For blob URLs, try to read the content
                        if (target.tagName === 'A' && target.href && target.href.startsWith('blob:')) {
                            logDownloadInfo('Blob URL clicked: ' + target.href);
                            
                            // Find matching blob
                            const blobInfo = window.blobUrls.find(b => b.url === target.href);
                            if (blobInfo && blobInfo.blob) {
                                // Read the blob content
                                const reader = new FileReader();
                                reader.onload = function() {
                                    const content = reader.result;
                                    logDownloadInfo('Read blob from click, length: ' + content.length);
                                    
                                    window.csvContent = content;
                                    window.downloadData = {
                                        content: content,
                                        timestamp: Date.now(),
                                        type: blobInfo.blob.type || 'text/csv'
                                    };
                                };
                                reader.readAsText(blobInfo.blob);
                            }
                        }
                    }
                    
                    target = target.parentElement;
                }
            }, true);

            // Export specific helper to find the Export CSV button exactly as shown in screenshot
            window.findAndClickExportButton = function() {
                logDownloadInfo('Looking for EXPORT CSV button...');
                
                // Specific approach for finding the exact EXPORT CSV button from the screenshot
                // Find by button text matching exactly "EXPORT CSV"
                const exactButtonMatches = Array.from(document.querySelectorAll('button'))
                    .filter(el => el.textContent.trim() === 'EXPORT CSV');
                
                // Find by class name containing "export"
                const exportButtons = Array.from(document.querySelectorAll('button[class*="export"], .export-button, .export-summary-btn'));
                
                // First check if button is disabled
                const allButtons = [...exactButtonMatches, ...exportButtons];
                
                if (allButtons.length > 0) {
                    // Check if any buttons are enabled
                    const enabledButtons = allButtons.filter(btn => !btn.disabled && !btn.hasAttribute('disabled'));
                    
                    if (enabledButtons.length > 0) {
                        // Use the first enabled button
                        const button = enabledButtons[0];
                        logDownloadInfo('Found enabled export button: ' + button.outerHTML);
                        button.click();
                        return 'Clicked enabled export button';
                    } else {
                        // All buttons are disabled - try to wait and see if they become enabled
                        const firstButton = allButtons[0];
                        logDownloadInfo('Found export button but it is disabled: ' + firstButton.outerHTML);
                        
                        // Try to find why it might be disabled
                        if (document.querySelectorAll('.loading, .loading-indicator, .spinner').length > 0) {
                            logDownloadInfo('Page appears to be loading, button may be temporarily disabled');
                        }
                        
                        // Log all buttons found for debugging
                        allButtons.forEach((btn, i) => {
                            logDownloadInfo(`Button ${i+1}: ${btn.outerHTML}`);
                        });
                        
                        return 'Export button found but disabled';
                    }
                }
                
                // Try more general approach if specific methods fail
                const anyExportButtons = Array.from(document.querySelectorAll('button, a, span, div'))
                    .filter(el => el.textContent && 
                        (el.textContent.toLowerCase().includes('export') || 
                         el.textContent.toLowerCase().includes('csv')));
                
                if (anyExportButtons.length > 0) {
                    // Sort by likelihood of being the export button
                    anyExportButtons.sort((a, b) => {
                        // Exact match gets highest priority
                        const aText = a.textContent.toLowerCase().trim();
                        const bText = b.textContent.toLowerCase().trim();
                        
                        if (aText === 'export csv' && bText !== 'export csv') return -1;
                        if (bText === 'export csv' && aText !== 'export csv') return 1;
                        
                        // Enabled buttons get priority
                        if (!a.disabled && b.disabled) return -1;
                        if (a.disabled && !b.disabled) return 1;
                        
                        // Button elements get priority
                        if (a.tagName === 'BUTTON' && b.tagName !== 'BUTTON') return -1;
                        if (b.tagName === 'BUTTON' && a.tagName !== 'BUTTON') return 1;
                        
                        // Otherwise sort by simplicity/brevity of text
                        return aText.length - bText.length;
                    });
                    
                    // Warn if best candidate is disabled
                    const bestButton = anyExportButtons[0];
                    if (bestButton.disabled || bestButton.hasAttribute('disabled')) {
                        logDownloadInfo(`Best candidate button is disabled: ${bestButton.outerHTML}`);
                        return 'Best export button candidate is disabled';
                    }
                    
                    // Click the best candidate
                    logDownloadInfo('Clicking: ' + bestButton.tagName + ' with text: ' + bestButton.textContent.trim());
                    bestButton.click();
                    return 'Clicked button: ' + bestButton.textContent.trim();
                }
                
                logDownloadInfo('No export buttons found');
                return 'No buttons found';
            };
            
            logDownloadInfo('Download interception setup complete');
        """)
        
        # Find all possible download directories - ONLY USE WRITABLE DIRECTORIES
        download_dir = "/tmp"
        possible_download_dirs = [
            "/tmp", 
            "/tmp/downloads",
            "/opt/render/downloads",
            "/opt/render/project/src/downloads",
            "/opt/render/tmp"
        ]
        
        # Filter out empty paths and ensure directories exist
        possible_download_dirs = [d for d in possible_download_dirs if d and d != '/downloads']
        for d in possible_download_dirs:
            try:
                os.makedirs(d, exist_ok=True)
                logger.info(f"Created/verified directory: {d}")
            except Exception as e:
                logger.warning(f"Could not create directory {d}: {e}")
                # Remove from list if we can't create it
                if d in possible_download_dirs:
                    possible_download_dirs.remove(d)
            
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
        
        # Click the export button using our enhanced methods
        export_clicked = False
        
        logger.info("Looking for and clicking the EXPORT CSV button...")
        try:
            # Try our specific finder for the Export CSV button from screenshot
            result = browser.execute_script("return window.findAndClickExportButton();")
            logger.info(f"JavaScript export button click result: {result}")
            export_clicked = True
            
            # Check if button was found but disabled
            if "disabled" in result:
                logger.warning("Export button is disabled - waiting to see if it becomes enabled")
                
                # Wait up to 30 seconds to see if button becomes enabled
                wait_time = 30
                start_wait = time.time()
                button_enabled = False
                
                while time.time() - start_wait < wait_time:
                    # Check if button is now enabled
                    check_result = browser.execute_script("""
                        const exportButtons = Array.from(document.querySelectorAll('button[class*="export"], .export-button, .export-summary-btn, button'));
                        const enabledButtons = exportButtons.filter(btn => {
                            return btn.textContent.toLowerCase().includes('export') && 
                                   !btn.disabled && 
                                   !btn.hasAttribute('disabled');
                        });
                        if (enabledButtons.length > 0) {
                            console.log('Found enabled button:', enabledButtons[0].outerHTML);
                            enabledButtons[0].click();
                            return true;
                        }
                        return false;
                    """)
                    
                    if check_result:
                        logger.info("Export button became enabled and was clicked")
                        button_enabled = True
                        export_clicked = True
                        break
                    
                    # Wait a bit before checking again
                    time.sleep(3)
                
                if not button_enabled:
                    logger.warning("Export button remained disabled after waiting - will rely on table extraction instead")
                    # Force table extraction by returning None early
                    return browser.execute_script("""
                        // Try to check why button might be disabled
                        const statusText = document.querySelector('.status-text, .error-text, .message')?.textContent;
                        if (statusText) {
                            console.log('Status message found:', statusText);
                        }
                        
                        // Check if we're in a preview or limited data mode
                        const previewMode = document.querySelector('.preview-mode, .limited-data');
                        if (previewMode) {
                            console.log('Page appears to be in preview mode');
                        }
                        
                        // log any error messages
                        const errors = document.querySelectorAll('.error, .alert, [role="alert"]');
                        if (errors.length > 0) {
                            console.log('Found error messages:');
                            errors.forEach(err => console.log(' - ' + err.textContent.trim()));
                        }
                        
                        return null;
                    """)
            else:
                # Button was clicked or not found
                export_clicked = True
            
            # If that didn't work, use Selenium to try specific XPath or CSS selector
            if "No buttons found" in result:
                logger.warning("JavaScript button finder failed, trying Selenium...")
                
                # Try specific XPath matching the screenshot's EXPORT CSV button
                try:
                    # Try precise XPath for EXPORT CSV button in upper right corner
                    export_button = browser.find_element(By.XPATH, "//button[text()='EXPORT CSV']")
                    logger.info("Found EXPORT CSV button with exact text match")
                    export_button.click()
                    export_clicked = True
                except Exception as e1:
                    logger.warning(f"Exact EXPORT CSV button not found: {str(e1)}")
                    
                    # Try CSS selector from screenshot context
                    try:
                        export_button = browser.find_element(By.CSS_SELECTOR, ".export-csv-button, button.export, [data-test='export-csv-button']")
                        logger.info("Found export button with CSS selector")
                        export_button.click()
                        export_clicked = True
                    except Exception as e2:
                        logger.warning(f"CSS selector approach failed: {str(e2)}")
                        
                        # Try general button finding
                        locators = [
                            (By.XPATH, "//button[contains(text(), 'EXPORT CSV')]"),
                            (By.XPATH, "//button[contains(text(), 'Export CSV')]"),
                            (By.XPATH, "//button[contains(text(), 'EXPORT')]"),
                            (By.XPATH, "//button[contains(text(), 'Export')]"),
                            (By.XPATH, "//button[contains(text(), 'CSV')]"),
                            (By.CSS_SELECTOR, "[id*='export']"),
                            (By.CSS_SELECTOR, "[class*='export']"),
                        ]
                        
                        for locator_type, locator_value in locators:
                            try:
                                elements = browser.find_elements(locator_type, locator_value)
                                if elements:
                                    logger.info(f"Found {len(elements)} export buttons with {locator_value}")
                                    for el in elements:
                                        try:
                                            logger.info(f"Trying to click button: {el.text}")
                                            el.click()
                                            export_clicked = True
                                            break
                                        except:
                                            continue
                                    if export_clicked:
                                        break
                            except Exception as e:
                                logger.warning(f"Error with locator {locator_value}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error clicking export button: {str(e)}")
        
        # Take screenshot after clicking export
        take_screenshot(browser, "after_export_attempt")
        
        # Get download logs even if we couldn't click export
        try:
            download_logs = browser.execute_script("return window.downloadLogs || [];")
            if download_logs:
                logger.info("Download debug logs from browser:")
                for log_entry in download_logs:
                    logger.info(f"  {log_entry.get('time', '')}: {log_entry.get('message', '')}")
        except Exception as e:
            logger.warning(f"Could not retrieve download logs: {str(e)}")
        
        if not export_clicked:
            logger.error("Could not click export button with any method")
            # Return to direct data extraction as fallback
            return None
        
        # Wait for download to complete
        logger.info("Waiting for download to complete...")
        wait_time = 30  # 30 seconds wait maximum - reduced from 60
        start_time = time.time()
        
        # First try to get data directly from the intercepted content
        while time.time() - start_time < wait_time:
            # Check for download data from JavaScript interception
            for data_var in ["downloadData", "csvContent"]:
                try:
                    csv_data = browser.execute_script(f"return window.{data_var};")
                    if csv_data:
                        if isinstance(csv_data, dict) and csv_data.get('content'):
                            csv_content = csv_data.get('content')
                        else:
                            csv_content = csv_data
                            
                        if csv_content and isinstance(csv_content, str):
                            logger.info(f"Found download data directly from {data_var}!")
                            
                            # Save the content to a file
                            file_path = os.path.join("/tmp", f"blob_download_{int(time.time())}.csv")
                            
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(csv_content)
                            
                            logger.info(f"Saved blob data to {file_path}")
                            
                            # Verify it's a valid CSV
                            try:
                                df = pd.read_csv(file_path)
                                logger.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
                                return file_path
                            except Exception as e:
                                logger.warning(f"Data is not a valid CSV: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error checking {data_var}: {str(e)}")
            
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
            
            # Log progress every 10 seconds
            elapsed = int(time.time() - start_time)
            if elapsed % 10 == 0:
                logger.info(f"Still waiting for download... ({elapsed}s elapsed)")
            
            time.sleep(2)
        
        logger.warning("No new CSV files found after waiting")
        
        # APPROACH 3: Fall back to trying the API (though it was failing with 404 in logs)
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
                
                # Try the call logs API endpoint with different path
                call_logs_url = f"https://api.ringba.com/v2/accounts/{account_id}/call-logs"
                params = {
                    'startDate': start_date,
                    'endDate': end_date,
                    'format': 'csv'
                }
                
                logger.info(f"Making revised API request to: {call_logs_url}")
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
