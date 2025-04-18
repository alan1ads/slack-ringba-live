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
        
        # Take screenshot of the full page
        take_screenshot(browser, "before_table_extraction")
        
        # Take additional debugging screenshots of the page structure
        browser.execute_script("""
            // Highlight table elements for debugging
            const tables = document.querySelectorAll('table, [role="grid"], [role="table"]');
            const oldBorders = [];
            
            tables.forEach((table, i) => {
                oldBorders[i] = table.style.border;
                table.style.border = '5px solid red';
                table.setAttribute('data-debug', 'highlighted-table-' + i);
            });
            
            // Highlight headers
            const headers = document.querySelectorAll('th, [role="columnheader"]');
            headers.forEach((header, i) => {
                header.style.backgroundColor = 'yellow';
                header.setAttribute('data-debug', 'highlighted-header-' + i);
            });
            
            // Add debug info
            const debugDiv = document.createElement('div');
            debugDiv.style.position = 'fixed';
            debugDiv.style.top = '0';
            debugDiv.style.left = '0';
            debugDiv.style.backgroundColor = 'rgba(0,0,0,0.8)';
            debugDiv.style.color = 'white';
            debugDiv.style.padding = '10px';
            debugDiv.style.zIndex = '9999';
            debugDiv.style.maxHeight = '200px';
            debugDiv.style.overflow = 'auto';
            
            debugDiv.innerHTML = `
                <div>Tables found: ${tables.length}</div>
                <div>Headers found: ${headers.length}</div>
                <div>Headers text: ${Array.from(headers).map(h => h.textContent.trim()).join(', ')}</div>
            `;
            
            document.body.appendChild(debugDiv);
        """)
        
        # Take a screenshot with highlighted elements
        take_screenshot(browser, "table_elements_highlighted")
        
        # Get page HTML for debugging
        page_html = browser.execute_script("return document.documentElement.outerHTML;")
        html_path = os.path.join(screenshots_dir, f"{int(time.time())}_page_source.html")
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(page_html)
        logger.info(f"Saved page HTML to {html_path}")
        
        # Get detailed DOM info about table elements
        table_info = browser.execute_script("""
            const tables = document.querySelectorAll('table, [role="grid"], [role="table"]');
            const tableInfo = [];
            
            tables.forEach((table, i) => {
                const cells = table.querySelectorAll('td, th, [role="cell"], [role="columnheader"]');
                const rows = table.querySelectorAll('tr, [role="row"]');
                
                tableInfo.push({
                    index: i,
                    tag: table.tagName,
                    id: table.id,
                    className: table.className,
                    cellCount: cells.length,
                    rowCount: rows.length,
                    isVisible: table.offsetParent !== null,
                    rect: {
                        top: table.getBoundingClientRect().top,
                        left: table.getBoundingClientRect().left,
                        width: table.getBoundingClientRect().width,
                        height: table.getBoundingClientRect().height
                    }
                });
            });
            
            return tableInfo;
        """)
        
        # Log detailed table information
        if table_info:
            logger.info(f"Found {len(table_info)} potential tables on page:")
            for i, table in enumerate(table_info):
                logger.info(f"Table {i}: {table.get('tag', 'unknown')} (class='{table.get('className', '')}', id='{table.get('id', '')}') - {table.get('rowCount', 0)} rows, {table.get('cellCount', 0)} cells, visible: {table.get('isVisible', False)}")
        else:
            logger.warning("No tables found on page for debugging")
        
        # Try direct Ringba UI structure extraction
        logger.info("Trying direct extraction based on Ringba UI structure...")
        ringba_structure_data = browser.execute_script("""
            console.log('Starting direct Ringba UI structure extraction...');
            
            // Specific method to extract data from ag-Grid components (which Ringba uses)
            function extractAgGridData() {
                console.log('Looking for ag-Grid components...');
                
                // First identify the ag-Grid containers
                const agGridElements = document.querySelectorAll('.ag-root, [class*="ag-root"]');
                console.log(`Found ${agGridElements.length} ag-Grid elements`);
                
                // Look specifically for the summary grid at the bottom of the page (where Target and RPC columns are)
                // This is likely the second grid based on the screenshot
                let summaryGrid = null;
                let targetColumnIndex = -1;
                let rpcColumnIndex = -1;
                
                // First look for the "Summary" section which contains our grid
                const summarySection = document.querySelector('.summary-section, [id*="summary"], [class*="summary"]');
                if (summarySection) {
                    summaryGrid = summarySection.querySelector('.ag-root, [class*="ag-root"]');
                }
                
                // If we didn't find it that way, check if one of the grids we found has Target and RPC headers
                if (!summaryGrid && agGridElements.length > 0) {
                    // Check each grid for Target and RPC headers
                    for (const grid of agGridElements) {
                        const headerCells = grid.querySelectorAll('.ag-header-cell, [class*="header-cell"], [role="columnheader"]');
                        
                        // Check if this grid has both Target and RPC headers
                        let hasTarget = false;
                        let hasRPC = false;
                        
                        headerCells.forEach((cell, index) => {
                            const text = cell.textContent.trim();
                            if (text === 'Target') {
                                hasTarget = true;
                                targetColumnIndex = index;
                            } else if (text === 'RPC') {
                                hasRPC = true;
                                rpcColumnIndex = index;
                            }
                        });
                        
                        if (hasTarget && hasRPC) {
                            summaryGrid = grid;
                            break;
                        }
                    }
                    
                    // If we didn't find a grid with both Target and RPC, use the last grid (often the main data grid)
                    if (!summaryGrid && agGridElements.length > 0) {
                        summaryGrid = agGridElements[agGridElements.length - 1];
                    }
                }
                
                if (summaryGrid) {
                    console.log('Found summary grid for extraction');
                    
                    // First identify the column headers to find Target and RPC columns
                    const headerCells = summaryGrid.querySelectorAll('.ag-header-cell, [class*="header-cell"], [role="columnheader"]');
                    
                    // Extract header texts
                    const headers = [];
                    headerCells.forEach(cell => {
                        const headerText = cell.textContent.trim();
                        headers.push(headerText);
                        
                        // Track the indices of our target columns
                        if (headerText === 'Target') {
                            targetColumnIndex = headers.length - 1;
                        } else if (headerText === 'RPC') {
                            rpcColumnIndex = headers.length - 1;
                        }
                    });
                    
                    console.log(`Found headers: ${headers.join(', ')}`);
                    console.log(`Target column index: ${targetColumnIndex}, RPC column index: ${rpcColumnIndex}`);
                    
                    // Try two approaches to get the cell data
                    
                    // APPROACH 1: Use direct cell access
                    const rows = [];
                    
                    // Find all row elements in the grid
                    const rowElements = summaryGrid.querySelectorAll('.ag-row, [class*="ag-row"], [role="row"]');
                    
                    // Skip the first row if it looks like a header row
                    const startIndex = rowElements.length > 0 && rowElements[0].classList.contains('ag-header-row') ? 1 : 0;
                    
                    // Process each data row
                    for (let i = startIndex; i < rowElements.length; i++) {
                        const rowElement = rowElements[i];
                        
                        // Skip if this is a header row
                        if (rowElement.classList.contains('ag-header-row') || 
                            rowElement.getAttribute('role') === 'columnheader') {
                            continue;
                        }
                        
                        // Get cells in this row
                        const cells = rowElement.querySelectorAll('.ag-cell, [class*="ag-cell"], [role="gridcell"]');
                        
                        // If Target and RPC column indices are known, use them directly
                        if (targetColumnIndex >= 0 && rpcColumnIndex >= 0 && cells.length > Math.max(targetColumnIndex, rpcColumnIndex)) {
                            const targetText = cells[targetColumnIndex].textContent.trim();
                            const rpcText = cells[rpcColumnIndex].textContent.trim();
                            
                            // Add this row if we got both values
                            if (targetText && rpcText) {
                                rows.push({
                                    Target: targetText,
                                    RPC: rpcText
                                });
                            }
                        } 
                        // Otherwise try to map cells to headers
                        else if (cells.length > 0 && headers.length > 0) {
                            const rowData = {};
                            
                            for (let j = 0; j < Math.min(cells.length, headers.length); j++) {
                                rowData[headers[j]] = cells[j].textContent.trim();
                            }
                            
                            // Only add if we have Target data
                            if (rowData.Target && (rowData.RPC || rowData.Revenue)) {
                                rows.push(rowData);
                            }
                        }
                    }
                    
                    // If we found rows, return them
                    if (rows.length > 0) {
                        console.log(`Extracted ${rows.length} rows from ag-Grid`);
                        return { headers, rows };
                    }
                    
                    // APPROACH 2: Use cell position to find data values
                    // This works better with complex ag-Grid layouts with cell spans
                    
                    // Find the exact positions of the Target and RPC column headers
                    let targetHeaderPosition = null;
                    let rpcHeaderPosition = null;
                    
                    headerCells.forEach(cell => {
                        const text = cell.textContent.trim();
                        const rect = cell.getBoundingClientRect();
                        
                        if (text === 'Target') {
                            targetHeaderPosition = {
                                left: rect.left,
                                width: rect.width,
                                center: rect.left + rect.width / 2
                            };
                        } else if (text === 'RPC') {
                            rpcHeaderPosition = {
                                left: rect.left,
                                width: rect.width,
                                center: rect.left + rect.width / 2
                            };
                        }
                    });
                    
                    // If we found position info for our columns
                    if (targetHeaderPosition && rpcHeaderPosition) {
                        // Find all cell elements that could contain data (including those outside the grid)
                        const allCells = document.querySelectorAll('.ag-cell, [class*="ag-cell"], .cell, td, [role="gridcell"]');
                        
                        // Group cells by their y-position to determine rows
                        const cellsByRow = {};
                        
                        allCells.forEach(cell => {
                            const rect = cell.getBoundingClientRect();
                            
                            // Skip cells outside the grid area
                            if (rect.top < 100) return; // Skip header areas
                            
                            // Group by y-position (rounded to handle slight offsets)
                            const rowY = Math.round(rect.top / 5) * 5;
                            
                            if (!cellsByRow[rowY]) {
                                cellsByRow[rowY] = [];
                            }
                            
                            cellsByRow[rowY].push({
                                element: cell,
                                text: cell.textContent.trim(),
                                left: rect.left,
                                center: rect.left + rect.width / 2,
                                width: rect.width
                            });
                        });
                        
                        // Process each row of cells
                        const positionRows = [];
                        
                        Object.values(cellsByRow).forEach(rowCells => {
                            // Find the cell most aligned with Target column
                            const targetCell = rowCells.find(cell => 
                                Math.abs(cell.center - targetHeaderPosition.center) < targetHeaderPosition.width / 2);
                            
                            // Find the cell most aligned with RPC column
                            const rpcCell = rowCells.find(cell => 
                                Math.abs(cell.center - rpcHeaderPosition.center) < rpcHeaderPosition.width / 2);
                            
                            // If we found both cells
                            if (targetCell && rpcCell) {
                                // Skip if either cell doesn't have text or RPC doesn't look like a dollar amount
                                if (!targetCell.text || !rpcCell.text || !rpcCell.text.includes('$')) return;
                                
                                positionRows.push({
                                    Target: targetCell.text,
                                    RPC: rpcCell.text
                                });
                            }
                        });
                        
                        if (positionRows.length > 0) {
                            console.log(`Extracted ${positionRows.length} rows using position-based approach`);
                            return {
                                headers: ['Target', 'RPC'],
                                rows: positionRows
                            };
                        }
                    }
                }
                
                return null;
            }
            
            // Method to look for any visible table structure with Target and RPC columns
            function findTableWithTargetAndRPC() {
                // Check if there are any rows with target/RPC pairs visible in any part of the page
                // Look specifically for dollar amounts which are likely RPC values
                const dollarElements = Array.from(document.querySelectorAll('*'))
                    .filter(el => {
                        if (el.children.length > 0) return false;
                        const text = el.textContent.trim();
                        return text.startsWith('$') && /\\$\\d+(\\.\\d+)?/.test(text);
                    });
                
                console.log(`Found ${dollarElements.length} dollar value elements`);
                
                // For each dollar value, attempt to find the corresponding Target value
                const rows = [];
                
                dollarElements.forEach(dollarEl => {
                    const dollarRect = dollarEl.getBoundingClientRect();
                    const dollarValue = dollarEl.textContent.trim();
                    
                    // Skip headers or labels
                    if (dollarValue === '$' || dollarValue === 'RPC' || dollarValue.includes('Threshold')) return;
                    
                    // Try to find the Target value in the same row (horizontally aligned)
                    const sameRowElements = Array.from(document.querySelectorAll('*'))
                        .filter(el => {
                            if (el.children.length > 0) return false;
                            const rect = el.getBoundingClientRect();
                            const text = el.textContent.trim();
                            
                            // Skip if empty or too short
                            if (!text || text.length < 2) return false;
                            
                            // Skip if it's another dollar value or column header
                            if (text.startsWith('$') || text === 'Target' || text === 'RPC') return false;
                            
                            // Check if it's in the same horizontal line (within 10px)
                            return Math.abs(rect.top - dollarRect.top) < 10;
                        });
                    
                    if (sameRowElements.length > 0) {
                        // Find the most likely Target element - typically to the left of the RPC value
                        // Sort by x-position (left to right)
                        sameRowElements.sort((a, b) => {
                            return a.getBoundingClientRect().left - b.getBoundingClientRect().left;
                        });
                        
                        // Look for elements to the left of the RPC value
                        const elementsToLeft = sameRowElements.filter(el => 
                            el.getBoundingClientRect().right < dollarRect.left);
                        
                        if (elementsToLeft.length > 0) {
                            // The rightmost element to the left is typically the Target name
                            const targetElement = elementsToLeft[elementsToLeft.length - 1];
                            
                            rows.push({
                                Target: targetElement.textContent.trim(),
                                RPC: dollarValue
                            });
                        }
                    }
                });
                
                if (rows.length > 0) {
                    console.log(`Constructed ${rows.length} Target/RPC pairs from dollar values`);
                    return {
                        headers: ['Target', 'RPC'],
                        rows: rows
                    };
                }
                
                return null;
            }
            
            // Try exact extraction from highlighted areas in screenshot
            function extractHighlightedArea() {
                // Look for all elements under headings "Target" and "RPC"
                const targetHeader = Array.from(document.querySelectorAll('th, td, div, span'))
                    .find(el => el.textContent.trim() === 'Target' && el.getBoundingClientRect().height < 50);
                
                const rpcHeader = Array.from(document.querySelectorAll('th, td, div, span'))
                    .find(el => el.textContent.trim() === 'RPC' && el.getBoundingClientRect().height < 50);
                
                if (targetHeader && rpcHeader) {
                    console.log('Found Target and RPC headers - using highlighted area extraction');
                    
                    // Get positions for these headers
                    const targetHeaderRect = targetHeader.getBoundingClientRect();
                    const rpcHeaderRect = rpcHeader.getBoundingClientRect();
                    
                    // Find elements that might be data cells vertically aligned below these headers
                    const allElements = Array.from(document.querySelectorAll('*'))
                        .filter(el => el.children.length === 0 && el.textContent.trim().length > 0)
                        .map(el => {
                            const rect = el.getBoundingClientRect();
                            return {
                                element: el,
                                text: el.textContent.trim(),
                                rect: rect
                            };
                        });
                    
                    // Group elements by their vertical position to represent rows
                    const rowGroups = {};
                    allElements.forEach(item => {
                        // Skip the header elements themselves
                        if (item.element === targetHeader || item.element === rpcHeader) return;
                        
                        // Skip if above the headers
                        if (item.rect.top <= Math.max(targetHeaderRect.bottom, rpcHeaderRect.bottom)) return;
                        
                        // Group by vertical position (rounded to nearest 5px to handle slight variations)
                        const rowKey = Math.round(item.rect.top / 5) * 5;
                        if (!rowGroups[rowKey]) {
                            rowGroups[rowKey] = [];
                        }
                        rowGroups[rowKey].push(item);
                    });
                    
                    // Process each row to extract Target and RPC pairs
                    const rows = [];
                    
                    Object.values(rowGroups).forEach(rowItems => {
                        let targetValue = null;
                        let rpcValue = null;
                        
                        // Find a value horizontally aligned with the Target header
                        const targetCandidates = rowItems.filter(item => {
                            const alignedWithTarget = Math.abs(item.rect.left - targetHeaderRect.left) < targetHeaderRect.width / 2 ||
                                                     (item.rect.left > targetHeaderRect.left - 20 && 
                                                      item.rect.right < targetHeaderRect.right + 20);
                            return alignedWithTarget && !item.text.startsWith('$');
                        });
                        
                        if (targetCandidates.length > 0) {
                            // Use the item best aligned with the Target header
                            targetCandidates.sort((a, b) => {
                                return Math.abs(a.rect.left - targetHeaderRect.left) - 
                                       Math.abs(b.rect.left - targetHeaderRect.left);
                            });
                            targetValue = targetCandidates[0].text;
                        }
                        
                        // Find a value horizontally aligned with the RPC header
                        const rpcCandidates = rowItems.filter(item => {
                            const alignedWithRPC = Math.abs(item.rect.left - rpcHeaderRect.left) < rpcHeaderRect.width / 2 ||
                                                  (item.rect.left > rpcHeaderRect.left - 20 && 
                                                   item.rect.right < rpcHeaderRect.right + 20);
                            return alignedWithRPC && item.text.includes('$');
                        });
                        
                        if (rpcCandidates.length > 0) {
                            // Use the item best aligned with the RPC header
                            rpcCandidates.sort((a, b) => {
                                return Math.abs(a.rect.left - rpcHeaderRect.left) - 
                                       Math.abs(b.rect.left - rpcHeaderRect.left);
                            });
                            rpcValue = rpcCandidates[0].text;
                        }
                        
                        // Add row if we found both values
                        if (targetValue && rpcValue) {
                            rows.push({
                                Target: targetValue,
                                RPC: rpcValue
                            });
                        }
                    });
                    
                    if (rows.length > 0) {
                        console.log(`Extracted ${rows.length} rows from highlighted areas`);
                        return {
                            headers: ['Target', 'RPC'],
                            rows: rows
                        };
                    }
                }
                
                return null;
            }
            
            // Try each approach in sequence
            console.log('Starting with ag-Grid extraction...');
            let result = extractAgGridData();
            
            if (!result || !result.rows || result.rows.length === 0) {
                console.log('ag-Grid extraction failed, trying highlighted area extraction...');
                result = extractHighlightedArea();
            }
            
            if (!result || !result.rows || result.rows.length === 0) {
                console.log('Highlighted area extraction failed, trying dollar value extraction...');
                result = findTableWithTargetAndRPC();
            }
            
            if (!result || !result.rows || result.rows.length === 0) {
                console.log('All specific extraction methods failed, trying original methods...');
                
                // Fall back to original extraction methods
                function extractRingbaUI() {
                    // Look for the "Summary" text/header on the page
                    const summaryHeaders = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6, div'))
                        .filter(el => el.textContent.trim() === 'Summary');
                    
                    console.log(`Found ${summaryHeaders.length} Summary headers`);
                    
                    // Look for the campaign/target/publisher column headers in ANY context
                    const targetColumnTextContent = ['Campaign', 'Publisher', 'Target', 'Buyer', 'RPC', 'Revenue'];
                    const columnHeaders = [];
                    
                    // Find all elements with these text contents
                    targetColumnTextContent.forEach(text => {
                        const elements = Array.from(document.querySelectorAll('*'))
                            .filter(el => el.textContent.trim() === text);
                        
                        if (elements.length > 0) {
                            console.log(`Found ${elements.length} elements with text "${text}"`);
                            columnHeaders.push(...elements);
                        }
                    });
                    
                    // If we found column headers, try to find their parent table
                    if (columnHeaders.length > 0) {
                        console.log(`Found ${columnHeaders.length} potential column headers`);
                        
                        // Group headers that are in the same container (likely the same row)
                        const headerGroups = {};
                        columnHeaders.forEach(header => {
                            // Look for parent elements that might be a row or header container
                            let parent = header.parentElement;
                            let depth = 0;
                            const maxDepth = 5; // Don't go too far up the tree
                            
                            while (parent && depth < maxDepth) {
                                const key = parent.tagName + '|' + parent.className;
                                if (!headerGroups[key]) {
                                    headerGroups[key] = { element: parent, headers: [] };
                                }
                                headerGroups[key].headers.push({
                                    element: header,
                                    text: header.textContent.trim()
                                });
                                parent = parent.parentElement;
                                depth++;
                            }
                        });
                        
                        // Find the parent with the most headers (likely the header row)
                        let bestParent = null;
                        let maxHeaders = 0;
                        
                        Object.values(headerGroups).forEach(group => {
                            if (group.headers.length > maxHeaders) {
                                maxHeaders = group.headers.length;
                                bestParent = group.element;
                            }
                        });
                        
                        if (bestParent) {
                            console.log(`Found header container with ${maxHeaders} headers`);
                            
                            // Try to find the table this header belongs to
                            let tableElement = null;
                            let parent = bestParent;
                            let depth = 0;
                            const maxDepth = 5;
                            
                            while (parent && depth < maxDepth) {
                                if (parent.tagName === 'TABLE' || 
                                    parent.getAttribute('role') === 'grid' || 
                                    parent.getAttribute('role') === 'table') {
                                    tableElement = parent;
                                    break;
                                }
                                
                                // Also check if this element contains rows and cells
                                const hasCells = parent.querySelectorAll('td, th, [role="cell"], [role="columnheader"]').length > 0;
                                const hasRows = parent.querySelectorAll('tr, [role="row"]').length > 0;
                                
                                if (hasCells && hasRows) {
                                    tableElement = parent;
                                    break;
                                }
                                
                                parent = parent.parentElement;
                                depth++;
                            }
                            
                            if (tableElement) {
                                console.log('Found table element containing headers');
                                
                                // Extract all headers from this row
                                const headerRow = bestParent;
                                const headerCells = headerRow.querySelectorAll('th, td, div, span');
                                const headers = Array.from(headerCells)
                                    .map(cell => cell.textContent.trim())
                                    .filter(text => text.length > 0);
                                
                                console.log('Extracted headers:', headers);
                                
                                // Find all rows that might contain data
                                // 1. Look for siblings of the header row
                                let dataRows = [];
                                const siblings = [];
                                let sibling = headerRow.nextElementSibling;
                                
                                while (sibling) {
                                    siblings.push(sibling);
                                    sibling = sibling.nextElementSibling;
                                }
                                
                                if (siblings.length > 0) {
                                    console.log(`Found ${siblings.length} sibling rows`);
                                    dataRows = siblings;
                                } else {
                                    // 2. Look for children of the table that are not the header row
                                    const allRows = tableElement.querySelectorAll('tr, [role="row"], div[class*="row"]');
                                    dataRows = Array.from(allRows).filter(row => row !== headerRow);
                                    console.log(`Found ${dataRows.length} potential data rows`);
                                }
                                
                                // Extract data from rows
                                const rows = [];
                                dataRows.forEach(row => {
                                    // Get all cells in this row
                                    const cells = row.querySelectorAll('td, [role="cell"], div, span');
                                    if (cells.length === 0) return;
                                    
                                    const rowData = {};
                                    const cellTexts = Array.from(cells)
                                        .map(cell => cell.textContent.trim())
                                        .filter(text => text.length > 0);
                                    
                                    // Map cell texts to headers
                                    for (let i = 0; i < Math.min(cellTexts.length, headers.length); i++) {
                                        rowData[headers[i]] = cellTexts[i];
                                    }
                                    
                                    // Only include rows with sufficient data
                                    if (Object.keys(rowData).length >= 2) {
                                        rows.push(rowData);
                                    }
                                });
                                
                                console.log(`Extracted ${rows.length} data rows`);
                                return { headers, rows };
                            }
                        }
                    }
                    
                    return null;
                }
                
                result = extractRingbaUI();
            }
            
            return result || { headers: [], rows: [] };
        """)
        
        # Check if we got data from the Ringba UI structure extraction
        if ringba_structure_data and 'rows' in ringba_structure_data and ringba_structure_data['rows']:
            logger.info(f"Successfully extracted {len(ringba_structure_data['rows'])} rows from Ringba UI structure")
            
            # Log the headers we found
            if 'headers' in ringba_structure_data:
                logger.info(f"Extracted headers: {ringba_structure_data['headers']}")
            
            # Convert to DataFrame
            df = pd.DataFrame(ringba_structure_data['rows'])
            
            # Save to CSV file
            file_path = os.path.join("/tmp", f"ringba_ui_extract_{int(time.time())}.csv")
            df.to_csv(file_path, index=False)
            
            logger.info(f"Saved Ringba UI structure data to {file_path}")
            return file_path
        
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
        from flask import Flask, send_from_directory, render_template_string
        import glob
        app = Flask(__name__)
        
        @app.route('/')
        def home():
            return "Ringba Export Service is running. Scheduled runs at 11 AM, 2 PM, and 4:30 PM ET."
        
        @app.route('/screenshots')
        def list_screenshots():
            """List all screenshots with links to view them"""
            screenshots = []
            for file_path in glob.glob(f"{screenshots_dir}/*.png"):
                file_name = os.path.basename(file_path)
                screenshots.append({
                    'name': file_name,
                    'path': f'/screenshots/{file_name}',
                    'timestamp': os.path.getmtime(file_path)
                })
            
            # Sort by timestamp (newest first)
            screenshots.sort(key=lambda x: x['timestamp'], reverse=True)
            
            # Generate HTML to display the screenshots
            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Ringba Screenshots</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    h1 { color: #333; }
                    .screenshot-list { display: flex; flex-wrap: wrap; }
                    .screenshot-item { 
                        margin: 10px; 
                        padding: 10px; 
                        border: 1px solid #ddd; 
                        border-radius: 5px;
                        width: 300px;
                    }
                    img { max-width: 100%; }
                    .timestamp { color: #666; font-size: 12px; }
                </style>
            </head>
            <body>
                <h1>Ringba Screenshots</h1>
                <div class="screenshot-list">
                    {% for screenshot in screenshots %}
                    <div class="screenshot-item">
                        <h3>{{ screenshot.name }}</h3>
                        <div class="timestamp">{{ screenshot.timestamp|datetime }}</div>
                        <a href="{{ screenshot.path }}" target="_blank">
                            <img src="{{ screenshot.path }}" alt="{{ screenshot.name }}">
                        </a>
                    </div>
                    {% endfor %}
                </div>
            </body>
            </html>
            """
            
            # Add datetime filter for timestamp formatting
            from datetime import datetime
            def datetime_filter(timestamp):
                return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            return render_template_string(
                html, 
                screenshots=screenshots,
                datetime=datetime_filter
            )
        
        @app.route('/screenshots/<filename>')
        def get_screenshot(filename):
            """Serve a specific screenshot file"""
            return send_from_directory(screenshots_dir, filename)
        
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
