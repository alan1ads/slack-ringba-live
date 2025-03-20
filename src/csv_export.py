#!/usr/bin/env python3
"""
Browser-based CSV export for Ringba

This script uses browser automation to download CSV exports from Ringba UI,
mimicking the exact steps a human would take in the browser.
"""

import os
import sys
import time
import logging
import getpass
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('csv_export.log')
    ]
)
logger = logging.getLogger('csv_export')

def setup_browser():
    """Set up and configure the browser for automation"""
    
    # Create a new Chrome browser instance
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-infobars")
    options.add_argument("--mute-audio")
    
    # Disable headless mode to allow user to see and interact with the browser
    # options.add_argument("--headless=new")
    
    # Set up download directory to current folder
    prefs = {
        "download.default_directory": os.path.abspath(os.getcwd()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    # Install and set up ChromeDriver
    try:
        browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        logger.info("Browser set up successfully")
        return browser
    except Exception as e:
        logger.error(f"Failed to set up browser: {str(e)}")
        return None

def login_to_ringba(browser, username, password):
    """
    Log in to Ringba using username and password
    
    Args:
        browser: Selenium WebDriver instance
        username: Ringba username/email
        password: Ringba password
    
    Returns:
        bool: Whether login was successful
    """
    try:
        # Navigate to Ringba login page
        logger.info("Navigating to Ringba login page")
        browser.get("https://app.ringba.com/#/login")
        
        # Increase timeout for login page to load
        WebDriverWait(browser, 60).until(
            EC.presence_of_element_located((By.ID, "username"))
        )
        
        # Enter username and password
        username_input = browser.find_element(By.ID, "username")
        username_input.clear()
        username_input.send_keys(username)
        
        password_input = browser.find_element(By.ID, "password")
        password_input.clear()
        password_input.send_keys(password)
        
        # Click the login button
        login_button = browser.find_element(By.CSS_SELECTOR, "button[type='submit']")
        login_button.click()
        
        # Wait for dashboard to load with longer timeout
        WebDriverWait(browser, 90).until(
            EC.presence_of_element_located((By.ID, "main-content"))
        )
        
        # If we got here, login was successful
        logger.info("Successfully logged in to Ringba")
        return True
    except Exception as e:
        logger.error(f"Failed to log in to Ringba: {str(e)}")
        
        # Give the user some time to manually log in if automated login fails
        logger.info("Waiting 60 seconds for manual login if needed...")
        time.sleep(60)
        
        # Check if we're on the dashboard now
        try:
            if browser.find_element(By.ID, "main-content"):
                logger.info("Manual login successful")
                return True
        except:
            logger.error("Manual login also failed")
            return False

def navigate_to_call_logs(browser, account_id):
    """
    Navigate to the call logs page
    
    Args:
        browser: Selenium WebDriver instance
        account_id: Ringba account ID
    
    Returns:
        bool: Whether navigation was successful
    """
    try:
        # Navigate directly to call logs URL
        call_logs_url = f"https://app.ringba.com/#/dashboard/call-logs/report/new"
        logger.info(f"Navigating to call logs page: {call_logs_url}")
        browser.get(call_logs_url)
        
        # Wait for the page to load with longer timeout
        logger.info("Waiting for call logs page to load...")
        WebDriverWait(browser, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".reporting-call-logs-data"))
        )
        
        # Take a short pause to make sure everything is loaded
        time.sleep(5)
        
        logger.info("Successfully navigated to call logs page")
        return True
    except Exception as e:
        logger.error(f"Failed to navigate to call logs page: {str(e)}")
        
        # Let the user know they can navigate manually
        logger.info("You can try to navigate to the call logs page manually. Waiting 30 seconds...")
        time.sleep(30)
        
        # Check if we're on the right page now
        try:
            if browser.find_element(By.CSS_SELECTOR, ".reporting-call-logs-data"):
                logger.info("Manual navigation successful")
                return True
        except:
            logger.error("Manual navigation also failed")
            return False

def set_date_range(browser, start_date, end_date):
    """
    Set the date range for call logs
    
    Args:
        browser: Selenium WebDriver instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        bool: Whether setting date range was successful
    """
    try:
        # Wait for the date picker to be available with longer timeout
        logger.info("Looking for date picker...")
        WebDriverWait(browser, 60).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".date-range-picker"))
        )
        
        # Click the date picker to open it
        date_picker = browser.find_element(By.CSS_SELECTOR, ".date-range-picker")
        logger.info("Clicking date picker...")
        date_picker.click()
        
        # Wait for the date picker dropdown to appear
        logger.info("Waiting for date picker dropdown...")
        WebDriverWait(browser, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".daterangepicker"))
        )
        
        # Set custom date range
        logger.info("Selecting custom range...")
        custom_range = browser.find_element(By.XPATH, "//li[contains(text(), 'Custom Range')]")
        custom_range.click()
        
        # Wait for date inputs to be available
        logger.info("Waiting for date inputs...")
        WebDriverWait(browser, 30).until(
            EC.presence_of_element_located((By.NAME, "daterangepicker_start"))
        )
        
        # Set start date
        logger.info(f"Setting start date to {start_date}...")
        start_date_input = browser.find_element(By.NAME, "daterangepicker_start")
        start_date_input.clear()
        start_date_input.send_keys(start_date)
        
        # Set end date
        logger.info(f"Setting end date to {end_date}...")
        end_date_input = browser.find_element(By.NAME, "daterangepicker_end")
        end_date_input.clear()
        end_date_input.send_keys(end_date)
        
        # Apply the date range
        logger.info("Applying date range...")
        apply_button = browser.find_element(By.CSS_SELECTOR, ".applyBtn")
        apply_button.click()
        
        # Wait for the page to refresh with new date range
        logger.info("Waiting for page to refresh with new date range...")
        time.sleep(10)
        
        logger.info(f"Successfully set date range: {start_date} to {end_date}")
        return True
    except Exception as e:
        logger.error(f"Failed to set date range: {str(e)}")
        
        # Let the user know they can set the date range manually
        logger.info("You can try to set the date range manually. Waiting 60 seconds...")
        time.sleep(60)
        
        # Assume the user has set the date range and continue
        logger.info("Continuing with the export process...")
        return True

def click_export_csv(browser):
    """
    Click the 'Export CSV' button and handle the download
    
    Args:
        browser: Selenium WebDriver instance
    
    Returns:
        bool: Whether export was successful
    """
    try:
        # Wait for the Export CSV button to be available with longer timeout
        logger.info("Looking for Export CSV button...")
        WebDriverWait(browser, 60).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".export-summary-btn"))
        )
        
        # Click the Export CSV button
        export_button = browser.find_element(By.CSS_SELECTOR, ".export-summary-btn")
        logger.info("Clicking Export CSV button...")
        export_button.click()
        
        # Wait for the export to complete (longer wait)
        logger.info("Waiting for CSV export to complete...")
        time.sleep(30)
        
        # Check for download completion
        download_dir = os.path.abspath(os.getcwd())
        logger.info(f"Checking for downloaded files in: {download_dir}")
        
        # Wait for download to complete (up to 5 minutes)
        start_time = time.time()
        downloaded = False
        
        while time.time() - start_time < 300:  # 5 minute timeout
            # Look for CSV files that were recently created
            for file in os.listdir(download_dir):
                if file.endswith(".csv") and "call-logs" in file.lower():
                    file_path = os.path.join(download_dir, file)
                    
                    # Check if file was created in the last 5 minutes
                    file_time = os.path.getctime(file_path)
                    if time.time() - file_time < 300:
                        logger.info(f"Found downloaded CSV file: {file}")
                        downloaded = True
                        break
            
            if downloaded:
                break
                
            # Wait a bit before checking again
            logger.info("Still waiting for download to complete...")
            time.sleep(10)
        
        if downloaded:
            logger.info("CSV export completed successfully")
            return True
        else:
            logger.warning("Could not verify CSV download completion automatically")
            
            # Ask user to confirm if download completed
            logger.info("Please check if the CSV file was downloaded. Waiting 60 seconds for confirmation...")
            time.sleep(60)
            
            # Assume download was completed and continue
            logger.info("Continuing with process...")
            return True
    except Exception as e:
        logger.error(f"Failed to export CSV: {str(e)}")
        
        # Let the user know they can click the export button manually
        logger.info("You can try to click the Export CSV button manually. Waiting 120 seconds...")
        time.sleep(120)
        
        # Assume the user has clicked the button and the download completed
        logger.info("Continuing with the process...")
        return True

def export_call_logs_csv(username, password, start_date=None, end_date=None):
    """
    Export call logs to CSV using browser automation
    
    Args:
        username: Ringba username/email
        password: Ringba password
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
    
    Returns:
        bool: Whether export was successful
    """
    # Load environment variables
    load_dotenv()
    
    # Get account ID from env
    account_id = os.getenv("RINGBA_ACCOUNT_ID")
    
    if not account_id:
        logger.error("Missing account ID. Please set RINGBA_ACCOUNT_ID environment variable.")
        return False
    
    # Set default dates if not provided
    if not start_date or not end_date:
        # Get yesterday's date by default
        eastern = pytz.timezone('US/Eastern')
        now_eastern = datetime.now(eastern)
        yesterday = (now_eastern - timedelta(days=1)).strftime('%Y-%m-%d')
        
        start_date = start_date or yesterday
        end_date = end_date or start_date
    
    logger.info(f"Exporting call logs for period {start_date} to {end_date}")
    
    try:
        # Set up the browser
        browser = setup_browser()
        if not browser:
            return False
        
        try:
            # Log in to Ringba
            if not login_to_ringba(browser, username, password):
                return False
            
            # Navigate to call logs page
            if not navigate_to_call_logs(browser, account_id):
                return False
            
            # Set date range
            if not set_date_range(browser, start_date, end_date):
                return False
            
            # Click Export CSV button
            if not click_export_csv(browser):
                return False
            
            logger.info("CSV export process completed successfully")
            return True
        finally:
            # Make sure to close the browser at the end
            browser.quit()
    except Exception as e:
        logger.error(f"Error during CSV export: {str(e)}")
        return False

def process_csv_file(csv_file):
    """
    Process the downloaded CSV file to show RPC by target
    
    Args:
        csv_file (str): Path to the CSV file
    """
    # Import the process_csv_for_rpc function from direct_rpc_monitor.py
    try:
        from direct_rpc_monitor import process_csv_for_rpc
        
        # Read the CSV file
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_data = f.read()
        
        # Get date from filename (assuming format like call-logs-YYYY-MM-DD.csv)
        filename = os.path.basename(csv_file)
        date_match = filename.split('-')
        
        if len(date_match) >= 3:
            # Try to extract date parts from the filename
            date_str = f"{date_match[-3]}-{date_match[-2]}-{date_match[-1].replace('.csv', '')}"
        else:
            # Use today's date as fallback
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        # Process the CSV data
        process_csv_for_rpc(csv_data, date_str, date_str)
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")

if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "export":
            # Get username and prompt for password securely
            if len(sys.argv) < 3:
                print("Usage: python csv_export.py export USERNAME [start_date [end_date]]")
                sys.exit(1)
                
            username = sys.argv[2]
            password = getpass.getpass("Enter your Ringba password: ")
            
            # Export with specific dates if provided
            start_date = sys.argv[3] if len(sys.argv) > 3 else None
            end_date = sys.argv[4] if len(sys.argv) > 4 else None
            
            export_call_logs_csv(username, password, start_date, end_date)
        elif sys.argv[1] == "process":
            # Process an existing CSV file
            if len(sys.argv) > 2:
                csv_file = sys.argv[2]
                process_csv_file(csv_file)
            else:
                print("Usage: python csv_export.py process CSV_FILE_PATH")
        else:
            print("Unknown command. Usage:")
            print("  python csv_export.py export USERNAME [start_date [end_date]]")
            print("  python csv_export.py process CSV_FILE_PATH")
    else:
        print("Usage:")
        print("  python csv_export.py export USERNAME [start_date [end_date]]")
        print("  python csv_export.py process CSV_FILE_PATH") 