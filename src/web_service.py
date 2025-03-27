#!/usr/bin/env python3
"""
Web service version of the Ringba exporter
Runs continuously and executes exports at scheduled times
"""

import os
import time
import logging
from datetime import datetime, timedelta
import pytz
from flask import Flask, jsonify
import threading
import sys
import signal
from simple_export import export_csv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('web_service.log')
    ]
)
logger = logging.getLogger('web_service')

# Create Flask app
app = Flask(__name__)

# Global variables for last run tracking
last_morning_run = None
last_midday_run = None
last_afternoon_run = None

def perform_test_run():
    """Perform a test run when the service is first deployed"""
    logger.info("Performing initial test run for web service deployment...")
    try:
        # Set environment variable to identify this as a test run
        os.environ['RUN_LABEL'] = 'test'
        
        # Run the export with today's date
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

def is_time_to_run(target_hour, target_minute, last_run_time):
    """Check if it's time to run based on target time and last run"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(pytz.utc).astimezone(eastern)
    
    # If we've already run today, don't run again
    if last_run_time and last_run_time.date() == now.date():
        return False
    
    # Check if we're within the run window (5 minutes before/after target time)
    current_hour = now.hour
    current_minute = now.minute
    
    # Convert times to minutes for easier comparison
    current_time_mins = current_hour * 60 + current_minute
    target_time_mins = target_hour * 60 + target_minute
    
    # Return True if within 5 minute window of target time
    return abs(current_time_mins - target_time_mins) <= 5

def scheduled_task():
    """Main scheduled task that checks times and runs exports"""
    global last_morning_run, last_midday_run, last_afternoon_run
    
    # Get scheduled times from environment
    try:
        morning_time = os.getenv('MORNING_CHECK_TIME', '11:00')
        midday_time = os.getenv('MIDDAY_CHECK_TIME', '14:00')
        afternoon_time = os.getenv('AFTERNOON_CHECK_TIME', '16:30')
        
        morning_hour = int(morning_time.split(':')[0])
        morning_minute = int(morning_time.split(':')[1]) if ':' in morning_time else 0
        
        midday_hour = int(midday_time.split(':')[0])
        midday_minute = int(midday_time.split(':')[1]) if ':' in midday_time else 0
        
        afternoon_hour = int(afternoon_time.split(':')[0])
        afternoon_minute = int(afternoon_time.split(':')[1]) if ':' in afternoon_time else 0
    except Exception as e:
        logger.error(f"Error parsing time settings: {str(e)}")
        # Fallback to default times
        morning_hour, morning_minute = 11, 0
        midday_hour, midday_minute = 14, 0
        afternoon_hour, afternoon_minute = 16, 30
    
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(pytz.utc).astimezone(eastern)
    logger.info(f"Checking scheduled tasks at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Morning run check (11 AM ET)
    if is_time_to_run(morning_hour, morning_minute, last_morning_run):
        logger.info(f"Starting morning run at {now.strftime('%H:%M:%S')}")
        try:
            # Set environment variable for the run type
            os.environ['RUN_LABEL'] = 'morning'
            # Run the export
            export_csv()
            # Update last run time
            last_morning_run = now
            logger.info("Morning run completed successfully")
        except Exception as e:
            logger.error(f"Error in morning run: {str(e)}")
    
    # Midday run check (2 PM ET)
    if is_time_to_run(midday_hour, midday_minute, last_midday_run):
        logger.info(f"Starting midday run at {now.strftime('%H:%M:%S')}")
        try:
            # Set environment variable for the run type
            os.environ['RUN_LABEL'] = 'midday'
            # Run the export
            export_csv()
            # Update last run time
            last_midday_run = now
            logger.info("Midday run completed successfully")
        except Exception as e:
            logger.error(f"Error in midday run: {str(e)}")
    
    # Afternoon run check (4:30 PM ET)
    if is_time_to_run(afternoon_hour, afternoon_minute, last_afternoon_run):
        logger.info(f"Starting afternoon run at {now.strftime('%H:%M:%S')}")
        try:
            # Set environment variable for the run type
            os.environ['RUN_LABEL'] = 'afternoon'
            # Run the export
            export_csv()
            # Update last run time
            last_afternoon_run = now
            logger.info("Afternoon run completed successfully")
        except Exception as e:
            logger.error(f"Error in afternoon run: {str(e)}")

def scheduler_thread():
    """Background thread that runs scheduled tasks"""
    logger.info("Starting scheduler thread")
    while True:
        try:
            scheduled_task()
        except Exception as e:
            logger.error(f"Error in scheduler: {str(e)}")
        
        # Check every minute
        time.sleep(60)

@app.route('/')
def home():
    """Home endpoint that shows status"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(pytz.utc).astimezone(eastern)
    
    morning_time = os.getenv('MORNING_CHECK_TIME', '11:00')
    midday_time = os.getenv('MIDDAY_CHECK_TIME', '14:00')
    afternoon_time = os.getenv('AFTERNOON_CHECK_TIME', '16:30')
    
    return jsonify({
        'status': 'running',
        'current_time': now.strftime('%Y-%m-%d %H:%M:%S %Z'),
        'scheduled_times': {
            'morning': morning_time + ' ET',
            'midday': midday_time + ' ET',
            'afternoon': afternoon_time + ' ET'
        },
        'last_runs': {
            'morning': last_morning_run.strftime('%Y-%m-%d %H:%M:%S %Z') if last_morning_run else None,
            'midday': last_midday_run.strftime('%Y-%m-%d %H:%M:%S %Z') if last_midday_run else None,
            'afternoon': last_afternoon_run.strftime('%Y-%m-%d %H:%M:%S %Z') if last_afternoon_run else None
        },
        'rpc_threshold': os.getenv('RPC_THRESHOLD', '12.0')
    })

@app.route('/trigger/<run_type>')
def trigger_run(run_type):
    """Manually trigger a specific run type"""
    if run_type not in ['morning', 'midday', 'afternoon']:
        return jsonify({'error': 'Invalid run type. Must be morning, midday, or afternoon'}), 400
    
    # Create a thread to run the export to avoid blocking the request
    def run_export():
        global last_morning_run, last_midday_run, last_afternoon_run
        try:
            # Set environment variable for the run type
            os.environ['RUN_LABEL'] = run_type
            # Run the export
            export_csv()
            # Update last run time
            now = datetime.now(pytz.utc).astimezone(pytz.timezone('US/Eastern'))
            if run_type == 'morning':
                last_morning_run = now
            elif run_type == 'midday':
                last_midday_run = now
            else:
                last_afternoon_run = now
            logger.info(f"Manually triggered {run_type} run completed successfully")
        except Exception as e:
            logger.error(f"Error in manually triggered {run_type} run: {str(e)}")
    
    # Start the thread
    thread = threading.Thread(target=run_export)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'status': 'triggered',
        'run_type': run_type,
        'message': f"{run_type} run has been triggered and is running in the background"
    })

if __name__ == '__main__':
    # Perform a test run when the service first starts
    logger.info("Web service starting up - performing initial test run...")
    perform_test_run()
    logger.info("Initial test run complete, starting scheduler...")
    
    # Start the scheduler in a background thread
    scheduler = threading.Thread(target=scheduler_thread)
    scheduler.daemon = True
    scheduler.start()
    
    # Start the web server
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port) 