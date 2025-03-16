#!/usr/bin/env python3
"""
Test script to demonstrate how to use call logs to calculate historical RPC values.
This script will check RPC values for yesterday at 10 AM and 3 PM EST.
"""

import os
import logging
import json
import sys
from datetime import datetime, timedelta
import pytz
import dotenv
from ringba_api import RingbaAPI
from slack_notifier import SlackNotifier

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('historical_calllogs.log')
    ]
)
logger = logging.getLogger('historical_calllogs')

# Window size for RPC calculation (hours before and after the time point)
RPC_WINDOW_HOURS = 1

def get_datetime_window(base_time, window_hours=RPC_WINDOW_HOURS):
    """
    Get a time window centered around base_time
    
    Args:
        base_time (datetime): Center of the window
        window_hours (int): Hours before and after the center
    
    Returns:
        tuple: (start_time, end_time) as datetime objects
    """
    start_time = base_time - timedelta(hours=window_hours)
    end_time = base_time + timedelta(hours=window_hours)
    return start_time, end_time

def get_datetime_yesterday(hour, minute=0):
    """
    Get datetime object for yesterday at specified hour and minute in EST
    
    Args:
        hour (int): Hour in 24-hour format
        minute (int): Minute
    
    Returns:
        datetime: Datetime object for yesterday at specified time
    """
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    yesterday = now - timedelta(days=1)
    return eastern.localize(datetime(
        yesterday.year, 
        yesterday.month, 
        yesterday.day, 
        hour, 
        minute
    ))

def main():
    """Test historical RPC calculation using call logs"""
    logger.info("Starting historical RPC calculation test using call logs")
    
    # Load environment variables
    dotenv.load_dotenv()
    RINGBA_API_TOKEN = os.getenv('RINGBA_API_TOKEN')
    RINGBA_ACCOUNT_ID = os.getenv('RINGBA_ACCOUNT_ID')
    SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
    TARGET_NAME = os.getenv('TARGET_NAME', '')
    RPC_THRESHOLD = float(os.getenv('RPC_THRESHOLD', 10.0))
    
    # Display settings
    logger.info("==== Settings ====")
    logger.info(f"Target Name: {TARGET_NAME if TARGET_NAME else 'ALL targets'}")
    logger.info(f"RPC Threshold: ${RPC_THRESHOLD:.2f}")
    logger.info(f"Account ID: {RINGBA_ACCOUNT_ID}")
    logger.info("=================")
    
    # Validate environment variables
    if not RINGBA_API_TOKEN:
        logger.error("Missing RINGBA_API_TOKEN. Please check your .env file.")
        sys.exit(1)
    
    if not RINGBA_ACCOUNT_ID:
        logger.error("Missing RINGBA_ACCOUNT_ID. Please check your .env file.")
        sys.exit(1)
    
    if not SLACK_WEBHOOK_URL:
        logger.error("Missing SLACK_WEBHOOK_URL. Please check your .env file.")
        sys.exit(1)
    
    # Initialize API client
    ringba_api = RingbaAPI(RINGBA_API_TOKEN, RINGBA_ACCOUNT_ID)
    slack_notifier = SlackNotifier(SLACK_WEBHOOK_URL)
    
    # Send notification that we're starting
    try:
        slack_notifier.send_notification("🔍 *Historical RPC Calculation Test Started*\nUsing call logs to calculate RPC for yesterday at 10 AM and 3 PM EST")
    except Exception as e:
        logger.error(f"Failed to send initial notification to Slack: {str(e)}")
        # Continue anyway, as this is not critical
    
    # Test authentication
    try:
        ringba_api.authenticate()
        logger.info("Successfully authenticated with Ringba API")
    except Exception as e:
        error_message = f"Failed to authenticate with Ringba API: {str(e)}"
        logger.error(error_message)
        slack_notifier.send_notification(f"❌ *Error in Historical Calculation Test*\n{error_message}")
        sys.exit(1)
    
    try:
        # Get all enabled targets
        targets_data = ringba_api.get_all_targets(include_stats=True, enabled_only=True)
        all_targets = targets_data.get('targets', [])
        
        if not all_targets:
            message = "No active targets found in Ringba account"
            logger.warning(message)
            slack_notifier.send_notification(f"⚠️ *Warning*\n{message}")
            return
        
        logger.info(f"Found {len(all_targets)} active targets")
        
        # Get time points for yesterday at 10 AM and 3 PM EST
        yesterday_10am = get_datetime_yesterday(10)
        yesterday_3pm = get_datetime_yesterday(15)
        
        # Get time windows
        morning_start, morning_end = get_datetime_window(yesterday_10am)
        afternoon_start, afternoon_end = get_datetime_window(yesterday_3pm)
        
        # Store results
        morning_results = []
        afternoon_results = []
        dropped_targets = []
        
        # Calculate RPC for each target at both time points
        for target in all_targets:
            target_id = target.get('id')
            target_name = target.get('name')
            
            # Skip if we're monitoring a specific target and this isn't it
            if TARGET_NAME and TARGET_NAME.lower() != 'all' and target_name != TARGET_NAME:
                continue
            
            # Calculate morning RPC
            logger.info(f"Calculating morning RPC for {target_name} (ID: {target_id}) - Time window: {morning_start} to {morning_end}")
            morning_rpc = ringba_api.get_historical_rpc_by_call_logs(target_id, target_name, morning_start, morning_end)
            
            # Log and store result
            logger.info(f"Morning RPC for {target_name}: ${morning_rpc:.2f}")
            
            morning_result = {
                'id': target_id,
                'name': target_name,
                'rpc': morning_rpc
            }
            morning_results.append(morning_result)
            
            # Calculate afternoon RPC
            logger.info(f"Calculating afternoon RPC for {target_name} (ID: {target_id}) - Time window: {afternoon_start} to {afternoon_end}")
            afternoon_rpc = ringba_api.get_historical_rpc_by_call_logs(target_id, target_name, afternoon_start, afternoon_end)
            
            # Log and store result
            logger.info(f"Afternoon RPC for {target_name}: ${afternoon_rpc:.2f}")
            
            afternoon_result = {
                'id': target_id,
                'name': target_name,
                'rpc': afternoon_rpc
            }
            afternoon_results.append(afternoon_result)
            
            # Check for drops below threshold
            if morning_rpc >= RPC_THRESHOLD and afternoon_rpc < RPC_THRESHOLD:
                drop_percentage = ((morning_rpc - afternoon_rpc) / morning_rpc) * 100 if morning_rpc > 0 else 0
                
                dropped_targets.append({
                    'id': target_id,
                    'name': target_name,
                    'morning_rpc': morning_rpc,
                    'afternoon_rpc': afternoon_rpc,
                    'drop_percentage': drop_percentage
                })
        
        # Send notification with morning results
        morning_message = f"📊 *Yesterday's Morning RPC Values (10 AM EST)*\n\n"
        
        # Sort by RPC value (high to low)
        morning_results.sort(key=lambda x: x['rpc'], reverse=True)
        
        for result in morning_results:
            morning_message += f"• *{result['name']}*: ${result['rpc']:.2f}\n"
        
        morning_message += f"\nTargets above ${RPC_THRESHOLD:.2f} threshold: {len([r for r in morning_results if r['rpc'] >= RPC_THRESHOLD])}"
        
        slack_notifier.send_notification(morning_message)
        
        # Send notification with afternoon results
        afternoon_message = f"📊 *Yesterday's Afternoon RPC Values (3 PM EST)*\n\n"
        
        # Sort by RPC value (high to low)
        afternoon_results.sort(key=lambda x: x['rpc'], reverse=True)
        
        for result in afternoon_results:
            afternoon_message += f"• *{result['name']}*: ${result['rpc']:.2f}\n"
        
        afternoon_message += f"\nTargets above ${RPC_THRESHOLD:.2f} threshold: {len([r for r in afternoon_results if r['rpc'] >= RPC_THRESHOLD])}"
        
        slack_notifier.send_notification(afternoon_message)
        
        # Send notification for dropped targets
        if dropped_targets:
            dropped_message = f"⚠️ *Targets That Dropped Below ${RPC_THRESHOLD:.2f} RPC*\n\n"
            
            for target in dropped_targets:
                dropped_message += f"• *{target['name']}*\n" \
                                 f"  - Morning RPC: *${target['morning_rpc']:.2f}*\n" \
                                 f"  - Afternoon RPC: *${target['afternoon_rpc']:.2f}*\n" \
                                 f"  - Drop: *{target['drop_percentage']:.1f}%*\n\n"
            
            slack_notifier.send_notification(dropped_message)
            logger.info(f"Found {len(dropped_targets)} targets that dropped below threshold")
        else:
            logger.info("No targets dropped below threshold")
            slack_notifier.send_notification("✅ *Results*\nNo targets dropped below threshold yesterday")
        
        # Test complete
        slack_notifier.send_notification("✅ *Historical RPC Calculation Test Completed Successfully*")
        logger.info("Historical RPC calculation test completed successfully")
    
    except Exception as e:
        error_message = f"Error in historical calculation test: {str(e)}"
        logger.error(error_message)
        slack_notifier.send_notification(f"❌ *Error in Historical Calculation Test*\n{error_message}")
        sys.exit(1)

if __name__ == "__main__":
    main() 