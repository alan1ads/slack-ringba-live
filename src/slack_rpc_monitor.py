#!/usr/bin/env python3
"""
Ringba RPC Monitor with Slack Integration

This script performs two daily checks:
1. Morning check (10am EST): Finds all targets with RPC above $10
2. Afternoon check (3pm EST): Checks if any morning targets fell below $10 RPC

Notifications are sent to Slack for both checks.
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import pytz
import pickle
import schedule

# Import custom modules
from ringba_api import RingbaAPI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('slack_rpc_monitor.log')
    ]
)
logger = logging.getLogger('slack_rpc_monitor')

# File to store morning targets
MORNING_TARGETS_FILE = 'morning_targets.pkl'

# RPC threshold
RPC_THRESHOLD = 10.0

def send_slack_message(message, blocks=None):
    """
    Send a message to Slack using the webhook URL from environment variables
    
    Args:
        message (str): The text message to send
        blocks (list, optional): List of Slack blocks for formatted messages
    
    Returns:
        bool: Whether the message was sent successfully
    """
    # Get Slack webhook URL from environment variables
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    
    if not webhook_url:
        logger.error("No Slack webhook URL found in environment variables")
        return False
    
    # Prepare the payload
    payload = {
        "text": message
    }
    
    # Add blocks if provided
    if blocks:
        payload["blocks"] = blocks
    
    try:
        # Send the message to Slack
        response = requests.post(webhook_url, json=payload)
        
        if response.status_code == 200:
            logger.info("Slack message sent successfully")
            return True
        else:
            logger.error(f"Failed to send Slack message: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending Slack message: {str(e)}")
        return False

def format_target_for_slack(target, is_morning=True):
    """
    Format a target as a Slack message block
    
    Args:
        target (dict): The target data
        is_morning (bool): Whether this is a morning notification (True) or afternoon (False)
    
    Returns:
        dict: A Slack block for the target
    """
    emoji = ":chart_with_upwards_trend:" if is_morning else ":chart_with_downwards_trend:"
    color = "#36a64f" if is_morning else "#ff5252"
    
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*{target['name']}*\n{emoji} RPC: *${target['rpc']:.2f}* | Calls: {target.get('calls', 'N/A')} | Revenue: ${target.get('revenue', 0):.2f}"
        },
        "accessory": {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "View in Ringba"
            },
            "url": f"https://app.ringba.com/targets/{target['id']}/overview"
        }
    }

def morning_check():
    """
    Perform the morning check (10am EST) to find targets with RPC above threshold
    """
    logger.info(f"Performing morning check for targets with RPC above ${RPC_THRESHOLD}")
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token or not account_id:
        logger.error("Missing API token or account ID in .env file")
        return
    
    # Initialize API
    api = RingbaAPI(api_token, account_id)
    
    # Test authentication
    if not api.test_auth():
        logger.error("Authentication failed")
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during morning check")
        return
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get all targets
    response = api.get_all_targets()
    if not response or 'targets' not in response:
        logger.error("Failed to retrieve targets")
        send_slack_message(f"⚠️ *ALERT*: Failed to retrieve targets during morning check")
        return
    
    targets = response['targets']
    
    # Filter enabled targets
    enabled_targets = [t for t in targets if isinstance(t, dict) and t.get('enabled', False)]
    
    logger.info(f"Found {len(enabled_targets)} enabled targets out of {len(targets)} total targets")
    
    # Collect RPC data for enabled targets
    targets_above_threshold = []
    
    for target in enabled_targets:
        target_id = target.get('id')
        target_name = target.get('name', 'Unknown')
        
        # Calculate RPC for this target
        rpc = api.calculate_rpc_for_target(target_id, today)
        
        if rpc is not None and rpc >= RPC_THRESHOLD:
            # Get the calls and revenue
            counts = api.get_target_counts(target_id, today)
            calls = 0
            revenue = 0
            
            if counts and isinstance(counts, dict):
                calls = counts.get('totalCalls', 0)
                revenue = counts.get('payout', 0)
            
            targets_above_threshold.append({
                'id': target_id,
                'name': target_name,
                'rpc': rpc,
                'calls': calls,
                'revenue': revenue
            })
    
    # Save the targets above threshold for afternoon comparison
    with open(MORNING_TARGETS_FILE, 'wb') as f:
        pickle.dump(targets_above_threshold, f)
    
    # Send Slack notification if any targets are above threshold
    if targets_above_threshold:
        # Sort by RPC (highest first)
        targets_above_threshold.sort(key=lambda x: x['rpc'], reverse=True)
        
        # Prepare Slack message
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔔 Morning RPC Alert - {today}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(targets_above_threshold)}* targets have RPC above *${RPC_THRESHOLD}*"
                }
            },
            {"type": "divider"}
        ]
        
        # Add each target as a block
        for target in targets_above_threshold:
            blocks.append(format_target_for_slack(target, is_morning=True))
        
        # Send to Slack
        send_slack_message(
            f"Morning RPC Alert: {len(targets_above_threshold)} targets above ${RPC_THRESHOLD}",
            blocks=blocks
        )
        
        logger.info(f"Found {len(targets_above_threshold)} targets above ${RPC_THRESHOLD} RPC in morning check")
    else:
        # Send notification that no targets are above threshold
        send_slack_message(f"🔔 Morning RPC Alert: No targets found with RPC above ${RPC_THRESHOLD}")
        logger.info("No targets found above RPC threshold in morning check")

def afternoon_check():
    """
    Perform the afternoon check (3pm EST) to find morning targets that fell below threshold
    """
    logger.info(f"Performing afternoon check for targets that fell below ${RPC_THRESHOLD}")
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token or not account_id:
        logger.error("Missing API token or account ID in .env file")
        return
    
    # Try to load morning targets
    try:
        with open(MORNING_TARGETS_FILE, 'rb') as f:
            morning_targets = pickle.load(f)
    except FileNotFoundError:
        logger.warning("No morning targets file found. Skipping afternoon check.")
        send_slack_message("⚠️ *ALERT*: No morning targets data found for afternoon comparison")
        return
    except Exception as e:
        logger.error(f"Error loading morning targets: {str(e)}")
        send_slack_message(f"⚠️ *ALERT*: Error loading morning targets data: {str(e)}")
        return
    
    # If no morning targets, nothing to check
    if not morning_targets:
        logger.info("No morning targets were above threshold. Nothing to check.")
        return
    
    # Initialize API
    api = RingbaAPI(api_token, account_id)
    
    # Test authentication
    if not api.test_auth():
        logger.error("Authentication failed")
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during afternoon check")
        return
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Check each morning target
    targets_below_threshold = []
    
    for target in morning_targets:
        target_id = target.get('id')
        target_name = target.get('name', 'Unknown')
        morning_rpc = target.get('rpc', 0)
        
        # Calculate current RPC
        current_rpc = api.calculate_rpc_for_target(target_id, today)
        
        if current_rpc is not None and current_rpc < RPC_THRESHOLD:
            # Get the calls and revenue
            counts = api.get_target_counts(target_id, today)
            calls = 0
            revenue = 0
            
            if counts and isinstance(counts, dict):
                calls = counts.get('totalCalls', 0)
                revenue = counts.get('payout', 0)
            
            # Calculate the RPC change
            rpc_change = current_rpc - morning_rpc
            
            targets_below_threshold.append({
                'id': target_id,
                'name': target_name,
                'rpc': current_rpc,
                'morning_rpc': morning_rpc,
                'rpc_change': rpc_change,
                'calls': calls,
                'revenue': revenue
            })
    
    # Send Slack notification if any targets fell below threshold
    if targets_below_threshold:
        # Sort by RPC change (biggest drop first)
        targets_below_threshold.sort(key=lambda x: x['rpc_change'])
        
        # Prepare Slack message
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔔 Afternoon RPC Alert - {today}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(targets_below_threshold)}* targets have fallen below *${RPC_THRESHOLD}* RPC since the morning check"
                }
            },
            {"type": "divider"}
        ]
        
        # Add each target as a block
        for target in targets_below_threshold:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{target['name']}*\n:chart_with_downwards_trend: Morning RPC: *${target['morning_rpc']:.2f}* → Current RPC: *${target['rpc']:.2f}* (Change: ${target['rpc_change']:.2f})\nCalls: {target.get('calls', 'N/A')} | Revenue: ${target.get('revenue', 0):.2f}"
                },
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View in Ringba"
                    },
                    "url": f"https://app.ringba.com/targets/{target['id']}/overview"
                }
            })
        
        # Send to Slack
        send_slack_message(
            f"Afternoon RPC Alert: {len(targets_below_threshold)} targets fell below ${RPC_THRESHOLD}",
            blocks=blocks
        )
        
        logger.info(f"Found {len(targets_below_threshold)} targets that fell below ${RPC_THRESHOLD} RPC in afternoon check")
    else:
        # Send notification that no targets fell below threshold
        morning_count = len(morning_targets)
        send_slack_message(f"🔔 Afternoon RPC Alert: All {morning_count} morning targets are still above ${RPC_THRESHOLD} RPC")
        logger.info(f"No targets fell below RPC threshold in afternoon check out of {morning_count} morning targets")

def manual_run():
    """Run both checks manually for testing"""
    print("Running morning check...")
    morning_check()
    
    print("\nRunning afternoon check...")
    afternoon_check()

def schedule_jobs():
    """Schedule the morning and afternoon checks"""
    # Define timezone for EST
    eastern = pytz.timezone('US/Eastern')
    
    # Schedule the morning check at 10am EST
    schedule.every().day.at("10:00").do(morning_check)
    logger.info("Scheduled morning check for 10:00am EST")
    
    # Schedule the afternoon check at 3pm EST
    schedule.every().day.at("15:00").do(afternoon_check)
    logger.info("Scheduled afternoon check for 3:00pm EST")
    
    # Run the scheduler
    logger.info("Starting scheduler. Press Ctrl+C to exit.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "morning":
            # Run morning check only
            morning_check()
        elif sys.argv[1] == "afternoon":
            # Run afternoon check only
            afternoon_check()
        elif sys.argv[1] == "test":
            # Run both checks for testing
            manual_run()
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage: python slack_rpc_monitor.py [morning|afternoon|test]")
            print("       If no arguments are provided, the script will run as a scheduler")
    else:
        # Run as scheduler by default
        schedule_jobs() 