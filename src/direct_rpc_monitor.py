#!/usr/bin/env python3
"""
Improved Ringba RPC Monitor with Slack Integration using Direct API

This script performs two daily checks using Ringba's direct insights API:
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

# Import the new direct API client
from ringba_direct_api import RingbaDirectAPI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('direct_rpc_monitor.log')
    ]
)
logger = logging.getLogger('direct_rpc_monitor')

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
    
    # Format tags information if available
    tags_text = ""
    if target.get('tags'):
        tags = target['tags']
        if isinstance(tags, dict):
            # Tags are in format {tag_name: count}
            top_tags = sorted(tags.items(), key=lambda x: x[1], reverse=True)[:3]  # Get top 3 tags
            if top_tags:
                tags_text = "\n:label: *Tags*: " + ", ".join([f"{tag} ({count})" for tag, count in top_tags])
        elif isinstance(tags, list):
            # Tags are in a list format
            if len(tags) > 0:
                tags_text = "\n:label: *Tags*: " + ", ".join(tags[:3])  # Show up to 3 tags
    
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*{target['name']}*\n{emoji} RPC: *${target['rpc']:.2f}* | Calls: {target.get('calls', 'N/A')} | Revenue: ${target.get('revenue', 0):.2f}{tags_text}"
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
    using the direct insights API
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
    
    # Initialize the direct API client
    api = RingbaDirectAPI(api_token, account_id)
    
    # Test authentication
    if not api.test_auth():
        logger.error("Authentication failed")
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during morning check")
        return
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Checking real-time data for date: {today} at {datetime.now().strftime('%H:%M:%S')}")
    
    # Get targets above threshold using the direct method
    targets_above_threshold = api.get_targets_above_threshold(RPC_THRESHOLD, today)
    
    # Log the number of targets found
    logger.info(f"Found {len(targets_above_threshold)} targets above RPC threshold")
    
    # Save the targets above threshold for afternoon comparison
    with open(MORNING_TARGETS_FILE, 'wb') as f:
        pickle.dump(targets_above_threshold, f)
    
    # Send Slack notification if any targets are above threshold
    if targets_above_threshold:
        # Sort by RPC (highest first)
        targets_above_threshold.sort(key=lambda x: x['rpc'], reverse=True)
        
        # Prepare Slack message
        current_time = datetime.now().strftime('%I:%M %p ET')
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔔 Morning RPC Alert - {today} at {current_time}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(targets_above_threshold)}* targets have RPC above *${RPC_THRESHOLD}* as of {current_time}"
                }
            },
            {"type": "divider"}
        ]
        
        # Add each target as a block
        for target in targets_above_threshold:
            blocks.append(format_target_for_slack(target, is_morning=True))
        
        # Send to Slack
        send_slack_message(
            f"Morning RPC Alert: {len(targets_above_threshold)} targets above ${RPC_THRESHOLD} as of {current_time}",
            blocks=blocks
        )
        
        logger.info(f"Found {len(targets_above_threshold)} targets above ${RPC_THRESHOLD} RPC in morning check")
    else:
        # Send notification that no targets are above threshold
        current_time = datetime.now().strftime('%I:%M %p ET')
        send_slack_message(f"🔔 Morning RPC Alert: No targets found with RPC above ${RPC_THRESHOLD} as of {current_time}")
        logger.info("No targets found above RPC threshold in morning check")

def afternoon_check():
    """
    Perform the afternoon check (3pm EST) to find morning targets that fell below threshold
    using the direct insights API
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
    
    # Initialize the direct API client
    api = RingbaDirectAPI(api_token, account_id)
    
    # Test authentication
    if not api.test_auth():
        logger.error("Authentication failed")
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during afternoon check")
        return
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Checking real-time data for date: {today} at {datetime.now().strftime('%H:%M:%S')}")
    
    # Get current insights data for all targets
    insights_data = api.get_insights(start_date=today, end_date=today, group_by="targetId")
    
    if not insights_data or "items" not in insights_data:
        logger.error("Failed to get insights data for afternoon check")
        send_slack_message(f"⚠️ *ALERT*: Failed to retrieve current RPC data during afternoon check")
        return
    
    # Get call logs to extract tag information
    call_logs = api.get_call_logs(start_date=today, end_date=today)
    calls_by_target = {}
    
    # Group calls by target and collect tag information
    if call_logs and "items" in call_logs:
        for call in call_logs.get("items", []):
            target_id = call.get("targetId")
            if target_id:
                if target_id not in calls_by_target:
                    calls_by_target[target_id] = []
                calls_by_target[target_id].append(call)
    
    # Get tag information
    tags_info = api.get_tags()
    tags_dict = {t.get('id'): t for t in tags_info if 'id' in t}
    
    # Create a dictionary of current RPC values by target ID
    current_rpc_by_target = {}
    for item in insights_data.get("items", []):
        target_id = item.get("targetId")
        if target_id:
            rpc = item.get("rpc", 0)
            calls = item.get("calls", 0)
            revenue = item.get("revenue", 0)
            
            # Extract tag information from calls for this target
            target_calls = calls_by_target.get(target_id, [])
            target_tags = {}
            
            for call in target_calls:
                tag_ids = call.get("tagIds", [])
                for tag_id in tag_ids:
                    tag_info = tags_dict.get(tag_id, {})
                    tag_name = tag_info.get("name", "Unknown Tag")
                    if tag_name not in target_tags:
                        target_tags[tag_name] = 0
                    target_tags[tag_name] += 1
            
            current_rpc_by_target[target_id] = {
                'rpc': rpc,
                'calls': calls,
                'revenue': revenue,
                'tags': target_tags
            }
    
    # Check each morning target to see if RPC fell below threshold
    targets_below_threshold = []
    
    for target in morning_targets:
        target_id = target.get('id')
        target_name = target.get('name', 'Unknown')
        morning_rpc = target.get('rpc', 0)
        
        # Get current RPC data
        current_data = current_rpc_by_target.get(target_id, {})
        current_rpc = current_data.get('rpc', 0)
        
        if current_rpc < RPC_THRESHOLD:
            # Calculate the RPC change
            rpc_change = current_rpc - morning_rpc
            
            targets_below_threshold.append({
                'id': target_id,
                'name': target_name,
                'rpc': current_rpc,
                'morning_rpc': morning_rpc,
                'rpc_change': rpc_change,
                'calls': current_data.get('calls', 0),
                'revenue': current_data.get('revenue', 0),
                'tags': current_data.get('tags', {})
            })
    
    # Send Slack notification if any targets fell below threshold
    if targets_below_threshold:
        # Sort by RPC change (biggest drop first)
        targets_below_threshold.sort(key=lambda x: x['rpc_change'])
        
        # Prepare Slack message
        current_time = datetime.now().strftime('%I:%M %p ET')
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔔 Afternoon RPC Alert - {today} at {current_time}",
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
            # Format tags information
            tags_text = ""
            if target.get('tags'):
                tags = target['tags']
                if isinstance(tags, dict):
                    # Tags are in format {tag_name: count}
                    top_tags = sorted(tags.items(), key=lambda x: x[1], reverse=True)[:3]  # Get top 3 tags
                    if top_tags:
                        tags_text = "\n:label: *Tags*: " + ", ".join([f"{tag} ({count})" for tag, count in top_tags])
                elif isinstance(tags, list):
                    # Tags are in a list format
                    if len(tags) > 0:
                        tags_text = "\n:label: *Tags*: " + ", ".join(tags[:3])  # Show up to 3 tags
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{target['name']}*\n:chart_with_downwards_trend: Morning RPC: *${target['morning_rpc']:.2f}* → Current RPC: *${target['rpc']:.2f}* (Change: ${target['rpc_change']:.2f})\nCalls: {target.get('calls', 'N/A')} | Revenue: ${target.get('revenue', 0):.2f}{tags_text}"
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
            f"Afternoon RPC Alert: {len(targets_below_threshold)} targets fell below ${RPC_THRESHOLD} as of {current_time}",
            blocks=blocks
        )
        
        logger.info(f"Found {len(targets_below_threshold)} targets that fell below ${RPC_THRESHOLD} RPC in afternoon check")
    else:
        # Send notification that no targets fell below threshold
        morning_count = len(morning_targets)
        current_time = datetime.now().strftime('%I:%M %p ET')
        send_slack_message(f"🔔 Afternoon RPC Alert: All {morning_count} morning targets are still above ${RPC_THRESHOLD} RPC as of {current_time}")
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
    
    # Get scheduled times from environment variables
    morning_check_time = os.getenv('MORNING_CHECK_TIME', '10:00')  # Default to 10:00 AM if not set
    afternoon_check_time = os.getenv('AFTERNOON_CHECK_TIME', '15:00')  # Default to 3:00 PM if not set
    
    # Schedule the morning check
    schedule.every().day.at(morning_check_time).do(morning_check)
    logger.info(f"Scheduled morning check for {morning_check_time} EST")
    
    # Schedule the afternoon check
    schedule.every().day.at(afternoon_check_time).do(afternoon_check)
    logger.info(f"Scheduled afternoon check for {afternoon_check_time} EST")
    
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
            print("Usage: python direct_rpc_monitor.py [morning|afternoon|test]")
            print("       If no arguments are provided, the script will run as a scheduler")
    else:
        # Run as scheduler by default
        schedule_jobs() 