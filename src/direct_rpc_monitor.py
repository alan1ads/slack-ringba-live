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
from apscheduler.schedulers.blocking import BlockingScheduler
import csv
import io

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
    using UI-matching RPC data from 00:00 to now
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
    
    # Get current time in EST
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    
    # Get today's date in EST
    today = now_eastern.strftime('%Y-%m-%d')
    logger.info(f"Checking real-time data for date: {today} at {now_eastern.strftime('%H:%M:%S %Z%z')}")
    logger.info(f"Getting RPC data from 00:00 to now")
    
    # Get all targets with RPC data using UI-matching calculation
    all_targets_rpc = api.get_ui_matching_rpc(start_date=today, end_date=today)
    
    # Filter for targets above threshold
    targets_above_threshold = [t for t in all_targets_rpc if t['rpc'] >= RPC_THRESHOLD]
    
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
        current_time = now_eastern.strftime('%I:%M %p EST')
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
                    "text": f"*{len(targets_above_threshold)}* targets have RPC above *${RPC_THRESHOLD}* from 00:00 to {current_time}"
                }
            },
            {"type": "divider"}
        ]
        
        # Add each target as a block
        for target in targets_above_threshold:
            blocks.append(format_target_for_slack(target, is_morning=True))
        
        # Send to Slack
        send_slack_message(
            f"Morning RPC Alert: {len(targets_above_threshold)} targets above ${RPC_THRESHOLD} from 00:00 to {current_time}",
            blocks=blocks
        )
        
        logger.info(f"Found {len(targets_above_threshold)} targets above ${RPC_THRESHOLD} RPC in morning check")
    else:
        # Send notification that no targets are above threshold
        current_time = now_eastern.strftime('%I:%M %p EST')
        send_slack_message(f"🔔 Morning RPC Alert: No targets found with RPC above ${RPC_THRESHOLD} from 00:00 to {current_time}")
        logger.info("No targets found above RPC threshold in morning check")

def afternoon_check():
    """
    Perform the afternoon check (3pm EST) to find morning targets that fell below threshold
    using UI-matching RPC data from 00:00 to now
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
    
    # Get current time in EST
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    
    # Get today's date in EST
    today = now_eastern.strftime('%Y-%m-%d')
    logger.info(f"Checking real-time data for date: {today} at {now_eastern.strftime('%H:%M:%S %Z%z')}")
    logger.info(f"Getting RPC data from 00:00 to now")
    
    # Get current RPC data for all targets using UI-matching calculation
    all_targets_rpc = api.get_ui_matching_rpc(start_date=today, end_date=today)
    
    # Create a dictionary of current RPC values by target ID
    current_rpc_by_target = {t['id']: t for t in all_targets_rpc}
    
    # Check each morning target to see if RPC fell below threshold
    targets_below_threshold = []
    
    for target in morning_targets:
        target_id = target.get('id')
        target_name = target.get('name', 'Unknown')
        morning_rpc = target.get('rpc', 0)
        
        # Get current RPC data
        current_data = current_rpc_by_target.get(target_id)
        
        # If no current data, this target may not have had any calls since morning
        if not current_data:
            logger.warning(f"No current data for target {target_name} ({target_id}) that was above threshold this morning")
            continue
            
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
                'revenue': current_data.get('revenue', 0)
            })
    
    # Send Slack notification if any targets fell below threshold
    if targets_below_threshold:
        # Sort by RPC change (biggest drop first)
        targets_below_threshold.sort(key=lambda x: x['rpc_change'])
        
        # Prepare Slack message
        current_time = now_eastern.strftime('%I:%M %p EST')
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
        current_time = now_eastern.strftime('%I:%M %p EST')
        send_slack_message(f"🔔 Afternoon RPC Alert: All {morning_count} morning targets are still above ${RPC_THRESHOLD} RPC as of {current_time}")
        logger.info(f"No targets fell below RPC threshold in afternoon check out of {morning_count} morning targets")

def manual_run():
    """Run both checks manually for testing"""
    print("Running morning check...")
    morning_check()
    
    print("\nRunning afternoon check...")
    afternoon_check()

def schedule_jobs():
    """Schedule the morning and afternoon checks using EST timezone"""
    # Clear any existing jobs
    schedule.clear()
    
    # Define timezone for EST
    eastern = pytz.timezone('US/Eastern')
    
    # Get scheduled times from environment variables
    morning_check_time = os.getenv('MORNING_CHECK_TIME', '10:00')  # Default to 10:00 AM if not set
    afternoon_check_time = os.getenv('AFTERNOON_CHECK_TIME', '15:00')  # Default to 3:00 PM if not set
    
    # Get current time in EST
    now = datetime.now(eastern)
    logger.info(f"Current time in EST: {now.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
    
    # Create a custom job class that handles EST timezone
    class EstScheduler:
        def __init__(self):
            self.morning_time = morning_check_time
            self.afternoon_time = afternoon_check_time
            self.morning_job_run_today = False
            self.afternoon_job_run_today = False
            
        def check_and_run_jobs(self):
            now_est = datetime.now(eastern)
            current_time = now_est.strftime('%H:%M')
            current_date = now_est.strftime('%Y-%m-%d')
            
            # Check if it's past the morning time and haven't run today
            if current_time >= self.morning_time and not self.morning_job_run_today:
                logger.info(f"Running morning check at {current_time} EST")
                morning_check()
                self.morning_job_run_today = True
                
            # Check if it's past the afternoon time and haven't run today
            if current_time >= self.afternoon_time and not self.afternoon_job_run_today:
                logger.info(f"Running afternoon check at {current_time} EST")
                afternoon_check()
                self.afternoon_job_run_today = True
                
            # Reset flags at midnight EST
            if current_time == '00:00':
                logger.info("Resetting job flags for new day")
                self.morning_job_run_today = False
                self.afternoon_job_run_today = False
    
    est_scheduler = EstScheduler()
    
    logger.info(f"Scheduled morning check for {morning_check_time} EST")
    logger.info(f"Scheduled afternoon check for {afternoon_check_time} EST")
    logger.info("Starting scheduler in EST timezone. Press Ctrl+C to exit.")
    
    try:
        # Custom scheduling loop to handle EST timezone
        while True:
            est_scheduler.check_and_run_jobs()
            time.sleep(30)  # Check every 30 seconds
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

def immediate_rpc_test(check_date=None):
    """
    Perform an immediate test to check and send current RPC data to Slack
    
    Args:
        check_date (str, optional): Date to check in YYYY-MM-DD format. If None, checks both today and yesterday.
    """
    logger.info("Performing immediate RPC test to check real-time data")
    
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
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during immediate test")
        return
    
    # Log the exact time of the test
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    logger.info(f"RPC test at exactly: {now_eastern.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
    
    # Get date(s) to check
    dates_to_check = []
    if check_date:
        # Use the specific date provided
        dates_to_check.append(check_date)
        logger.info(f"Checking data for specified date: {check_date}")
    else:
        # Check both today and yesterday
        today = now_eastern.strftime('%Y-%m-%d')
        yesterday = (now_eastern - timedelta(days=1)).strftime('%Y-%m-%d')
        dates_to_check = [today, yesterday]
        logger.info(f"Checking data for both today ({today}) and yesterday ({yesterday})")
    
    # Results for Slack
    all_results = {}
    
    for check_date in dates_to_check:
        logger.info(f"Checking data for date: {check_date}")
        
        # Get insights data for targets on the current date
        insights_data = api.get_insights(start_date=check_date, end_date=check_date, group_by="targetId")
        
        if not insights_data or "items" not in insights_data:
            logger.error(f"Failed to get insights data for {check_date}")
            continue
        
        # Extract items with RPC data
        items = insights_data.get("items", [])
        
        # Log all data retrieved
        target_count = len(items)
        logger.info(f"Retrieved data for {target_count} targets on {check_date}")
        
        for item in items:
            target_id = item.get("targetId", "Unknown")
            calls = item.get("calls", 0)
            rpc = item.get("rpc", 0)
            revenue = item.get("revenue", 0)
            logger.info(f"Target {target_id}: Calls={calls}, RPC=${rpc:.2f}, Revenue=${revenue:.2f}")
        
        # Store for this date
        all_results[check_date] = items
    
    # Check if we have any data for any date
    total_items = sum(len(items) for items in all_results.values())
    if total_items == 0:
        # No data for any dates
        logger.warning("No data found for any of the checked dates")
        send_slack_message(f"⚠️ *ALERT*: No RPC data found for checked dates: {', '.join(dates_to_check)}")
        return
    
    # Build a Slack message to show all RPC data
    current_time = now_eastern.strftime('%I:%M %p EST')
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🔔 RPC Data Report - Generated at {current_time}",
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Retrieved data for *{total_items}* targets across {len(dates_to_check)} date(s)"
            }
        },
        {"type": "divider"}
    ]
    
    # Process each date
    for date, items in all_results.items():
        if not items:
            continue
            
        # Add date header
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Data for {date}* ({len(items)} targets)"
            }
        })
        
        # Add all targets with their data for this date
        for item in sorted(items, key=lambda x: x.get("rpc", 0), reverse=True):
            target_id = item.get("targetId", "Unknown")
            # Try to get target name
            target_name = item.get("targetName", "Unknown Target")
            # If no target name in the item, try to get it from target details
            if target_name == "Unknown Target" and target_id != "Unknown":
                target_detail = api.get_target_details(target_id)
                if target_detail:
                    target_name = target_detail.get("name", target_name)
            
            calls = item.get("calls", 0)
            rpc = item.get("rpc", 0)
            revenue = item.get("revenue", 0)
            
            # Skip if no calls or revenue
            if calls == 0 and revenue == 0:
                continue
            
            # Add target to blocks
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{target_name}*\nCalls: {calls} | RPC: *${rpc:.2f}* | Revenue: ${revenue:.2f}"
                },
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View in Ringba"
                    },
                    "url": f"https://app.ringba.com/targets/{target_id}/overview"
                }
            })
    
    # Send to Slack
    send_slack_message(
        f"RPC Data Report: {total_items} targets across {len(dates_to_check)} date(s)",
        blocks=blocks
    )
    
    logger.info(f"Immediate RPC test completed and sent to Slack")

def real_time_rpc_check():
    """
    Perform a real-time RPC check to get today's RPC data from 00:00 to now and send to Slack
    using the Ringba UI-matching RPC calculation
    """
    logger.info("Performing real-time RPC check (using UI-matching RPC calculation)")
    
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
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during real-time check")
        return
    
    # Get current time in EST
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    
    # Get today's date in EST
    today = now_eastern.strftime('%Y-%m-%d')
    logger.info(f"Getting real-time RPC data for {today} from 00:00 to {now_eastern.strftime('%H:%M:%S %Z%z')}")
    
    # Get all targets with RPC data using UI-matching calculation
    all_targets_rpc = api.get_ui_matching_rpc(start_date=today, end_date=today)
    
    # Log the results
    logger.info(f"Found {len(all_targets_rpc)} targets with calls today")
    
    for target in all_targets_rpc:
        logger.info(f"Target {target['name']}: Calls={target['calls']}, RPC=${target['rpc']:.2f}, Revenue=${target['revenue']:.2f}")
    
    # Send Slack notification with today's RPC data
    if all_targets_rpc:
        # Sort by RPC (highest first)
        all_targets_rpc.sort(key=lambda x: x['rpc'], reverse=True)
        
        # Calculate total calls and revenue
        total_calls = sum(target['calls'] for target in all_targets_rpc)
        total_revenue = sum(target['revenue'] for target in all_targets_rpc)
        avg_rpc = total_revenue / total_calls if total_calls > 0 else 0
        
        # Prepare Slack message
        current_time = now_eastern.strftime('%I:%M %p EST')
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔔 Real-Time RPC Data - {today} at {current_time}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(all_targets_rpc)}* targets with *{total_calls:,}* calls today (00:00 to {current_time})\n"
                           f"Total Revenue: *${total_revenue:,.2f}* | Average RPC: *${avg_rpc:.2f}*"
                }
            },
            {"type": "divider"}
        ]
        
        # Add targets above threshold first with a heading
        targets_above = [t for t in all_targets_rpc if t['rpc'] >= RPC_THRESHOLD]
        if targets_above:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔝 Targets Above ${RPC_THRESHOLD} RPC*"
                }
            })
            
            for target in targets_above:
                # Format the target name - add emoji based on RPC value
                if target['rpc'] >= 30:
                    emoji = "🔥" # Fire emoji for very high RPC
                elif target['rpc'] >= 20:
                    emoji = "⭐" # Star emoji for high RPC
                else:
                    emoji = "✅" # Checkmark for good RPC
                    
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{emoji} {target['name']}*\n"
                              f"RPC: *${target['rpc']:.2f}* | Calls: *{target['calls']:,}* | Revenue: *${target['revenue']:,.2f}*"
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
        
        # Add other targets with a heading
        targets_below = [t for t in all_targets_rpc if t['rpc'] < RPC_THRESHOLD]
        if targets_below:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Other Targets*"
                }
            })
            
            # Create a compact summary for targets below threshold
            compact_summary = ""
            for target in targets_below[:10]:  # Limit to top 10 to avoid message size limits
                compact_summary += f"• *{target['name']}*: RPC ${target['rpc']:.2f} | Calls {target['calls']:,} | Revenue ${target['revenue']:,.2f}\n"
            
            if len(targets_below) > 10:
                compact_summary += f"• *+{len(targets_below) - 10} more targets...*\n"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": compact_summary
                }
            })
        
        # Send to Slack
        send_slack_message(
            f"Real-Time RPC Data: {len(all_targets_rpc)} targets with {total_calls:,} calls today, {len(targets_above)} above ${RPC_THRESHOLD}",
            blocks=blocks
        )
        
        logger.info(f"Real-time RPC data sent to Slack: {len(all_targets_rpc)} targets total, {len(targets_above)} above threshold")
    else:
        # Send notification that no targets have calls today
        current_time = now_eastern.strftime('%I:%M %p EST')
        send_slack_message(f"🔔 Real-Time RPC Alert: No targets found with calls today as of {current_time}")
        logger.info("No targets found with calls today")

def historical_rpc_check(start_date, end_date=None):
    """
    Perform a historical RPC check to get RPC data for a specific date range
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format. If not provided, will use start_date.
    """
    logger.info(f"Performing historical RPC check for period {start_date} to {end_date or start_date}")
    
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
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during historical check")
        return
    
    # Get all targets with RPC data from the given date range
    all_targets_rpc = api.get_dashboard_rpc(start_date=start_date, end_date=end_date)
    
    # Log the results
    logger.info(f"Found {len(all_targets_rpc)} targets with calls for period {start_date} to {end_date or start_date}")
    
    for target in all_targets_rpc:
        logger.info(f"Target {target['name']}: Calls={target['calls']}, RPC=${target['rpc']:.2f}, Revenue=${target['revenue']:.2f}")
    
    # Send Slack notification with the historical RPC data
    if all_targets_rpc:
        # Sort by RPC (highest first)
        all_targets_rpc.sort(key=lambda x: x['rpc'], reverse=True)
        
        # Calculate total calls and revenue
        total_calls = sum(target['calls'] for target in all_targets_rpc)
        total_revenue = sum(target['revenue'] for target in all_targets_rpc)
        avg_rpc = total_revenue / total_calls if total_calls > 0 else 0
        
        # Format date range
        date_range = f"{start_date}"
        if end_date and end_date != start_date:
            date_range = f"{start_date} to {end_date}"
        
        # Prepare Slack message
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 Historical RPC Data - {date_range}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(all_targets_rpc)}* targets with *{total_calls:,}* calls\n"
                           f"Total Revenue: *${total_revenue:,.2f}* | Average RPC: *${avg_rpc:.2f}*"
                }
            },
            {"type": "divider"}
        ]
        
        # Add targets above threshold first with a heading
        targets_above = [t for t in all_targets_rpc if t['rpc'] >= RPC_THRESHOLD]
        if targets_above:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔝 Targets Above ${RPC_THRESHOLD} RPC*"
                }
            })
            
            for target in targets_above:
                # Format the target name - add emoji based on RPC value
                if target['rpc'] >= 30:
                    emoji = "🔥" # Fire emoji for very high RPC
                elif target['rpc'] >= 20:
                    emoji = "⭐" # Star emoji for high RPC
                else:
                    emoji = "✅" # Checkmark for good RPC
                    
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{emoji} {target['name']}*\n"
                              f"RPC: *${target['rpc']:.2f}* | Calls: *{target['calls']:,}* | Revenue: *${target['revenue']:,.2f}*"
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
        
        # Add other targets with a heading
        targets_below = [t for t in all_targets_rpc if t['rpc'] < RPC_THRESHOLD]
        if targets_below:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Other Targets*"
                }
            })
            
            # Create a compact summary for targets below threshold
            compact_summary = ""
            for target in targets_below[:10]:  # Limit to top 10 to avoid message size limits
                compact_summary += f"• *{target['name']}*: RPC ${target['rpc']:.2f} | Calls {target['calls']:,} | Revenue ${target['revenue']:,.2f}\n"
            
            if len(targets_below) > 10:
                compact_summary += f"• *+{len(targets_below) - 10} more targets...*\n"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": compact_summary
                }
            })
        
        # Send to Slack
        send_slack_message(
            f"Historical RPC Data for {date_range}: {len(all_targets_rpc)} targets with {total_calls:,} calls, {len(targets_above)} above ${RPC_THRESHOLD}",
            blocks=blocks
        )
        
        logger.info(f"Historical RPC data sent to Slack: {len(all_targets_rpc)} targets total, {len(targets_above)} above threshold")
    else:
        # Send notification that no targets were found
        send_slack_message(f"📊 Historical RPC Alert: No targets found with calls for period {start_date} to {end_date or start_date}")
        logger.info(f"No targets found with calls for period {start_date} to {end_date or start_date}")

def compare_rpc_methods(start_date=None, end_date=None):
    """
    Compare different RPC calculation methods to find which one matches the UI
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format 
        end_date (str, optional): End date in YYYY-MM-DD format. If not provided, will use start_date.
    """
    logger.info(f"Running RPC methods comparison for period {start_date} to {end_date or start_date}")
    
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
        send_slack_message(f"⚠️ *ALERT*: Ringba API authentication failed during comparison check")
        return
    
    # Run comparison 
    comparison_results = api.compare_rpc_calculations(start_date=start_date, end_date=end_date)
    
    if not comparison_results or "comparison" not in comparison_results:
        logger.error("Failed to compare RPC calculation methods")
        return
    
    # Format comparison results for output
    comparisons = comparison_results["comparison"]
    
    # Sort by percentage difference (highest first)
    comparisons.sort(key=lambda x: x['percentage_diff'], reverse=True)
    
    # Print comparison table
    print(f"\n{'=' * 100}")
    print(f"RPC CALCULATION METHOD COMPARISON FOR {start_date} to {end_date or start_date}")
    print(f"{'=' * 100}")
    print(f"{'TARGET NAME':<30} {'INSIGHTS RPC':>15} {'CALLLOGS RPC':>15} {'DIFF':>10} {'DIFF %':>10}")
    print(f"{'-' * 30} {'-' * 15} {'-' * 15} {'-' * 10} {'-' * 10}")
    
    for comp in comparisons:
        target_name = comp['name']
        insights_rpc = comp['insights_rpc']
        calllogs_rpc = comp['calllogs_rpc']
        diff = comp['difference']
        diff_pct = comp['percentage_diff']
        
        print(f"{target_name[:30]:<30} {insights_rpc:>15.2f} {calllogs_rpc:>15.2f} {diff:>10.2f} {diff_pct:>10.2f}%")
    
    print(f"{'=' * 100}\n")
    
    # Get raw call logs to check if RPC field is directly available
    raw_logs = api.get_raw_calllogs(start_date=start_date, end_date=end_date)
    
    # Check if there's a direct RPC field in the call logs
    has_direct_rpc = False
    if raw_logs and "report" in raw_logs and "records" in raw_logs["report"]:
        records = raw_logs["report"]["records"]
        if records and len(records) > 0:
            first_record = records[0]
            if "rpc" in first_record:
                has_direct_rpc = True
                print(f"FOUND DIRECT RPC FIELD IN CALL LOGS: {first_record['rpc']}")
                
    # Log conclusions
    print("\nCONCLUSION:")
    if has_direct_rpc:
        print("The call logs API returns a direct RPC field, which may be the value shown in the UI.")
    
    # Check percentage differences to determine which method is more likely used by the UI
    high_diff_count = sum(1 for c in comparisons if c['percentage_diff'] > 10)
    low_diff_count = sum(1 for c in comparisons if c['percentage_diff'] <= 10)
    
    if high_diff_count > low_diff_count:
        print("There are significant differences between calculation methods.")
        print("The Ringba UI is likely using a different calculation than our script.")
    else:
        print("Most targets have similar RPC values between calculation methods.")
        if low_diff_count > 0:
            most_accurate = "insights API" if low_diff_count > high_diff_count else "call logs calculation"
            print(f"The {most_accurate} method appears to more closely match the UI.")

def check_target_rpc(target_id):
    """
    Check detailed RPC calculation for a specific target ID to help verify against UI values
    
    Args:
        target_id (str): The target ID to check
    """
    logger.info(f"Checking detailed RPC calculation for target ID: {target_id}")
    
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
        return
    
    # Get target details
    target_info = api.get_target_details(target_id)
    if not target_info:
        logger.error(f"Failed to get details for target ID: {target_id}")
        return
    
    target_name = target_info.get("name", "Unknown Target")
    print(f"\n{'=' * 100}")
    print(f"TARGET RPC VERIFICATION FOR: {target_name} (ID: {target_id})")
    print(f"{'=' * 100}")
    
    # Get today's date
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    today = now_eastern.strftime('%Y-%m-%d')
    
    # Get UI-matching RPC data
    ui_data = api.get_ui_matching_rpc(start_date=today, end_date=today)
    
    # Find the target in the results
    ui_target = next((t for t in ui_data if t.get('id') == target_id), None)
    
    if ui_target:
        print(f"UI-MATCHING RPC: ${ui_target.get('rpc', 0):.2f}")
        print(f"Calls: {ui_target.get('calls', 0)}")
        print(f"Revenue: ${ui_target.get('revenue', 0):.2f}")
        print(f"Source: {ui_target.get('source', 'Unknown')}")
    else:
        print("Target not found in UI-matching RPC data")
    
    # Get raw call logs for this target to show payout details
    print(f"\nINDIVIDUAL CALL DETAILS:")
    print(f"{'-' * 100}")
    
    # Get call logs for today
    call_logs = api.get_call_logs(start_date=today, end_date=today)
    
    if call_logs and "items" in call_logs:
        target_calls = [c for c in call_logs["items"] if c.get("targetId") == target_id]
        
        if target_calls:
            total_payout = 0
            for i, call in enumerate(target_calls):
                # Get payout amount
                payout = 0
                if "payoutAmount" in call and call["payoutAmount"] is not None:
                    try:
                        payout = float(call["payoutAmount"])
                    except (ValueError, TypeError):
                        pass
                elif "payout" in call and call["payout"] is not None:
                    try:
                        if isinstance(call["payout"], (int, float)):
                            payout = float(call["payout"])
                        elif isinstance(call["payout"], str):
                            payout_str = call["payout"].replace('$', '').replace(',', '')
                            payout = float(payout_str)
                    except (ValueError, TypeError):
                        pass
                
                total_payout += payout
                
                # Print call details
                connect_time = call.get("connectTime", "Unknown")
                if isinstance(connect_time, (int, float)):
                    try:
                        connect_dt = datetime.fromtimestamp(connect_time / 1000, tz=eastern)
                        connect_time = connect_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                print(f"Call #{i+1}: Time: {connect_time} | Payout: ${payout:.2f}")
                
                # If available, print original RPC from call
                if "rpc" in call:
                    print(f"         Original RPC from call: ${call.get('rpc', 0)}")
            
            # Calculate RPC
            calls_count = len(target_calls)
            calculated_rpc = total_payout / calls_count if calls_count > 0 else 0
            
            print(f"\nSUMMARY:")
            print(f"Total Calls: {calls_count}")
            print(f"Total Payout: ${total_payout:.2f}")
            print(f"Calculated RPC: ${calculated_rpc:.2f}")
            
            if ui_target:
                diff = abs(calculated_rpc - ui_target.get('rpc', 0))
                print(f"Difference from UI-matching RPC: ${diff:.2f}")
        else:
            print("No calls found for this target today")
    else:
        print("Failed to get call logs")
    
    print(f"{'=' * 100}")

def find_target_public_ids():
    """
    Retrieve and display Public IDs (PI prefixed) used in the Ringba UI links
    """
    logger.info("Finding target public IDs used in Ringba UI links")
    
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
        return
    
    # Get all targets
    all_targets = api.get_targets()
    
    if not all_targets:
        logger.error("Failed to get targets")
        return
    
    print(f"\n{'=' * 100}")
    print(f"PUBLIC IDS FOR RINGBA UI LINKS")
    print(f"{'=' * 100}")
    print(f"{'TARGET NAME':<50} {'INTERNAL ID':<36} {'PUBLIC ID':<36}")
    print(f"{'-' * 50} {'-' * 36} {'-' * 36}")
    
    # Try to get target details to extract public IDs
    for target in all_targets:
        target_id = target.get('id')
        target_name = target.get('name', 'Unknown')
        
        if not target_id:
            continue
        
        # Get target details which may include publicId
        target_details = api.get_target_details(target_id)
        
        if target_details and 'publicId' in target_details:
            public_id = target_details.get('publicId')
            print(f"{target_name[:50]:<50} {target_id:<36} {public_id:<36}")
            logger.info(f"Found public ID for {target_name}: {public_id}")
        else:
            print(f"{target_name[:50]:<50} {target_id:<36} {'Not found':<36}")
    
    print(f"{'=' * 100}")

def export_call_logs_csv(start_date=None, end_date=None, output_file=None, job_id=None):
    """
    Export call logs to CSV directly from Ringba using the same approach as the UI
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        output_file (str, optional): Output file name
        job_id (str, optional): Resume an existing export job
    """
    # Load environment variables
    api_token = os.getenv("RINGBA_API_TOKEN")
    account_id = os.getenv("RINGBA_ACCOUNT_ID")
    
    if not api_token or not account_id:
        logger.error("Missing API token or account ID. Please set RINGBA_API_TOKEN and RINGBA_ACCOUNT_ID environment variables.")
        return
    
    # Set up headers for API requests
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json"
    }
    
    # Set up dates
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    
    if not start_date:
        start_date = now_eastern.strftime('%Y-%m-%d')
    
    if not end_date:
        end_date = start_date
    
    # Set default output file name if not provided
    if not output_file:
        output_file = f"ringba_call_logs_{start_date}.csv"
    
    logger.info(f"Exporting call logs for period {start_date} to {end_date}")
    
    # Based on the UI analysis, the export happens at a different endpoint
    # Directly download the CSV from the call logs page as the UI does
    
    # First, we need to format dates for the request
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Set time to start of day (00:00:00) for start_date and end of day (23:59:59) for end_date
    start_date_obj = eastern.localize(start_date_obj.replace(hour=0, minute=0, second=0))
    end_date_obj = eastern.localize(end_date_obj.replace(hour=23, minute=59, second=59))
    
    # Convert to millis timestamp as used in UI requests
    start_millis = int(start_date_obj.timestamp() * 1000)
    end_millis = int(end_date_obj.timestamp() * 1000)
    
    # Based on the screenshot, trying the direct download URL approach
    try:
        # First, try the most direct approach - download from the dashboard URL format
        download_url = f"https://app.ringba.com/export/call-logs?accountId={account_id}&start={start_millis}&end={end_millis}"
        
        logger.info(f"Attempting to download CSV directly from: {download_url}")
        
        # Session to maintain cookies across requests
        session = requests.Session()
        
        # Add token to session
        session.headers.update({
            "Authorization": f"Token {api_token}",
            "Accept": "*/*"
        })
        
        # Try to download
        response = session.get(download_url)
        
        # Check if successful and looks like CSV
        if response.status_code == 200 and ('csv' in response.headers.get('Content-Type', '').lower() or ',' in response.text):
            # Save to file
            with open(output_file, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"CSV downloaded successfully to {output_file}")
            
            # Process the CSV to show RPC by target
            with open(output_file, 'r', encoding='utf-8') as f:
                csv_data = f.read()
            
            process_csv_for_rpc(csv_data, start_date, end_date)
            return True
        else:
            logger.warning(f"Direct download failed with status {response.status_code}. Content-Type: {response.headers.get('Content-Type')}")
            logger.warning(f"Trying alternative methods...")
            
            # Try to get the file via the export button click simulation
            # Based on the screenshot showing 3 files when clicking Export CSV
            export_url = f"https://app.ringba.com/api/export/calls?accountId={account_id}"
            
            payload = {
                "start": start_millis,
                "end": end_millis,
                "format": "csv",
                "timezone": "America/New_York"  # EST timezone as seen in UI
            }
            
            logger.info(f"Trying export API at: {export_url}")
            export_response = session.post(export_url, json=payload)
            
            if export_response.status_code == 200:
                # This might return a URL or file ID
                response_data = export_response.json()
                logger.info(f"Export API response: {response_data}")
                
                # Check if we got a download URL
                if isinstance(response_data, dict) and "url" in response_data:
                    download_url = response_data["url"]
                    logger.info(f"Got download URL: {download_url}")
                    
                    # Download the file
                    file_response = session.get(download_url)
                    
                    if file_response.status_code == 200:
                        with open(output_file, 'wb') as f:
                            f.write(file_response.content)
                        
                        logger.info(f"CSV downloaded successfully to {output_file}")
                        
                        # Process the CSV
                        with open(output_file, 'r', encoding='utf-8') as f:
                            csv_data = f.read()
                        
                        process_csv_for_rpc(csv_data, start_date, end_date)
                        return True
            
            logger.error(f"All CSV download attempts failed. Status: {export_response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error downloading CSV: {str(e)}")
        return False

def resume_export_job(job_id=None):
    """
    Resume a previously started export job
    
    Args:
        job_id (str, optional): The job ID to resume. If not provided, reads from last_export_job.txt
    """
    if not job_id:
        # Try to read from file
        try:
            if not os.path.exists("last_export_job.txt"):
                logger.error("No previous export job found")
                return False
            
            with open("last_export_job.txt", "r") as f:
                job_data = f.read().strip().split(",")
                
                if len(job_data) >= 1:
                    job_id = job_data[0]
                    
                    # Get additional parameters if available
                    start_date = job_data[1] if len(job_data) > 1 else None
                    end_date = job_data[2] if len(job_data) > 2 else None
                    output_file = job_data[3] if len(job_data) > 3 else None
                    
                    logger.info(f"Resuming export job {job_id}")
                    return export_call_logs_csv(start_date, end_date, output_file, job_id)
                else:
                    logger.error("Invalid job data in last_export_job.txt")
                    return False
        except Exception as e:
            logger.error(f"Error resuming export job: {str(e)}")
            return False
    else:
        # Use provided job ID
        return export_call_logs_csv(None, None, None, job_id)

def process_csv_for_rpc(csv_data, start_date, end_date):
    """
    Process the CSV data to calculate and display RPC by target
    
    Args:
        csv_data (str): CSV data as string
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    """
    logger.info("Processing CSV data to calculate RPC by target")
    
    try:
        # Parse CSV data
        csv_reader = csv.DictReader(io.StringIO(csv_data))
        
        # Group data by target
        targets = {}
        
        for row in csv_reader:
            target_id = row.get('targetId', '')
            target_name = row.get('targetName', 'Unknown')
            
            # Skip if no target ID
            if not target_id:
                continue
            
            # Initialize target data if not already in dictionary
            if target_id not in targets:
                targets[target_id] = {
                    'name': target_name,
                    'calls': 0,
                    'revenue': 0.0,
                    'connected_calls': 0
                }
            
            # Check if call was connected
            has_connected = row.get('hasConnected', '').lower() == 'true'
            
            # Check if call had payout
            has_payout = row.get('hasPayout', '').lower() == 'true'
            
            # Get payout amount
            payout_str = row.get('payoutAmount', '0')
            try:
                # Handle potential currency formatting
                payout_str = payout_str.replace('$', '').replace(',', '')
                payout = float(payout_str) if payout_str else 0.0
            except (ValueError, TypeError):
                payout = 0.0
            
            # Update target stats
            targets[target_id]['calls'] += 1
            
            if has_connected:
                targets[target_id]['connected_calls'] += 1
            
            if has_payout:
                targets[target_id]['revenue'] += payout
    
        # Calculate RPC for each target and prepare results
        rpc_by_target = []
        
        for target_id, data in targets.items():
            calls = data['calls']
            revenue = data['revenue']
            rpc = revenue / calls if calls > 0 else 0.0
            
            rpc_by_target.append({
                'id': target_id,
                'name': data['name'],
                'calls': calls,
                'connected_calls': data['connected_calls'],
                'revenue': revenue,
                'rpc': rpc
            })
        
        # Sort by RPC (highest first)
        rpc_by_target.sort(key=lambda x: x['rpc'], reverse=True)
        
        # Print RPC report
        print(f"\n{'=' * 100}")
        print(f"RPC REPORT FROM CSV DATA FOR PERIOD: {start_date} to {end_date}")
        print(f"{'=' * 100}")
        print(f"{'TARGET NAME':<50} {'CALLS':<10} {'CONNECTED':<10} {'REVENUE':<15} {'RPC':<10}")
        print(f"{'-' * 50} {'-' * 10} {'-' * 10} {'-' * 15} {'-' * 10}")
        
        for target in rpc_by_target:
            print(f"{target['name'][:50]:<50} {target['calls']:<10} {target['connected_calls']:<10} ${target['revenue']:<14.2f} ${target['rpc']:<9.2f}")
        
        # Calculate and print totals
        total_calls = sum(t['calls'] for t in rpc_by_target)
        total_connected = sum(t['connected_calls'] for t in rpc_by_target)
        total_revenue = sum(t['revenue'] for t in rpc_by_target)
        total_rpc = total_revenue / total_calls if total_calls > 0 else 0.0
        
        print(f"{'-' * 50} {'-' * 10} {'-' * 10} {'-' * 15} {'-' * 10}")
        print(f"{'TOTAL':<50} {total_calls:<10} {total_connected:<10} ${total_revenue:<14.2f} ${total_rpc:<9.2f}")
        print(f"{'=' * 100}")
        
        # Calculate connection rate
        connection_rate = (total_connected / total_calls * 100) if total_calls > 0 else 0.0
        print(f"Connection Rate: {connection_rate:.2f}%")
        print(f"{'=' * 100}")
        
        logger.info(f"CSV processing complete: Found {len(rpc_by_target)} targets with total RPC: ${total_rpc:.2f}")
        
    except Exception as e:
        logger.error(f"Error processing CSV data: {str(e)}")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "morning":
            morning_check()
        elif command == "afternoon":
            afternoon_check()
        elif command == "test":
            manual_run()
        elif command == "now":
            real_time_rpc_check()
        elif command == "historical":
            # Check if we have date parameters
            if len(sys.argv) > 2:
                start_date = sys.argv[2]
                end_date = sys.argv[3] if len(sys.argv) > 3 else None
                historical_rpc_check(start_date, end_date)
            else:
                print("Usage for historical check: python direct_rpc_monitor.py historical YYYY-MM-DD [YYYY-MM-DD]")
        elif command == "yesterday":
            # Get yesterday's date
            eastern = pytz.timezone('US/Eastern')
            now_eastern = datetime.now(eastern)
            yesterday = (now_eastern - timedelta(days=1)).strftime('%Y-%m-%d')
            historical_rpc_check(yesterday)
        elif command == "compare":
            # Check if we have date parameters
            if len(sys.argv) > 2:
                start_date = sys.argv[2]
                end_date = sys.argv[3] if len(sys.argv) > 3 else None
                compare_rpc_methods(start_date, end_date)
            else:
                # Use today by default
                eastern = pytz.timezone('US/Eastern')
                now_eastern = datetime.now(eastern)
                today = now_eastern.strftime('%Y-%m-%d')
                compare_rpc_methods(today)
        elif command == "verify":
            # Check if we have target ID parameter
            if len(sys.argv) > 2:
                target_id = sys.argv[2]
                check_target_rpc(target_id)
            else:
                print("Usage: python direct_rpc_monitor.py verify TARGET_ID")
        elif command == "public_ids":
            # Find target public IDs used in UI links
            find_target_public_ids()
        elif command == "export_csv":
            # Export call logs to CSV
            if len(sys.argv) > 2:
                start_date = sys.argv[2]
                end_date = sys.argv[3] if len(sys.argv) > 3 else None
                output_file = sys.argv[4] if len(sys.argv) > 4 else None
                export_call_logs_csv(start_date, end_date, output_file)
            else:
                # Use today by default
                eastern = pytz.timezone('US/Eastern')
                now_eastern = datetime.now(eastern)
                today = now_eastern.strftime('%Y-%m-%d')
                export_call_logs_csv(today)
        elif command == "resume_export":
            # Resume a previously started export job
            if len(sys.argv) > 2:
                job_id = sys.argv[2]
                resume_export_job(job_id)
            else:
                # Try to resume the last export job
                resume_export_job()
        else:
            print("Unknown command. Usage: python direct_rpc_monitor.py [morning|afternoon|test|now|historical|yesterday|compare|verify|public_ids|export_csv|resume_export]")
    else:
        # Start the scheduler
        scheduler = BlockingScheduler()
        
        # Schedule the morning check at 10 AM EST
        scheduler.add_job(morning_check, 'cron', hour=10, minute=0, timezone='US/Eastern')
        
        # Schedule the afternoon check at 3 PM EST
        scheduler.add_job(afternoon_check, 'cron', hour=15, minute=0, timezone='US/Eastern')
        
        print("Scheduler started. Press Ctrl+C to exit.")
        
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            print("Scheduler stopped.")
            pass 