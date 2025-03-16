#!/usr/bin/env python3
"""
Ringba RPC Monitor - Main Script
Monitors Revenue Per Call (RPC) for targets on the Ringba platform.
Sends notifications to Slack when RPC drops below the threshold.
"""

import os
import sys
import time
import logging
import json
import dotenv
from datetime import datetime, timedelta
import schedule
import pytz

# Import custom modules
from ringba_api import RingbaAPI
from slack_notifier import SlackNotifier

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ringba_monitor.log')
    ]
)
logger = logging.getLogger('ringba_monitor.main')

# Dictionary to store morning targets
morning_targets = {}

def check_morning_rpc(api, notifier, target_name, rpc_threshold):
    """Check RPC for targets in the morning and save targets above threshold"""
    global morning_targets
    
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    
    logger.info(f"=== Starting Morning RPC Check ({now_eastern.strftime('%Y-%m-%d %H:%M:%S %Z')}) ===")
    logger.info(f"Target Name: {target_name}")
    logger.info(f"RPC Threshold: ${rpc_threshold:.2f}")
    
    # Send notification that check is starting
    notifier.send_notification(
        f"🔍 *Morning RPC Check Started* ({now_eastern.strftime('%Y-%m-%d %H:%M:%S %Z')})\n"
        f"• Target Name: {target_name}\n"
        f"• RPC Threshold: ${rpc_threshold:.2f}\n"
        f"• Account ID: {api.account_id}"
    )
    
    # Test authentication
    if not api.test_auth():
        error_msg = f"❌ *Authentication Error*\nFailed to authenticate with Ringba API. Please check your API token and account ID."
        logger.error(error_msg)
        notifier.send_notification(error_msg)
        return
    
    # Get all targets or specific target based on configuration
    if target_name.lower() == "all":
        # Get all targets above threshold
        targets_above_threshold = api.find_targets_above_threshold(rpc_threshold)
        if not targets_above_threshold:
            logger.warning(f"No targets found with RPC above ${rpc_threshold:.2f}")
            notifier.send_notification(
                f"⚠️ *Morning RPC Check Completed*\n"
                f"No targets found with RPC >= ${rpc_threshold:.2f}.\n"
                f"There will be no targets to check in the afternoon."
            )
            return
            
        # Save targets with RPC above threshold
        morning_targets = {}
        
        targets_list = []
        for target in targets_above_threshold:
            target_id = target.get('id')
            target_name = target.get('name', 'Unknown')
            rpc = target.get('rpc', 0)
            
            morning_targets[target_id] = {
                'name': target_name,
                'morning_rpc': rpc
            }
            targets_list.append(f"• {target_name}: ${rpc:.2f}")
            
        # Format and send the morning report
        message = (
            f"✅ *Morning RPC Check Completed*\n"
            f"Found {len(morning_targets)} targets with RPC >= ${rpc_threshold:.2f}:\n"
            f"{chr(10).join(targets_list)}\n\n"
            f"These targets will be monitored for the afternoon check."
        )
        logger.info(f"Saved {len(morning_targets)} targets with RPC above threshold")
        notifier.send_notification(message)
    else:
        # Try to find the target by name
        targets, _ = api.get_all_targets()
        target_found = False
        
        for target in targets:
            if target.get('name') == target_name:
                target_id = target.get('id')
                rpc = api.calculate_rpc_for_target(target_id)
                
                if rpc is not None and rpc >= rpc_threshold:
                    # Save this target for afternoon check
                    morning_targets = {
                        target_id: {
                            'name': target_name,
                            'morning_rpc': rpc
                        }
                    }
                    
                    message = (
                        f"✅ *Morning RPC Check Completed*\n"
                        f"Target '{target_name}' has RPC ${rpc:.2f} which is above threshold ${rpc_threshold:.2f}.\n"
                        f"This target will be monitored for the afternoon check."
                    )
                    logger.info(f"Target '{target_name}' has RPC ${rpc:.2f}")
                    notifier.send_notification(message)
                    target_found = True
                    break
                elif rpc is not None:
                    message = (
                        f"⚠️ *Morning RPC Check Completed*\n"
                        f"Target '{target_name}' has RPC ${rpc:.2f} which is below threshold ${rpc_threshold:.2f}.\n"
                        f"There will be no targets to check in the afternoon."
                    )
                    logger.warning(f"Target '{target_name}' has RPC ${rpc:.2f} which is below threshold")
                    notifier.send_notification(message)
                    target_found = True
                    break
        
        if not target_found:
            error_msg = f"❌ *Target Not Found*\nCould not find target with name: {target_name}"
            logger.error(error_msg)
            notifier.send_notification(error_msg)
            return

def check_afternoon_rpc(api, notifier, rpc_threshold):
    """Check if any targets dropped below threshold in the afternoon"""
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)
    
    logger.info(f"=== Starting Afternoon RPC Check ({now_eastern.strftime('%Y-%m-%d %H:%M:%S %Z')}) ===")
    
    # Send notification that check is starting
    notifier.send_notification(
        f"🔍 *Afternoon RPC Check Started* ({now_eastern.strftime('%Y-%m-%d %H:%M:%S %Z')})\n"
        f"Checking {len(morning_targets)} targets from morning check"
    )
    
    # Test authentication
    if not api.test_auth():
        error_msg = f"❌ *Authentication Error*\nFailed to authenticate with Ringba API. Please check your API token and account ID."
        logger.error(error_msg)
        notifier.send_notification(error_msg)
        return
    
    # If no targets saved from morning, skip check
    if not morning_targets:
        message = "ℹ️ *No Targets to Check*\nNo targets were saved from the morning check."
        logger.info(message)
        notifier.send_notification(message)
        return
    
    # Check each target from morning
    dropped_targets = []
    
    for target_id, target_info in morning_targets.items():
        target_name = target_info['name']
        morning_rpc = target_info['morning_rpc']
        
        # Get current RPC
        current_rpc = api.calculate_rpc_for_target(target_id)
        
        if current_rpc is None:
            logger.error(f"Failed to get current RPC for target: {target_name}")
            continue
        
        logger.info(f"Target: {target_name}, Morning RPC: ${morning_rpc:.2f}, Current RPC: ${current_rpc:.2f}")
        
        # Check if RPC dropped below threshold
        if current_rpc < rpc_threshold:
            dropped_targets.append({
                'name': target_name,
                'morning_rpc': morning_rpc,
                'current_rpc': current_rpc,
                'drop_percentage': ((morning_rpc - current_rpc) / morning_rpc) * 100
            })
    
    # Send notification if any targets dropped
    if dropped_targets:
        # Sort dropped targets by percentage drop (highest first)
        dropped_targets.sort(key=lambda x: x['drop_percentage'], reverse=True)
        
        # Format the message
        drops = []
        for target in dropped_targets:
            drops.append(
                f"• *{target['name']}*\n"
                f"  - Morning: ${target['morning_rpc']:.2f}\n"
                f"  - Current: ${target['current_rpc']:.2f}\n"
                f"  - Drop: {target['drop_percentage']:.1f}%"
            )
        
        message = (
            f"🚨 *ALERT: {len(dropped_targets)} Targets Below RPC Threshold*\n\n"
            f"{chr(10).join(drops)}"
        )
        
        logger.warning(f"{len(dropped_targets)} targets dropped below threshold")
        notifier.send_notification(message)
    else:
        message = "✅ *Afternoon Check Completed*\nNo targets dropped below the threshold."
        logger.info("No targets dropped below threshold")
        notifier.send_notification(message)

def main():
    """Main function to set up and start the RPC monitoring"""
    print("Starting Ringba RPC Monitor...")
    
    # Load environment variables
    dotenv.load_dotenv()
    
    # Validate environment variables
    required_vars = [
        'RINGBA_API_TOKEN', 
        'RINGBA_ACCOUNT_ID',
        'SLACK_WEBHOOK_URL',
        'TARGET_NAME',
        'RPC_THRESHOLD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please make sure these variables are set in your .env file")
        sys.exit(1)
    
    # Get configuration
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
    target_name = os.getenv('TARGET_NAME', 'all')
    rpc_threshold = float(os.getenv('RPC_THRESHOLD', 10.0))
    
    # Set up API and notifier
    api = RingbaAPI(api_token, account_id)
    notifier = SlackNotifier(slack_webhook)
    
    # Test authentication
    logger.info("Testing authentication with Ringba API")
    if not api.test_auth():
        logger.error(f"Account ID '{account_id}' appears to be invalid")
        notifier.send_notification(
            f"❌ *Setup Error*\nFailed to authenticate with Ringba API. "
            f"Account ID '{account_id}' not found. Please check your RINGBA_ACCOUNT_ID in .env file."
        )
        sys.exit(1)
    
    # Schedule the checks
    # Morning check at 10:00 AM EST
    schedule.every().day.at("10:00").do(
        check_morning_rpc, api, notifier, target_name, rpc_threshold
    )
    
    # Afternoon check at 15:00 (3:00 PM) EST
    schedule.every().day.at("15:00").do(
        check_afternoon_rpc, api, notifier, rpc_threshold
    )
    
    # Send startup notification
    notifier.send_notification(
        "🚀 *Ringba RPC Monitor Started*\n"
        f"• Morning check scheduled for 10:00 AM EST\n"
        f"• Afternoon check scheduled for 3:00 PM EST\n"
        f"• Target: {target_name}\n"
        f"• RPC Threshold: ${rpc_threshold:.2f}"
    )
    
    # Display next scheduled runs
    print("Schedule:")
    for job in schedule.get_jobs():
        print(f"- {job}")
    
    # Run the scheduler
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Sleep for 60 seconds
        except KeyboardInterrupt:
            print("Keyboard interrupt received. Shutting down...")
            notifier.send_notification("💤 *Ringba RPC Monitor Stopped*\nThe monitoring service has been stopped.")
            break
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            notifier.send_notification(f"⚠️ *Error*\nAn error occurred: {str(e)}")
            # Sleep for 5 minutes before retrying
            time.sleep(300)

if __name__ == "__main__":
    main() 