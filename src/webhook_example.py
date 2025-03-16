#!/usr/bin/env python3
"""
Webhook Example for RPC Monitoring with Ringba

This script demonstrates how to:
1. Set up a webhook listener server
2. Register a webhook with Ringba
3. Process webhook notifications to monitor RPC

Note: This is just an example and would need additional work to be production-ready.
"""

import os
import json
import logging
from datetime import datetime
import pytz
from flask import Flask, request, jsonify
import requests
import dotenv
from ringba_api import RingbaAPI
from slack_notifier import SlackNotifier

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('webhook_rpc_monitor.log')
    ]
)
logger = logging.getLogger('webhook_rpc_monitor')

# Load environment variables
dotenv.load_dotenv()
RINGBA_API_TOKEN = os.getenv('RINGBA_API_TOKEN')
RINGBA_ACCOUNT_ID = os.getenv('RINGBA_ACCOUNT_ID')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
RPC_THRESHOLD = float(os.getenv('RPC_THRESHOLD', 10.0))

# Store for targets that are being monitored
monitored_targets = {}

# Initialize Flask app for webhook listener
app = Flask(__name__)

# Initialize API client
ringba_api = RingbaAPI(RINGBA_API_TOKEN, RINGBA_ACCOUNT_ID)
slack_notifier = SlackNotifier(SLACK_WEBHOOK_URL)

@app.route('/webhook/rpc', methods=['POST'])
def rpc_webhook_handler():
    """Handle incoming webhooks from Ringba"""
    try:
        data = request.json
        logger.info(f"Received webhook notification: {data}")
        
        # Process the webhook data
        # This would depend on what data Ringba sends in webhooks
        # For example, if it includes call data:
        
        # Get target info
        target_id = data.get('target', {}).get('id')
        target_name = data.get('target', {}).get('name')
        
        if not target_id or not target_name:
            logger.warning("Missing target information in webhook data")
            return jsonify({"status": "error", "message": "Missing target information"}), 400
        
        # Get or calculate current RPC
        current_rpc = calculate_rpc_from_webhook(data)
        
        # Check if we're monitoring this target
        if target_id in monitored_targets:
            # Get the morning RPC we stored
            morning_rpc = monitored_targets[target_id].get('morning_rpc')
            
            # Check if RPC has dropped below threshold
            if current_rpc < RPC_THRESHOLD and morning_rpc >= RPC_THRESHOLD:
                # Calculate drop percentage
                drop_percentage = ((morning_rpc - current_rpc) / morning_rpc) * 100
                
                # Send alert
                message = f"⚠️ *RPC Alert - Target Dropped Below ${RPC_THRESHOLD:.2f}*\n\n" \
                         f"• *{target_name}*\n" \
                         f"  - Morning RPC: *${morning_rpc:.2f}*\n" \
                         f"  - Current RPC: *${current_rpc:.2f}*\n" \
                         f"  - Drop: *{drop_percentage:.1f}%*\n\n"
                
                slack_notifier.send_notification(message)
                logger.info(f"Alert sent for target {target_name}: RPC dropped from ${morning_rpc:.2f} to ${current_rpc:.2f}")
        
        return jsonify({"status": "success"}), 200
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

def calculate_rpc_from_webhook(data):
    """
    Calculate RPC from webhook data
    This is a placeholder - the actual implementation would depend on what data Ringba provides
    """
    # This is just an example - actual implementation would depend on webhook payload
    calls = data.get('calls', 1)
    revenue = data.get('revenue', 0)
    
    return revenue / calls if calls > 0 else 0

def register_webhook(callback_url):
    """
    Register a webhook with Ringba API
    
    Args:
        callback_url (str): The URL where webhook notifications should be sent
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # This is hypothetical - actual implementation would depend on Ringba's webhook registration API
        url = f"{ringba_api.BASE_URL}/{ringba_api.account_id}/WebHooks"
        
        payload = {
            "name": "RPC Monitor Webhook",
            "url": callback_url,
            "events": ["call.completed"],  # Assuming this is when revenue is finalized
            "isActive": True,
            "headers": {}
        }
        
        response = requests.post(url, json=payload, headers=ringba_api.get_headers())
        response.raise_for_status()
        
        logger.info(f"Successfully registered webhook with Ringba API: {callback_url}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to register webhook: {str(e)}")
        return False

def check_morning_targets():
    """Check RPC for targets at 10 AM and save targets above threshold for monitoring"""
    logger.info("Running morning check (10 AM EST)")
    
    try:
        # Clear the monitored targets
        monitored_targets.clear()
        
        # Get all targets that are enabled
        targets_data = ringba_api.get_all_targets(include_stats=True, enabled_only=True)
        targets = targets_data.get('targets', [])
        
        # Process each target
        for target in targets:
            target_id = target.get('id')
            target_name = target.get('name')
            
            # Get target details with stats
            target_details = ringba_api.get_target_details(target_id)
            
            # Calculate RPC
            rpc = ringba_api.calculate_rpc_for_target(target_details)
            logger.info(f"Morning RPC for {target_name} (ID: {target_id}): ${rpc:.2f}")
            
            # If RPC is above threshold, add to monitored targets
            if rpc >= RPC_THRESHOLD:
                monitored_targets[target_id] = {
                    'id': target_id,
                    'name': target_name,
                    'morning_rpc': rpc
                }
        
        logger.info(f"Now monitoring {len(monitored_targets)} targets with RPC above ${RPC_THRESHOLD:.2f}")
    
    except Exception as e:
        logger.error(f"Error in morning check: {str(e)}")

def main():
    """Main function to initialize and run the webhook monitor"""
    
    try:
        # Test authentication
        ringba_api.authenticate()
        logger.info("Successfully authenticated with Ringba API")
        
        # Register webhook (you would need to provide your actual server URL here)
        server_url = "https://your-server.com/webhook/rpc"
        register_webhook(server_url)
        
        # Check existing webhooks
        webhooks = ringba_api.get_webhooks()
        logger.info(f"Existing webhooks: {webhooks}")
        
        # Run initial morning check
        check_morning_targets()
        
        # Start the Flask server to listen for webhooks
        # In a production environment, you'd use a proper WSGI server
        app.run(host='0.0.0.0', port=5000)
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main() 