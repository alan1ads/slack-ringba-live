#!/usr/bin/env python3
"""
Test script for the direct Ringba API client
"""

import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ringba_direct_api import RingbaDirectAPI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_direct_api')

def main():
    """
    Main function to test the direct Ringba API client
    """
    # Load environment variables
    load_dotenv()
    
    # Get API token and account ID from environment variables
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token or not account_id:
        logger.error("API token or account ID missing from environment variables")
        return False
    
    logger.info(f"Testing direct API client with account ID: {account_id}")
    
    # Initialize the API client
    api = RingbaDirectAPI(api_token, account_id)
    
    # Test authentication
    if not api.test_auth():
        logger.error("Authentication failed")
        return False
    
    logger.info("Authentication successful")
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Test getting insights data for yesterday
    logger.info(f"Testing insights API for yesterday ({yesterday})")
    insights = api.get_insights(start_date=yesterday, end_date=yesterday, group_by="targetId")
    
    if not insights or "items" not in insights:
        logger.warning("No insights data found for yesterday")
    else:
        items = insights.get("items", [])
        logger.info(f"Found {len(items)} items in insights data")
        
        # Display the first 5 items with highest RPC
        for i, item in enumerate(items[:5]):
            target_id = item.get("targetId", "Unknown")
            rpc = item.get("rpc", 0)
            calls = item.get("calls", 0)
            revenue = item.get("revenue", 0)
            
            # Get target details to get the name
            target_details = api.get_target_details(target_id)
            target_name = target_details.get("name", "Unknown") if target_details else "Unknown"
            
            logger.info(f"Target {i+1}: {target_name} (ID: {target_id})")
            logger.info(f"  RPC: ${rpc:.2f}, Calls: {calls}, Revenue: ${revenue:.2f}")
    
    # Test getting targets above threshold
    threshold = 10.0
    logger.info(f"Testing getting targets with RPC above ${threshold} for yesterday")
    targets_above_threshold = api.get_targets_above_threshold(threshold, yesterday)
    
    if not targets_above_threshold:
        logger.warning(f"No targets found with RPC above ${threshold} for yesterday")
    else:
        logger.info(f"Found {len(targets_above_threshold)} targets with RPC above ${threshold}")
        
        # Display the first 5 targets with highest RPC
        for i, target in enumerate(sorted(targets_above_threshold, key=lambda x: x['rpc'], reverse=True)[:5]):
            logger.info(f"Target {i+1}: {target['name']} (ID: {target['id']})")
            logger.info(f"  RPC: ${target['rpc']:.2f}, Calls: {target['calls']}, Revenue: ${target['revenue']:.2f}")
    
    # Test getting call logs
    logger.info(f"Testing call logs API for yesterday")
    call_logs = api.get_call_logs(start_date=yesterday, end_date=yesterday)
    
    if not call_logs or "items" not in call_logs:
        logger.warning("No call logs found for yesterday")
    else:
        items = call_logs.get("items", [])
        logger.info(f"Found {len(items)} call logs")
        
        # Display the first 3 call logs
        for i, item in enumerate(items[:3]):
            target_name = item.get("targetName", "Unknown")
            target_id = item.get("targetId", "Unknown")
            duration = item.get("duration", 0)
            connect_time = item.get("connectTime", "Unknown")
            payout = item.get("payout", 0)
            
            logger.info(f"Call {i+1}: {target_name} (ID: {target_id})")
            logger.info(f"  Duration: {duration} seconds, Connect Time: {connect_time}, Payout: ${payout:.2f}")
    
    logger.info("Test completed successfully")
    return True

if __name__ == "__main__":
    main() 