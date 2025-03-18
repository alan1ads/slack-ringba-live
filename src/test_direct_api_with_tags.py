#!/usr/bin/env python3
"""
Test script for the direct Ringba API client with tag information
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
logger = logging.getLogger('test_direct_api_tags')

def main():
    """
    Main function to test the direct Ringba API client with tag information
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
    
    # Test getting available call log columns
    logger.info("Testing call log columns API")
    columns = api.get_call_log_columns()
    
    if not columns:
        logger.warning("No call log columns found")
    else:
        logger.info(f"Found {len(columns)} call log columns")
        # Display the first 5 columns
        for i, column in enumerate(columns[:5]):
            column_name = column.get("name", "Unknown")
            column_type = column.get("type", "Unknown")
            logger.info(f"Column {i+1}: {column_name} (Type: {column_type})")
    
    # Test getting tags
    logger.info("Testing tags API")
    tags = api.get_tags()
    
    if not tags:
        logger.warning("No tags found")
    else:
        logger.info(f"Found {len(tags)} tags")
        # Display the first 5 tags
        for i, tag in enumerate(tags[:5]):
            tag_id = tag.get("id", "Unknown")
            tag_name = tag.get("name", "Unknown")
            created_by = tag.get("createdBy", "Unknown")
            logger.info(f"Tag {i+1}: {tag_name} (ID: {tag_id}, Created by: {created_by})")
    
    # Get today's date and yesterday
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Test getting call logs with tag info for today
    logger.info(f"Testing call logs API with tag info for today ({today})")
    call_logs = api.get_call_logs(start_date=today, end_date=today)
    
    if not call_logs or "items" not in call_logs:
        logger.warning("No call logs found for today")
    else:
        items = call_logs.get("items", [])
        logger.info(f"Found {len(items)} call logs")
        
        # Count calls with tags
        calls_with_tags = 0
        for item in items:
            if item.get("tagIds") and len(item.get("tagIds", [])) > 0:
                calls_with_tags += 1
        
        logger.info(f"Found {calls_with_tags} calls with tags out of {len(items)} calls")
        
        # Display the first 3 call logs with tags
        tag_calls_shown = 0
        for item in items:
            if item.get("tagIds") and len(item.get("tagIds", [])) > 0:
                if tag_calls_shown >= 3:
                    break
                    
                target_name = item.get("targetName", "Unknown")
                target_id = item.get("targetId", "Unknown")
                duration = item.get("duration", 0)
                connect_time = item.get("connectTime", "Unknown")
                payout = item.get("payout", 0)
                tag_ids = item.get("tagIds", [])
                
                logger.info(f"Call with tags {tag_calls_shown+1}: {target_name} (ID: {target_id})")
                logger.info(f"  Duration: {duration} seconds, Connect Time: {connect_time}, Payout: ${payout:.2f}")
                logger.info(f"  Tag IDs: {tag_ids}")
                
                tag_calls_shown += 1
    
    # Test getting targets above threshold with tag info
    threshold = 10.0
    logger.info(f"Testing getting targets with RPC above ${threshold} for today with tag info")
    targets_above_threshold = api.get_targets_above_threshold(threshold, today)
    
    if not targets_above_threshold:
        logger.warning(f"No targets found with RPC above ${threshold} for today")
    else:
        logger.info(f"Found {len(targets_above_threshold)} targets with RPC above ${threshold}")
        
        # Count targets with tags
        targets_with_tags = 0
        for target in targets_above_threshold:
            if target.get("tags") and len(target.get("tags", {})) > 0:
                targets_with_tags += 1
        
        logger.info(f"Found {targets_with_tags} targets with tags out of {len(targets_above_threshold)} targets")
        
        # Display the first 3 targets with tags
        targets_shown = 0
        for target in sorted(targets_above_threshold, key=lambda x: x['rpc'], reverse=True):
            if targets_shown >= 3:
                break
                
            logger.info(f"Target {targets_shown+1}: {target['name']} (ID: {target['id']})")
            logger.info(f"  RPC: ${target['rpc']:.2f}, Calls: {target['calls']}, Revenue: ${target['revenue']:.2f}")
            
            # Display tags if any
            if target.get("tags") and len(target.get("tags", {})) > 0:
                tags = target.get("tags", {})
                logger.info(f"  Tags: {tags}")
            
            targets_shown += 1
    
    logger.info("Test with tag information completed successfully")
    return True

if __name__ == "__main__":
    main() 