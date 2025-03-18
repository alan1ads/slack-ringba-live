#!/usr/bin/env python3
"""
Test script to find targets with high RPC
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from ringba_api import RingbaAPI

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('rpc_tester')

def main():
    load_dotenv()
    
    # Get configuration
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token or not account_id:
        logger.error("Missing API token or account ID in environment variables")
        return
    
    # Initialize API
    api = RingbaAPI(api_token, account_id)
    
    # Test authentication
    if not api.test_auth():
        logger.error("Authentication failed")
        return
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Checking data for date: {today}")
    
    # Get all targets
    response = api.get_all_targets()
    if not response or 'targets' not in response:
        logger.error("Failed to retrieve targets")
        return
    
    targets = response['targets']
    
    # Filter enabled targets
    enabled_targets = [t for t in targets if isinstance(t, dict) and t.get('enabled', False)]
    
    logger.info(f"Found {len(enabled_targets)} enabled targets out of {len(targets)} total targets")
    
    # Check each target for RPC
    high_rpc_targets = []
    
    for target in enabled_targets[:10]:  # Just test first 10 for speed
        target_id = target.get('id')
        target_name = target.get('name', 'Unknown')
        
        logger.info(f"Checking target: {target_name} ({target_id})")
        
        # Get the stats
        counts = api.get_target_counts(target_id, today)
        logger.info(f"Raw counts data: {counts}")
        
        if counts and isinstance(counts, dict):
            calls = counts.get('totalCalls', 0)
            revenue = counts.get('payout', 0)
            
            if calls > 0:
                rpc = revenue / calls
                logger.info(f"Target has {calls} calls, ${revenue} revenue, RPC: ${rpc:.2f}")
                
                if rpc >= 10.0:
                    high_rpc_targets.append({
                        'id': target_id,
                        'name': target_name,
                        'calls': calls,
                        'revenue': revenue,
                        'rpc': rpc
                    })
            else:
                logger.info(f"Target has 0 calls")
    
    # Show summary
    if high_rpc_targets:
        logger.info(f"Found {len(high_rpc_targets)} targets with RPC above $10.00")
        for target in high_rpc_targets:
            logger.info(f"High RPC Target: {target['name']} - RPC: ${target['rpc']:.2f}")
    else:
        logger.info("No targets found with RPC above $10.00")

if __name__ == "__main__":
    main() 