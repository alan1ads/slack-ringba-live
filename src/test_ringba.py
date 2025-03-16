#!/usr/bin/env python3
"""
Ringba RPC Monitor - Test Script

This script tests the Ringba API integration using our updated implementation
that uses the correct Bearer token format and targets array in the response.
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Import custom modules
from ringba_api import RingbaAPI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_ringba.log')
    ]
)
logger = logging.getLogger('ringba_test')

def main():
    """Main test function"""
    print("\n==============================================")
    print("   Ringba API Integration Test ")
    print("==============================================\n")
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token or not account_id:
        print("Error: Missing API token or account ID in .env file")
        sys.exit(1)
    
    print(f"Using Account ID: {account_id}")
    print(f"API Token: {api_token[:5]}...{api_token[-5:]}\n")
    
    # Initialize API
    api = RingbaAPI(api_token, account_id)
    
    # Test authentication
    print("1. Testing Authentication...")
    if api.test_auth():
        print("✅ Authentication successful")
    else:
        print("❌ Authentication failed")
        sys.exit(1)
    
    # Get all targets
    print("\n2. Getting All Targets...")
    targets, stats = api.get_all_targets(include_stats=True)
    
    if targets:
        print(f"✅ Found {len(targets)} targets")
        
        # Display first few targets
        print("\nFirst 5 targets:")
        for i, target in enumerate(targets[:5], 1):
            enabled_status = "🟢 Enabled" if target.get('enabled', False) else "🔴 Disabled"
            print(f"  {i}. {target.get('name')} (ID: {target.get('id')}) - {enabled_status}")
    else:
        print("❌ Failed to get targets")
        sys.exit(1)
    
    # Get target details for the first target
    if targets:
        first_target = targets[0]
        target_id = first_target.get('id')
        target_name = first_target.get('name')
        
        print(f"\n3. Getting Details for Target: {target_name}...")
        target_details = api.get_target_details(target_id)
        
        if target_details:
            print(f"✅ Successfully retrieved target details")
            
            # Display some key details
            print(f"  - Name: {target_details.get('name')}")
            print(f"  - Enabled: {target_details.get('enabled')}")
            print(f"  - TimeZone: {target_details.get('timeZone')}")
        else:
            print(f"❌ Failed to get target details")
    
    # Get target counts for the first enabled target
    enabled_targets = [t for t in targets if t.get('enabled', False)]
    
    if enabled_targets:
        test_target = enabled_targets[0]
        target_id = test_target.get('id')
        target_name = test_target.get('name')
        
        # Get yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')
        
        print(f"\n4. Getting Counts for Target: {target_name} on {date_str}...")
        counts_data = api.get_target_counts(target_id, date_str)
        
        if counts_data:
            print("✅ Successfully retrieved target counts")
            if isinstance(counts_data, list) and len(counts_data) > 1:
                print(f"  - Count data structure: {counts_data[0:2]}")
            elif isinstance(counts_data, dict):
                print(f"  - Count data structure: {list(counts_data.keys())}")
            else:
                print(f"  - Count data structure: {type(counts_data)}")
        else:
            print("❌ Failed to retrieve target counts")
    
    # Calculate RPC for all enabled targets
    print("\n5. Calculating RPC for Enabled Targets...")
    
    for target in enabled_targets[:5]:  # Limit to first 5 to keep output manageable
        target_id = target.get('id')
        target_name = target.get('name')
        
        rpc = api.calculate_rpc_for_target(target_id)
        
        if rpc is not None:
            print(f"  • {target_name}: RPC = ${rpc:.2f}")
        else:
            print(f"  • {target_name}: Failed to calculate RPC")
    
    # Test finding targets above threshold
    threshold = 5.0  # Use a relatively low threshold for testing
    print(f"\n6. Finding Targets Above RPC Threshold (${threshold:.2f})...")
    
    targets_above = api.find_targets_above_threshold(threshold)
    
    if targets_above:
        print(f"✅ Found {len(targets_above)} targets above threshold")
        
        # Display first few
        for i, target in enumerate(targets_above[:5], 1):
            print(f"  {i}. {target.get('name')}: RPC = ${target.get('rpc', 0):.2f}")
    else:
        print(f"No targets found with RPC above ${threshold:.2f}")
    
    print("\n==============================================")
    print("   Test Completed Successfully! ")
    print("==============================================\n")

if __name__ == "__main__":
    main() 