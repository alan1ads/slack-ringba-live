#!/usr/bin/env python3
"""
Script to systematically try different account ID formats to find valid targets.
"""

import os
import sys
import requests
import json
import logging
import dotenv
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('account_search.log')
    ]
)
logger = logging.getLogger('account_search')

def main():
    """Try different accountId formats to find valid targets"""
    print("=" * 50)
    print("  Ringba Account and Target Finder")
    print("=" * 50)
    
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    current_account_id = os.getenv('RINGBA_ACCOUNT_ID', '')
    
    if not api_token:
        print("Error: RINGBA_API_TOKEN not found in .env file.")
        sys.exit(1)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    base_api_url = "https://api.ringba.com/v2"
    
    # Variations to try for account ID
    account_id_variants = []
    
    # Always include the current account ID if it exists
    if current_account_id:
        account_id_variants.append(current_account_id)
    
    # Add variations of the account ID
    if current_account_id.startswith('RA'):
        # Try without the RA prefix
        account_id_variants.append(current_account_id[2:])
    elif not current_account_id.startswith('RA'):
        # Try with RA prefix
        account_id_variants.append(f"RA{current_account_id}")
    
    # Try account ID with and without possible hyphens
    if '-' in current_account_id:
        account_id_variants.append(current_account_id.replace('-', ''))
    else:
        # Try adding hyphens every 8 chars (common UUID format)
        if len(current_account_id) >= 32:
            id_with_hyphens = current_account_id
            for i in [8, 16, 24]:
                if i < len(id_with_hyphens):
                    id_with_hyphens = id_with_hyphens[:i] + '-' + id_with_hyphens[i:]
            account_id_variants.append(id_with_hyphens)
    
    # Get profile data to look for clues
    print("\nFetching profile data for clues...")
    try:
        response = requests.get(f"{base_api_url}/profile", headers=headers)
        if response.status_code == 200:
            profile_data = response.json()
            
            # Extract potential account IDs from profile
            account_references = []
            
            def scan_for_ids(data, path=""):
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, (dict, list)):
                            scan_for_ids(value, f"{path}.{key}" if path else key)
                        elif isinstance(value, str) and len(value) > 10:
                            # Look for ID-like strings
                            if "id" in key.lower() or "account" in key.lower():
                                account_references.append(value)
                            # Also match strings that look like "RA..." (common Ringba format)
                            elif value.startswith("RA") and len(value) > 15:
                                account_references.append(value)
                            # UUID-like strings
                            elif len(value) >= 32 and all(c in '0123456789abcdefABCDEF-' for c in value):
                                account_references.append(value)
                elif isinstance(data, list):
                    for i, item in enumerate(data):
                        scan_for_ids(item, f"{path}[{i}]")
            
            scan_for_ids(profile_data)
            
            # Add found references if they're not duplicates
            for ref in account_references:
                if ref not in account_id_variants:
                    account_id_variants.append(ref)
            
            print(f"Found {len(account_references)} potential IDs in profile data")
    except Exception as e:
        print(f"Error fetching profile: {str(e)}")
    
    # Try fetching API Tokens for more clues
    print("\nFetching API tokens for more clues...")
    try:
        response = requests.get(f"{base_api_url}/ApiTokens", headers=headers)
        if response.status_code == 200:
            tokens_data = response.json()
            
            if 'items' in tokens_data:
                tokens = tokens_data['items']
                print(f"Found {len(tokens)} API tokens")
                
                # Look for account references in token data
                for token in tokens:
                    scan_for_ids(token)
    except Exception as e:
        print(f"Error fetching tokens: {str(e)}")
    
    # Try URL variations from the API docs
    print(f"\nFound {len(account_id_variants)} account ID variants to try")
    print("Trying all account ID variants...")
    
    found_targets = False
    successful_account_id = None
    targets = []
    
    for account_id in account_id_variants:
        if len(account_id) < 10:  # Skip obviously invalid IDs
            continue
            
        print(f"\nTrying account ID: {account_id}")
        
        # First try the targets endpoint
        targets_url = f"{base_api_url}/{account_id}/targets"
        try:
            print(f"Testing: {targets_url}")
            response = requests.get(targets_url, headers=headers)
            status = response.status_code
            print(f"Status: {status}")
            
            if status == 200:
                print("✓ Success! Found valid account ID and targets endpoint")
                found_targets = True
                successful_account_id = account_id
                
                try:
                    data = response.json()
                    if 'items' in data:
                        targets = data['items']
                        print(f"Found {len(targets)} targets")
                        
                        # Try to get details and stats for first few targets
                        for i, target in enumerate(targets[:3]):
                            if 'id' in target:
                                target_id = target['id']
                                target_name = target.get('name', 'Unknown')
                                print(f"\nTarget {i+1}: {target_name} (ID: {target_id})")
                                
                                # Try to get counts/stats
                                counts_url = f"{base_api_url}/{account_id}/targets/{target_id}/Counts"
                                print(f"Testing counts: {counts_url}")
                                
                                try:
                                    counts_response = requests.get(counts_url, headers=headers)
                                    if counts_response.status_code == 200:
                                        print("✓ Successfully retrieved target stats")
                                        stats = counts_response.json()
                                        
                                        # Calculate RPC if possible
                                        if 'revenue' in stats and 'callCount' in stats:
                                            revenue = float(stats['revenue'])
                                            call_count = int(stats['callCount'])
                                            if call_count > 0:
                                                rpc = revenue / call_count
                                                print(f"  Revenue: ${revenue:.2f}")
                                                print(f"  Calls: {call_count}")
                                                print(f"  RPC: ${rpc:.2f}")
                                            else:
                                                print(f"  Revenue: ${revenue:.2f}")
                                                print(f"  Calls: {call_count}")
                                                print(f"  RPC: N/A (no calls)")
                                    else:
                                        print(f"✗ Failed to get target stats: {counts_response.status_code}")
                                except Exception as e:
                                    print(f"Error getting target stats: {str(e)}")
                                
                                # Slight delay to avoid rate limiting
                                time.sleep(0.5)
                    else:
                        print("No 'items' found in response")
                except Exception as e:
                    print(f"Error parsing targets: {str(e)}")
                    
                # Break out of the loop if we found a working account ID
                break
            else:
                print(f"✗ Failed with this account ID")
        except Exception as e:
            print(f"Error: {str(e)}")
    
    if found_targets:
        print("\n" + "=" * 50)
        print(f"SUCCESS! Found working account ID: {successful_account_id}")
        print(f"Found {len(targets)} targets")
        print("\nRecommended next steps:")
        print(f"1. Update your .env file with this account ID:")
        print(f"   RINGBA_ACCOUNT_ID={successful_account_id}")
        print("2. Run the test script: python src/test_historical_calllogs.py")
        print("=" * 50)
    else:
        print("\n" + "=" * 50)
        print("Could not find a working account ID")
        print("Please contact Ringba support to get your correct account ID")
        print("=" * 50)

if __name__ == "__main__":
    main() 