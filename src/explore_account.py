#!/usr/bin/env python3
"""
Script to thoroughly explore a Ringba account structure to find targets and other resources.
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
        logging.FileHandler('account_exploration.log')
    ]
)
logger = logging.getLogger('account_exploration')

def main():
    """Explore Ringba account structure to find targets and resources"""
    print("=" * 80)
    print("  Ringba Account Structure Explorer")
    print("=" * 80)
    
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token:
        print("Error: RINGBA_API_TOKEN not found in .env file.")
        sys.exit(1)
    
    if not account_id:
        print("Error: RINGBA_ACCOUNT_ID not found in .env file.")
        sys.exit(1)
    
    print(f"\nAccount ID: {account_id}")
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    base_api_url = "https://api.ringba.com/v2"
    
    # List of endpoints to try
    endpoints = [
        "targets",
        "target-groups",
        "targetgroups",
        "campaigns",
        "campaign-groups",
        "campaigngroups", 
        "buyers",
        "payout-groups", 
        "tag-groups",
        "call-routes",
        "callroutes",
        "tags",
        "publishers",
        "affiliates",
        "numbers",
        "inbounds",
        "calllogs",
        "webhooks",
        "integrations",
        "users",
        "billing",
        "reports"
    ]
    
    # Try to explore all endpoints
    print("\nExploring account endpoints...")
    
    for endpoint in endpoints:
        url = f"{base_api_url}/{account_id}/{endpoint}"
        print(f"\nTesting: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            status = response.status_code
            print(f"Status: {status}")
            
            if status == 200:
                print("✓ Success!")
                
                try:
                    data = response.json()
                    
                    # Try to determine the type of response
                    if isinstance(data, dict):
                        if 'items' in data:
                            items = data['items']
                            print(f"Found {len(items)} items")
                            
                            # Display first few items for inspection
                            for i, item in enumerate(items[:3]):
                                if i >= 3:
                                    break
                                    
                                print(f"\nItem {i+1}:")
                                if isinstance(item, dict):
                                    # Try to show a name or identifier
                                    if 'name' in item:
                                        print(f"  Name: {item['name']}")
                                    if 'id' in item:
                                        print(f"  ID: {item['id']}")
                                    
                                    # Show other interesting fields
                                    interesting_fields = ['type', 'status', 'enabled', 'count', 'revenue']
                                    for field in interesting_fields:
                                        if field in item:
                                            print(f"  {field.capitalize()}: {item[field]}")
                                else:
                                    print(f"  {item}")
                            
                            # Show full data of first item if requested
                            if len(items) > 0 and input("\nShow full data of first item? (y/n): ").lower() == 'y':
                                print(json.dumps(items[0], indent=2))
                            
                            # Save results to file if there are items
                            if len(items) > 0:
                                filename = f"ringba_{endpoint}_{len(items)}_items.json"
                                with open(filename, 'w') as f:
                                    json.dump(data, f, indent=2)
                                print(f"\nSaved data to {filename}")
                        else:
                            # Not a collection, show the data
                            print("\nResponse data:")
                            print(json.dumps(data, indent=2))
                    elif isinstance(data, list):
                        print(f"Found {len(data)} items in list")
                        
                        # Display first few items
                        for i, item in enumerate(data[:3]):
                            if i >= 3:
                                break
                                
                            print(f"\nItem {i+1}:")
                            if isinstance(item, dict):
                                # Try to show a name or identifier
                                if 'name' in item:
                                    print(f"  Name: {item['name']}")
                                if 'id' in item:
                                    print(f"  ID: {item['id']}")
                            else:
                                print(f"  {item}")
                        
                        # Save results to file if there are items
                        if len(data) > 0:
                            filename = f"ringba_{endpoint}_{len(data)}_items.json"
                            with open(filename, 'w') as f:
                                json.dump(data, f, indent=2)
                            print(f"\nSaved data to {filename}")
                except Exception as e:
                    print(f"Error parsing response: {str(e)}")
            else:
                print(f"✗ Failed with status {status}")
                if status == 401:
                    print("  ⚠️ Authentication failed. Check your API token.")
                elif status == 403:
                    print("  ⚠️ Permission denied. Your API token may not have access to this resource.")
                elif status == 404:
                    print("  ⚠️ Resource not found. This endpoint may not exist for your account.")
                
                # Try to parse error response
                try:
                    error_data = response.json()
                    print("  Error details:", json.dumps(error_data, indent=2))
                except:
                    pass
        except Exception as e:
            print(f"Error: {str(e)}")
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)
    
    # Try to find sub-accounts or organizations
    print("\n\nChecking for sub-accounts or organizations...")
    org_endpoints = [
        "organizations",
        "organization",
        "accounts",
        "sub-accounts",
        "clients"
    ]
    
    for endpoint in org_endpoints:
        url = f"{base_api_url}/{endpoint}"
        print(f"\nTesting: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            status = response.status_code
            print(f"Status: {status}")
            
            if status == 200:
                print("✓ Success!")
                
                try:
                    data = response.json()
                    
                    # Handle collection response
                    if isinstance(data, dict) and 'items' in data:
                        items = data['items']
                        print(f"Found {len(items)} items")
                        
                        # Display first few items
                        for i, item in enumerate(items[:5]):
                            if i >= 5:
                                break
                                
                            print(f"\nItem {i+1}:")
                            if isinstance(item, dict):
                                # Try to show a name or identifier
                                if 'name' in item:
                                    print(f"  Name: {item['name']}")
                                if 'id' in item:
                                    print(f"  ID: {item['id']}")
                    elif isinstance(data, list):
                        print(f"Found {len(data)} items in list")
                        
                        # Display first few items
                        for i, item in enumerate(data[:5]):
                            if i >= 5:
                                break
                                
                            print(f"\nItem {i+1}:")
                            if isinstance(item, dict):
                                # Try to show a name or identifier
                                if 'name' in item:
                                    print(f"  Name: {item['name']}")
                                if 'id' in item:
                                    print(f"  ID: {item['id']}")
                except Exception as e:
                    print(f"Error parsing response: {str(e)}")
            else:
                print(f"✗ Failed with status {status}")
        except Exception as e:
            print(f"Error: {str(e)}")
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)
    
    # Print summary
    print("\n" + "=" * 80)
    print("Exploration Summary:")
    print("1. Look at the JSON files created to find more information about your account resources")
    print("2. Check if you found any targets in the 'targets' endpoint")
    print("3. If no targets were found, you may need to:")
    print("   a. Check other endpoints like 'campaigns' or 'buyers' for your targets")
    print("   b. Contact Ringba support to ensure your API token has the right permissions")
    print("   c. Verify you're using the correct account ID if you have multiple accounts")
    print("=" * 80)

if __name__ == "__main__":
    main() 