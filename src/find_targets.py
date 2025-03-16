#!/usr/bin/env python3
"""
Script to find targets in various locations within the Ringba API.
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
        logging.FileHandler('find_targets.log')
    ]
)
logger = logging.getLogger('find_targets')

def main():
    """Find targets across different account structures"""
    print("=" * 50)
    print("  Ringba Target Finder")
    print("=" * 50)
    
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
    
    # Try both Bearer and Token auth formats
    headers_formats = [
        {"Content-Type": "application/json", "Authorization": f"Bearer {api_token}"},
        {"Content-Type": "application/json", "Authorization": f"Token {api_token}"}
    ]
    
    base_api_url = "https://api.ringba.com/v2"
    
    # Paths to try for finding targets
    target_paths = [
        f"/{account_id}/targets",
        f"/accounts/{account_id}/targets",
        f"/clients/{account_id}/targets",
        f"/organizations/{account_id}/targets"
    ]
    
    # First check for targets directly
    found_targets = False
    
    for headers in headers_formats:
        auth_type = "Bearer" if "Bearer" in headers["Authorization"] else "Token"
        print(f"\nTrying with {auth_type} authentication...")
        
        for path in target_paths:
            url = f"{base_api_url}{path}"
            print(f"\nChecking: {url}")
            
            try:
                response = requests.get(url, headers=headers)
                status = response.status_code
                print(f"Status: {status}")
                
                if status == 200:
                    print("✓ Success!")
                    data = response.json()
                    
                    if isinstance(data, dict) and 'items' in data:
                        items = data['items']
                        if items:
                            print(f"✅ Found {len(items)} targets!")
                            found_targets = True
                            for i, item in enumerate(items[:5]):
                                if i >= 5:
                                    break
                                    
                                print(f"\nTarget {i+1}:")
                                if 'name' in item:
                                    print(f"  Name: {item['name']}")
                                if 'id' in item:
                                    print(f"  ID: {item['id']}")
                            
                            # Try to get stats for first target
                            if items and 'id' in items[0]:
                                target_id = items[0]['id']
                                stats_url = f"{base_api_url}{path.rsplit('/', 1)[0]}/{target_id}/Counts"
                                print(f"\nGetting stats for first target: {stats_url}")
                                
                                try:
                                    stats_response = requests.get(stats_url, headers=headers)
                                    if stats_response.status_code == 200:
                                        stats = stats_response.json()
                                        print("Stats found:")
                                        if 'revenue' in stats:
                                            print(f"  Revenue: ${float(stats['revenue']):.2f}")
                                        if 'callCount' in stats:
                                            print(f"  Calls: {stats['callCount']}")
                                except Exception as e:
                                    print(f"Could not get stats: {str(e)}")
                        else:
                            print("✓ Endpoint works but no targets found (empty list)")
                    elif isinstance(data, list):
                        if data:
                            print(f"✅ Found {len(data)} targets in list format!")
                            found_targets = True
                            for i, item in enumerate(data[:5]):
                                if i >= 5:
                                    break
                                    
                                print(f"\nTarget {i+1}:")
                                if isinstance(item, dict):
                                    if 'name' in item:
                                        print(f"  Name: {item['name']}")
                                    if 'id' in item:
                                        print(f"  ID: {item['id']}")
                        else:
                            print("✓ Endpoint works but no targets found (empty list)")
                    else:
                        print("Response format unexpected, may not contain targets")
            except Exception as e:
                print(f"Error: {str(e)}")
    
    # If we haven't found targets yet, look for other accounts
    if not found_targets:
        print("\n\nSearching for related accounts...")
        
        # API paths to check for related accounts
        account_paths = [
            "/accounts",
            "/organizations",
            "/clients",
            f"/{account_id}/accounts",
            f"/{account_id}/clients"
        ]
        
        related_accounts = []
        
        for headers in headers_formats:
            for path in account_paths:
                url = f"{base_api_url}{path}"
                print(f"\nChecking: {url}")
                
                try:
                    response = requests.get(url, headers=headers)
                    status = response.status_code
                    print(f"Status: {status}")
                    
                    if status == 200:
                        print("✓ Success!")
                        data = response.json()
                        
                        if isinstance(data, dict) and 'items' in data:
                            items = data['items']
                            print(f"Found {len(items)} related accounts")
                            
                            for item in items:
                                if 'id' in item:
                                    name = item.get('name', 'Unknown')
                                    related_id = item['id']
                                    related_accounts.append((name, related_id))
                                    print(f"  • {name} (ID: {related_id})")
                        elif isinstance(data, list):
                            print(f"Found {len(data)} related accounts in list format")
                            
                            for item in data:
                                if isinstance(item, dict) and 'id' in item:
                                    name = item.get('name', 'Unknown')
                                    related_id = item['id']
                                    related_accounts.append((name, related_id))
                                    print(f"  • {name} (ID: {related_id})")
                except Exception as e:
                    print(f"Error: {str(e)}")
        
        # Check for targets in related accounts
        if related_accounts:
            print("\n\nChecking for targets in related accounts...")
            
            for name, related_id in related_accounts:
                print(f"\nChecking targets in: {name} (ID: {related_id})")
                
                target_url = f"{base_api_url}/{related_id}/targets"
                print(f"Testing: {target_url}")
                
                try:
                    # Try both header formats
                    for headers in headers_formats:
                        response = requests.get(target_url, headers=headers)
                        status = response.status_code
                        
                        if status == 200:
                            print("✓ Success!")
                            data = response.json()
                            
                            if isinstance(data, dict) and 'items' in data:
                                items = data['items']
                                if items:
                                    print(f"✅ Found {len(items)} targets in this account!")
                                    for i, item in enumerate(items[:3]):
                                        print(f"\nTarget {i+1}:")
                                        if 'name' in item:
                                            print(f"  Name: {item['name']}")
                                        if 'id' in item:
                                            print(f"  ID: {item['id']}")
                                    
                                    print(f"\nDo you want to update your .env file to use this account? (y/n): ", end='')
                                    choice = input().lower()
                                    if choice == 'y':
                                        update_env_file(related_id)
                                        print(f"Updated .env file with account ID: {related_id}")
                                        return  # Exit after updating
                                else:
                                    print("No targets found in this account (empty list)")
                            break  # No need to try other header format if successful
                except Exception as e:
                    print(f"Error: {str(e)}")
    
    # Print summary
    print("\n" + "=" * 50)
    if found_targets:
        print("✅ Successfully found targets! You can now run the monitoring script.")
    else:
        print("❌ No targets found. You may need to:")
        print("1. Contact Ringba support to verify API permissions")
        print("2. Request documentation specific to your account structure")
        print("3. Check if targets might be under a different name or path")
    print("=" * 50)

def update_env_file(new_account_id):
    """Update the .env file with a new account ID"""
    dotenv_path = '.env'
    
    # Read the current .env file
    with open(dotenv_path, 'r') as f:
        lines = f.readlines()
    
    # Update the account ID
    updated = False
    for i, line in enumerate(lines):
        if line.startswith('RINGBA_ACCOUNT_ID='):
            lines[i] = f'RINGBA_ACCOUNT_ID={new_account_id}\n'
            updated = True
            break
    
    # Add the account ID if it doesn't exist
    if not updated:
        lines.append(f'RINGBA_ACCOUNT_ID={new_account_id}\n')
    
    # Write the updated .env file
    with open(dotenv_path, 'w') as f:
        f.writelines(lines)

if __name__ == "__main__":
    main() 