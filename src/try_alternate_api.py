#!/usr/bin/env python3
"""
Try alternate API structures for Ringba.
Some Ringba instances may use a different API structure.
"""

import os
import sys
import requests
import json
import logging
import dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('alternate_api.log')
    ]
)
logger = logging.getLogger('alternate_api')

def main():
    """Test alternate API structures for Ringba"""
    print("=" * 50)
    print("  Ringba Alternate API Structure Test")
    print("=" * 50)
    
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token:
        print("Error: RINGBA_API_TOKEN not found in .env file.")
        sys.exit(1)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    # Try alternate base URLs
    base_urls = [
        "https://api.ringba.com/v2",
        "https://api.ringba.com/v1",
        "https://api.ringba.com",
        "https://app.ringba.com/api",
        "https://app.ringba.com/api/v2"
    ]
    
    # Try alternate account endpoints
    account_endpoints = [
        "/accounts",
        "/account",
        "/organizations",
        "/organization",
        "/clients",
        "/client",
        "/affiliates"
    ]
    
    print("\nTrying alternate API structures...")
    
    # First test each base URL with a simple ping
    working_bases = []
    for base_url in base_urls:
        print(f"\nTesting base URL: {base_url}")
        try:
            response = requests.get(f"{base_url}/ping", headers=headers)
            print(f"Ping status: {response.status_code}")
            
            if response.status_code < 500:  # Even 404 is interesting here
                working_bases.append(base_url)
        except Exception as e:
            print(f"Error: {str(e)}")
    
    # Try alternative account discovery endpoints
    for base_url in working_bases:
        print(f"\nTesting account endpoints with base: {base_url}")
        
        for endpoint in account_endpoints:
            url = f"{base_url}{endpoint}"
            print(f"Testing: {url}")
            
            try:
                response = requests.get(url, headers=headers)
                status = response.status_code
                print(f"Status: {status}")
                
                if status == 200:
                    print("✓ Success!")
                    try:
                        data = response.json()
                        print(json.dumps(data, indent=2))
                    except:
                        print("Could not parse JSON response")
                else:
                    print("✗ Failed")
            except Exception as e:
                print(f"Error: {str(e)}")
    
    # If we have an account ID, try some specific endpoints with that ID
    if account_id:
        print(f"\nTrying endpoints with account ID: {account_id}")
        
        # Try different patterns for how the account ID might be used
        patterns = [
            "{base}/accounts/{id}",
            "{base}/{id}",
            "{base}/account/{id}",
            "{base}/clients/{id}",
            "{base}/organizations/{id}"
        ]
        
        # Try different endpoint formats
        endpoints = [
            "targets",
            "target-groups",
            "campaigns",
            "buyers",
            "numbers",
            "calls",
            "calllogs",
            "reporting"
        ]
        
        for base_url in working_bases:
            for pattern in patterns:
                account_url = pattern.format(base=base_url, id=account_id)
                print(f"\nTrying pattern: {account_url}")
                
                # First try the account endpoint itself
                try:
                    response = requests.get(account_url, headers=headers)
                    print(f"Status: {response.status_code}")
                    if response.status_code == 200:
                        print("✓ Account endpoint works!")
                        try:
                            data = response.json()
                            print(json.dumps(data, indent=2))
                        except:
                            pass
                except Exception as e:
                    print(f"Error: {str(e)}")
                
                # Now try with sub-endpoints
                for endpoint in endpoints:
                    url = f"{account_url}/{endpoint}"
                    print(f"Testing: {url}")
                    
                    try:
                        response = requests.get(url, headers=headers)
                        status = response.status_code
                        print(f"Status: {status}")
                        
                        if status == 200:
                            print("✓ Success!")
                            try:
                                data = response.json()
                                if isinstance(data, dict) and 'items' in data:
                                    print(f"Found {len(data['items'])} items")
                                elif isinstance(data, list):
                                    print(f"Found {len(data)} items")
                            except:
                                pass
                    except Exception as e:
                        print(f"Error: {str(e)}")
    
    print("\n" + "=" * 50)
    print("Summary:")
    print("If you found any working endpoints, update your code to use those specific patterns.")
    print("If no endpoints work, contact Ringba support for assistance.")
    print("=" * 50)

if __name__ == "__main__":
    main() 