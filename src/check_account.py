#!/usr/bin/env python3
"""
Diagnostic script to check Ringba API access and identify the correct account ID.
"""

import os
import sys
import requests
import json
import dotenv

def main():
    """Test Ringba API access and find account information"""
    print("=" * 50)
    print("  Ringba API Diagnostic Tool")
    print("=" * 50)
    
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    
    if not api_token:
        print("Error: RINGBA_API_TOKEN not found in .env file.")
        print("Please run src/get_token.py first to obtain a token.")
        sys.exit(1)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    # First check if we can access the API at all
    print("\nTesting basic API access...")
    
    base_api_url = "https://api.ringba.com/v2"
    
    # List of endpoints to try
    endpoints = [
        "/accounts",
        "/ApiTokens",
        "/token/info"
    ]
    
    successful_endpoint = None
    
    for endpoint in endpoints:
        url = f"{base_api_url}{endpoint}"
        print(f"Trying endpoint: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            print(f"✓ Success! Endpoint {endpoint} is accessible.")
            successful_endpoint = endpoint
            break
        except Exception as e:
            print(f"✗ Failed to access {endpoint}: {str(e)}")
    
    if successful_endpoint is None:
        print("\nFailed to access any Ringba API endpoints.")
        print("This suggests your API token may be invalid or expired.")
        print("Please run src/get_token.py to obtain a new token.")
        sys.exit(1)
    
    # Now try to find account information
    print("\nLooking for account information...")
    
    # If we were able to access /accounts, use that
    if successful_endpoint == "/accounts":
        try:
            accounts_url = f"{base_api_url}/accounts"
            response = requests.get(accounts_url, headers=headers)
            response.raise_for_status()
            
            accounts_data = response.json()
            accounts = accounts_data.get('items', [])
            
            if accounts:
                print(f"\nFound {len(accounts)} accounts accessible with your token:")
                for account in accounts:
                    print(f"- Account Name: {account.get('name')}")
                    print(f"  Account ID: {account.get('id')}")
                    print(f"  Create Date: {account.get('createDate', 'Unknown')}")
                    print("")
                
                print("\nUpdate your .env file with the correct RINGBA_ACCOUNT_ID from above.")
            else:
                print("No accounts found that are accessible with your token.")
        except Exception as e:
            print(f"Error retrieving account information: {str(e)}")
    
    # If we got token info, use that to check
    elif successful_endpoint == "/token/info":
        try:
            token_info_url = f"{base_api_url}/token/info"
            response = requests.get(token_info_url, headers=headers)
            response.raise_for_status()
            
            token_info = response.json()
            print("\nToken Information:")
            print(json.dumps(token_info, indent=2))
            
            # Try to extract account ID if available
            if "accountId" in token_info:
                account_id = token_info["accountId"]
                print(f"\nDetected Account ID: {account_id}")
                print("Update your .env file with this RINGBA_ACCOUNT_ID.")
        except Exception as e:
            print(f"Error retrieving token information: {str(e)}")
    
    # Try a few common endpoints using the current account ID
    current_account_id = os.getenv('RINGBA_ACCOUNT_ID')
    if current_account_id:
        print(f"\nTesting endpoints for account ID: {current_account_id}")
        
        account_endpoints = [
            f"/accounts/{current_account_id}",
            f"/accounts/{current_account_id}/targets",
            f"/accounts/{current_account_id}/buyers",
            f"/accounts/{current_account_id}/calllogs"
        ]
        
        for endpoint in account_endpoints:
            url = f"{base_api_url}{endpoint}"
            print(f"Trying: {url}")
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                print(f"✓ Success! Endpoint {endpoint} is accessible.")
            except Exception as e:
                print(f"✗ Failed to access {endpoint}: {str(e)}")
    
    print("\n" + "=" * 50)
    print("  Diagnostics Complete")
    print("=" * 50)

if __name__ == "__main__":
    main() 