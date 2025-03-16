#!/usr/bin/env python3
"""
Script to specifically help find your Ringba account ID.
"""

import os
import sys
import requests
import json
import dotenv

def main():
    """Find Ringba account ID"""
    print("=" * 50)
    print("  Ringba Account ID Finder")
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
    
    print("\nAttempting to find your Ringba account ID...")
    
    base_api_url = "https://api.ringba.com/v2"
    
    # List of endpoints that might reveal account information
    endpoints = [
        "/ApiTokens",
        "/token/info",
        "/users/current",
        "/profile",
        "/me",
        "/campaigns", 
        "/campaigngroups",
        "/targets"
    ]
    
    print("\nTrying various API endpoints...")
    
    for endpoint in endpoints:
        url = f"{base_api_url}{endpoint}"
        print(f"Checking: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print("Response contains data. Analyzing...")
                    
                    # Different endpoints might have different structures
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if 'account' in key.lower():
                                print(f"Possible account info found in '{key}': {value}")
                            
                            # Check for any field containing "RA" which is typical for Ringba account IDs
                            if isinstance(value, str) and 'RA' in value:
                                print(f"Potential account ID found in '{key}': {value}")
                    
                    # If it's a list of items, check each one
                    if isinstance(data, list) or (isinstance(data, dict) and 'items' in data):
                        items = data if isinstance(data, list) else data.get('items', [])
                        print(f"Found {len(items)} items, checking for account information...")
                        
                        for i, item in enumerate(items):
                            if isinstance(item, dict):
                                for key, value in item.items():
                                    if 'account' in key.lower():
                                        print(f"Item {i+1}: Possible account info in '{key}': {value}")
                                    
                                    # Check for any field containing "RA"
                                    if isinstance(value, str) and 'RA' in value:
                                        print(f"Item {i+1}: Potential account ID found in '{key}': {value}")
                    
                    print(f"Full response data: {json.dumps(data, indent=2)[:1000]}...")  # Limit to first 1000 chars
                    print("-" * 50)
                    
                except Exception as e:
                    print(f"Could not parse response as JSON: {str(e)}")
            
        except Exception as e:
            print(f"Error accessing endpoint: {str(e)}")
        
        print("")  # Line break between endpoints
    
    # Try to get the first page of data from different account resources
    print("\nTrying direct resource requests...")
    resources = ["targets", "buyers", "campaigns", "calllogs", "numbers", "invoices"]
    
    found_working_endpoints = []
    
    # First try with no account ID (some APIs might not need it)
    for resource in resources:
        direct_url = f"{base_api_url}/{resource}"
        print(f"Trying direct access to {direct_url}")
        
        try:
            response = requests.get(direct_url, headers=headers)
            if response.status_code == 200:
                print(f"✓ Success! Can access {resource} directly.")
                found_working_endpoints.append(resource)
                
                # Check response for potential account IDs
                try:
                    data = response.json()
                    if isinstance(data, dict) and 'items' in data:
                        items = data.get('items', [])
                        print(f"Found {len(items)} {resource}.")
                except:
                    pass
            else:
                print(f"Cannot access {resource} directly: {response.status_code}")
        except Exception as e:
            print(f"Error: {str(e)}")
    
    # Try with sample account IDs
    test_account_ids = [
        "RA123456789",
        "RAb7defd67dd9d4c2f9f72af3cdfc40e32",
        "R123456",
        "RBA123456",
        "R-123456"
    ]
    
    if found_working_endpoints:
        # If some endpoints work without account ID, we're done
        print("\nSome API endpoints work without an account ID!")
        print("Try updating your code to use these endpoints directly.")
        for endpoint in found_working_endpoints:
            print(f"- {endpoint}")
    else:
        # Otherwise suggest checking the web dashboard
        print("\nCould not automatically determine your account ID.")
        print("\nPlease check your Ringba web dashboard:")
        print("1. Log in to app.ringba.com")
        print("2. Look at the URL in your browser - it might contain your account ID")
        print("3. Check account settings in the dashboard")
        
        print("\nYou could also contact Ringba support to confirm your account ID.")
        
        # Suggest a common format to try
        print("\nTry these common account ID formats in your .env file:")
        print("1. Look for an ID that starts with 'RA' followed by letters/numbers")
        print("2. If your company name is 'XYZ Company', try 'RAXYZ' or similar")
    
    print("\n" + "=" * 50)

if __name__ == "__main__":
    main() 