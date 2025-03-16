#!/usr/bin/env python3
"""
Direct test script that tries to access Ringba data without using an account ID.
This is a workaround for when the account ID is unknown or inaccessible.
"""

import os
import sys
import requests
import json
import logging
import dotenv
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('direct_test.log')
    ]
)
logger = logging.getLogger('direct_test')

def main():
    """Test direct access to Ringba API endpoints"""
    print("=" * 50)
    print("  Ringba Direct API Test")
    print("=" * 50)
    
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
    
    if not api_token:
        print("Error: RINGBA_API_TOKEN not found in .env file.")
        sys.exit(1)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    base_api_url = "https://api.ringba.com/v2"
    
    # Try all available API endpoints
    print("\nTrying all available API endpoints...")
    
    endpoints = [
        "/ApiTokens",
        "/token/info",
        "/users/current",
        "/profile",
        "/me",
        "/users",
        "/campaigns",
        "/campaigngroups",
        "/targets",
        "/buyers",
        "/numbers",
        "/calllogs",
        "/invoices",
        "/ping"
    ]
    
    working_endpoints = []
    
    for endpoint in endpoints:
        url = f"{base_api_url}{endpoint}"
        print(f"Testing: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            status = response.status_code
            print(f"Status: {status}")
            
            if status == 200:
                print("✓ Success!")
                working_endpoints.append(endpoint)
                
                # Try to parse and display some data
                try:
                    data = response.json()
                    if isinstance(data, dict) and 'items' in data:
                        print(f"Found {len(data['items'])} items")
                    elif isinstance(data, list):
                        print(f"Found {len(data)} items")
                except:
                    pass
            else:
                print("✗ Failed")
        except Exception as e:
            print(f"Error: {str(e)}")
        
        print("")
    
    if working_endpoints:
        print("\nWorking endpoints:")
        for endpoint in working_endpoints:
            print(f"- {endpoint}")
        
        # If we have working endpoints, try to get some useful data
        print("\nAttempting to extract useful data from working endpoints...")
        
        # If token info works, try to get more details
        if "/token/info" in working_endpoints:
            try:
                print("\nGetting token information...")
                response = requests.get(f"{base_api_url}/token/info", headers=headers)
                token_info = response.json()
                print(json.dumps(token_info, indent=2))
            except Exception as e:
                print(f"Error getting token info: {str(e)}")
        
        # If API tokens endpoint works, try to list tokens
        if "/ApiTokens" in working_endpoints:
            try:
                print("\nGetting API tokens...")
                response = requests.get(f"{base_api_url}/ApiTokens", headers=headers)
                tokens_data = response.json()
                if 'items' in tokens_data:
                    tokens = tokens_data['items']
                    print(f"Found {len(tokens)} API tokens")
                    for i, token in enumerate(tokens):
                        print(f"Token {i+1}:")
                        print(f"  Name: {token.get('name', 'Unknown')}")
                        print(f"  Created: {token.get('createDate', 'Unknown')}")
                        print(f"  Last Used: {token.get('lastUsedDate', 'Unknown')}")
            except Exception as e:
                print(f"Error getting API tokens: {str(e)}")
        
        # Try to get current user
        if "/users/current" in working_endpoints:
            try:
                print("\nGetting current user information...")
                response = requests.get(f"{base_api_url}/users/current", headers=headers)
                user_data = response.json()
                print(json.dumps(user_data, indent=2))
            except Exception as e:
                print(f"Error getting current user: {str(e)}")
        
        # Print summary
        print("\nSummary:")
        print(f"Found {len(working_endpoints)} working endpoints")
        print("You may need to modify the application code to use these endpoints directly")
        print("or contact Ringba support to get your correct account ID.")
    else:
        print("\nNo working endpoints found. You may need to:")
        print("1. Generate a new API token")
        print("2. Contact Ringba support")
    
    # Send test to Slack if webhook is available
    if slack_webhook:
        try:
            print("\nTesting Slack webhook...")
            slack_data = {
                "text": "🔍 *Ringba Direct API Test*\nThis is a test notification from the Ringba RPC Monitor"
            }
            response = requests.post(
                slack_webhook, 
                data=json.dumps(slack_data),
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 200:
                print("✓ Successfully sent test notification to Slack")
            else:
                print(f"✗ Failed to send to Slack: {response.status_code}")
        except Exception as e:
            print(f"Error sending to Slack: {str(e)}")
    
    print("\n" + "=" * 50)

if __name__ == "__main__":
    main() 