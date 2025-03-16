#!/usr/bin/env python3
"""
Quick test script to validate the new API token with different authentication methods
"""

import requests
import sys
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('quick_test.log')
    ]
)
logger = logging.getLogger('api_tester')

def main():
    """Test different authentication methods with the new API token"""
    print("\n============================================================")
    print("  Ringba API Token Test")
    print("============================================================\n")
    
    # The new API token
    new_api_token = "09f0c9f0c033544593cea5409fad971c23237045d50a344f11990f4d10f9e1f1b2ef176ce0b8a3761b82297f9bc5f9db26fcdf367cd1d9602b143afc2b9cf8a068be1f9b3fa67dc3751295ab51c36f7cb82f16549f08e70f4757d5929a5adc42b4034c69faa14b464dd4f3daa2130324ed6cf3e1"
    
    # Account ID
    account_id = "RAb7defd67dd9d4c2f9f72af3cdfc40e32"
    
    print(f"Testing with Account ID: {account_id}")
    print(f"Using new API Token: {new_api_token[:5]}...{new_api_token[-5:]}\n")
    
    # Try different authorization formats
    auth_formats = [
        {"name": "Bearer", "header": f"Bearer {new_api_token}"},
        {"name": "Token", "header": f"Token {new_api_token}"},
        {"name": "No prefix", "header": new_api_token}
    ]
    
    # URLs to test
    urls = [
        f"https://api.ringba.com/v2/{account_id}/targets",
        f"https://api.ringba.com/v2/accounts/{account_id}/targets", 
        f"https://app.ringba.com/api/v2/{account_id}/targets"
    ]
    
    # Try all combinations
    for url in urls:
        print(f"\nTesting URL: {url}")
        
        for auth in auth_formats:
            headers = {
                "Content-Type": "application/json",
                "Authorization": auth["header"]
            }
            
            print(f"\n  Using {auth['name']} authentication")
            
            try:
                response = requests.get(url, headers=headers)
                status = response.status_code
                print(f"  Status: {status}")
                
                if status == 200:
                    print(f"  ✅ SUCCESS with {auth['name']} auth!")
                    
                    # Parse response to find correct field
                    data = response.json()
                    print("  Response structure:")
                    print(json.dumps(data, indent=2)[:500] + "...")
                    
                    # Try to identify fields in the response
                    if 'targets' in data:
                        targets = data['targets']
                        print(f"  Found 'targets' field with {len(targets)} targets")
                    elif 'items' in data:
                        targets = data['items']
                        print(f"  Found 'items' field with {len(targets)} targets")
                    
                    # Display first few targets if any
                    if 'targets' in data and len(data['targets']) > 0:
                        print("\n  First few targets:")
                        for i, target in enumerate(data['targets'][:3]):
                            print(f"    Target {i+1}: {target.get('name', 'Unknown')} (ID: {target.get('id', 'Unknown')})")
                else:
                    print(f"  ❌ Failed: {response.text[:100]}...")
            except Exception as e:
                print(f"  ❌ Error: {str(e)}")
    
    print("\n============================================================")
    print("  Test Complete")
    print("============================================================\n")

if __name__ == "__main__":
    main() 