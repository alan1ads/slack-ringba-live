#!/usr/bin/env python3
"""
Fetch and save Ringba profile data to help identify account information.
"""

import os
import sys
import requests
import json
import logging
import dotenv
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('profile_data.log')
    ]
)
logger = logging.getLogger('profile_fetch')

def main():
    """Fetch profile data from Ringba API"""
    print("=" * 50)
    print("  Ringba Profile Data Fetcher")
    print("=" * 50)
    
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    
    if not api_token:
        print("Error: RINGBA_API_TOKEN not found in .env file.")
        sys.exit(1)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    base_api_url = "https://api.ringba.com/v2"
    
    # Fetch profile data
    print("\nFetching profile data...")
    try:
        response = requests.get(f"{base_api_url}/profile", headers=headers)
        if response.status_code == 200:
            profile_data = response.json()
            
            # Save to file with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ringba_profile_{timestamp}.json"
            
            with open(filename, 'w') as f:
                json.dump(profile_data, f, indent=2)
            
            print(f"✓ Profile data saved to {filename}")
            
            # Display key information
            print("\nKey Account Information:")
            print("------------------------")
            
            # Scan through the profile data to find potential account IDs or references
            account_references = []
            
            def scan_for_ids(data, path=""):
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, (dict, list)):
                            scan_for_ids(value, f"{path}.{key}" if path else key)
                        elif isinstance(value, str) and len(value) > 10:
                            # Look for ID-like strings
                            if "id" in key.lower() or "account" in key.lower():
                                account_references.append((f"{path}.{key}" if path else key, value))
                            # Also match strings that look like "RA..." (common Ringba format)
                            elif value.startswith("RA") and len(value) > 15:
                                account_references.append((f"{path}.{key}" if path else key, value))
                elif isinstance(data, list):
                    for i, item in enumerate(data):
                        scan_for_ids(item, f"{path}[{i}]")
            
            scan_for_ids(profile_data)
            
            if account_references:
                print("Potential account ID references found:")
                for path, value in account_references:
                    print(f"  {path}: {value}")
                
                # Also suggest updating the .env file
                if len(account_references) > 0:
                    most_likely = None
                    for path, value in account_references:
                        if "account" in path.lower() and value.startswith("RA"):
                            most_likely = value
                            break
                    
                    if most_likely:
                        print(f"\nMost likely account ID: {most_likely}")
                        print("Consider updating your .env file with this ID.")
            else:
                print("No obvious account ID references found in the profile data.")
                print("Check the saved JSON file for more details.")
            
            # Print the full profile data for inspection
            print("\nComplete Profile Data:")
            print(json.dumps(profile_data, indent=2))
            
        else:
            print(f"✗ Failed to fetch profile: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Error: {str(e)}")
    
    print("\n" + "=" * 50)
    print("Next steps:")
    print("1. Look in the saved JSON file for account information")
    print("2. Contact Ringba support with this error message:")
    print("   'API token has limited access, most endpoints return 404.'")
    print("3. Ask Ringba support about the correct API structure for your account")
    print("=" * 50)

if __name__ == "__main__":
    main() 