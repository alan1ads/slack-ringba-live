#!/usr/bin/env python3
"""
Utility script to get a Ringba API Bearer token using username and password.
This token can then be used in the .env file for the RPC monitoring application.
"""

import os
import sys
import requests
import json
import dotenv
from getpass import getpass
from ringba_api import RingbaAPI

def main():
    """Get a Bearer token from Ringba API"""
    print("=" * 50)
    print("  Ringba API Bearer Token Generator")
    print("=" * 50)
    print("\nThis script will help you get a Bearer token for the Ringba API.")
    print("The token will be used in your .env file as RINGBA_API_TOKEN.")
    
    # Get username and password
    username = input("Enter your Ringba username (email): ")
    
    # Explain that password will be hidden during entry
    print("\nNote: For security reasons, your password will NOT be displayed as you type.")
    print("Just type your password and press Enter when done.")
    
    try:
        # First try with getpass (secure, no echo)
        password = getpass("Enter your Ringba password (hidden input): ")
    except Exception as e:
        # If getpass fails, fall back to regular input
        print("\nSecure password entry failed. Using alternative method.")
        print("Warning: Your password will be visible as you type.")
        password = input("Enter your Ringba password: ")
    
    if not username or not password:
        print("Error: Username and password are required.")
        sys.exit(1)
    
    try:
        # Get Bearer token
        print("\nAttempting to get Bearer token from Ringba API...")
        token = RingbaAPI.get_bearer_token(username, password)
        
        print("\n" + "=" * 50)
        print("Bearer token obtained successfully!")
        print("=" * 50)
        print("\nToken: " + token)
        print("\nUpdate your .env file with:")
        print(f"RINGBA_API_TOKEN={token}")
        
        # Ask if user wants to automatically update .env file
        update_env = input("\nDo you want to automatically update your .env file? (y/n): ")
        
        if update_env.lower() == 'y':
            # Check if .env file exists
            if not os.path.exists(".env"):
                print("Creating new .env file...")
                with open(".env", "w") as f:
                    f.write(f"RINGBA_API_TOKEN={token}\n")
                    # Add placeholders for other required variables
                    f.write("RINGBA_ACCOUNT_ID=your_account_id\n")
                    f.write("SLACK_WEBHOOK_URL=your_webhook_url\n")
                    f.write("TARGET_NAME=all\n")
                    f.write("RPC_THRESHOLD=10.0\n")
                print("Created new .env file with token.")
            else:
                # Load current .env file
                dotenv.load_dotenv()
                
                # Prepare new content
                env_vars = {}
                with open(".env", "r") as f:
                    for line in f:
                        if "=" in line:
                            key, value = line.strip().split("=", 1)
                            env_vars[key] = value
                
                # Update token
                env_vars["RINGBA_API_TOKEN"] = token
                
                # Write back to .env file
                with open(".env", "w") as f:
                    for key, value in env_vars.items():
                        f.write(f"{key}={value}\n")
                
                print("Updated .env file with new token.")
        
        print("\nYou can now run the application with:")
        print("python src/main.py")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nTrying direct API call method...")
        direct_token_attempt(username, password)

# Alternative direct entry method
def manual_entry():
    """Manually enter credentials and get token"""
    print("=" * 50)
    print("  Manual Bearer Token Generation")
    print("=" * 50)
    
    username = input("Enter your Ringba username (email): ")
    password = input("Enter your Ringba password (will be visible): ")
    
    try:
        print("\nAttempting to get token using RingbaAPI method...")
        token = RingbaAPI.get_bearer_token(username, password)
        print("\nToken: " + token)
        
        # Save to .env file
        save = input("Save to .env file? (y/n): ")
        if save.lower() == 'y':
            with open(".env", "w") as f:
                f.write(f"RINGBA_API_TOKEN={token}\n")
                f.write("RINGBA_ACCOUNT_ID=your_account_id\n")
                f.write("SLACK_WEBHOOK_URL=your_webhook_url\n")
                f.write("TARGET_NAME=all\n")
                f.write("RPC_THRESHOLD=10.0\n")
            print("Token saved to .env file.")
    except Exception as e:
        print(f"Error with RingbaAPI method: {str(e)}")
        print("\nTrying direct API call method...")
        direct_token_attempt(username, password)

def direct_token_attempt(username, password):
    """Try a direct API call to get a token"""
    print("\nAttempting direct token request...")
    url = "https://api.ringba.com/v2/token"
    
    # Try different header combinations
    header_options = [
        {"Content-Type": "application/x-www-form-urlencoded"},
        {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
        {"Content-Type": "application/json"}
    ]
    
    # Try different data formats
    data_options = [
        # Form encoded
        {"grant_type": "password", "username": username, "password": password},
        # JSON
        json.dumps({"grant_type": "password", "username": username, "password": password})
    ]
    
    success = False
    
    # Try all combinations
    for headers in header_options:
        print(f"\nTrying headers: {headers}")
        
        for i, data in enumerate(data_options):
            print(f"Trying data format {i+1}...")
            
            try:
                # For the JSON option, use json parameter instead of data
                if "application/json" in headers.get("Content-Type", ""):
                    response = requests.post(url, headers=headers, json=data_options[0])
                else:
                    response = requests.post(url, headers=headers, data=data)
                
                print(f"Status code: {response.status_code}")
                print(f"Response: {response.text[:200]}...")  # First 200 chars
                
                if response.status_code == 200:
                    try:
                        token_data = response.json()
                        token = token_data.get('access_token')
                        if token:
                            print("\nSuccess! Bearer token obtained.")
                            print(f"Token: {token}")
                            
                            # Save to .env file
                            save = input("Save to .env file? (y/n): ")
                            if save.lower() == 'y':
                                with open(".env", "w") as f:
                                    f.write(f"RINGBA_API_TOKEN={token}\n")
                                    f.write("RINGBA_ACCOUNT_ID=your_account_id\n")
                                    f.write("SLACK_WEBHOOK_URL=your_webhook_url\n")
                                    f.write("TARGET_NAME=all\n")
                                    f.write("RPC_THRESHOLD=10.0\n")
                                print("Token saved to .env file.")
                            
                            success = True
                            break
                    except Exception as e:
                        print(f"Error parsing response: {str(e)}")
            except Exception as e:
                print(f"Request error: {str(e)}")
        
        if success:
            break
    
    if not success:
        print("\nAll token request attempts failed.")
        print("Please check your credentials and try again.")
        print("If issues persist, consider generating an API token directly on the Ringba platform.")


if __name__ == "__main__":
    # Check if manual mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        manual_entry()
    else:
        main()
        print("\nIf you're having trouble with password entry, try running:")
        print("python src/get_token.py --manual") 