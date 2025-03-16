#!/usr/bin/env python3
"""
Script to update the .env file with correct configuration values.
"""

import os
import sys
import dotenv

def main():
    """Update .env file with user input values"""
    print("=" * 50)
    print("  Ringba Monitor Configuration Updater")
    print("=" * 50)
    
    # Check if .env file exists
    if not os.path.exists(".env"):
        print("No .env file found. Creating a new one.")
        env_vars = {}
    else:
        # Load current .env file
        dotenv.load_dotenv()
        
        # Read existing values
        env_vars = {}
        with open(".env", "r") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    env_vars[key] = value
    
    # Display current values
    print("\nCurrent configuration values:")
    for key in ["RINGBA_API_TOKEN", "RINGBA_ACCOUNT_ID", "SLACK_WEBHOOK_URL", "TARGET_NAME", "RPC_THRESHOLD"]:
        value = env_vars.get(key, "Not set")
        if key == "RINGBA_API_TOKEN" and value != "Not set":
            # Don't show the full token
            display_value = value[:10] + "..." + value[-5:] if len(value) > 15 else value
        else:
            display_value = value
        print(f"{key}: {display_value}")
    
    print("\nEnter new values (press Enter to keep current values):")
    
    # Get new values
    account_id = input(f"RINGBA_ACCOUNT_ID [{env_vars.get('RINGBA_ACCOUNT_ID', 'Not set')}]: ")
    slack_webhook = input(f"SLACK_WEBHOOK_URL [{env_vars.get('SLACK_WEBHOOK_URL', 'Not set')}]: ")
    target_name = input(f"TARGET_NAME [{env_vars.get('TARGET_NAME', 'all')}]: ")
    rpc_threshold = input(f"RPC_THRESHOLD [{env_vars.get('RPC_THRESHOLD', '10.0')}]: ")
    
    # Update values if provided
    if account_id:
        env_vars["RINGBA_ACCOUNT_ID"] = account_id
    if slack_webhook:
        env_vars["SLACK_WEBHOOK_URL"] = slack_webhook
    if target_name:
        env_vars["TARGET_NAME"] = target_name
    if rpc_threshold:
        env_vars["RPC_THRESHOLD"] = rpc_threshold
    
    # Make sure we have the token
    if "RINGBA_API_TOKEN" not in env_vars:
        print("\nError: RINGBA_API_TOKEN not found.")
        token = input("Enter your Ringba API token: ")
        if token:
            env_vars["RINGBA_API_TOKEN"] = token
        else:
            print("No token provided. Aborting.")
            return
    
    # Write updated values to .env file
    with open(".env", "w") as f:
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")
    
    print("\nConfiguration updated successfully!")
    print("You can now run the application with:")
    print("python src/main.py")
    
    # Check for placeholder values
    if env_vars.get("RINGBA_ACCOUNT_ID") in ["your_account_id", "Not set", ""]:
        print("\nWarning: RINGBA_ACCOUNT_ID is not set correctly.")
        print("Please run 'python src/find_account_id.py' to find your account ID.")
    
    if env_vars.get("SLACK_WEBHOOK_URL") in ["your_webhook_url", "Not set", ""]:
        print("\nWarning: SLACK_WEBHOOK_URL is not set correctly.")
        print("Please enter a valid Slack webhook URL.")

if __name__ == "__main__":
    main() 