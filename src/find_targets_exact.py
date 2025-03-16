#!/usr/bin/env python3
"""
Script to find targets using the exact format from Ringba API documentation.
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
        logging.FileHandler('ringba_api.log')
    ]
)
logger = logging.getLogger('ringba_api')

def main():
    """Find targets using exact Ringba API documentation format"""
    print("=" * 60)
    print("  Ringba Target Finder (Based on Official Documentation)")
    print("=" * 60)
    
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token:
        print("ERROR: RINGBA_API_TOKEN not found in .env file.")
        sys.exit(1)
    
    if not account_id:
        print("ERROR: RINGBA_ACCOUNT_ID not found in .env file.")
        sys.exit(1)
    
    print(f"Account ID: {account_id}")
    print(f"Using API Token: {api_token[:5]}...{api_token[-5:]}")
    
    # Use exact header format from documentation
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {api_token}"
    }
    
    base_url = "https://api.ringba.com/v2"
    
    # 1. Get all targets (with stats)
    print("\n1. Getting all targets with stats...")
    targets_url = f"{base_url}/{account_id}/targets?includeStats=true"
    print(f"URL: {targets_url}")
    
    try:
        response = requests.get(targets_url, headers=headers)
        status = response.status_code
        print(f"Status: {status}")
        
        if status == 200:
            print("✅ SUCCESS: Targets endpoint accessible!")
            data = response.json()
            
            # Check for targets array as shown in documentation
            if 'targets' in data:
                targets = data['targets']
                print(f"Found {len(targets)} targets")
                
                if len(targets) > 0:
                    print("\nDetails of first few targets:")
                    for i, target in enumerate(targets[:5]):
                        if i >= 5:
                            break
                            
                        print(f"\nTarget {i+1}:")
                        print(f"  Name: {target.get('name', 'Unknown')}")
                        print(f"  ID: {target.get('id', 'Unknown')}")
                        print(f"  Enabled: {target.get('enabled', 'Unknown')}")
                        
                        # Try to find number if available
                        if 'instructions' in target and 'number' in target['instructions']:
                            print(f"  Phone Number: {target['instructions']['number']}")
                    
                    # Display stats if available
                    if 'stats' in data:
                        print("\nTarget Stats Overview:")
                        stats = data['stats']
                        print(json.dumps(stats, indent=2))
                    
                    # Update RINGBA_API_WORKING=true in .env
                    update_env_file("RINGBA_API_WORKING", "true")
                    
                    # Save targets to a file for reference
                    with open('ringba_targets.json', 'w') as f:
                        json.dump(data, f, indent=2)
                    print("\nSaved all target data to ringba_targets.json")
                    
                else:
                    print("No targets found in this account (empty list)")
            else:
                print("WARNING: 'targets' field not found in API response")
                print("Response structure:", json.dumps(data, indent=2)[:200] + "...")
        else:
            print(f"❌ ERROR: Failed to access targets, status code {status}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"❌ ERROR: Exception when accessing targets: {str(e)}")
    
    # 2. Try to get targets with a different field name (items) just in case
    print("\n2. Checking for alternate response format...")
    try:
        if status == 200 and 'items' in data:
            items = data['items']
            print(f"Found {len(items)} targets in 'items' field")
            
            if len(items) > 0:
                print("\nDetails of first few targets:")
                for i, target in enumerate(items[:3]):
                    print(f"\nTarget {i+1}:")
                    print(f"  Name: {target.get('name', 'Unknown')}")
                    print(f"  ID: {target.get('id', 'Unknown')}")
    except:
        pass
    
    # 3. Try to get a specific target if we found any
    target_id = None
    if status == 200 and 'targets' in data and len(data['targets']) > 0:
        target_id = data['targets'][0]['id']
        target_name = data['targets'][0]['name']
        
        print(f"\n3. Getting details for target: {target_name} (ID: {target_id})...")
        target_url = f"{base_url}/{account_id}/targets/{target_id}"
        print(f"URL: {target_url}")
        
        try:
            response = requests.get(target_url, headers=headers)
            status = response.status_code
            print(f"Status: {status}")
            
            if status == 200:
                print("✅ SUCCESS: Retrieved target details!")
                target_data = response.json()
                
                # Print some key details
                if 'target' in target_data:
                    target = target_data['target']
                    print(f"Name: {target.get('name', 'Unknown')}")
                    print(f"Enabled: {target.get('enabled', 'Unknown')}")
                    
                    if 'instructions' in target:
                        instructions = target['instructions']
                        print(f"Call Type: {instructions.get('callType', 'Unknown')}")
                        if 'number' in instructions:
                            print(f"Phone Number: {instructions['number']}")
                
                # Print stats if available
                if 'stats' in target_data:
                    print("\nTarget Stats:")
                    for key, stats in target_data['stats'].items():
                        print(f"Stats for {key}:")
                        for stat_name, value in stats.items():
                            print(f"  {stat_name}: {value}")
            else:
                print(f"❌ ERROR: Failed to get target details, status code {status}")
        except Exception as e:
            print(f"❌ ERROR: Exception when getting target details: {str(e)}")
    
    # 4. Get target counts (specifically for RPC calculation)
    if target_id:
        print(f"\n4. Getting counts/stats for target: {target_name} (ID: {target_id})...")
        counts_url = f"{base_url}/{account_id}/targets/{target_id}/Counts"
        print(f"URL: {counts_url}")
        
        try:
            response = requests.get(counts_url, headers=headers)
            status = response.status_code
            print(f"Status: {status}")
            
            if status == 200:
                print("✅ SUCCESS: Retrieved target counts!")
                counts_data = response.json()
                
                if 'stats' in counts_data:
                    print("\nTarget Counts/Stats:")
                    for target_key, stats in counts_data['stats'].items():
                        print(f"Stats for target {target_key}:")
                        
                        # Calculate RPC if possible
                        if 'totalSum' in stats and 'total' in stats and stats['total'] > 0:
                            revenue = float(stats['totalSum'])
                            calls = int(stats['total'])
                            rpc = revenue / calls
                            print(f"  Revenue: ${revenue:.2f}")
                            print(f"  Calls: {calls}")
                            print(f"  RPC: ${rpc:.2f}")
                        else:
                            for stat_name, value in stats.items():
                                print(f"  {stat_name}: {value}")
            else:
                print(f"❌ ERROR: Failed to get target counts, status code {status}")
        except Exception as e:
            print(f"❌ ERROR: Exception when getting target counts: {str(e)}")
    
    # 5. Try to get target inbound references
    if target_id:
        print(f"\n5. Getting inbound references for target: {target_name} (ID: {target_id})...")
        refs_url = f"{base_url}/{account_id}/targets/{target_id}/InboundReferences"
        print(f"URL: {refs_url}")
        
        try:
            response = requests.get(refs_url, headers=headers)
            status = response.status_code
            print(f"Status: {status}")
            
            if status == 200:
                print("✅ SUCCESS: Retrieved inbound references!")
                refs_data = response.json()
                
                if 'campaigns' in refs_data:
                    campaigns = refs_data['campaigns']
                    print(f"Found {len(campaigns)} campaigns referencing this target")
                    
                    for i, campaign in enumerate(campaigns):
                        print(f"\nCampaign {i+1}:")
                        print(f"  Name: {campaign.get('campaignName', 'Unknown')}")
                        print(f"  ID: {campaign.get('campaignId', 'Unknown')}")
                        print(f"  Is Default Target: {campaign.get('campaignDefaultTarget', 'Unknown')}")
            else:
                print(f"❌ ERROR: Failed to get inbound references, status code {status}")
        except Exception as e:
            print(f"❌ ERROR: Exception when getting inbound references: {str(e)}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Ringba API Test Summary")
    print("=" * 60)
    
    if status == 200 and 'targets' in data:
        print(f"✅ SUCCESS: Found {len(data['targets'])} targets in your account")
        print("You can now use the RPC monitoring scripts with this account.")
        print("\nTo update your monitoring scripts to use the correct format:")
        print("1. Ensure headers use 'Token' instead of 'Bearer'")
        print("2. Make sure API URLs use the format /v2/{account_id}/targets")
        print("3. Look for 'targets' array in responses instead of 'items'")
    else:
        print("❌ ERROR: Could not find targets in your account")
        print("Possible reasons:")
        print("1. Your API token doesn't have sufficient permissions")
        print("2. Your account structure is different from the documentation")
        print("3. Your account ID is incorrect")
        print("\nRecommended next steps:")
        print("1. Contact Ringba support with the error messages above")
        print("2. Request specific API documentation for your account")
        print("3. Ask for a new API token with proper permissions")
    
    print("=" * 60)

def update_env_file(key, value):
    """Update or add a key-value pair in the .env file"""
    dotenv_path = '.env'
    
    # Read the current .env file
    with open(dotenv_path, 'r') as f:
        lines = f.readlines()
    
    # Update the key if it exists
    updated = False
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}\n"
            updated = True
            break
    
    # Add the key if it doesn't exist
    if not updated:
        lines.append(f"{key}={value}\n")
    
    # Write the updated .env file
    with open(dotenv_path, 'w') as f:
        f.writelines(lines)

if __name__ == "__main__":
    main() 