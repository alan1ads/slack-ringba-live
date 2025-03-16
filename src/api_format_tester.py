#!/usr/bin/env python3
"""
Script to test different Ringba API formats and authentication methods to find what works.
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
        logging.FileHandler('api_test.log')
    ]
)
logger = logging.getLogger('api_tester')

def main():
    """Test different formats of Ringba API to find what works"""
    print("=" * 60)
    print("  Ringba API Format Tester")
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
    
    print(f"Testing with Account ID: {account_id}")
    print(f"Using API Token: {api_token[:5]}...{api_token[-5:]}")
    
    # Try different authorization formats
    auth_formats = [
        {"name": "Bearer", "header": f"Bearer {api_token}"},
        {"name": "Token", "header": f"Token {api_token}"},
        {"name": "No prefix", "header": api_token}
    ]
    
    # Try different URL formats
    base_urls = [
        "https://api.ringba.com/v2",
        "https://api.ringba.com/v1",
        "https://app.ringba.com/api/v2"
    ]
    
    # Try different endpoint formats
    endpoint_formats = [
        f"/{account_id}/targets",
        f"/accounts/{account_id}/targets"
    ]
    
    # Try different response fields
    response_fields = ["targets", "items"]
    
    # Track which combination works
    working_combination = None
    
    # Try all combinations
    for base_url in base_urls:
        for endpoint in endpoint_formats:
            url = f"{base_url}{endpoint}"
            
            print(f"\nTesting URL: {url}")
            
            for auth in auth_formats:
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": auth["header"]
                }
                
                print(f"\n  Using {auth['name']} authentication")
                
                # Try with include stats
                test_urls = [
                    {"name": "Basic URL", "url": url},
                    {"name": "With includeStats", "url": f"{url}?includeStats=true"}
                ]
                
                for test_url in test_urls:
                    try:
                        print(f"    Testing: {test_url['name']}")
                        response = requests.get(test_url["url"], headers=headers)
                        status = response.status_code
                        print(f"    Status: {status}")
                        
                        if status == 200:
                            print(f"    ✅ SUCCESS with {auth['name']} auth at {test_url['name']}")
                            
                            # Parse response to find correct field
                            data = response.json()
                            
                            print("    Looking for targets in response...")
                            for field in response_fields:
                                if field in data:
                                    targets = data[field]
                                    print(f"    ✅ Found '{field}' field with {len(targets)} targets")
                                    # Save the working combination
                                    working_combination = {
                                        "base_url": base_url,
                                        "endpoint": endpoint,
                                        "auth_format": auth["name"],
                                        "auth_header": auth["header"],
                                        "response_field": field,
                                        "url_format": test_url["name"],
                                        "full_url": test_url["url"],
                                        "target_count": len(targets)
                                    }
                                    
                                    # Display first few targets if any
                                    if len(targets) > 0:
                                        print("\n    First few targets:")
                                        for i, target in enumerate(targets[:3]):
                                            if i >= 3:
                                                break
                                            print(f"      Target {i+1}: {target.get('name', 'Unknown')} (ID: {target.get('id', 'Unknown')})")
                            
                            # If successful but no recognized field, display structure
                            if not working_combination:
                                print("    Response structure:")
                                print(json.dumps(data, indent=2)[:500] + "...")
                        else:
                            error_text = response.text if len(response.text) < 100 else response.text[:100] + "..."
                            print(f"    ❌ Failed: {error_text}")
                    except Exception as e:
                        print(f"    ❌ Error: {str(e)}")
    
    # Summary and update configuration
    print("\n" + "=" * 60)
    print("API Format Test Summary")
    print("=" * 60)
    
    if working_combination:
        print("✅ FOUND WORKING COMBINATION:")
        print(f"Base URL: {working_combination['base_url']}")
        print(f"Endpoint Format: {working_combination['endpoint']}")
        print(f"Auth Format: {working_combination['auth_format']}")
        print(f"Response Field: {working_combination['response_field']}")
        print(f"URL Format: {working_combination['url_format']}")
        print(f"Target Count: {working_combination['target_count']}")
        
        # Update the configuration file
        try:
            # Create or update api_config.json
            config = {
                "base_url": working_combination['base_url'],
                "endpoint_format": working_combination['endpoint'],
                "auth_format": working_combination['auth_format'],
                "response_field": working_combination['response_field'],
                "account_id": account_id
            }
            
            with open('api_config.json', 'w') as f:
                json.dump(config, f, indent=2)
            print("\nSaved working configuration to api_config.json")
            
            # Also update the .env file with the correct auth format
            update_env_file("RINGBA_AUTH_FORMAT", working_combination['auth_format'])
            print("Updated .env file with RINGBA_AUTH_FORMAT")
            
            # Create a simple test script
            create_test_script(working_combination)
            print("Created a custom test script 'src/custom_test.py' that uses the working format")
            
            # Return success with instructions
            print("\nNext steps:")
            print("1. Update your RingbaAPI class to use the working format")
            print("2. Run the custom test script: python src/custom_test.py")
        except Exception as e:
            print(f"Error saving configuration: {str(e)}")
    else:
        print("❌ NO WORKING COMBINATION FOUND")
        print("\nPossible issues:")
        print("1. Your API token may have expired or been revoked")
        print("2. Your account ID may be incorrect")
        print("3. Your Ringba account might use a completely different API structure")
        print("\nRecommended next steps:")
        print("1. Generate a new API token from Ringba")
        print("2. Double-check your account ID")
        print("3. Contact Ringba support for assistance with API access")
    
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

def create_test_script(config):
    """Create a custom test script that uses the working format"""
    script_content = f'''#!/usr/bin/env python3
"""
Custom Ringba API test script created with the working configuration.
"""

import os
import requests
import json
import dotenv
import sys

def main():
    # Load environment variables
    dotenv.load_dotenv()
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token or not account_id:
        print("ERROR: API Token or Account ID not found in .env file.")
        sys.exit(1)
    
    # Use the working configuration
    base_url = "{config['base_url']}"
    endpoint = "{config['endpoint']}"
    auth_format = "{config['auth_format']}"
    response_field = "{config['response_field']}"
    
    # Build URL
    url = f"{{base_url}}{{endpoint}}"
    
    # Build auth header
    if auth_format == "Bearer":
        auth_header = f"Bearer {{api_token}}"
    elif auth_format == "Token":
        auth_header = f"Token {{api_token}}"
    else:
        auth_header = api_token
    
    headers = {{
        "Content-Type": "application/json",
        "Authorization": auth_header
    }}
    
    print("=" * 60)
    print("  Ringba API Custom Test")
    print("=" * 60)
    print(f"URL: {{url}}")
    print(f"Auth format: {{auth_format}}")
    
    try:
        # Get targets
        response = requests.get(url, headers=headers)
        status = response.status_code
        print(f"Status: {{status}}")
        
        if status == 200:
            print("✅ SUCCESS: Connected to API!")
            data = response.json()
            
            if response_field in data:
                targets = data[response_field]
                print(f"Found {{len(targets)}} targets")
                
                # Display targets
                if targets:
                    print("\\nTargets:")
                    for i, target in enumerate(targets):
                        print(f"  {{i+1}}. {{target.get('name', 'Unknown')}} (ID: {{target.get('id', 'Unknown')}})")
                        print(f"     Enabled: {{target.get('enabled', 'Unknown')}}")
                else:
                    print("No targets found (empty list)")
            else:
                print(f"WARNING: '{response_field}' field not found in response")
                print("Response format:")
                print(json.dumps(data, indent=2)[:200] + "...")
        else:
            print(f"❌ ERROR: Failed with status {{status}}")
            print(f"Response: {{response.text}}")
    except Exception as e:
        print(f"❌ ERROR: {{str(e)}}")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
'''
    
    # Write the test script
    with open('src/custom_test.py', 'w') as f:
        f.write(script_content)

if __name__ == "__main__":
    main() 