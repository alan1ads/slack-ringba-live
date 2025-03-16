#!/usr/bin/env python3
import os
import sys
import dotenv
from ringba_api import RingbaAPI

# Load environment variables
dotenv.load_dotenv()
RINGBA_API_TOKEN = os.getenv('RINGBA_API_TOKEN')
RINGBA_ACCOUNT_ID = os.getenv('RINGBA_ACCOUNT_ID')

def main():
    """Fetch and display all targets from Ringba account"""
    
    # Check if credentials are set
    if not all([RINGBA_API_TOKEN, RINGBA_ACCOUNT_ID]):
        print("Error: Missing Ringba credentials in .env file")
        sys.exit(1)
    
    print(f"Connecting to Ringba account: {RINGBA_ACCOUNT_ID}")
    
    # Initialize Ringba API
    ringba_api = RingbaAPI(RINGBA_API_TOKEN, RINGBA_ACCOUNT_ID)
    
    try:
        # Authenticate
        ringba_api.authenticate()
        print("Successfully authenticated with Ringba API")
        
        # Get all targets
        targets_data = ringba_api.get_all_targets(include_stats=True, enabled_only=False)
        all_targets = targets_data.get('targets', [])
        
        # Separate enabled and disabled targets
        enabled_targets = [t for t in all_targets if t.get('enabled', False)]
        disabled_targets = [t for t in all_targets if not t.get('enabled', False)]
        
        # Display enabled targets
        if enabled_targets:
            print("\nActive targets in your Ringba account:")
            print("----------------------------------------")
            for i, target in enumerate(sorted(enabled_targets, key=lambda x: x.get('name', '')), 1):
                target_id = target.get('id')
                target_name = target.get('name')
                
                # Get target details with stats
                target_details = ringba_api.get_target_details(target_id)
                
                # Calculate RPC
                rpc = ringba_api.calculate_rpc_for_target(target_details)
                
                print(f"{i}. {target_name} (ID: {target_id}) - Current RPC: ${rpc:.2f}")
        else:
            print("\nNo active targets found in your Ringba account.")
        
        # Display disabled targets count
        if disabled_targets:
            print(f"\nFound {len(disabled_targets)} disabled targets (not shown)")
            
            # Ask if user wants to see disabled targets
            show_disabled = input("\nDo you want to see disabled targets? (y/n): ").lower().strip() == 'y'
            
            if show_disabled:
                print("\nDisabled targets:")
                print("----------------------------------------")
                for i, target in enumerate(sorted(disabled_targets, key=lambda x: x.get('name', '')), 1):
                    target_id = target.get('id')
                    target_name = target.get('name')
                    print(f"{i}. {target_name} (ID: {target_id})")
        
        # Print usage instructions
        print("\nTo monitor a specific target, add its exact name to your .env file:")
        print('TARGET_NAME="Target Name Here"')
        
        print("\nTo monitor all targets, leave TARGET_NAME empty or set to 'ALL'")
        
        # Print RPC threshold
        rpc_threshold = os.getenv('RPC_THRESHOLD', '10.0')
        print(f"\nCurrent RPC threshold: ${rpc_threshold}")
        print("To change the threshold, modify the RPC_THRESHOLD value in your .env file")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 