#!/usr/bin/env python3
"""
Ringba Live RPC Data Reporter

This script retrieves and displays real-time RPC data for all active targets in Ringba,
providing a snapshot of current performance.
"""

import os
import sys
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
from tabulate import tabulate

# Import custom modules
from ringba_api import RingbaAPI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('live_rpc_data.log')
    ]
)
logger = logging.getLogger('live_rpc')

def main():
    """Main function to retrieve and display live RPC data"""
    print("\n==============================================")
    print("   Ringba Live RPC Data Report")
    print("==============================================\n")
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    api_token = os.getenv('RINGBA_API_TOKEN')
    account_id = os.getenv('RINGBA_ACCOUNT_ID')
    
    if not api_token or not account_id:
        print("Error: Missing API token or account ID in .env file")
        sys.exit(1)
    
    print(f"Account ID: {account_id}")
    print(f"API Token: {api_token[:5]}...{api_token[-5:]}")
    
    # Initialize API
    api = RingbaAPI(api_token, account_id)
    
    # Test authentication
    if not api.test_auth():
        print("❌ Authentication failed. Please check your API token and account ID.")
        sys.exit(1)
    
    # Get today's and yesterday's dates
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Ask user for the date to use
    print("\nSelect date for RPC data:")
    print(f"1. Today ({today})")
    print(f"2. Yesterday ({yesterday})")
    print("3. Custom date (YYYY-MM-DD)")
    
    choice = input("\nEnter your choice (1-3): ")
    
    if choice == '1':
        date_str = today
        print(f"\nFetching LIVE RPC data for {date_str}...")
    elif choice == '2':
        date_str = yesterday
        print(f"\nFetching RPC data for yesterday ({date_str})...")
    elif choice == '3':
        date_str = input("Enter custom date (YYYY-MM-DD): ")
        # Basic validation
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            print(f"\nFetching RPC data for {date_str}...")
        except ValueError:
            print("Invalid date format. Using today's date instead.")
            date_str = today
            print(f"\nFetching LIVE RPC data for {date_str}...")
    else:
        print("Invalid choice. Using today's date.")
        date_str = today
        print(f"\nFetching LIVE RPC data for {date_str}...")
    
    # Get all targets directly from API
    print("Retrieving all targets from Ringba API...")
    response = api.get_all_targets()
    
    # Process the response to get list of targets
    targets = []
    if response and isinstance(response, dict) and 'targets' in response:
        targets = response['targets']
        print(f"Found {len(targets)} total targets.")
    else:
        print("❌ No targets found in the API response.")
        sys.exit(1)
    
    if not targets:
        print("❌ No targets found.")
        sys.exit(1)
    
    # Filter enabled targets
    enabled_targets = []
    for target in targets:
        if isinstance(target, dict) and target.get('enabled', True):
            enabled_targets.append(target)
    
    print(f"Found {len(enabled_targets)} enabled targets.")
    
    if len(enabled_targets) == 0:
        print("No enabled targets found. Nothing to report.")
        sys.exit(0)
    
    # Collect RPC data for all enabled targets
    rpc_data = []
    
    print("\nCalculating RPC for all enabled targets...")
    for i, target in enumerate(enabled_targets):
        target_id = target.get('id')
        target_name = target.get('name', 'Unknown')
        
        print(f"Processing {i+1}/{len(enabled_targets)}: {target_name}")
        
        try:
            # Get counts data
            counts = api.get_target_counts(target_id, date_str)
            
            # Extract calls and revenue data
            calls = 0
            revenue = 0
            rpc = 0
            
            if counts:
                print(f"  Counts type: {type(counts)}")
                
                if isinstance(counts, list) and len(counts) > 1:
                    # Handle list format with transactionId and stats
                    if 'stats' in counts[1]:
                        stats = counts[1]['stats']
                        calls = stats.get('totalCalls', 0)
                        revenue = stats.get('payout', 0)
                    else:
                        # Try to access stats directly from second element
                        stats = counts[1]
                        calls = stats.get('totalCalls', 0)
                        revenue = stats.get('payout', 0)
                elif isinstance(counts, dict):
                    # Handle direct dictionary format
                    calls = counts.get('totalCalls', 0)
                    revenue = counts.get('payout', 0)
            
            if calls > 0:
                rpc = revenue / calls
            
            # Add to data collection
            rpc_data.append({
                'id': target_id,
                'name': target_name,
                'calls': calls,
                'revenue': revenue,
                'rpc': rpc
            })
            
            print(f"  Calls: {calls}, Revenue: ${revenue:.2f}, RPC: ${rpc:.2f}")
        except Exception as e:
            logger.error(f"Error processing target {target_name}: {str(e)}")
            print(f"  Error: {str(e)}")
            # Still add to data with zero values
            rpc_data.append({
                'id': target_id,
                'name': target_name,
                'calls': 0,
                'revenue': 0,
                'rpc': 0,
                'error': str(e)
            })
    
    # Sort by RPC (highest first)
    rpc_data.sort(key=lambda x: x['rpc'], reverse=True)
    
    # Prepare table data
    table_data = []
    for i, data in enumerate(rpc_data):
        table_data.append([
            i + 1,
            data['name'],
            f"${data['rpc']:.2f}",
            data['calls'],
            f"${data['revenue']:.2f}"
        ])
    
    # Display results
    print(f"\nRPC Data for {date_str}:")
    print(tabulate(
        table_data,
        headers=["#", "Target Name", "RPC", "Calls", "Revenue"],
        tablefmt="pretty"
    ))
    
    # Summary statistics
    total_calls = sum(data['calls'] for data in rpc_data)
    total_revenue = sum(data['revenue'] for data in rpc_data)
    avg_rpc = total_revenue / total_calls if total_calls > 0 else 0
    
    print("\nSummary:")
    print(f"Total Calls: {total_calls}")
    print(f"Total Revenue: ${total_revenue:.2f}")
    print(f"Average RPC: ${avg_rpc:.2f}")
    
    # Highlight top performers
    print("\nTop 5 Performers:")
    top_performers = [t for t in rpc_data if t['calls'] > 0][:5]
    for i, data in enumerate(top_performers):
        print(f"{i+1}. {data['name']}: ${data['rpc']:.2f} RPC with {data['calls']} calls")
    
    print("\n==============================================")
    print("   Report Complete")
    print("==============================================\n")

if __name__ == "__main__":
    main() 