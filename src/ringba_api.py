#!/usr/bin/env python3
import requests
import logging
from datetime import datetime, timedelta
import json
import os
import pytz
import sys
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ringba_api.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('ringba_monitor.ringba_api')

class RingbaAPI:
    """
    Class to interact with the Ringba API.
    Based on the API format tester results, we use Bearer token with the format:
    https://api.ringba.com/v2/{account_id}/targets
    and the response contains targets in the 'targets' field.
    """
    
    def __init__(self, api_token, account_id):
        """Initialize with API token and account ID"""
        self.api_token = api_token
        self.account_id = account_id
        
        # Store the different possible header formats
        self.auth_formats = [
            {"name": "Token", "header": f"Token {self.api_token}"},  # Token format first, confirmed working
            {"name": "Bearer", "header": f"Bearer {self.api_token}"},
            {"name": "No prefix", "header": self.api_token}
        ]
        
        # Start with Token format as default
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": self.auth_formats[0]["header"]
        }
        
        self.current_auth_format = self.auth_formats[0]["name"]
        
        # Base URL with account ID
        self.base_url = f"https://api.ringba.com/v2/{self.account_id}"
        logger.info(f"RingbaAPI initialized with account ID: {self.account_id}")
        
        # Try to detect the working auth format
        self._detect_working_format()
    
    def _detect_working_format(self):
        """Try different authentication formats to find which one works"""
        logger.info("Attempting to detect working authentication format")
        
        for auth_format in self.auth_formats:
            test_headers = {
                "Content-Type": "application/json",
                "Authorization": auth_format["header"]
            }
            
            url = f"{self.base_url}/targets"
            
            try:
                logger.info(f"Testing auth format: {auth_format['name']}")
                response = requests.get(url, headers=test_headers)
                
                if response.status_code == 200:
                    logger.info(f"✅ Auth format {auth_format['name']} works!")
                    self.headers["Authorization"] = auth_format["header"]
                    self.current_auth_format = auth_format["name"]
                    return True
                else:
                    logger.info(f"❌ Auth format {auth_format['name']} failed with status {response.status_code}")
            except Exception as e:
                logger.error(f"Error testing auth format {auth_format['name']}: {str(e)}")
        
        logger.warning("⚠️ Could not find a working authentication format")
        return False
    
    def test_auth(self):
        """Test authentication with the API"""
        logger.info("Testing authentication with Ringba API")
        
        # Try each authentication format if the default one fails
        for auth_format in self.auth_formats:
            try:
                url = f"{self.base_url}/targets"
                test_headers = {
                    "Content-Type": "application/json", 
                    "Authorization": auth_format["header"]
                }
                
                logger.info(f"Testing with auth format: {auth_format['name']}")
                response = requests.get(url, headers=test_headers)
                
                if response.status_code == 200:
                    logger.info(f"Successfully authenticated with Ringba API using {auth_format['name']} format")
                    # Update the headers to use the successful format
                    self.headers["Authorization"] = auth_format["header"]
                    self.current_auth_format = auth_format["name"]
                    return True
                else:
                    logger.warning(f"Authentication with {auth_format['name']} failed: {response.status_code}")
            except Exception as e:
                logger.error(f"Error testing authentication with {auth_format['name']}: {str(e)}")
        
        logger.error("All authentication formats failed")
        return False
    
    def get_all_targets(self, include_stats=False):
        """
        Get all targets from the Ringba API
        
        Args:
            include_stats (bool, optional): Whether to include stats in the response. Defaults to False.
            
        Returns:
            dict: The API response containing targets
        """
        logger.info("Fetching all targets")
        
        try:
            url = f"{self.base_url}/targets"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check if the response has the expected structure
                if 'targets' in data:
                    targets = data['targets']
                    logger.info(f"Found {len(targets)} targets in 'targets' field")
                    
                    # Log the first few targets for debugging
                    for i, target in enumerate(targets[:3]):
                        if isinstance(target, dict):
                            logger.info(f"Target {i+1}: {target.get('name', 'Unknown')} (ID: {target.get('id', 'Unknown')})")
                    
                    return data
                else:
                    logger.error("Unexpected response format: 'targets' field not found")
                    return None
            else:
                logger.error(f"Failed to fetch targets: {response.status_code}")
                logger.debug(f"Response: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching targets: {str(e)}")
            return None
    
    def get_target_details(self, target_id):
        """Get details for a specific target"""
        logger.info(f"Fetching details for target ID: {target_id}")
        try:
            url = f"{self.base_url}/targets/{target_id}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # API returns data in a 'target' field according to documentation
                if 'target' in data:
                    target_data = data['target']
                    logger.info(f"Successfully fetched details for target: {target_data.get('name', 'Unknown')}")
                    return target_data
                else:
                    # Fallback for potential different format
                    logger.info(f"Successfully fetched details for target: {data.get('name', 'Unknown')}")
                    return data
            else:
                logger.error(f"Failed to fetch target details: {response.status_code}")
                logger.debug(f"Response: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching target details: {str(e)}")
            return None
    
    def get_target_counts(self, target_id, date_str=None):
        """
        Get counts for a specific target
        
        Args:
            target_id (str): The ID of the target
            date_str (str, optional): The date string in format YYYY-MM-DD. Defaults to yesterday.
            
        Returns:
            dict or list: The counts data from the API
        """
        try:
            # Default to yesterday if no date provided
            if not date_str:
                yesterday = datetime.now() - timedelta(days=1)
                date_str = yesterday.strftime('%Y-%m-%d')
            
            # Prepare the URL for the stats API endpoint
            # According to our testing, this is the correct endpoint
            url = f"{self.base_url}/targets/{target_id}/counts?interval=daily&startDate={date_str}&endDate={date_str}"
            logger.info(f"Fetching stats for target ID: {target_id}")
            
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                counts_data = response.json()
                logger.info(f"Successfully fetched counts for target ID: {target_id}")
                return counts_data
            else:
                logger.error(f"Error fetching counts: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Exception fetching counts: {str(e)}")
            return None
    
    def calculate_rpc_for_target(self, target_id, date_str=None):
        """
        Calculate Revenue Per Call (RPC) for a specific target
        
        Args:
            target_id (str): The ID of the target
            date_str (str, optional): The date string in format YYYY-MM-DD. Defaults to today.
            
        Returns:
            float: The calculated RPC, or None if calculation fails
        """
        target_details = self.get_target_details(target_id)
        if not target_details:
            return None
        
        target_name = target_details.get('name', 'Unknown')
        logger.info(f"Calculating RPC for target: {target_name} (ID: {target_id}) on {date_str}")
        
        counts = self.get_target_counts(target_id, date_str)
        if not counts:
            return None
        
        try:
            # Check for the new response format with 'stats' field
            if isinstance(counts, list) and 'transactionId' in counts and 'stats' in counts:
                logger.info("Processing stats format")
                stats = counts[1]  # The stats data should be in the second element
                
                if isinstance(stats, dict):
                    total_calls = stats.get('totalCalls', 0)
                    payout = stats.get('payout', 0)
                    
                    if total_calls > 0:
                        rpc = payout / total_calls
                        logger.info(f"Target: {target_name}, Total Calls: {total_calls}, Payout: ${payout}, RPC: ${rpc:.2f}")
                        return rpc
                    else:
                        logger.info(f"Target: {target_name}, No calls recorded for the date")
                        return 0
                else:
                    logger.error(f"Unexpected stats format: {type(stats)}")
                    return None
            
            # Original format handling
            elif isinstance(counts, dict):
                total_calls = counts.get('totalCalls', 0)
                payout = counts.get('payout', 0)
                
                if total_calls > 0:
                    rpc = payout / total_calls
                    logger.info(f"Target: {target_name}, Total Calls: {total_calls}, Payout: ${payout}, RPC: ${rpc:.2f}")
                    return rpc
                else:
                    logger.info(f"Target: {target_name}, No calls recorded for the date")
                    return 0
            else:
                logger.error(f"Unrecognized counts data format: {list(counts.keys()) if isinstance(counts, dict) else counts}")
                return None
        except Exception as e:
            logger.error(f"Error calculating RPC for target {target_name}: {str(e)}")
            return None
    
    def find_targets_above_threshold(self, threshold, date_str=None):
        """
        Find all targets with RPC above the given threshold
        
        Args:
            threshold (float): The RPC threshold
            date_str (str, optional): The date string in format YYYY-MM-DD. Defaults to yesterday.
            
        Returns:
            list: A list of dictionaries containing target info above the threshold
        """
        logger.info(f"Finding targets above RPC threshold: ${threshold}")
        
        # Get all targets first
        response = self.get_all_targets()
        if not response:
            logger.error("Failed to get targets list")
            return []
        
        # Check if the response contains targets
        all_targets = []
        if 'targets' in response:
            all_targets = response['targets']
        
        above_threshold = []
        
        for target in all_targets:
            target_id = target.get('id')
            target_name = target.get('name', 'Unknown')
            is_enabled = target.get('enabled', False)
            
            # Skip disabled targets
            if not is_enabled:
                logger.info(f"Skipping disabled target: {target_name}")
                continue
            
            # Calculate RPC for this target
            rpc = self.calculate_rpc_for_target(target_id, date_str)
            
            if rpc is not None and rpc >= threshold:
                above_threshold.append({
                    'id': target_id,
                    'name': target_name,
                    'rpc': rpc
                })
        
        return above_threshold
    
    def get_target_inbound_references(self, target_id):
        """Get campaigns that reference this target"""
        logger.info(f"Getting inbound references for target ID: {target_id}")
        try:
            # Use correct capitalization for InboundReferences as per documentation
            url = f"{self.base_url}/targets/{target_id}/InboundReferences"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'campaigns' in data:
                    campaigns = data['campaigns']
                    logger.info(f"Found {len(campaigns)} campaigns referencing this target")
                    return campaigns
                else:
                    logger.warning("No campaigns found referencing this target")
                    return []
            else:
                logger.error(f"Failed to get inbound references: {response.status_code}")
                logger.debug(f"Response: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error getting inbound references: {str(e)}")
            return []
    
    def get_target_stats(self, target_id):
        """
        Get stats for a specific target
        
        Args:
            target_id (str): Target ID
        
        Returns:
            dict: Target stats
        """
        logger.info(f"Getting stats for target ID {target_id}")
        
        url = f"{self.base_url}/accounts/{self.account_id}/targets/{target_id}/stats"
        headers = self.headers
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            stats_data = response.json()
            logger.info(f"Successfully retrieved stats for target ID {target_id}")
            return stats_data
            
        except Exception as e:
            logger.error(f"Error getting target stats: {str(e)}")
            raise Exception(f"Failed to get stats for target ID {target_id}") from e
    
    def get_target_groups(self):
        """
        Get all target groups in the account
        
        Returns:
            list: List of target groups
        """
        logger.info("Getting all target groups")
        
        url = f"{self.base_url}/accounts/{self.account_id}/targetgroups"
        headers = self.headers
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            groups = response.json().get('items', [])
            logger.info(f"Found {len(groups)} target groups")
            return groups
            
        except Exception as e:
            logger.error(f"Error getting target groups: {str(e)}")
            raise Exception("Failed to get target groups from Ringba API") from e
    
    def get_buyers(self):
        """
        Get all buyers in the account
        
        Returns:
            list: List of buyers
        """
        logger.info("Getting all buyers")
        
        url = f"{self.base_url}/accounts/{self.account_id}/buyers"
        headers = self.headers
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            buyers = response.json().get('items', [])
            logger.info(f"Found {len(buyers)} buyers")
            return buyers
            
        except Exception as e:
            logger.error(f"Error getting buyers: {str(e)}")
            raise Exception("Failed to get buyers from Ringba API") from e
    
    def get_call_logs_simple(self, start_date=None, end_date=None, page=1, page_size=100):
        """
        Get call logs within a date range
        
        Args:
            start_date (datetime, optional): Start date for filtering calls
            end_date (datetime, optional): End date for filtering calls
            page (int, optional): Page number for pagination
            page_size (int, optional): Number of items per page
        
        Returns:
            dict: Call logs data
        """
        logger.info(f"Getting call logs from {start_date} to {end_date} (page {page})")
        
        url = f"{self.base_url}/accounts/{self.account_id}/calllogs?page={page}&pageSize={page_size}"
        
        # Add date filters if provided
        if start_date:
            # Format to ISO 8601 format required by API
            start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            url += f"&startDate={start_str}"
        
        if end_date:
            # Format to ISO 8601 format required by API
            end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            url += f"&endDate={end_str}"
        
        headers = self.headers
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            calls = data.get('items', [])
            logger.info(f"Found {len(calls)} call logs")
            return data
            
        except Exception as e:
            logger.error(f"Error getting call logs: {str(e)}")
            raise Exception("Failed to get call logs from Ringba API") from e
    
    def get_call_details(self, call_ids):
        """
        Get detailed information for specific calls
        
        Args:
            call_ids (list): List of call IDs
        
        Returns:
            list: Detailed call information
        """
        if not call_ids:
            logger.warning("No call IDs provided for details")
            return []
        
        logger.info(f"Getting details for {len(call_ids)} calls")
        
        url = f"{self.base_url}/accounts/{self.account_id}/calllogs/detail"
        headers = self.headers
        
        # Ringba expects a comma-separated list of call IDs
        call_ids_str = ",".join(call_ids)
        params = {"ids": call_ids_str}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            calls = response.json()
            logger.info(f"Successfully retrieved details for {len(calls)} calls")
            return calls
            
        except Exception as e:
            logger.error(f"Error getting call details: {str(e)}")
            raise Exception("Failed to get call details from Ringba API") from e
    
    def get_call_logs(self, start_time, end_time, additional_fields=None):
        """
        Get all call logs within a time range, handling pagination
        
        Args:
            start_time (datetime): Start time for filtering calls
            end_time (datetime): End time for filtering calls
            additional_fields (list, optional): Additional fields to include
        
        Returns:
            list: All call logs within the time range
        """
        logger.info(f"Getting all call logs from {start_time} to {end_time}")
        
        all_calls = []
        page = 1
        page_size = 100  # Maximum allowed by API
        
        # Convert times to UTC if they have timezone info
        if start_time.tzinfo:
            start_time = start_time.astimezone(datetime.timezone.utc)
        if end_time.tzinfo:
            end_time = end_time.astimezone(datetime.timezone.utc)
        
        # Format times for API
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        while True:
            try:
                # Get page of call logs
                url = f"{self.base_url}/accounts/{self.account_id}/calllogs?page={page}&pageSize={page_size}&startDate={start_str}&endDate={end_str}"
                
                # Add additional fields if specified
                if additional_fields:
                    fields_str = ",".join(additional_fields)
                    url += f"&fields={fields_str}"
                
                headers = self.headers
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                data = response.json()
                calls = data.get('items', [])
                
                # Add calls to result
                all_calls.extend(calls)
                
                # Check if there are more pages
                total_pages = data.get('totalPages', 0)
                if page >= total_pages or not calls:
                    break
                
                # Move to next page
                page += 1
                
            except Exception as e:
                logger.error(f"Error getting call logs (page {page}): {str(e)}")
                break
        
        logger.info(f"Retrieved a total of {len(all_calls)} call logs")
        return all_calls
    
    def get_webhooks(self):
        """
        Get all webhooks in the account
        
        Returns:
            list: List of webhooks
        """
        logger.info("Getting all webhooks")
        
        url = f"{self.base_url}/accounts/{self.account_id}/webhooks"
        headers = self.headers
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            webhooks = response.json().get('items', [])
            logger.info(f"Found {len(webhooks)} webhooks")
            return webhooks
            
        except Exception as e:
            logger.error(f"Error getting webhooks: {str(e)}")
            raise Exception("Failed to get webhooks from Ringba API") from e
    
    def get_historical_rpc_by_call_logs(self, target_id, target_name, start_time, end_time):
        """
        Calculate historical RPC for a target using call logs
        
        Args:
            target_id (str): Target ID
            target_name (str): Target name for logging
            start_time (datetime): Start time
            end_time (datetime): End time
        
        Returns:
            float: RPC value
        """
        logger.info(f"Calculating historical RPC for {target_name} (ID: {target_id}) from {start_time} to {end_time}")
        
        try:
            # Get call logs for the specified time period
            calls = self.get_call_logs(start_time, end_time)
            
            # Filter calls for the specific target
            target_calls = [c for c in calls if c.get('targetId') == target_id]
            
            if not target_calls:
                logger.warning(f"No calls found for target {target_name} in the specified time period")
                return 0
            
            # Calculate total calls and revenue
            total_calls = len(target_calls)
            total_revenue = sum(float(c.get('targetRevenue', 0)) for c in target_calls)
            
            # Calculate RPC
            rpc = total_revenue / total_calls if total_calls > 0 else 0
            
            logger.info(f"Target {target_name}: Found {total_calls} calls with total revenue ${total_revenue:.2f}, RPC: ${rpc:.2f}")
            return rpc
            
        except Exception as e:
            logger.error(f"Error calculating historical RPC for {target_name}: {str(e)}")
            return 0
    
    def calculate_historical_rpc(self, target_id, target_name, start_time, end_time):
        """
        Calculate historical RPC using multiple methods
        
        This method tries multiple approaches to get historical RPC:
        1. Use call logs API if available
        2. Fall back to target stats if call logs unavailable
        
        Args:
            target_id (str): Target ID
            target_name (str): Target name for logging
            start_time (datetime): Start time
            end_time (datetime): End time
        
        Returns:
            float: RPC value
        """
        logger.info(f"Calculating historical RPC for {target_name} using multiple methods")
        
        try:
            # Try using call logs first (most accurate)
            try:
                rpc = self.get_historical_rpc_by_call_logs(target_id, target_name, start_time, end_time)
                if rpc > 0:
                    logger.info(f"Successfully calculated RPC for {target_name} using call logs: ${rpc:.2f}")
                    return rpc
            except Exception as e:
                logger.warning(f"Could not calculate RPC using call logs: {str(e)}")
            
            # Fall back to target stats
            logger.info(f"Falling back to target stats for {target_name}")
            target_details = self.get_target_details(target_id)
            rpc = self.calculate_rpc_for_target(target_details)
            
            logger.info(f"Calculated RPC for {target_name} using target stats: ${rpc:.2f}")
            return rpc
            
        except Exception as e:
            logger.error(f"All methods failed to calculate RPC for {target_name}: {str(e)}")
            return 0 