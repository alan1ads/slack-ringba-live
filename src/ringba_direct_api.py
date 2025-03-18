#!/usr/bin/env python3
"""
Improved Ringba API client that uses direct endpoints for call logs and insights
to get pre-calculated RPC data.
"""

import requests
import logging
import json
from datetime import datetime, timedelta
import os
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

logger = logging.getLogger('ringba_direct.api')

class RingbaDirectAPI:
    """
    Improved Ringba API client that uses direct endpoints for call logs and insights
    to get pre-calculated RPC data.
    """
    
    def __init__(self, api_token, account_id):
        """Initialize with API token and account ID"""
        self.api_token = api_token
        self.account_id = account_id
        
        # Try different authentication formats
        self.auth_formats = [
            {"name": "Bearer", "header": f"Bearer {self.api_token}"},
            {"name": "Token", "header": f"Token {self.api_token}"},
            {"name": "NoPrefix", "header": f"{self.api_token}"}
        ]
        
        # Check if auth format is specified in environment variables
        auth_format_env = os.getenv('RINGBA_AUTH_FORMAT')
        if auth_format_env:
            logger.info(f"Using auth format from environment variable: {auth_format_env}")
            # Set the specified auth format as default
            self.headers = {
                "Content-Type": "application/json",
                "Authorization": f"{auth_format_env} {self.api_token}" if auth_format_env != "NoPrefix" else self.api_token
            }
            self.current_auth_format = auth_format_env
        else:
            # Default to Bearer token initially
            self.headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_token}"
            }
            
            # Try to detect the working authentication format
            self._detect_working_format()
        
        # Base URL with account ID
        self.base_url = f"https://api.ringba.com/v2/{self.account_id}"
        logger.info(f"RingbaDirectAPI initialized with account ID: {self.account_id}")
        
        # If we don't have a working format yet, try to detect one
        if not hasattr(self, 'current_auth_format'):
            self._detect_working_format()
    
    def _detect_working_format(self):
        """Test different authentication formats to find the one that works"""
        logger.info("Attempting to detect working authentication format")
        
        for auth_format in self.auth_formats:
            test_headers = {
                "Content-Type": "application/json",
                "Authorization": auth_format["header"]
            }
            
            # Try a simple API call to test authentication
            try:
                url = f"{self.base_url}/targets"
                response = requests.get(url, headers=test_headers)
                
                if response.status_code == 200:
                    logger.info(f"Auth format {auth_format['name']} works!")
                    # Update the headers with the working format
                    self.headers = test_headers
                    self.current_auth_format = auth_format["name"]
                    return True
                else:
                    logger.info(f"Auth format {auth_format['name']} failed with status {response.status_code}")
            except Exception as e:
                logger.error(f"Error testing auth format {auth_format['name']}: {str(e)}")
        
        logger.error("No working authentication format found")
        return False
    
    def test_auth(self):
        """Test authentication with the API"""
        logger.info("Testing authentication with Ringba API")
        
        # If we already found a working format, use it
        if hasattr(self, 'current_auth_format'):
            logger.info(f"Testing with auth format: {self.current_auth_format}")
            
            try:
                # Use the targets endpoint to test authentication
                url = f"{self.base_url}/targets"
                response = requests.get(url, headers=self.headers)
                
                if response.status_code == 200:
                    logger.info(f"Successfully authenticated with Ringba API using {self.current_auth_format} format")
                    return True
                else:
                    logger.error(f"Authentication failed: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Error testing authentication: {str(e)}")
        
        # If we don't have a working format yet, or it failed, try to detect one
        if not hasattr(self, 'current_auth_format') or response.status_code != 200:
            if self._detect_working_format():
                return True
        
        return False
    
    def get_call_logs(self, start_date=None, end_date=None):
        """
        Get call logs for a specific date range using the direct calllogs endpoint
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            dict: The API response containing call logs with pre-calculated metrics
        """
        logger.info(f"Fetching call logs from {start_date} to {end_date}")
        
        # If no dates provided, use today
        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        # Prepare the request body for call logs endpoint
        request_body = {
            "startDate": f"{start_date}T00:00:00.000Z",
            "endDate": f"{end_date}T23:59:59.999Z",
            "reportStart": f"{start_date}T00:00:00.000Z",
            "reportEnd": f"{end_date}T23:59:59.999Z",
            "timeField": "connectTime",
            "timeZone": "America/New_York",  # Using Eastern time
            "fields": [
                "targetName",
                "targetId",
                "inboundPhoneNumber",
                "duration",
                "connectTime",
                "tagIds",
                "tagNames",
                "buyerName",
                "payout",
                "campaignName"
            ],
            "filters": [],
            "page": 1,
            "pageSize": 5000,  # Request a large page size to get all calls
            "sortField": "connectTime",
            "sortDirection": "desc"
        }
        
        try:
            url = f"{self.base_url}/calllogs"
            response = requests.post(url, headers=self.headers, json=request_body)
            
            if response.status_code == 200:
                data = response.json()
                total_calls = data.get("totalItems", 0)
                logger.info(f"Successfully fetched {total_calls} call logs")
                return data
            else:
                logger.error(f"Failed to fetch call logs: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching call logs: {str(e)}")
            return None
    
    def get_insights(self, start_date=None, end_date=None, group_by="targetId"):
        """
        Get insights data with pre-calculated RPC using the insights endpoint
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            group_by (str): Field to group by (targetId, campaignId, etc.)
            
        Returns:
            dict: The API response containing insights data with pre-calculated metrics
        """
        logger.info(f"Fetching insights from {start_date} to {end_date} grouped by {group_by}")
        
        # If no dates provided, use today
        if not start_date:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        # Prepare the request body for insights endpoint
        request_body = {
            "startDate": f"{start_date}T00:00:00.000Z",
            "endDate": f"{end_date}T23:59:59.999Z",
            "reportStart": f"{start_date}T00:00:00.000Z",
            "reportEnd": f"{end_date}T23:59:59.999Z",
            "timeField": "connectTime",
            "timeZone": "America/New_York",  # Using Eastern time
            "groupBy": [group_by],
            "metrics": [
                "calls",
                "connected",
                "uniqueCalls",
                "revenue",
                "payout",
                "duration",
                "conversionRate",
                "rpc"  # Request RPC directly from the API
            ],
            "filters": [],
            "sortField": "rpc",  # Sort by RPC to get highest RPC values first
            "sortDirection": "desc",
            "page": 1,
            "pageSize": 1000  # Request a large page size to get all targets
        }
        
        try:
            url = f"{self.base_url}/insights"
            response = requests.post(url, headers=self.headers, json=request_body)
            
            if response.status_code == 200:
                data = response.json()
                total_items = len(data.get("items", []))
                logger.info(f"Successfully fetched insights with {total_items} items")
                return data
            else:
                logger.error(f"Failed to fetch insights: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching insights: {str(e)}")
            return None
    
    def get_targets(self):
        """
        Get all targets from the Ringba API
        
        Returns:
            list: List of targets with their details
        """
        logger.info("Fetching all targets")
        
        try:
            url = f"{self.base_url}/targets"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                targets = data.get('targets', [])
                logger.info(f"Successfully fetched {len(targets)} targets")
                return targets
            else:
                logger.error(f"Failed to fetch targets: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error fetching targets: {str(e)}")
            return []
    
    def get_target_details(self, target_id):
        """
        Get details for a specific target
        
        Args:
            target_id (str): Target ID
            
        Returns:
            dict: Target details
        """
        logger.info(f"Fetching details for target ID: {target_id}")
        
        try:
            url = f"{self.base_url}/targets/{target_id}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # API might return in different formats
                if 'target' in data:
                    target_data = data['target']
                else:
                    target_data = data
                    
                target_name = target_data.get('name', 'Unknown')
                logger.info(f"Successfully fetched details for target: {target_name}")
                return target_data
            else:
                logger.error(f"Failed to fetch target details: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching target details: {str(e)}")
            return None
    
    def get_targets_above_threshold(self, threshold, date=None):
        """
        Get all targets with RPC above the specified threshold
        
        Args:
            threshold (float): RPC threshold value
            date (str): Date to check in YYYY-MM-DD format
            
        Returns:
            list: List of targets with RPC above threshold
        """
        logger.info(f"Finding targets with RPC above ${threshold}")
        
        # If no date provided, use today
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        
        # Get insights data grouped by targetId
        insights_data = self.get_insights(start_date=date, end_date=date, group_by="targetId")
        
        if not insights_data or "items" not in insights_data:
            logger.error("Failed to get insights data")
            return []
        
        # Extract items with RPC above threshold
        items = insights_data.get("items", [])
        targets_above_threshold = []
        
        # Get all targets to get additional information
        all_targets = self.get_targets()
        target_dict = {t.get('id'): t for t in all_targets if 'id' in t}
        
        # Get call logs to extract tag information
        call_logs = self.get_call_logs(start_date=date, end_date=date)
        calls_by_target = {}
        
        # Group calls by target and collect tag information
        if call_logs and "items" in call_logs:
            for call in call_logs.get("items", []):
                target_id = call.get("targetId")
                if target_id:
                    if target_id not in calls_by_target:
                        calls_by_target[target_id] = []
                    calls_by_target[target_id].append(call)
        
        # Get tag information
        tags_info = self.get_tags()
        tags_dict = {t.get('id'): t for t in tags_info if 'id' in t}
        
        for item in items:
            target_id = item.get("targetId")
            
            # Skip if no target ID
            if not target_id:
                continue
                
            # Get RPC value
            rpc = item.get("rpc", 0)
            
            if rpc >= threshold:
                # Get target details from all targets dict
                target_info = target_dict.get(target_id, {})
                target_name = target_info.get("name", "Unknown Target")
                enabled = target_info.get("enabled", False)
                
                # Skip disabled targets
                if not enabled:
                    logger.info(f"Skipping disabled target: {target_name}")
                    continue
                
                # Get calls and revenue
                calls = item.get("calls", 0)
                revenue = item.get("revenue", 0)
                
                # Extract tag information from calls for this target
                target_calls = calls_by_target.get(target_id, [])
                target_tags = {}
                
                for call in target_calls:
                    tag_ids = call.get("tagIds", [])
                    for tag_id in tag_ids:
                        tag_info = tags_dict.get(tag_id, {})
                        tag_name = tag_info.get("name", "Unknown Tag")
                        if tag_name not in target_tags:
                            target_tags[tag_name] = 0
                        target_tags[tag_name] += 1
                
                targets_above_threshold.append({
                    'id': target_id,
                    'name': target_name,
                    'rpc': rpc,
                    'calls': calls,
                    'revenue': revenue,
                    'enabled': enabled,
                    'tags': target_tags
                })
                
                logger.info(f"Target '{target_name}' has RPC of ${rpc:.2f} with {calls} calls and ${revenue:.2f} revenue")
                if target_tags:
                    logger.info(f"Tags for '{target_name}': {target_tags}")
        
        logger.info(f"Found {len(targets_above_threshold)} targets with RPC above ${threshold}")
        return targets_above_threshold
    
    def get_call_log_columns(self):
        """
        Get available call log columns from the API
        
        Returns:
            list: List of available call log column definitions
        """
        logger.info("Fetching call log columns")
        
        try:
            url = f"{self.base_url}/calllogs/columns"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                columns = data.get('columns', [])
                logger.info(f"Successfully fetched {len(columns)} call log columns")
                return columns
            else:
                logger.error(f"Failed to fetch call log columns: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error fetching call log columns: {str(e)}")
            return []
    
    def get_tags(self):
        """
        Get all tags from the Ringba API
        
        Returns:
            list: List of tags with their details
        """
        logger.info("Fetching all tags")
        
        try:
            url = f"{self.base_url}/tags"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # Handle different response formats
                tags = []
                if isinstance(data, list):
                    # API returned a list of tags directly
                    tags = data
                elif isinstance(data, dict):
                    # API returned an object with tags property
                    tags = data.get('tags', [])
                
                logger.info(f"Successfully fetched {len(tags)} tags")
                return tags
            else:
                logger.error(f"Failed to fetch tags: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error fetching tags: {str(e)}")
            return [] 