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
import pytz

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
            eastern = pytz.timezone('US/Eastern')
            now_eastern = datetime.now(eastern)
            start_date = now_eastern.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        # Format start and end date with timezone
        # Format: 2025-03-18T00:00:00.000-04:00
        start_datetime = f"{start_date}T00:00:00.000-04:00"
        end_datetime = f"{end_date}T23:59:59.999-04:00"
        
        logger.info(f"Using date range: {start_datetime} to {end_datetime}")
        
        # Prepare the request body for call logs endpoint
        request_body = {
            "startDate": start_datetime,
            "endDate": end_datetime,
            "reportStart": start_datetime,
            "reportEnd": end_datetime,
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
                "payoutAmount",  # Added this field to get payout in a consistent format
                "campaignName",
                "hasConverted",
                "conversionAmount"
            ],
            "filters": [],
            "page": 1,
            "pageSize": 5000,  # Request a large page size to get all calls
            "sortField": "connectTime",
            "sortDirection": "desc"
        }
        
        # Log the full request body for debugging
        logger.info(f"Call logs request: {json.dumps(request_body, indent=2)}")
        
        try:
            url = f"{self.base_url}/calllogs"
            response = requests.post(url, headers=self.headers, json=request_body)
            
            # Log the response headers and status
            logger.info(f"Call logs API response status: {response.status_code}")
            logger.debug(f"Call logs API response headers: {response.headers}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract calls from the report records
                if "report" in data and "records" in data["report"]:
                    calls = data["report"]["records"]
                    total_calls = len(calls)
                    logger.info(f"Successfully fetched {total_calls} call logs")
                    
                    # Return in a standardized format with items key for consistency
                    return {
                        "items": calls,
                        "totalItems": total_calls,
                        "originalResponse": data
                    }
                else:
                    logger.error("API response missing expected structure (report -> records)")
                    logger.debug(f"Response structure: {json.dumps(data, indent=2)[:1000]}...")  # Log first 1000 chars
                    return {"items": [], "totalItems": 0, "originalResponse": data}
            else:
                logger.error(f"Failed to fetch call logs: {response.status_code} - {response.text}")
                return {"items": [], "totalItems": 0}
        except Exception as e:
            logger.error(f"Error fetching call logs: {str(e)}")
            return {"items": [], "totalItems": 0}
    
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
            eastern = pytz.timezone('US/Eastern')
            now_eastern = datetime.now(eastern)
            start_date = now_eastern.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        # Format start and end date with timezone
        start_datetime = f"{start_date}T00:00:00.000-04:00"
        end_datetime = f"{end_date}T23:59:59.999-04:00"
        
        logger.info(f"Using date range: {start_datetime} to {end_datetime}")
        
        # Prepare the request body for insights endpoint
        request_body = {
            "startDate": start_datetime,
            "endDate": end_datetime,
            "reportStart": start_datetime,
            "reportEnd": end_datetime,
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
        
        # Log the full request body for debugging
        logger.info(f"Insights request: {json.dumps(request_body, indent=2)}")
        
        try:
            url = f"{self.base_url}/insights"
            response = requests.post(url, headers=self.headers, json=request_body)
            
            # Log the response headers and status
            logger.info(f"Insights API response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Print the complete response structure for debugging
                logger.info(f"Insights API raw response structure: {json.dumps(data, indent=2)[:5000]}")
                
                # Extract items from the response
                if "report" in data and "records" in data["report"]:
                    items = data["report"]["records"]
                    total_items = len(items)
                    logger.info(f"Successfully fetched insights with {total_items} items from reports.records")
                    
                    # Return in a standardized format
                    return {
                        "items": items,
                        "totalItems": total_items,
                        "originalResponse": data
                    }
                elif "items" in data:
                    total_items = len(data["items"])
                    logger.info(f"Successfully fetched insights with {total_items} items from items array")
                    return data
                else:
                    logger.error("API response missing expected structure (report->records or items)")
                    logger.debug(f"Response structure: {json.dumps(data, indent=2)[:1000]}...")
                    return {"items": [], "totalItems": 0, "originalResponse": data}
            else:
                logger.error(f"Failed to fetch insights: {response.status_code} - {response.text}")
                return {"items": [], "totalItems": 0}
        except Exception as e:
            logger.error(f"Error fetching insights: {str(e)}")
            return {"items": [], "totalItems": 0}
    
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
    
    def get_target_counts(self, target_id):
        """
        Get real-time counts for a specific target using the direct counts endpoint
        
        Args:
            target_id (str): The target ID to get counts for
            
        Returns:
            dict: The target counts data
        """
        logger.info(f"Fetching real-time counts for target ID: {target_id}")
        
        try:
            url = f"{self.base_url}/targets/{target_id}/Counts"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched counts for target ID: {target_id}")
                return data
            else:
                logger.error(f"Failed to fetch target counts: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching target counts: {str(e)}")
            return None
    
    def get_target_rpc_today(self, target_id):
        """
        Get current day's RPC for a target from 00:00 to now
        
        Args:
            target_id (str): The target ID to get RPC for
            
        Returns:
            dict: RPC and related data for the target
        """
        logger.info(f"Calculating today's RPC for target ID: {target_id}")
        
        counts_data = self.get_target_counts(target_id)
        if not counts_data:
            logger.error(f"No counts data found for target ID: {target_id}")
            return {
                'id': target_id,
                'name': "Unknown", 
                'rpc': 0,
                'calls': 0,
                'revenue': 0
            }
        
        # Try to get target details for name
        target_info = self.get_target_details(target_id)
        target_name = target_info.get("name", "Unknown Target") if target_info else "Unknown Target"
        
        # Extract stats from the response
        stats = counts_data.get('stats', {})
        target_stats = stats.get(target_id.lower().replace('ta', 't'), {})
        
        # Get today's values (currentDaySum for revenue, currentDay for calls)
        revenue = target_stats.get('currentDaySum', 0)
        calls = target_stats.get('currentDay', 0)
        
        # Calculate RPC (avoid division by zero)
        rpc = revenue / calls if calls > 0 else 0
        
        logger.info(f"Target '{target_name}' has {calls} calls and ${revenue:.2f} revenue today, RPC: ${rpc:.2f}")
        
        return {
            'id': target_id,
            'name': target_name,
            'rpc': rpc,
            'calls': calls,
            'revenue': revenue
        }
    
    def get_dashboard_rpc(self, start_date=None, end_date=None):
        """
        Get RPC data directly matching what's shown in the Ringba dashboard
        by using call logs and aggregating the data
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            list: List of targets with their RPC data matching the dashboard
        """
        logger.info(f"Getting dashboard RPC data from {start_date} to {end_date}")
        
        # If no dates provided, use today
        if not start_date:
            eastern = pytz.timezone('US/Eastern')
            now_eastern = datetime.now(eastern)
            start_date = now_eastern.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        # Get all calls for the date range
        call_logs = self.get_call_logs(start_date=start_date, end_date=end_date)
        
        if not call_logs or "items" not in call_logs or not call_logs["items"]:
            logger.error("Failed to get call logs for dashboard RPC")
            return []
        
        calls = call_logs["items"]
        logger.info(f"Retrieved {len(calls)} calls for dashboard RPC")
        
        # Get all targets to get additional information
        all_targets = self.get_targets()
        target_dict = {t.get('id'): t for t in all_targets if 'id' in t}
        
        # Get mapping from internal IDs to public IDs
        target_public_ids = self.get_target_public_id_mapping()
        logger.info(f"Retrieved mapping for {len(target_public_ids)} target public IDs")
        
        # Aggregate data by target
        target_metrics = {}
        
        for call in calls:
            target_id = call.get("targetId")
            
            # Skip calls without target ID
            if not target_id:
                continue
            
            # Skip calls that haven't connected
            if not call.get("hasConnected", False):
                continue
            
            # Initialize target data if not already done
            if target_id not in target_metrics:
                target_name = call.get("targetName", "Unknown Target")
                
                # Get more details from target info if available
                public_id = target_id  # Default to internal ID
                if target_id in target_dict:
                    target_info = target_dict[target_id]
                    target_name = target_info.get("name", target_name)
                
                # Try to get public ID from mapping
                if target_id in target_public_ids:
                    public_id = target_public_ids[target_id]
                    
                target_metrics[target_id] = {
                    'id': public_id,  # Use public ID for UI links
                    'internal_id': target_id,  # Keep internal ID for reference
                    'name': target_name,
                    'calls': 0,
                    'connected': 0,
                    'revenue': 0,
                    'payout': 0,
                    'enabled': target_id in target_dict and target_dict[target_id].get("enabled", False)
                }
            
            # Get call data
            target_metrics[target_id]['calls'] += 1
            
            # Check if call was connected
            if call.get("hasConnected", False):
                target_metrics[target_id]['connected'] += 1
            
            # Get payout amount, trying different fields
            payout = 0
            
            # Try payoutAmount first (most reliable)
            if "payoutAmount" in call and call["payoutAmount"] is not None:
                try:
                    if isinstance(call["payoutAmount"], (int, float)):
                        payout = float(call["payoutAmount"])
                    elif isinstance(call["payoutAmount"], str):
                        payout_str = call["payoutAmount"].replace('$', '').replace(',', '')
                        payout = float(payout_str)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert payoutAmount '{call['payoutAmount']}' to float")
            
            # Fallback to regular payout
            elif "payout" in call and call["payout"] is not None:
                payout_raw = call["payout"]
                if isinstance(payout_raw, (int, float)):
                    payout = float(payout_raw)
                elif isinstance(payout_raw, str):
                    try:
                        payout_str = payout_raw.replace('$', '').replace(',', '')
                        payout = float(payout_str)
                    except ValueError:
                        logger.warning(f"Could not convert payout string '{payout_raw}' to float")
                elif isinstance(payout_raw, dict) and 'amount' in payout_raw:
                    try:
                        payout = float(payout_raw.get('amount', 0))
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert payout amount '{payout_raw.get('amount')}' to float")
            
            # Try conversionAmount as a last resort
            elif "conversionAmount" in call and call["conversionAmount"] is not None and call.get("hasConverted", False):
                try:
                    if isinstance(call["conversionAmount"], (int, float)):
                        payout = float(call["conversionAmount"])
                    elif isinstance(call["conversionAmount"], str):
                        payout_str = call["conversionAmount"].replace('$', '').replace(',', '')
                        payout = float(payout_str)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert conversionAmount '{call['conversionAmount']}' to float")
            
            # Add payout to revenue (accumulated)
            target_metrics[target_id]['revenue'] += payout
            target_metrics[target_id]['payout'] += payout
        
        # Calculate RPC for each target
        targets_with_rpc = []
        
        for target_id, metrics in target_metrics.items():
            calls = metrics['calls']
            revenue = metrics['revenue']
            
            # Calculate RPC
            rpc = 0
            if calls > 0:
                rpc = revenue / calls
            
            # Update metrics with RPC
            metrics['rpc'] = rpc
            
            # Add to results
            targets_with_rpc.append(metrics)
            
            logger.info(f"Target '{metrics['name']}' has RPC of ${rpc:.2f} with {calls} calls and ${revenue:.2f} revenue")
        
        # Sort by RPC (highest first)
        targets_with_rpc.sort(key=lambda x: x['rpc'], reverse=True)
        
        logger.info(f"Found {len(targets_with_rpc)} targets with RPC data for dashboard view")
        return targets_with_rpc
    
    def compare_rpc_calculations(self, start_date=None, end_date=None):
        """
        Compare different RPC calculation methods to find which one matches the UI
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            dict: Dictionary with results from different calculation methods
        """
        logger.info(f"Comparing RPC calculation methods for {start_date} to {end_date}")
        
        # If no dates provided, use today
        if not start_date:
            eastern = pytz.timezone('US/Eastern')
            now_eastern = datetime.now(eastern)
            start_date = now_eastern.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        results = {
            "insights_api": [],       # RPC from insights API endpoint
            "calllogs_calculated": [] # RPC calculated manually from call logs
        }
        
        # Method 1: Get RPC data from insights API endpoint
        insights_data = self.get_insights(start_date=start_date, end_date=end_date, group_by="targetId")
        if insights_data and "items" in insights_data:
            items = insights_data.get("items", [])
            
            # Get all targets to get additional information
            all_targets = self.get_targets()
            target_dict = {t.get('id'): t for t in all_targets if 'id' in t}
            
            for item in items:
                target_id = item.get("targetId")
                
                # Skip if no target ID
                if not target_id:
                    continue
                
                # Try to get target name
                target_name = "Unknown Target"
                if target_id in target_dict:
                    target_name = target_dict[target_id].get("name", "Unknown Target")
                    
                rpc = item.get("rpc", 0)
                calls = item.get("calls", 0)
                revenue = item.get("revenue", 0)
                connected = item.get("connected", 0)
                
                results["insights_api"].append({
                    'id': target_id,
                    'name': target_name,
                    'rpc': rpc,
                    'calls': calls,
                    'revenue': revenue,
                    'connected': connected,
                    'calculation_method': 'insights_api'
                })
                
                logger.info(f"Insights API: Target '{target_name}' has RPC of ${rpc:.2f} with {calls} calls and ${revenue:.2f} revenue")
        
        # Method 2: Calculate RPC from call logs (same as get_dashboard_rpc)
        calllogs_targets = self.get_dashboard_rpc(start_date=start_date, end_date=end_date)
        for target in calllogs_targets:
            target['calculation_method'] = 'calllogs_calculated'
            results["calllogs_calculated"].append(target)
        
        # Compare the results
        comparison = []
        
        # Create a mapping of target IDs to results for easier comparison
        insights_map = {t['id']: t for t in results["insights_api"]}
        calllogs_map = {t['id']: t for t in results["calllogs_calculated"]}
        
        # Find all unique target IDs
        all_target_ids = set(list(insights_map.keys()) + list(calllogs_map.keys()))
        
        for target_id in all_target_ids:
            insights_data = insights_map.get(target_id, {})
            calllogs_data = calllogs_map.get(target_id, {})
            
            # Skip if either method doesn't have data for this target
            if not insights_data or not calllogs_data:
                continue
            
            # Get values for comparison
            insights_rpc = insights_data.get('rpc', 0)
            calllogs_rpc = calllogs_data.get('rpc', 0)
            target_name = insights_data.get('name', calllogs_data.get('name', 'Unknown Target'))
            
            # Calculate difference
            rpc_difference = abs(insights_rpc - calllogs_rpc)
            percentage_diff = 100 * rpc_difference / max(insights_rpc, calllogs_rpc, 0.0001)
            
            comparison.append({
                'id': target_id,
                'name': target_name,
                'insights_rpc': insights_rpc,
                'calllogs_rpc': calllogs_rpc,
                'difference': rpc_difference,
                'percentage_diff': percentage_diff
            })
            
            logger.info(f"Comparison for target '{target_name}':")
            logger.info(f"  - Insights API RPC: ${insights_rpc:.2f}")
            logger.info(f"  - Call Logs RPC: ${calllogs_rpc:.2f}")
            logger.info(f"  - Difference: ${rpc_difference:.2f} ({percentage_diff:.2f}%)")
        
        # Add comparison to results
        results["comparison"] = comparison
        
        # Log summary
        logger.info(f"Compared RPC calculation methods for {len(comparison)} targets")
        return results
    
    def get_raw_calllogs(self, start_date=None, end_date=None):
        """
        Get raw call logs directly without transformation
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            dict: The direct API response containing raw call logs
        """
        logger.info(f"Fetching raw call logs from {start_date} to {end_date}")
        
        # If no dates provided, use today
        if not start_date:
            eastern = pytz.timezone('US/Eastern')
            now_eastern = datetime.now(eastern)
            start_date = now_eastern.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        # Format start and end date with timezone
        start_datetime = f"{start_date}T00:00:00.000-04:00"
        end_datetime = f"{end_date}T23:59:59.999-04:00"
        
        logger.info(f"Using date range: {start_datetime} to {end_datetime}")
        
        # Prepare the request body
        request_body = {
            "startDate": start_datetime,
            "endDate": end_datetime,
            "reportStart": start_datetime,
            "reportEnd": end_datetime,
            "timeField": "connectTime",
            "timeZone": "America/New_York",
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
                "payoutAmount",
                "campaignName",
                "hasConverted",
                "conversionAmount",
                "rpc",  # Try to get RPC directly if available
                "hasRpcCalculation"  # Add this field to check if RPC is calculated
            ],
            "page": 1,
            "pageSize": 5000,
            "sortField": "connectTime",
            "sortDirection": "desc"
        }
        
        try:
            url = f"{self.base_url}/calllogs"
            response = requests.post(url, headers=self.headers, json=request_body)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch raw call logs: {response.status_code} - {response.text}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching raw call logs: {str(e)}")
            return {}
            
    def get_ui_matching_rpc(self, start_date=None, end_date=None):
        """
        Get RPC data directly matching what's shown in the Ringba UI
        by using the proper API endpoint and calculation
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            list: List of targets with their RPC data matching the UI
        """
        logger.info(f"Getting UI-matching RPC data from {start_date} to {end_date}")
        
        # If no dates provided, use today
        if not start_date:
            eastern = pytz.timezone('US/Eastern')
            now_eastern = datetime.now(eastern)
            start_date = now_eastern.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date
        
        # Format start and end date with timezone
        start_datetime = f"{start_date}T00:00:00.000-04:00"
        end_datetime = f"{end_date}T23:59:59.999-04:00"
        
        logger.info(f"Using date range: {start_datetime} to {end_datetime}")
        
        # Get all targets to get additional information including public IDs
        all_targets = self.get_targets()
        target_dict = {t.get('id'): t for t in all_targets if 'id' in t}
        
        # Get mapping from internal IDs to public IDs
        target_public_ids = self.get_target_public_id_mapping()
        logger.info(f"Retrieved mapping for {len(target_public_ids)} target public IDs")
        
        # First, try to get RPC directly from insights API (most likely to match UI)
        request_body = {
            "startDate": start_datetime,
            "endDate": end_datetime,
            "reportStart": start_datetime,
            "reportEnd": end_datetime,
            "timeField": "connectTime",
            "timeZone": "America/New_York",
            "groupBy": ["targetId"],
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
            "sortField": "rpc",
            "sortDirection": "desc",
            "page": 1,
            "pageSize": 1000
        }
        
        try:
            logger.info(f"Insights request body: {json.dumps(request_body, indent=2)}")
            url = f"{self.base_url}/insights"
            response = requests.post(url, headers=self.headers, json=request_body)
            
            logger.info(f"Insights API response status: {response.status_code}")
            
            if response.status_code == 200:
                insights_data = response.json()
                logger.info(f"Insights API raw response structure: {json.dumps(insights_data, indent=2)[:500]}...")
                
                # Extract targets with RPC data
                targets_with_rpc = []
                
                # Check for expected response structure
                if "report" in insights_data and "records" in insights_data["report"]:
                    items = insights_data["report"]["records"]
                    logger.info(f"Found {len(items)} records in report.records")
                    
                    for item in items:
                        target_id = item.get("targetId")
                        
                        # Skip if no target ID
                        if not target_id:
                            continue
                        
                        # Try to get target name and public ID
                        target_name = "Unknown Target"
                        public_id = target_id  # Default to internal ID
                        
                        if target_id in target_dict:
                            target_info = target_dict[target_id]
                            target_name = target_info.get("name", "Unknown Target")
                        
                        # Try to get public ID from mapping
                        if target_id in target_public_ids:
                            public_id = target_public_ids[target_id]
                            
                        # Get values directly from insights API (most likely to match UI)
                        rpc = item.get("rpc", 0)
                        calls = item.get("calls", 0)
                        revenue = item.get("revenue", 0)
                        connected = item.get("connected", 0)
                        enabled = target_id in target_dict and target_dict[target_id].get("enabled", False)
                        
                        targets_with_rpc.append({
                            'id': public_id,  # Use public ID for UI links
                            'internal_id': target_id,  # Keep internal ID for API calls
                            'name': target_name,
                            'rpc': rpc,
                            'calls': calls,
                            'revenue': revenue,
                            'connected': connected,
                            'enabled': enabled,
                            'source': 'insights_api'
                        })
                        
                        logger.info(f"Target '{target_name}' (public ID: {public_id}): UI RPC of ${rpc:.2f} with {calls} calls and ${revenue:.2f} revenue")
                
                # Try alternative response format
                elif "items" in insights_data:
                    items = insights_data["items"]
                    logger.info(f"Found {len(items)} items in items array")
                    
                    for item in items:
                        target_id = item.get("targetId")
                        
                        # Skip if no target ID
                        if not target_id:
                            continue
                        
                        # Try to get target name and public ID
                        target_name = "Unknown Target"
                        public_id = target_id  # Default to internal ID
                        
                        if target_id in target_dict:
                            target_info = target_dict[target_id]
                            target_name = target_info.get("name", "Unknown Target")
                        
                        # Try to get public ID from mapping
                        if target_id in target_public_ids:
                            public_id = target_public_ids[target_id]
                            
                        # Get values directly from insights API
                        rpc = item.get("rpc", 0)
                        calls = item.get("calls", 0)
                        revenue = item.get("revenue", 0)
                        connected = item.get("connected", 0)
                        enabled = target_id in target_dict and target_dict[target_id].get("enabled", False)
                        
                        targets_with_rpc.append({
                            'id': public_id,  # Use public ID for UI links
                            'internal_id': target_id,  # Keep internal ID for API calls
                            'name': target_name,
                            'rpc': rpc,
                            'calls': calls,
                            'revenue': revenue,
                            'connected': connected,
                            'enabled': enabled,
                            'source': 'insights_api'
                        })
                        
                        logger.info(f"Target '{target_name}' (public ID: {public_id}): UI RPC of ${rpc:.2f} with {calls} calls and ${revenue:.2f} revenue")
                
                # Sort by RPC (highest first)
                targets_with_rpc.sort(key=lambda x: x['rpc'], reverse=True)
                
                logger.info(f"Found {len(targets_with_rpc)} targets with UI RPC data")
                
                if targets_with_rpc:
                    return targets_with_rpc
                else:
                    logger.warning("No targets found in insights API response. Trying alternative method.")
            else:
                logger.error(f"Failed to fetch insights data: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Error fetching insights data: {str(e)}")
        
        # Try to get data from call logs endpoint
        logger.info("Trying call logs endpoint for RPC data")
        try:
            call_logs_body = {
                "startDate": start_datetime,
                "endDate": end_datetime,
                "reportStart": start_datetime,
                "reportEnd": end_datetime,
                "timeField": "connectTime",
                "timeZone": "America/New_York",
                "fields": [
                    "targetName",
                    "targetId", 
                    "inboundPhoneNumber",
                    "duration",
                    "connectTime",
                    "payout",
                    "payoutAmount",
                    "hasConverted",
                    "conversionAmount",
                    "rpc",  # Try to get RPC directly if available
                    "hasRpcCalculation"  # Check if RPC calculation is available
                ],
                "page": 1,
                "pageSize": 5000,
                "sortField": "connectTime",
                "sortDirection": "desc"
            }
            
            url = f"{self.base_url}/calllogs"
            response = requests.post(url, headers=self.headers, json=call_logs_body)
            
            logger.info(f"Call logs API response status: {response.status_code}")
            
            if response.status_code == 200:
                logs_data = response.json()
                logger.info(f"Call logs API response structure: {json.dumps(logs_data, indent=2)[:500]}...")
                
                # Check for RPC field in call logs
                if "report" in logs_data and "records" in logs_data["report"]:
                    records = logs_data["report"]["records"]
                    if records and len(records) > 0:
                        first_record = records[0]
                        if "rpc" in first_record:
                            logger.info(f"RPC field found in call logs: {first_record.get('rpc')}")
                            
                            # If RPC is directly available in call logs, use it
                            # Aggregate by target
                            target_metrics = {}
                            
                            for call in records:
                                target_id = call.get("targetId")
                                
                                # Skip calls without target ID
                                if not target_id:
                                    continue
                                
                                # Initialize target data if not already done
                                if target_id not in target_metrics:
                                    # Try to get public ID from mapping
                                    public_id = target_id  # Default to internal ID
                                    if target_id in target_public_ids:
                                        public_id = target_public_ids[target_id]
                                    
                                    target_metrics[target_id] = {
                                        'id': public_id,  # Use public ID for UI links
                                        'internal_id': target_id,  # Keep internal ID for reference
                                        'name': call.get("targetName", "Unknown Target"),
                                        'calls': 0,
                                        'connected': 0,
                                        'revenue': 0,
                                        'rpc_total': 0  # Use to calculate average RPC
                                    }
                                
                                # Get call data
                                target_metrics[target_id]['calls'] += 1
                                
                                # Add RPC from call (if available)
                                call_rpc = call.get("rpc", 0)
                                if call_rpc:
                                    target_metrics[target_id]['rpc_total'] += float(call_rpc)
                                
                                # Add payout to revenue
                                payout = 0
                                if "payoutAmount" in call and call["payoutAmount"] is not None:
                                    try:
                                        payout = float(call["payoutAmount"])
                                    except (ValueError, TypeError):
                                        pass
                                elif "payout" in call and call["payout"] is not None:
                                    try:
                                        if isinstance(call["payout"], (int, float)):
                                            payout = float(call["payout"])
                                        elif isinstance(call["payout"], str):
                                            payout_str = call["payout"].replace('$', '').replace(',', '')
                                            payout = float(payout_str)
                                    except (ValueError, TypeError):
                                        pass
                                
                                target_metrics[target_id]['revenue'] += payout
                            
                            # Calculate RPC for each target
                            targets_with_rpc = []
                            
                            for target_id, metrics in target_metrics.items():
                                calls = metrics['calls']
                                
                                # Use average RPC from calls if available, otherwise calculate from revenue
                                if metrics['rpc_total'] > 0:
                                    rpc = metrics['rpc_total'] / calls
                                    logger.info(f"Using average RPC from call logs for {metrics['name']}: ${rpc:.2f}")
                                else:
                                    revenue = metrics['revenue']
                                    rpc = revenue / calls if calls > 0 else 0
                                    logger.info(f"Calculated RPC for {metrics['name']}: ${rpc:.2f}")
                                
                                metrics['rpc'] = rpc
                                metrics['source'] = 'call_logs_rpc'
                                
                                targets_with_rpc.append(metrics)
                                
                                logger.info(f"Target '{metrics['name']}' has UI RPC of ${rpc:.2f} with {calls} calls and ${metrics['revenue']:.2f} revenue")
                            
                            # Sort by RPC (highest first)
                            targets_with_rpc.sort(key=lambda x: x['rpc'], reverse=True)
                            
                            logger.info(f"Found {len(targets_with_rpc)} targets with RPC data from call logs")
                            return targets_with_rpc
        except Exception as e:
            logger.error(f"Error processing call logs for RPC: {str(e)}")
        
        # If direct methods failed, fall back to dashboard calculation
        logger.info("Falling back to dashboard RPC calculation")
        return self.get_dashboard_rpc(start_date=start_date, end_date=end_date)
            
    def get_all_targets_rpc_today(self):
        """
        Get current day's RPC for all targets from 00:00 to now matching dashboard data
        
        Returns:
            list: List of targets with their today's RPC data matching dashboard view
        """
        logger.info("Getting today's RPC for all targets matching dashboard view")
        
        # Get today's date in eastern timezone
        eastern = pytz.timezone('US/Eastern')
        now_eastern = datetime.now(eastern)
        today = now_eastern.strftime('%Y-%m-%d')
        
        # Use the dashboard RPC method to get today's data
        return self.get_dashboard_rpc(start_date=today, end_date=today)
        
    def get_targets_above_threshold_today(self, threshold):
        """
        Get all targets with today's RPC (00:00 to now) above threshold using direct call logs
        
        Args:
            threshold (float): RPC threshold value
            
        Returns:
            list: List of targets with RPC above threshold
        """
        logger.info(f"Finding targets with today's RPC above ${threshold}")
        
        # Get all targets with RPC data
        all_targets_rpc = self.get_all_targets_rpc_today()
        
        # Filter for targets above threshold
        targets_above_threshold = [t for t in all_targets_rpc if t['rpc'] >= threshold]
        
        logger.info(f"Found {len(targets_above_threshold)} targets with RPC above ${threshold}")
        return targets_above_threshold
    
    def get_targets_with_details(self):
        """
        Get all targets with complete details including public IDs
        
        Returns:
            list: List of targets with complete details
        """
        logger.info("Fetching all targets with complete details")
        all_targets = self.get_targets()
        if not all_targets:
            logger.error("Failed to fetch targets list")
            return []
        
        # Create list to store targets with details
        targets_with_details = []
        
        # For each target, fetch its complete details
        for target in all_targets:
            target_id = target.get('id')
            if not target_id:
                continue
                
            logger.info(f"Fetching details for target: {target.get('name')} (ID: {target_id})")
            target_details = self.get_target_details(target_id)
            
            # Add to list if details were successfully retrieved
            if target_details:
                targets_with_details.append(target_details)
                
                # Log public ID if available
                if 'publicId' in target_details:
                    logger.info(f"Target {target_details.get('name')} has public ID: {target_details.get('publicId')}")
        
        logger.info(f"Successfully fetched details for {len(targets_with_details)} targets")
        return targets_with_details
    
    def get_target_public_id_mapping(self):
        """
        Get mapping from internal target IDs to public IDs
        
        Returns:
            dict: Dictionary mapping internal IDs to public IDs
        """
        logger.info("Building target ID to public ID mapping")
        
        # Create URL to get target public IDs directly
        url = f"{self.base_url}/targets/map"
        
        try:
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                mapping_data = response.json()
                
                # Build the mapping
                id_mapping = {}
                for item in mapping_data:
                    internal_id = item.get('id')
                    public_id = item.get('publicId')
                    name = item.get('name', 'Unknown')
                    
                    if internal_id and public_id:
                        id_mapping[internal_id] = public_id
                        logger.info(f"Target {name}: internal ID={internal_id}, public ID={public_id}")
                
                logger.info(f"Built mapping for {len(id_mapping)} targets")
                return id_mapping
            else:
                logger.error(f"Failed to get target ID mapping: {response.status_code} - {response.text}")
                
                # Fallback to individual target details
                logger.info("Falling back to individual target details for mapping")
                return self._build_mapping_from_details()
        except Exception as e:
            logger.error(f"Error getting target ID mapping: {str(e)}")
            
            # Fallback to individual target details
            logger.info("Falling back to individual target details for mapping")
            return self._build_mapping_from_details()
    
    def _build_mapping_from_details(self):
        """
        Build target ID to public ID mapping from individual target details
        
        Returns:
            dict: Dictionary mapping internal IDs to public IDs
        """
        id_mapping = {}
        
        # Get all targets
        all_targets = self.get_targets()
        
        # For each target, fetch its details to get the public ID
        for target in all_targets:
            target_id = target.get('id')
            if not target_id:
                continue
                
            target_details = self.get_target_details(target_id)
            
            if target_details and 'publicId' in target_details:
                public_id = target_details.get('publicId')
                name = target_details.get('name', 'Unknown')
                
                id_mapping[target_id] = public_id
                logger.info(f"Target {name}: internal ID={target_id}, public ID={public_id}")
        
        logger.info(f"Built mapping for {len(id_mapping)} targets from details")
        return id_mapping 