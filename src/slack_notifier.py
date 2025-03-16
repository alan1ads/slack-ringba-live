#!/usr/bin/env python3
import requests
import logging
import json

logger = logging.getLogger('ringba_monitor.slack_notifier')

class SlackNotifier:
    """Notifier for sending messages to a Slack channel via webhook"""
    
    def __init__(self, webhook_url):
        """Initialize with Slack webhook URL"""
        self.webhook_url = webhook_url
    
    def send_notification(self, message):
        """
        Send a notification to Slack
        
        Args:
            message (str): Message to send to Slack
        """
        logger.info(f"Sending notification to Slack")
        
        payload = {
            "text": message,
            "mrkdwn": True
        }
        
        response = requests.post(
            self.webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )
        
        response.raise_for_status()
        
        if response.status_code != 200:
            logger.error(f"Failed to send Slack notification. Status code: {response.status_code}")
            raise Exception(f"Failed to send Slack notification: {response.text}")
        
        logger.info("Successfully sent notification to Slack") 