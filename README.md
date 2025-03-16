# Ringba RPC Monitoring System

A monitoring system that checks Ringba targets' RPC (Revenue Per Call) values and sends alerts to Slack when specific thresholds are met.

## Features

- **Morning Check (10:00 AM EST)**: Identifies all targets with RPC above $10
- **Afternoon Check (3:00 PM EST)**: Checks if any morning targets fell below $10 RPC
- **Slack Notifications**: Sends detailed alerts with target information and direct links to Ringba
- **Manual Testing**: Run checks on demand for testing purposes

## Setup

1. **Environment Variables**:
   Create a `.env` file with the following variables:
   ```
   RINGBA_API_TOKEN=your_ringba_api_token
   RINGBA_ACCOUNT_ID=your_ringba_account_id
   SLACK_WEBHOOK_URL=your_slack_webhook_url
   RPC_THRESHOLD=10.0
   ```

2. **Dependencies**:
   Install required Python packages:
   ```
   pip install requests python-dotenv schedule pytz tabulate
   ```

3. **Slack Webhook**:
   Create a Slack App and obtain a webhook URL:
   - Go to [api.slack.com/apps](https://api.slack.com/apps)
   - Create a new app
   - Enable Incoming Webhooks
   - Create a webhook for your workspace
   - Copy the webhook URL to your `.env` file

## Usage

### Run as a Scheduled Service

To run the script as a scheduled service that performs checks at 10am and 3pm EST daily:

```
python src/slack_rpc_monitor.py
```

### Manual Checks

To run the checks manually for testing:

- **Morning Check Only**:
  ```
  python src/slack_rpc_monitor.py morning
  ```

- **Afternoon Check Only**:
  ```
  python src/slack_rpc_monitor.py afternoon
  ```

- **Both Checks (Test Mode)**:
  ```
  python src/slack_rpc_monitor.py test
  ```

## Notifications

Notifications will be sent to the Slack channel associated with your webhook URL. The notifications include:

- List of targets above the $10 RPC threshold (morning check)
- List of targets that fell below the threshold after being above it in the morning (afternoon check)
- Direct links to view these targets in the Ringba dashboard

## Logs

The script logs all activities to:
- `slack_rpc_monitor.log` - General monitoring logs
- Console output - Real-time status

## Customize

You can customize the RPC threshold by changing the `RPC_THRESHOLD` value in your `.env` file. 