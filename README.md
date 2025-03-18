# Ringba RPC Monitor with Slack Integration

This system monitors Ringba targets based on their Revenue Per Call (RPC) metrics and sends alerts to Slack. It uses Ringba's direct API endpoints to get real-time data including tag information.

## Features

- **Morning Check (10 AM EST)**: Identifies all enabled targets with RPC above $10
- **Afternoon Check (3 PM EST)**: Checks if any morning targets fell below the $10 RPC threshold
- **Tag Information**: Displays top tags associated with targets in Slack notifications
- **Real-time Data**: Gets current data during each check
- **Slack Integration**: Sends formatted alerts with detailed metrics and tag information

## Configuration

Create a `.env` file with the following variables:

```
RINGBA_API_TOKEN=your_api_token
RINGBA_ACCOUNT_ID=your_account_id
SLACK_WEBHOOK_URL=your_slack_webhook_url
RPC_THRESHOLD=10.0
RINGBA_AUTH_FORMAT=Token
MORNING_CHECK_TIME=10:00
AFTERNOON_CHECK_TIME=15:00
```

## Running the Monitor

### As a Scheduler

To run the monitor as a scheduler that performs checks at 10 AM and 3 PM:

```
python src/direct_rpc_monitor.py
```

### Manual Checks

To manually run the morning check:

```
python src/direct_rpc_monitor.py morning
```

To manually run the afternoon check:

```
python src/direct_rpc_monitor.py afternoon
```

To run both checks for testing:

```
python src/direct_rpc_monitor.py test
```

## Files

- `src/ringba_direct_api.py`: API client that communicates with Ringba
- `src/direct_rpc_monitor.py`: Main script for RPC monitoring and Slack notifications
- `src/test_direct_api.py`: Test script for the API client
- `src/test_direct_api_with_tags.py`: Test script for API client with tag information

## Slack Notifications

The system sends Slack notifications with the following information:

- Target name
- Current RPC value
- Call count
- Revenue
- Top 3 tags associated with the target (if available)
- Link to view the target in Ringba 