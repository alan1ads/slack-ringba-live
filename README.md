# Ringba CSV Export Service

This service automates the process of logging into Ringba, exporting CSV files from the Call Logs reporting section, and sending alerts to Slack when targets have an RPC below a specified threshold.

## Deployment Instructions for Render.com

### Important: Use Background Worker Instead of Web Service

The Chrome browser automation requires more resources than are typically available in a Web Service environment. For better reliability, deploy this as a **Background Worker** in Render.com.

### Steps to Deploy:

1. In your Render.com dashboard, click **New** and select **Background Worker**

2. Connect your GitHub repository

3. Configure the following settings:
   - **Name**: ringba-export-service
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python src/simple_export.py`

4. Add the following environment variables:
   - `RINGBA_USERNAME`: Your Ringba username
   - `RINGBA_PASSWORD`: Your Ringba password
   - `SLACK_WEBHOOK_URL`: Your Slack webhook URL
   - `RPC_THRESHOLD`: The RPC threshold for alerts (default: 12.0)
   - `MORNING_CHECK_TIME`: When to run the morning check (default: 11:00)
   - `MIDDAY_CHECK_TIME`: When to run the midday check (default: 14:00)
   - `AFTERNOON_CHECK_TIME`: When to run the afternoon check (default: 16:30)
   - `SHM_SIZE`: Shared memory size (set to: 2g)
   - `CHROME_OPTIONS`: Chrome configuration (set to: --headless=new --no-sandbox --disable-dev-shm-usage --disable-gpu --single-process)
   
5. Set the instance type to at least 512 MB RAM

6. For scheduling, use Render.com's cron job scheduling feature by adding the following cron expressions:
   ```
   0 11 * * 1-5   # Run at 11:00 AM ET Monday-Friday
   0 14 * * 1-5   # Run at 2:00 PM ET Monday-Friday
   30 16 * * 1-5  # Run at 4:30 PM ET Monday-Friday
   ```

### Troubleshooting

- If Chrome continues to crash, try increasing the memory allocation (1GB or higher)
- Check logs for error messages related to browser automation
- Ensure your Ringba credentials are correct and have access to the Call Logs section

## Local Development

To run the service locally:

1. Install dependencies: `pip install -r requirements.txt`
2. Create a `.env` file with the required environment variables
3. Run with: `python src/simple_export.py`

## Notes

- The service is designed to handle unstable container environments
- It includes multiple fallback methods for downloading CSV files
- Screenshots are saved for debugging purposes 