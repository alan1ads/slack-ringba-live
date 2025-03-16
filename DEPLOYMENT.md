# Ringba RPC Monitor Deployment Guide

Follow these steps to deploy the Ringba RPC Monitor to Render.com.

## Prerequisites

1. GitHub account
2. Render.com account
3. Git installed on your local machine

## Step 1: Create a GitHub repository

1. Go to [GitHub](https://github.com) and log in
2. Click on the "+" icon in the top-right corner and select "New repository"
3. Name your repository (e.g., "ringba-rpc-monitor")
4. Set it to private or public (private recommended for security)
5. Click "Create repository"
6. Follow the instructions on GitHub to push your code to the repository:

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/ringba-rpc-monitor.git
git push -u origin main
```

## Step 2: Deploy to Render.com

1. Go to [Render.com](https://render.com) and log in
2. Click on "New" and select "Background Worker"
3. Connect your GitHub account if not already connected
4. Select the repository you created in Step 1
5. Fill in the following details:
   - **Name**: Ringba RPC Monitor
   - **Environment**: Python
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python src/slack_rpc_monitor.py`
   
6. Under "Advanced" settings, add the following environment variables:
   - `RINGBA_API_TOKEN` = [Your Ringba API Token]
   - `RINGBA_ACCOUNT_ID` = [Your Ringba Account ID]
   - `SLACK_WEBHOOK_URL` = [Your Slack Webhook URL]
   - `RPC_THRESHOLD` = 10.0
   
7. Click "Create Background Worker"

## Step 3: Verify Deployment

1. Monitor the build logs to ensure that the deployment is successful
2. Check your Slack channel at 10am and 3pm EST to verify that notifications are being sent
3. You can also check the logs on Render.com to see if the script is running properly

## Troubleshooting

- If you're not receiving notifications, check the logs on Render.com
- Ensure your Slack webhook URL is correct
- Verify your Ringba API token is valid and has not expired

## Updating Your Deployment

To update your deployed application after making changes to your code:

1. Push the changes to your GitHub repository:
```bash
git add .
git commit -m "Description of changes"
git push origin main
```

2. Render will automatically detect the changes and redeploy your application 