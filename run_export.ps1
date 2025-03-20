# PowerShell script to run the export with credentials from .env file

# Get command line parameters
param (
    [string]$StartDate,
    [string]$EndDate,
    [switch]$Manual,
    [switch]$Help
)

# Show help if requested
if ($Help) {
    Write-Host "Usage: .\run_export.ps1 [-StartDate <date>] [-EndDate <date>] [-Manual] [-Help]"
    Write-Host ""
    Write-Host "Parameters:"
    Write-Host "  -StartDate  Start date for export (YYYY-MM-DD), defaults to today if not specified"
    Write-Host "  -EndDate    End date for export (YYYY-MM-DD), defaults to StartDate if not specified"
    Write-Host "  -Manual     Enable manual mode (pauses for user interaction)"
    Write-Host "  -Help       Show this help message"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\run_export.ps1                             # Export today's data (default)"
    Write-Host "  .\run_export.ps1 -StartDate 2023-03-19       # Export specific date"
    Write-Host "  .\run_export.ps1 -Manual                     # Export today's data in manual mode"
    exit 0
}

# Function to get value from .env file
function Get-EnvValue {
    param (
        [string]$key
    )
    
    $content = Get-Content .env
    foreach ($line in $content) {
        if ($line -match "^$key=(.*)$") {
            return $matches[1]
        }
    }
    return $null
}

# Get credentials from .env
$username = Get-EnvValue "RINGBA_USERNAME"
$password = Get-EnvValue "RINGBA_PASSWORD"

# Check if credentials were found
if (-not $username -or -not $password) {
    Write-Host "Error: Could not find RINGBA_USERNAME or RINGBA_PASSWORD in .env file"
    exit 1
}

# If we don't have dates from command line, use positional args
if (-not $StartDate -and $args.Count -ge 1) {
    $StartDate = $args[0]
}

if (-not $EndDate -and $args.Count -ge 2) {
    $EndDate = $args[1]
}

# Set EndDate to StartDate if only StartDate provided
if (-not $EndDate -and $StartDate) {
    $EndDate = $StartDate
}

# Get today's date for display purposes
$todayDate = Get-Date -Format "yyyy-MM-dd"

# Display info with colors
Write-Host "CSV Export Configuration:" -ForegroundColor Cyan
Write-Host "------------------------" -ForegroundColor Cyan
Write-Host "Using username: " -NoNewline; Write-Host $username -ForegroundColor Green
Write-Host "Using password: " -NoNewline; Write-Host "[HIDDEN]" -ForegroundColor Yellow
if ($StartDate -and $EndDate) {
    Write-Host "Date range: " -NoNewline; Write-Host "$StartDate to $EndDate" -ForegroundColor Green
} else {
    Write-Host "Date range: " -NoNewline; Write-Host "Today ($todayDate)" -ForegroundColor Green
}
if ($Manual) {
    Write-Host "Mode: " -NoNewline; Write-Host "MANUAL (will pause for user interaction)" -ForegroundColor Magenta
} else {
    Write-Host "Mode: " -NoNewline; Write-Host "AUTOMATIC" -ForegroundColor Green
}
Write-Host ""

# Create screenshots directory if it doesn't exist
if (-not (Test-Path "screenshots")) {
    New-Item -ItemType Directory -Path "screenshots"
}

# Function to run the export with retries
function Run-ExportWithRetry {
    param (
        [string]$username,
        [string]$password,
        [string]$startDate,
        [string]$endDate,
        [bool]$manualMode = $false,
        [int]$maxRetries = 3,
        [int]$retryDelay = 10
    )
    
    for ($i = 1; $i -le $maxRetries; $i++) {
        Write-Host "Attempt $i of $maxRetries..." -ForegroundColor Cyan
        
        # Build command array based on parameters
        $cmdArgs = @($username, $password)
        
        if ($startDate -and $endDate) {
            $cmdArgs += $startDate
            $cmdArgs += $endDate
        }
        
        # Run the Python script
        if ($manualMode) {
            Write-Host "Running in MANUAL mode - browser will stay open for interaction" -ForegroundColor Magenta
        }
        
        $process = Start-Process -FilePath "python" -ArgumentList (@("src/simple_export.py") + $cmdArgs) -NoNewWindow -PassThru -Wait
        $exitCode = $process.ExitCode
        
        Write-Host "Python script completed with exit code: $exitCode"
        
        # Check if screenshots directory contains any files
        $newScreenshots = (Get-ChildItem -Path "screenshots" | Where-Object { $_.LastWriteTime -gt (Get-Date).AddMinutes(-10) })
        if ($newScreenshots.Count -gt 0) {
            Write-Host "Screenshots were taken during execution:" -ForegroundColor Yellow
            foreach ($screenshot in $newScreenshots) {
                Write-Host "  - $($screenshot.Name)" -ForegroundColor Gray
            }
            Write-Host "Check 'screenshots' directory for details"
        }
        
        # Check if any CSV files were created
        $csvFiles = Get-ChildItem -Path "." -Filter "*.csv" | Where-Object { $_.LastWriteTime -gt (Get-Date).AddMinutes(-15) }
        if ($csvFiles.Count -gt 0) {
            Write-Host "Success! Found CSV files:" -ForegroundColor Green
            foreach ($file in $csvFiles) {
                Write-Host "  - $($file.Name) ($([math]::Round($file.Length / 1KB, 2)) KB)" -ForegroundColor Green
            }
            return $true
        }
        
        if ($i -lt $maxRetries) {
            Write-Host "No CSV files found. Retrying in $retryDelay seconds..." -ForegroundColor Yellow
            Start-Sleep -Seconds $retryDelay
        }
    }
    
    Write-Host "Failed after $maxRetries attempts. No CSV files were created." -ForegroundColor Red
    return $false
}

# Run the export with retries
$success = Run-ExportWithRetry -username $username -password $password -startDate $StartDate -endDate $EndDate -manualMode $Manual
if ($success) {
    Write-Host "Export completed successfully!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "Export failed after multiple attempts." -ForegroundColor Red
    Write-Host "Try running with -Manual flag for manual intervention." -ForegroundColor Yellow
    exit 1
} 