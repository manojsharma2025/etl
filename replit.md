# Attom Real Estate Data ETL Pipeline

## Overview

This is an automated Python ETL (Extract, Transform, Load) pipeline for processing large-scale Attom nationwide real estate data. The application downloads data from FTP/HTTP sources, filters by state codes, and uploads to DigitalOcean Spaces for consumption by .NET applications.

## Project Type

Backend data processing application with scheduled execution.

## Key Features

- Memory-efficient streaming processing of 2GB+ files
- Multi-state filtering (CA, TX, FL, etc.)
- Automated daily scheduling (default: 2:00 AM UTC)
- DigitalOcean Spaces integration for cloud storage
- Comprehensive logging system
- Support for multiple dataset types (Assessor, AVM, Parcel, etc.)

## Project Structure

```
.
├── src/
│   ├── extractors/       # FTP/HTTP download modules
│   ├── transformers/     # State-based filtering
│   ├── loaders/         # DigitalOcean Spaces uploader
│   ├── utils/           # Logging and configuration
│   ├── etl_pipeline.py  # Main ETL orchestrator
│   └── scheduler.py     # Daily scheduling system
├── config/
│   ├── config.json      # Main configuration
│   └── .env.example     # Environment variables template
├── data/                # Temporary processing directories
├── logs/                # ETL execution logs
└── main.py             # Application entry point
```

## Configuration

### Required Secrets

The application requires the following environment variables (configured via Replit Secrets):

- `SPACES_ACCESS_KEY`: DigitalOcean Spaces access key
- `SPACES_SECRET_KEY`: DigitalOcean Spaces secret key
- `FTP_USERNAME`: Attom FTP username
- `FTP_PASSWORD`: Attom FTP password

### Configuration File

Edit `config/config.json` to customize:
- **states**: List of state codes to filter (e.g., ["CA", "TX", "FL"])
- **datasets**: Configure dataset URLs and enable/disable specific datasets
- **spaces.bucket_name**: Your DigitalOcean Spaces bucket name
- **schedule.daily_time**: Time to run daily (format: "HH:MM", 24-hour UTC)

## Usage

### Run ETL Once (Manual)
```bash
python main.py run
```

### Run with Scheduler (Automated)
```bash
python main.py schedule
```

### Run Immediately Then Schedule
```bash
python main.py schedule --now
```

## Current Setup

- Python 3.11
- Dependencies: boto3, requests, schedule, python-dotenv
- Workflow configured to run scheduler with immediate execution
- Console-based output for monitoring

## Monitoring

View logs in the `logs/` directory or check the console output in the Replit interface.

## Recent Changes

- 2025-10-30: Enhanced FTP folder browsing and credential management
  - Updated config_loader.py to ONLY read credentials from .env file (no fallback to config.json)
  - Enhanced ftp_downloader.py with folder browsing capability and state-specific file filtering
  - Updated config.json to use ftp_folder for datasets (Assessor, AVM, etc.)
  - FTP connection successfully tested with data.attomdata.com
  - State filtering supports patterns like "_CA_", "_TX_", "_FL_" in filenames

- 2025-10-30: Initial Replit environment setup completed
  - Installed Python 3.11 and all dependencies
  - Created required directories (logs, data subdirectories)
  - Configured workflow for scheduler execution
  - Fixed read-only filesystem issue by changing downloads path from /Outgoing to data/downloads
  - Setup validation completed successfully

## User Preferences

None configured yet.

## Notes

- This is a data processing pipeline, not a web application
- Credentials must be configured via Replit Secrets before first run
- The application processes large files (2GB+) using memory-efficient streaming
- All temporary files are automatically cleaned up after processing
