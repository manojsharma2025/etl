# Attom Real Estate Data ETL Pipeline

An automated Python ETL pipeline for processing 142+ GB of Attom nationwide real estate data, filtering by state codes, and uploading to DigitalOcean Spaces for consumption by .NET applications.

## Overview

This application automatically:
- Downloads large ZIP files (2GB+) from Attom's FTP/HTTP data sources
- Extracts and streams TXT files line-by-line for memory efficiency
- Filters records by state code (SitusStateCode column)
- Compresses filtered data back into ZIP archives
- Uploads to DigitalOcean Spaces with date-based folder structure
- Logs all operations comprehensively
- Runs on a daily schedule

## Features

- **Memory-Efficient Processing**: Streams 2GB+ files line-by-line without loading into memory
- **Multi-State Filtering**: Filter data for multiple states (CA, TX, FL, etc.)
- **Automated Scheduling**: Daily execution at configurable time (default: 2:00 AM)
- **Comprehensive Logging**: Tracks downloads, record counts, errors, and processing time
- **DigitalOcean Spaces Integration**: S3-compatible cloud storage with date-based organization
- **Configurable Datasets**: Support for Assessor, AVM, Parcel, PROPERTYTOBOUNDARYMATCH_PARCEL, Recorder, and PreForeclosure
- **Error Handling**: Robust error handling with detailed logging

## Project Structure

```
.
├── src/
│   ├── extractors/
│   │   └── ftp_downloader.py       # FTP/HTTP download module
│   ├── transformers/
│   │   └── state_filter.py         # Streaming filter for state-based filtering
│   ├── loaders/
│   │   └── spaces_uploader.py      # DigitalOcean Spaces uploader
│   ├── utils/
│   │   ├── logger.py               # Comprehensive logging system
│   │   └── config_loader.py        # Configuration management
│   ├── etl_pipeline.py             # Main ETL orchestrator
│   └── scheduler.py                # Daily scheduling system
├── config/
│   ├── config.json                 # Main configuration file
│   └── .env.example                # Environment variables template
├── data/
│   ├── downloads/                  # Downloaded ZIP files (temporary)
│   ├── extracted/                  # Extracted TXT files (temporary)
│   ├── filtered/                   # Filtered files (temporary)
│   └── temp/                       # Temporary working directory
├── logs/                           # ETL process logs
├── main.py                         # Application entry point
└── README.md                       # This file
```

## Installation & Setup

### 1. Prerequisites

- Python 3.11+
- DigitalOcean Droplet (or any Linux server)
- DigitalOcean Spaces bucket
- Access to Attom data sources (FTP credentials)

### 2. Install Dependencies

All dependencies are already installed via `uv`:
- boto3 (DigitalOcean Spaces/S3)
- requests (HTTP downloads)
- schedule (Job scheduling)
- python-dotenv (Environment configuration)

### 3. Configure the Application

#### a. Copy and edit the environment file:

```bash
cp config/.env.example config/.env
```

Edit `config/.env` and add your credentials:
```
SPACES_ACCESS_KEY=your_digitalocean_spaces_access_key
SPACES_SECRET_KEY=your_digitalocean_spaces_secret_key
FTP_USERNAME=your_attom_ftp_username
FTP_PASSWORD=your_attom_ftp_password
```

#### b. Edit configuration file:

Edit `config/config.json` to customize:
- **states**: List of state codes to filter (e.g., ["CA", "TX", "FL"])
- **datasets**: Configure dataset URLs and enable/disable specific datasets
- **spaces.bucket_name**: Your DigitalOcean Spaces bucket name
- **schedule.daily_time**: Time to run daily (format: "HH:MM", 24-hour)

Example `config.json`:
```json
{
  "states": ["CA", "TX", "FL"],
  "datasets": [
    {
      "name": "Assessor",
      "enabled": true,
      "urls": ["ftp://ftp.attom.com/path/to/Assessor_National.zip"]
    }
  ],
  "spaces": {
    "bucket_name": "genieattomdata",
    "region": "sfo3"
  },
  "schedule": {
    "daily_time": "02:00"
  }
}
```

## Usage

### Run ETL Pipeline Once (Manual)

```bash
python src/etl_pipeline.py
```

### Run with Scheduler (Automated Daily)

```bash
python src/scheduler.py
```

Run immediately and then on schedule:
```bash
python src/scheduler.py --now
```

### Using the Main Entry Point

```bash
# Run once
python main.py run

# Run with scheduler
python main.py schedule

# Run immediately and then on schedule
python main.py schedule --now
```

## Deployment to Production

### Option 1: Using systemd (Recommended)

Create a systemd service file `/etc/systemd/system/attom-etl.service`:

```ini
[Unit]
Description=Attom ETL Pipeline Scheduler
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/attom-etl
ExecStart=/usr/bin/python3 /path/to/attom-etl/src/scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable attom-etl
sudo systemctl start attom-etl
sudo systemctl status attom-etl
```

### Option 2: Using Cron

Add to crontab:
```bash
0 2 * * * cd /path/to/attom-etl && python3 src/etl_pipeline.py >> logs/cron.log 2>&1
```

### Option 3: Using PM2 (Node.js process manager)

```bash
pm2 start src/scheduler.py --name attom-etl --interpreter python3
pm2 save
pm2 startup
```

## Output Structure

Filtered files are uploaded to DigitalOcean Spaces with this structure:

```
spaces://genieattomdata.sfo3.digitaloceanspaces.com/
├── 2025-10-29/
│   ├── Assessor_National_filtered_CA_20251029.zip
│   ├── Assessor_National_filtered_TX_20251029.zip
│   ├── Assessor_National_filtered_FL_20251029.zip
│   ├── AVM_National_filtered_CA_20251029.zip
│   └── ...
├── 2025-10-30/
│   └── ...
└── logs/
    └── 2025-10-29/
        └── etl_20251029_020000.log
```

## Monitoring

### View Logs

```bash
# View latest log
tail -f logs/etl_*.log

# View all logs
ls -lh logs/
```

### Check systemd Service

```bash
sudo systemctl status attom-etl
sudo journalctl -u attom-etl -f
```

## Configuration Reference

### Dataset Priority Levels

**High Priority** (enabled by default):
- Assessor
- AVM
- Parcel (via Jetstream)
- PROPERTYTOBOUNDARYMATCH_PARCEL

**Lower Priority** (disabled by default):
- Recorder
- PreForeclosure

Enable/disable datasets by setting `"enabled": true/false` in `config.json`.

### State Codes

Add any US state codes to the `states` array:
```json
"states": ["CA", "TX", "FL", "NY", "IL", "OH", "PA"]
```

### Schedule Configuration

The scheduler uses 24-hour format:
```json
"schedule": {
  "daily_time": "02:00"  // 2:00 AM daily
}
```

## Troubleshooting

### Issue: FTP download fails

- Verify FTP credentials in `.env`
- Check firewall allows FTP connections
- Test FTP access manually with `ftp` or FileZilla

### Issue: Out of memory errors

- The pipeline streams files line-by-line, so this should not occur
- Verify sufficient disk space in `data/` directories
- Check system resources with `df -h` and `free -m`

### Issue: Upload to Spaces fails

- Verify Spaces credentials in `.env`
- Check bucket name matches in `config.json`
- Verify network connectivity to DigitalOcean

### Issue: Scheduler not running

- Check systemd service status: `sudo systemctl status attom-etl`
- View logs: `sudo journalctl -u attom-etl`
- Verify Python path in systemd service file

## Future Enhancements

- Email notifications for job failures
- Web dashboard for monitoring
- Parallel processing of multiple datasets
- Incremental updates (only download changed files)
- Data validation and integrity checks
- Retry logic with exponential backoff

## Support

For issues or questions, check the logs in `logs/` directory for detailed error messages and processing information.

## License

Proprietary - Internal use only
