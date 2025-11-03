# Attom Real Estate Data ETL Pipeline

## Project Overview

This is a production-ready Python ETL (Extract-Transform-Load) pipeline designed to process over 142 GB of Attom nationwide real estate data. The application automatically downloads data from FTP sources, filters it by state codes, and uploads the processed files to DigitalOcean Spaces for consumption by .NET applications.

## Current State

**Status**: Fully functional and ready for deployment

The application includes:
- ✅ Complete ETL pipeline with Extract, Transform, and Load phases
- ✅ Memory-efficient streaming for 2GB+ files
- ✅ FTP/HTTP download module
- ✅ State-based filtering system
- ✅ DigitalOcean Spaces integration
- ✅ Comprehensive logging system
- ✅ Daily scheduling with Python `schedule` library
- ✅ Full documentation and deployment guides

## Architecture

### High-Priority Datasets (Currently Configured)
1. **Assessor** - Property assessment data
2. **AVM** - Automated Valuation Model data
3. **Parcel** - Parcel data (via Jetstream)
4. **PROPERTYTOBOUNDARYMATCH_PARCEL** - Property boundary matching

### Lower-Priority Datasets (Disabled by Default)
5. **Recorder** - Recording data
6. **PreForeclosure** - Pre-foreclosure data

### Project Structure
```
├── src/
│   ├── extractors/          # FTP/HTTP download modules
│   ├── transformers/        # State filtering and data processing
│   ├── loaders/             # DigitalOcean Spaces uploader
│   ├── utils/               # Logger and configuration loader
│   ├── etl_pipeline.py      # Main ETL orchestrator
│   └── scheduler.py         # Daily scheduling system
├── config/
│   ├── config.json          # Main configuration (states, datasets, URLs)
│   └── .env.example         # Credentials template
├── data/                    # Working directories (downloads, extracted, filtered)
├── logs/                    # ETL process logs
├── main.py                  # Application entry point
├── validate_setup.py        # Setup validation script
├── README.md                # User documentation
└── DEPLOYMENT.md            # Production deployment guide
```

## Key Features

### Memory Efficiency
- Processes 2GB+ files line-by-line without loading into memory
- Streams data to avoid memory exhaustion on large datasets

### State Filtering
- Configurable state list (e.g., CA, TX, FL)
- Filters by `SitusStateCode` column
- Preserves original file format and headers

### DigitalOcean Spaces Integration
- Date-based folder structure: `2025-10-29/Assessor_filtered_CA.zip`
- Automatic upload after processing
- S3-compatible boto3 integration

### Logging
- Comprehensive logs tracking:
  - Download progress
  - Record counts (processed vs. kept)
  - Processing duration
  - Errors and warnings
- Logs uploaded to Spaces for remote monitoring

### Scheduling
- Daily execution at configurable time (default: 2:00 AM)
- Compatible with systemd, cron, or pm2
- Manual execution also supported

## Quick Start

### 1. Configure Credentials

Create `config/.env` file:
```bash
SPACES_ACCESS_KEY=your_digitalocean_spaces_key
SPACES_SECRET_KEY=your_digitalocean_spaces_secret
FTP_USERNAME=your_attom_ftp_username
FTP_PASSWORD=your_attom_ftp_password
```

### 2. Update Configuration

Edit `config/config.json`:
- Update `states` array with target state codes
- Configure `datasets[].urls` with actual Attom FTP URLs
- Set `spaces.bucket_name` to your DigitalOcean Spaces bucket
- Adjust `schedule.daily_time` as needed

### 3. Run the Application

```bash
# Validate setup
python validate_setup.py

# Run ETL pipeline once
python main.py run

# Run with daily scheduler
python main.py schedule

# Run immediately and then on schedule
python main.py schedule --now
```

## Configuration

### State Codes
Currently configured states: **CA, TX, FL**

To add more states, edit `config/config.json`:
```json
"states": ["CA", "TX", "FL", "NY", "IL", "OH"]
```

### Dataset Configuration
Each dataset in `config.json` has:
- `name`: Dataset identifier
- `enabled`: true/false to enable/disable
- `urls`: Array of download URLs (FTP or HTTP)
- `description`: Dataset description

### Schedule
Daily execution time is configured in `config.json`:
```json
"schedule": {
  "daily_time": "02:00"
}
```

## Deployment

See `DEPLOYMENT.md` for complete production deployment instructions including:
- DigitalOcean Droplet setup
- Systemd service configuration
- Monitoring and maintenance
- Troubleshooting guide

## Recent Changes

**2025-10-29**: Initial implementation
- Complete ETL pipeline implementation
- All high-priority datasets configured
- Documentation and deployment guides created
- Setup validation script added

## User Preferences

None specified yet.

## Technical Stack

- **Language**: Python 3.11
- **Libraries**:
  - boto3 (DigitalOcean Spaces/S3)
  - requests (HTTP downloads)
  - schedule (job scheduling)
  - python-dotenv (environment variables)
- **Hosting**: DigitalOcean Droplet (production)
- **Storage**: DigitalOcean Spaces (S3-compatible)
- **Scheduling**: Python schedule + systemd/cron/pm2

## Next Steps

1. **Configure credentials**: Add actual credentials to `config/.env`
2. **Update FTP URLs**: Replace example URLs in `config.json` with actual Attom FTP endpoints
3. **Test locally**: Run `python main.py run` to test the pipeline
4. **Deploy to droplet**: Follow `DEPLOYMENT.md` for production deployment
5. **Monitor logs**: Check `logs/` directory and DigitalOcean Spaces for outputs

## Future Enhancements

- Email notifications for job failures
- Web dashboard for monitoring job status
- Parallel processing for multiple datasets
- Incremental updates (only download changed files)
- Data validation and integrity checks
- Retry logic with exponential backoff

## Notes

- The application is designed to run on a DigitalOcean Droplet but can run on any Linux server
- All temporary files are cleaned up after processing to save disk space
- Logs are preserved locally and uploaded to Spaces
- The .NET Assessor application can directly consume the filtered ZIPs from Spaces

What changed

config.json (Assessor dataset): set "post_process_downloaded" to "move" so processed downloaded ZIPs are moved into processed after extraction.
Updated the internal todo list to record this change as completed.
Behavior now

If post_process_downloaded == "move": downloaded ZIP will be moved from downloads → processed.
Otherwise (not "move"): pipeline will delete the downloaded ZIP after processing (default delete).
Note: the pipeline also supports "copy" and "delete" — you've chosen "move" which will remove the original after moving.
Would you like me to:


filter_ftp_action set  move/copy