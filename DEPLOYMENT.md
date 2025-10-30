# Deployment Guide - Attom ETL Pipeline

This guide walks through deploying the Attom ETL Pipeline to a DigitalOcean Droplet in production.

## Prerequisites

1. **DigitalOcean Droplet** running Ubuntu 20.04 or later
2. **DigitalOcean Spaces** bucket created
3. **Attom FTP credentials** (username and password)
4. **SSH access** to your droplet

## Step 1: Prepare the Droplet

SSH into your droplet:
```bash
ssh root@your_droplet_ip
```

Update system packages:
```bash
apt update && apt upgrade -y
```

Install Python 3.11 and required tools:
```bash
apt install -y python3.11 python3.11-venv python3-pip git
```

## Step 2: Clone or Upload the Application

### Option A: Via Git (if using version control)
```bash
cd /opt
git clone https://github.com/yourusername/attom-etl.git
cd attom-etl
```

### Option B: Via SCP (upload from local machine)
```bash
# From your local machine
scp -r /path/to/attom-etl root@your_droplet_ip:/opt/
```

## Step 3: Install Python Dependencies

```bash
cd /opt/attom-etl

# Install uv (fast Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.cargo/env

# Install dependencies
uv venv
source .venv/bin/activate
uv pip install boto3 requests schedule python-dotenv
```

## Step 4: Configure the Application

### Create .env file with credentials:

```bash
nano config/.env
```

Add your credentials:
```
SPACES_ACCESS_KEY=your_spaces_access_key_here
SPACES_SECRET_KEY=your_spaces_secret_key_here
FTP_USERNAME=your_ftp_username_here
FTP_PASSWORD=your_ftp_password_here
```

Save and exit (Ctrl+X, Y, Enter).

### Edit config.json:

```bash
nano config/config.json
```

Update the following:
- `states`: Add your target state codes
- `datasets[].urls`: Update with actual Attom FTP URLs
- `spaces.bucket_name`: Your DigitalOcean Spaces bucket name
- `schedule.daily_time`: Desired execution time

## Step 5: Test the Application

Run a test to ensure everything works:

```bash
python3 main.py run
```

Check the logs:
```bash
tail -f logs/etl_*.log
```

## Step 6: Set Up Systemd Service (Production)

Create systemd service file:
```bash
nano /etc/systemd/system/attom-etl.service
```

Add the following content:
```ini
[Unit]
Description=Attom ETL Pipeline Scheduler
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/attom-etl
Environment="PATH=/opt/attom-etl/.venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/opt/attom-etl/.venv/bin/python3 /opt/attom-etl/src/scheduler.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Save and exit.

Enable and start the service:
```bash
systemctl daemon-reload
systemctl enable attom-etl
systemctl start attom-etl
systemctl status attom-etl
```

## Step 7: Monitor the Application

### View service status:
```bash
systemctl status attom-etl
```

### View logs:
```bash
# System logs
journalctl -u attom-etl -f

# Application logs
tail -f /opt/attom-etl/logs/etl_*.log
```

### Check disk space:
```bash
df -h
```

### Monitor running processes:
```bash
htop
```

## Step 8: Verify DigitalOcean Spaces

Check that files are being uploaded:

1. Go to DigitalOcean Control Panel
2. Navigate to Spaces
3. Open your bucket
4. Look for dated folders (e.g., `2025-10-29/`)
5. Verify ZIP files are present

## Maintenance

### Manually trigger ETL:
```bash
cd /opt/attom-etl
python3 main.py run
```

### Restart service:
```bash
systemctl restart attom-etl
```

### View recent logs:
```bash
ls -lh logs/
tail -100 logs/etl_*.log
```

### Clean up old data:
```bash
# Remove downloaded files older than 7 days
find /opt/attom-etl/data/downloads -type f -mtime +7 -delete
find /opt/attom-etl/data/extracted -type f -mtime +7 -delete
find /opt/attom-etl/data/filtered -type f -mtime +7 -delete

# Remove old logs (older than 30 days)
find /opt/attom-etl/logs -type f -mtime +30 -delete
```

### Update application:
```bash
cd /opt/attom-etl
git pull  # if using git
systemctl restart attom-etl
```

## Troubleshooting

### Service won't start:
```bash
# Check status
systemctl status attom-etl

# View detailed logs
journalctl -u attom-etl -n 100

# Check Python path
which python3

# Test manually
cd /opt/attom-etl
python3 src/scheduler.py
```

### Out of disk space:
```bash
# Check space
df -h

# Clean up data directories
rm -rf /opt/attom-etl/data/downloads/*
rm -rf /opt/attom-etl/data/extracted/*
rm -rf /opt/attom-etl/data/filtered/*
```

### FTP connection issues:
```bash
# Test FTP manually
ftp ftp.attom.com
# Enter username and password
# Try to navigate directories
```

### Spaces upload fails:
```bash
# Test credentials
python3 -c "
import boto3
s3 = boto3.client('s3', 
    endpoint_url='https://sfo3.digitaloceanspaces.com',
    aws_access_key_id='YOUR_KEY',
    aws_secret_access_key='YOUR_SECRET')
print(s3.list_buckets())
"
```

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use environment variables** for all secrets
3. **Restrict file permissions**:
   ```bash
   chmod 600 config/.env
   chmod 700 config/
   ```
4. **Regular backups** of configuration files
5. **Monitor logs** for unauthorized access attempts

## Scaling Considerations

For larger deployments:
- Consider using multiple droplets for different datasets
- Implement parallel processing for multiple states
- Use a message queue (Redis/RabbitMQ) for job management
- Set up monitoring with Datadog or New Relic
- Implement alerting for failures

## Support

Check application logs for detailed error messages:
```bash
tail -f logs/etl_*.log
```

View the main README.md for more information.
