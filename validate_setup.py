#!/usr/bin/env python3
"""
Setup validation script for Attom ETL Pipeline
Validates configuration and displays system information
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'src'))

def validate_setup():
    """Validate the ETL pipeline setup"""
    print("="*80)
    print("ATTOM ETL PIPELINE - SETUP VALIDATION")
    print("="*80)
    print()
    
    errors = []
    warnings = []
    
    print("1. Checking directory structure...")
    required_dirs = ['src', 'config', 'logs', 'data/downloads', 'data/extracted', 
                     'data/filtered', 'data/temp']
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print(f"   ✓ {dir_path}")
        else:
            print(f"   ✗ {dir_path} - MISSING")
            errors.append(f"Missing directory: {dir_path}")
    print()
    
    print("2. Checking Python modules...")
    modules_ok = True
    try:
        import boto3
        print("   ✓ boto3 (DigitalOcean Spaces)")
    except ImportError:
        print("   ✗ boto3 - NOT INSTALLED")
        errors.append("boto3 not installed")
        modules_ok = False
    
    try:
        import requests
        print("   ✓ requests (HTTP downloads)")
    except ImportError:
        print("   ✗ requests - NOT INSTALLED")
        errors.append("requests not installed")
        modules_ok = False
    
    try:
        import schedule
        print("   ✓ schedule (Job scheduling)")
    except ImportError:
        print("   ✗ schedule - NOT INSTALLED")
        errors.append("schedule not installed")
        modules_ok = False
    
    try:
        from dotenv import load_dotenv
        print("   ✓ python-dotenv (Environment variables)")
    except ImportError:
        print("   ✗ python-dotenv - NOT INSTALLED")
        errors.append("python-dotenv not installed")
        modules_ok = False
    print()
    
    print("3. Checking configuration files...")
    config_file = Path('config/config.json')
    if config_file.exists():
        print("   ✓ config/config.json")
        try:
            import json
            with open(config_file, 'r') as f:
                config = json.load(f)
            print(f"   ✓ Configuration is valid JSON")
            print(f"      - States configured: {len(config.get('states', []))}")
            print(f"      - Datasets configured: {len(config.get('datasets', []))}")
            print(f"      - Schedule time: {config.get('schedule', {}).get('daily_time', 'Not set')}")
        except Exception as e:
            print(f"   ✗ Configuration parsing failed: {e}")
            errors.append(f"Config parsing error: {e}")
    else:
        print("   ✗ config/config.json - MISSING")
        errors.append("Missing config.json")
    
    env_example = Path('config/.env.example')
    if env_example.exists():
        print("   ✓ config/.env.example")
    else:
        print("   ⚠ config/.env.example - MISSING")
        warnings.append("Missing .env.example template")
    
    import os
    env_file = Path('config/.env')
    spaces_key = os.getenv('SPACES_ACCESS_KEY')
    spaces_secret = os.getenv('SPACES_SECRET_KEY')
    ftp_user = os.getenv('FTP_USERNAME')
    ftp_pass = os.getenv('FTP_PASSWORD')
    
    has_env_file = env_file.exists()
    has_env_vars = all([spaces_key, spaces_secret, ftp_user, ftp_pass])
    
    if has_env_vars:
        print("   ✓ Credentials configured (via environment variables)")
        print("      - SPACES_ACCESS_KEY: Set")
        print("      - SPACES_SECRET_KEY: Set")
        print("      - FTP_USERNAME: Set")
        print("      - FTP_PASSWORD: Set")
    elif has_env_file:
        print("   ✓ config/.env (credentials file exists)")
    else:
        print("   ⚠ Credentials not configured")
        warnings.append("Credentials not configured (neither .env file nor environment variables)")
    print()
    
    print("4. Checking ETL modules...")
    try:
        from utils.logger import ETLLogger
        print("   ✓ Logger module")
    except Exception as e:
        print(f"   ✗ Logger module failed: {e}")
        errors.append("Logger module error")
    
    try:
        from utils.config_loader import ConfigLoader
        print("   ✓ Config loader module")
    except Exception as e:
        print(f"   ✗ Config loader module failed: {e}")
        errors.append("Config loader error")
    
    try:
        from extractors.ftp_downloader import FTPDownloader
        print("   ✓ FTP downloader module")
    except Exception as e:
        print(f"   ✗ FTP downloader module failed: {e}")
        errors.append("FTP downloader error")
    
    try:
        from transformers.state_filter import StateFilter
        print("   ✓ State filter module")
    except Exception as e:
        print(f"   ✗ State filter module failed: {e}")
        errors.append("State filter error")
    
    try:
        from loaders.spaces_uploader import SpacesUploader
        print("   ✓ Spaces uploader module")
    except Exception as e:
        print(f"   ✗ Spaces uploader module failed: {e}")
        errors.append("Spaces uploader error")
    
    try:
        from etl_pipeline import AttomETLPipeline
        print("   ✓ ETL pipeline orchestrator")
    except Exception as e:
        print(f"   ✗ ETL pipeline orchestrator failed: {e}")
        errors.append("ETL pipeline error")
    
    try:
        from scheduler import ETLScheduler
        print("   ✓ Scheduler module")
    except Exception as e:
        print(f"   ✗ Scheduler module failed: {e}")
        errors.append("Scheduler error")
    print()
    
    print("="*80)
    print("VALIDATION SUMMARY")
    print("="*80)
    
    if errors:
        print(f"\n❌ ERRORS FOUND ({len(errors)}):")
        for error in errors:
            print(f"   - {error}")
    
    if warnings:
        print(f"\n⚠️  WARNINGS ({len(warnings)}):")
        for warning in warnings:
            print(f"   - {warning}")
    
    if not errors and not warnings:
        print("\n✅ ALL CHECKS PASSED!")
        print("\nThe ETL pipeline is properly configured and ready to use.")
        print("\nNext steps:")
        print("1. Configure credentials in config/.env")
        print("2. Update config/config.json with your Attom FTP URLs")
        print("3. Test with: python main.py run")
        print("4. Deploy with: python main.py schedule")
    elif not errors:
        print("\n✅ SETUP COMPLETE WITH WARNINGS")
        print("\nThe application is functional but has some warnings.")
        print("Review the warnings above and configure as needed.")
    else:
        print("\n❌ SETUP INCOMPLETE")
        print("\nPlease fix the errors above before using the application.")
        return 1
    
    print("="*80)
    return 0


if __name__ == "__main__":
    sys.exit(validate_setup())
