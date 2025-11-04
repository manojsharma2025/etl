import logging
import os
from datetime import datetime
from pathlib import Path


class ETLLogger:
    """Comprehensive logging system for ETL pipeline"""
    
    def __init__(self, log_dir="logs", log_level=logging.INFO):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        self.log_file = self.log_dir / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        self.logger = logging.getLogger('AttomETL')
        self.logger.setLevel(log_level)
        
        self.logger.handlers.clear()
        
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
    def info(self, message):
        self.logger.info(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def log_etl_start(self, dataset_name):
        self.info(f"{'='*80}")
        self.info(f"Starting ETL process for dataset: {dataset_name}")
        self.info(f"{'='*80}")
    
    def log_etl_end(self, dataset_name, duration):
        self.info(f"{'='*80}")
        self.info(f"Completed ETL process for dataset: {dataset_name}")
        self.info(f"Total duration: {duration:.2f} seconds")
        self.info(f"{'='*80}")
    
    def log_download_progress(self, filename, downloaded, total):
        if total > 0:
            percentage = (downloaded / total) * 100
            self.info(f"Downloading {filename}: {downloaded}/{total} bytes ({percentage:.1f}%)")
        else:
            self.info(f"Downloading {filename}: {downloaded} bytes")
    
    def log_filter_progress(self, filename, records_processed, records_kept):
        self.info(f"Filtering {filename}: Processed {records_processed} records, Kept {records_kept} records")
    
    def log_upload_progress(self, filename, status):
        self.info(f"Upload {filename}: {status}")
    
    def get_log_file_path(self):
        return str(self.log_file)
