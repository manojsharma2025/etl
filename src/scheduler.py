import schedule
import time
from datetime import datetime
from etl_pipeline import AttomETLPipeline


class ETLScheduler:
    """Automated daily scheduler for ETL pipeline"""
    
    def __init__(self, config_file="config/config.json"):
        self.config_file = config_file
        self.pipeline = None
        self.schedule_time = "02:00"
        
        self._load_schedule_config()
        
    def _load_schedule_config(self):
        """Load schedule configuration"""
        try:
            from utils.config_loader import ConfigLoader
            config_loader = ConfigLoader(self.config_file)
            self.schedule_time = config_loader.get_schedule_time()
            print(f"Scheduler initialized - Daily run scheduled at {self.schedule_time}")
        except Exception as e:
            print(f"Warning: Could not load schedule config, using default time {self.schedule_time}: {e}")
    
    def run_etl_job(self):
        """Execute the ETL pipeline job"""
        print(f"\n{'='*80}")
        print(f"ETL Job triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        try:
            self.pipeline = AttomETLPipeline(self.config_file)
            results = self.pipeline.run_all_datasets()
            
            print(f"\n{'='*80}")
            print(f"ETL Job completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Success: {results['success']}, Failed: {results['failed']}, Skipped: {results['skipped']}")
            print(f"{'='*80}\n")
            
        except Exception as e:
            print(f"ERROR: ETL job failed: {e}")
    
    def start(self, run_immediately=False):
        """
        Start the scheduler
        
        Args:
            run_immediately: If True, run ETL job immediately on start
        """
        print(f"Starting ETL Scheduler...")
        print(f"Scheduled to run daily at {self.schedule_time}")
        
        schedule.every().day.at(self.schedule_time).do(self.run_etl_job)
        
        if run_immediately:
            print("Running ETL job immediately...")
            self.run_etl_job()
        
        print("Scheduler is running. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            print("\nScheduler stopped by user.")


def main():
    """Main entry point for scheduler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Attom ETL Scheduler')
    parser.add_argument('--config', default='config/config.json', help='Path to config file')
    parser.add_argument('--now', action='store_true', help='Run ETL immediately on start')
    
    args = parser.parse_args()
    
    scheduler = ETLScheduler(args.config)
    scheduler.start(run_immediately=args.now)


if __name__ == "__main__":
    main()
