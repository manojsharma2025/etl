#!/usr/bin/env python3
"""
Attom Real Estate Data ETL Pipeline
Main entry point for the application
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'src'))

from etl_pipeline import AttomETLPipeline
from scheduler import ETLScheduler


def run_once(config_file):
    """Run ETL pipeline once"""
    print("Running ETL pipeline (single execution)...\n")
    
    try:
        pipeline = AttomETLPipeline(config_file)
        results = pipeline.run_all_datasets()
        
        print("\n" + "="*80)
        print("ETL PIPELINE EXECUTION COMPLETE")
        print("="*80)
        print(f"Total datasets: {results['total']}")
        print(f"Successful: {results['success']}")
        print(f"Failed: {results['failed']}")
        print(f"Skipped: {results['skipped']}")
        print(f"Total duration: {results['duration']:.2f} seconds")
        print("="*80)
        
        if results['failed'] > 0:
            print("\nWARNING: Some datasets failed to process. Check logs for details.")
            sys.exit(1)
        else:
            print("\nAll datasets processed successfully!")
            sys.exit(0)
            
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        print("Check logs for details.")
        sys.exit(1)


def run_scheduler(config_file, run_immediately):
    """Run ETL pipeline with scheduler"""
    print("Starting ETL scheduler...\n")
    
    try:
        scheduler = ETLScheduler(config_file)
        scheduler.start(run_immediately=run_immediately)
    except KeyboardInterrupt:
        print("\nScheduler stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        sys.exit(1)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Attom Real Estate Data ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s run                    # Run ETL pipeline once
  %(prog)s schedule               # Run with daily scheduler
  %(prog)s schedule --now         # Run immediately, then on schedule
  %(prog)s run --config custom.json  # Use custom config file
        """
    )
    
    parser.add_argument(
        'mode',
        choices=['run', 'schedule'],
        help='Execution mode: "run" for single execution, "schedule" for automated daily runs'
    )
    
    parser.add_argument(
        '--config',
        default='config/config.json',
        help='Path to configuration file (default: config/config.json)'
    )
    
    parser.add_argument(
        '--now',
        action='store_true',
        help='Run ETL immediately when using schedule mode (in addition to scheduled runs)'
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("ATTOM REAL ESTATE DATA ETL PIPELINE")
    print("="*80)
    print(f"Mode: {args.mode}")
    print(f"Config: {args.config}")
    print("="*80 + "\n")
    
    if args.mode == 'run':
        run_once(args.config)
    elif args.mode == 'schedule':
        run_scheduler(args.config, args.now)


if __name__ == "__main__":
    main()
