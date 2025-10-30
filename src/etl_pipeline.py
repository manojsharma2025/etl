import time
import os
from pathlib import Path
from datetime import datetime

from utils.logger import ETLLogger
from utils.config_loader import ConfigLoader
from extractors.ftp_downloader import FTPDownloader
from transformers.state_filter import StateFilter
from loaders.spaces_uploader import SpacesUploader


class AttomETLPipeline:
    """Main ETL Pipeline orchestrator for Attom real estate data"""
    
    def __init__(self, config_file="config/config.json"):
        self.config_loader = ConfigLoader(config_file)
        self.logger = ETLLogger()
        
        self.states = self.config_loader.get_states()
        self.datasets = self.config_loader.get_datasets()
        
        self.logger.info(f"Initialized ETL Pipeline")
        self.logger.info(f"Target states: {', '.join(self.states)}")
        self.logger.info(f"Datasets to process: {len(self.datasets)}")
        
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize ETL components"""
        working_dirs = self.config_loader.get_working_directories()
        
        ftp_config = self.config_loader.get_ftp_config()
        ftp_config['download_dir'] = working_dirs.get('downloads', '/Outgoing')
        self.downloader = FTPDownloader(self.logger, ftp_config)
        
        filter_config = {
            'extracted_dir': working_dirs.get('extracted', 'data/extracted'),
            'filtered_dir': working_dirs.get('filtered', 'data/filtered'),
            'states': self.states,
            'state_code_column': self.config_loader.get_state_code_column(),
            'delimiter': self.config_loader.get_file_delimiter()
        }
        self.filter = StateFilter(self.logger, filter_config)
        
        spaces_config = self.config_loader.get_spaces_config()
        self.uploader = SpacesUploader(self.logger, spaces_config)
    
    def process_dataset(self, dataset_config):
        """
        Process a single dataset through the full ETL pipeline
        
        Args:
            dataset_config: Dataset configuration dict with 'name', 'urls', 'enabled'
        
        Returns:
            Dict with processing results
        """
        dataset_name = dataset_config.get('name')
        
        if not dataset_config.get('enabled', True):
            self.logger.info(f"Dataset {dataset_name} is disabled, skipping...")
            return {'status': 'skipped', 'dataset': dataset_name}
        
        start_time = time.time()
        self.logger.log_etl_start(dataset_name)
        
        try:
            downloaded_files = self.downloader.download_dataset(dataset_config)
            
            if not downloaded_files:
                self.logger.warning(f"No files downloaded for {dataset_name}")
                return {'status': 'no_files', 'dataset': dataset_name}
            
            all_filtered_zips = []
            
            for zip_file in downloaded_files:
                try:
                    extracted_files = self.filter.extract_zip(zip_file)
                    
                    for extracted_file in extracted_files:
                        if extracted_file.suffix.lower() in ['.txt', '.csv']:
                            filtered_files = self.filter.filter_multiple_states(extracted_file)
                            
                            if filtered_files:
                                zip_name = f"{dataset_name}_{extracted_file.stem}_{datetime.now().strftime('%Y%m%d')}.zip"
                                filtered_zip = self.filter.compress_to_zip(filtered_files, zip_name)
                                all_filtered_zips.append(filtered_zip)
                                
                                for filtered_file in filtered_files:
                                    if filtered_file.exists():
                                        filtered_file.unlink()
                        
                        if extracted_file.exists():
                            extracted_file.unlink()
                    
                    if zip_file.exists():
                        zip_file.unlink()
                        
                except Exception as e:
                    self.logger.error(f"Error processing {zip_file.name}: {e}")
            
            uploaded_urls = []
            if all_filtered_zips:
                uploaded_urls = self.uploader.upload_multiple_files(all_filtered_zips)
                
                for filtered_zip in all_filtered_zips:
                    if filtered_zip.exists():
                        filtered_zip.unlink()
            
            duration = time.time() - start_time
            self.logger.log_etl_end(dataset_name, duration)
            
            return {
                'status': 'success',
                'dataset': dataset_name,
                'uploaded_files': len(uploaded_urls),
                'urls': uploaded_urls,
                'duration': duration
            }
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed for {dataset_name}: {e}")
            duration = time.time() - start_time
            self.logger.log_etl_end(dataset_name, duration)
            
            return {
                'status': 'failed',
                'dataset': dataset_name,
                'error': str(e),
                'duration': duration
            }
    
    def run_all_datasets(self):
        """
        Run ETL pipeline for all configured datasets
        
        Returns:
            Dict with overall results
        """
        self.logger.info("="*80)
        self.logger.info("STARTING FULL ETL PIPELINE RUN")
        self.logger.info("="*80)
        
        overall_start = time.time()
        results = []
        
        for dataset_config in self.datasets:
            result = self.process_dataset(dataset_config)
            results.append(result)
        
        overall_duration = time.time() - overall_start
        
        success_count = sum(1 for r in results if r['status'] == 'success')
        failed_count = sum(1 for r in results if r['status'] == 'failed')
        skipped_count = sum(1 for r in results if r['status'] in ['skipped', 'no_files'])
        
        self.logger.info("="*80)
        self.logger.info("ETL PIPELINE RUN COMPLETE")
        self.logger.info(f"Total datasets: {len(results)}")
        self.logger.info(f"Successful: {success_count}")
        self.logger.info(f"Failed: {failed_count}")
        self.logger.info(f"Skipped: {skipped_count}")
        self.logger.info(f"Total duration: {overall_duration:.2f} seconds")
        self.logger.info("="*80)
        
        try:
            log_file = self.logger.get_log_file_path()
            if os.path.exists(log_file):
                self.uploader.upload_log_file(log_file)
        except Exception as e:
            self.logger.warning(f"Failed to upload log file: {e}")
        
        return {
            'total': len(results),
            'success': success_count,
            'failed': failed_count,
            'skipped': skipped_count,
            'duration': overall_duration,
            'results': results
        }


def main():
    """Main entry point for running ETL pipeline"""
    try:
        pipeline = AttomETLPipeline()
        results = pipeline.run_all_datasets()
        
        if results['failed'] > 0:
            exit(1)
        else:
            exit(0)
            
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        exit(1)


if __name__ == "__main__":
    main()
