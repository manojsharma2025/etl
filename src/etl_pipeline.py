import time
import os
import shutil
from pathlib import Path
from datetime import datetime

from utils.logger import ETLLogger
from utils.config_loader import ConfigLoader
from extractors.ftp_downloader import FTPDownloader
from extractors.extractor import Extractor
from transformers.state_filter import StateFilter
from loaders.spaces_uploader import SpacesUploader
from loaders.ftp_uploader import FTPUploader


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
        ftp_config['download_dir'] = working_dirs.get('downloads', 'data/downloads')

        # Initialize Spaces uploader first so downloader can optionally upload
        spaces_config = self.config_loader.get_spaces_config()
        try:
            self.uploader = SpacesUploader(self.logger, spaces_config)
        except Exception as e:
            # If Spaces is not configured properly, warn but keep running in local-only mode
            self.logger.warning(f"Spaces uploader could not be initialized: {e}. Continuing in local-only mode.")
            self.uploader = None

        # Pass the optional uploader to the downloader so it may upload files as they're downloaded
        self.downloader = FTPDownloader(self.logger, ftp_config, states=self.states, spaces_uploader=self.uploader)

        # FTP uploader (for pushing filtered zips back to an FTP outgoing folder)
        try:
            self.ftp_uploader = FTPUploader(self.logger, ftp_config)
        except Exception as e:
            self.logger.warning(f"FTP uploader not initialized: {e}. FTP uploads will be disabled.")
            self.ftp_uploader = None
        
        filter_config = {
            'extracted_dir': working_dirs.get('extracted', 'data/extracted'),
            'filtered_dir': working_dirs.get('filtered', 'data/filtered'),
            'states': self.states,
            'state_code_column': self.config_loader.get_state_code_column(),
            'delimiter': self.config_loader.get_file_delimiter()
        }
        self.filter = StateFilter(self.logger, filter_config)

        # Extractor: move downloaded ZIP into the extracted area and unzip
        extracted_dir = working_dirs.get('extracted', 'data/extracted')
        self.extractor = Extractor(self.logger, extracted_dir)
        # Ensure processed directory exists for optional post-processing of filtered files
        self.processed_dir = Path(working_dirs.get('processed', 'data/processed'))
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    
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
                    # Move downloaded zip into extracted area and extract
                    extracted_files = self.extractor.move_and_extract(zip_file)
                    
                    for extracted_file in extracted_files:
                        if extracted_file.suffix.lower() in ['.txt', '.csv']:
                            # Use per-dataset states if provided in dataset config.
                            # Prefer `exstates` (explicit per-dataset states) for clarity,
                            # fall back to legacy `states` key, then pipeline/global states.
                            dataset_states = dataset_config.get('exstates') or dataset_config.get('states') or self.states
                            filtered_files = self.filter.filter_multiple_states(extracted_file, states=dataset_states)
                            
                            if filtered_files:
                                # Allow per-dataset prefix for filtered ZIP names. If
                                # `filtered_zip_prefix` is provided in dataset config,
                                # use it as the start of the filename. Otherwise fall
                                # back to the default pattern: {dataset_name}_{stem}_{YYYYMMDD}.zip
                                prefix = dataset_config.get('filtered_zip_prefix')
                                date_str = datetime.now().strftime('%Y%m%d')
                                if prefix:
                                    # When a prefix is explicitly provided, produce
                                    # the filename exactly as: {prefix}{stem}.zip
                                    # (user requested no date appended for prefixed names)
                                    zip_name = f"{prefix}{extracted_file.stem}.zip"
                                    self.logger.info(f"Using filtered_zip_prefix for ZIP name: {zip_name}")
                                else:
                                    zip_name = f"{dataset_name}_{extracted_file.stem}_{date_str}.zip"
                                    self.logger.info(f"Using default ZIP naming for ZIP name: {zip_name}")

                                # When a filtered_zip_prefix is provided, also prefix
                                # the filenames inside the ZIP so contents align with
                                # the ZIP naming convention expected downstream.
                                filtered_zip = self.filter.compress_to_zip(
                                    filtered_files,
                                    zip_name,
                                    inner_name_prefix=prefix
                                )
                                # Only remove the intermediate filtered files if
                                # the ZIP was successfully created and has non-zero size.
                                if filtered_zip and filtered_zip.exists() and filtered_zip.stat().st_size > 0:
                                    self.logger.info(f"Created filtered ZIP: {filtered_zip.name} (size={filtered_zip.stat().st_size})")
                                    # Decide whether to delete or move intermediate filtered files
                                    action = dataset_config.get('post_process_filtered', 'delete')
                                    for filtered_file in filtered_files:
                                        try:
                                            if not filtered_file.exists():
                                                continue
                                            if action == 'move':
                                                dest = self.processed_dir / filtered_file.name
                                                shutil.move(str(filtered_file), str(dest))
                                                self.logger.info(f"Moved intermediate filtered file to processed: {dest.name}")
                                            else:
                                                filtered_file.unlink()
                                                self.logger.debug(f"Deleted intermediate filtered file: {filtered_file.name}")
                                        except Exception as e:
                                            self.logger.warning(f"Failed to post-process intermediate filtered file {filtered_file}: {e}")
                                else:
                                    self.logger.error(f"Filtered ZIP was not created or is empty: {zip_name}; keeping intermediate files for inspection")

                                all_filtered_zips.append(filtered_zip)
                        
                        if extracted_file.exists():
                            extracted_file.unlink()
                    
                    if zip_file.exists():
                        # Post-process the original downloaded ZIP according to
                        # dataset configuration. Options:
                        #   - 'delete' (default): remove the downloaded ZIP
                        #   - 'move': move the downloaded ZIP to processed_dir
                        #   - 'copy': copy the downloaded ZIP to processed_dir and keep original
                        dl_action = dataset_config.get('post_process_downloaded', 'delete')
                        try:
                            if dl_action == 'move':
                                dest = self.processed_dir / zip_file.name
                                shutil.move(str(zip_file), str(dest))
                                self.logger.info(f"Moved downloaded ZIP to processed: {dest}")
                            elif dl_action == 'copy':
                                dest = self.processed_dir / zip_file.name
                                shutil.copy2(str(zip_file), str(dest))
                                self.logger.info(f"Copied downloaded ZIP to processed: {dest}")
                            else:
                                zip_file.unlink()
                                self.logger.debug(f"Deleted downloaded ZIP: {zip_file.name}")
                        except Exception as e:
                            self.logger.warning(f"Failed to post-process downloaded ZIP {zip_file}: {e}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {zip_file.name}: {e}")
            
            uploaded_urls = []
            if all_filtered_zips:
                # Upload to Spaces if configured
                uploaded_urls = []
                spaces_map = {}
                if self.uploader:
                    try:
                        uploaded_urls = self.uploader.upload_multiple_files(all_filtered_zips)
                        # Map local path -> upload success (True if a URL was returned)
                        for p, url in zip(all_filtered_zips, uploaded_urls):
                            spaces_map[str(p)] = bool(url)
                    except Exception as e:
                        self.logger.warning(f"Spaces upload failed: {e}")

                # Additionally, upload to dataset FTP upload folder if configured
                # Controlled by dataset flag `filter_ftp_upload`. If true,
                # the pipeline will attempt to upload filtered zips to the
                # specified `ftp_upload_folder`. If false (default), FTP upload is skipped.
                ftp_folder = dataset_config.get('ftp_upload_folder')
                ftp_map = {}
                if dataset_config.get('filter_ftp_upload', False) and ftp_folder and self.ftp_uploader:
                    try:
                        ftp_results = self.ftp_uploader.upload_multiple_files(all_filtered_zips, remote_folder=ftp_folder)
                        # Log ftp upload results and build map
                        for r in ftp_results:
                            path = r.get('path')
                            ok = r.get('success', False)
                            ftp_map[path] = ok
                            if ok:
                                self.logger.info(f"Uploaded to FTP: {path}")
                            else:
                                self.logger.warning(f"Failed FTP upload: {path}")
                    except Exception as e:
                        self.logger.warning(f"FTP upload failed: {e}")
                
                # Decide whether to delete local filtered zips. The dataset may
                # request FTP uploads via `filter_ftp_upload`. Additionally,
                # the dataset can control whether the FTP upload should behave
                # like a 'copy' (upload and keep local) or a 'move' (upload and
                # remove local copy) via `filter_ftp_action` which accepts
                # values 'copy' or 'move' (default: 'copy').
                require_ftp = dataset_config.get('filter_ftp_upload', False)
                ftp_action = dataset_config.get('filter_ftp_action', 'copy')  # 'copy'|'move'

                for filtered_zip in all_filtered_zips:
                    try:
                        pstr = str(filtered_zip)
                        space_ok = spaces_map.get(pstr, False)
                        ftp_ok = ftp_map.get(pstr, False)

                        # If the dataset explicitly requests FTP move semantics,
                        # delete the local file only when the FTP upload was
                        # verified. If the dataset requests 'copy', keep the
                        # local file even after a verified FTP upload.
                        should_delete = False
                        if ftp_action == 'move':
                            should_delete = bool(ftp_ok)
                        else:  # 'copy' (default)
                            if require_ftp:
                                # FTP is required but action is copy: keep local
                                should_delete = False
                            else:
                                # No FTP requirement: delete if uploaded to
                                # Spaces or FTP succeeded (legacy behaviour).
                                should_delete = bool(space_ok or ftp_ok)

                        if should_delete and filtered_zip.exists():
                            filtered_zip.unlink()
                            self.logger.debug(f"Deleted filtered ZIP after upload: {filtered_zip.name}")
                        else:
                            self.logger.info(f"Keeping filtered ZIP: {filtered_zip.name} (spaces_ok={space_ok}, ftp_ok={ftp_ok}, ftp_action={ftp_action})")
                    except Exception as e:
                        self.logger.warning(f"Failed while post-processing filtered ZIP {filtered_zip}: {e}")
            
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
