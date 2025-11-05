import os
import requests
from pathlib import Path
import hashlib
from urllib.parse import urlparse

class JetstreamDownloader:
    """Specialized downloader for Jetstream ZIP files with resume capability."""
    
    def __init__(self, logger, config):
        """
        Initialize Jetstream downloader.
        
        Args:
            logger: ETL logger instance
            config: Config dictionary containing directories and dataset settings
        """
        self.logger = logger
        self.config = config
        self.download_dir = Path(config.get('download_dir', 'data/downloads'))
        self.temp_dir = Path(config.get('temp_dir', 'data/temp'))
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
    def _get_temp_path(self, url):
        """Generate temp path for partial downloads."""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return self.download_dir / f"jetstream_{url_hash}.part"

    def _list_directory_contents(self, directory, indent=""):
        """
        Recursively list contents of a directory with proper indentation
        """
        directory = Path(directory)
        files = []
        for item in sorted(directory.iterdir()):
            if item.is_file():
                size = item.stat().st_size
                size_str = f"{size/1024/1024:.2f} MB" if size > 1024*1024 else f"{size/1024:.2f} KB"
                self.logger.info(f"{indent}📄 {item.name} ({size_str})")
                files.append(item)
            elif item.is_dir():
                self.logger.info(f"{indent}📁 {item.name}/")
                files.extend(self._list_directory_contents(item, indent + "  "))
        return files

    def _verify_ftp_upload(self, uploader, remote_folder, filename, retries=3):
        """
        Verify that a file was uploaded correctly to FTP
        """
        for attempt in range(retries):
            try:
                # Try to get file size from FTP
                size = uploader.ftp.size(f"{remote_folder.rstrip('/')}/{filename}")
                if size is not None and size > 0:
                    return True
                    
                if attempt < retries - 1:
                    self.logger.warning(f"Upload verification failed for {filename}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                if attempt < retries - 1:
                    self.logger.warning(f"Verification attempt {attempt + 1} failed for {filename}: {e}")
                    time.sleep(2 ** attempt)
                else:
                    self.logger.error(f"Final verification failed for {filename}: {e}")
        return False

    def _cleanup_temp_files(self, directory, pattern="*"):
        """
        Clean up temporary files in the specified directory
        """
        try:
            directory = Path(directory)
            for item in directory.glob(pattern):
                try:
                    if item.is_file():
                        item.unlink()
                        self.logger.debug(f"Cleaned up temporary file: {item.name}")
                    elif item.is_dir():
                        import shutil
                        shutil.rmtree(item)
                        self.logger.debug(f"Cleaned up temporary directory: {item.name}")
                except Exception as e:
                    self.logger.warning(f"Could not clean up {item}: {e}")
        except Exception as e:
            self.logger.warning(f"Error during cleanup of {directory}: {e}")

    def _upload_to_ftp(self, files, dataset_config):
        """
        Upload processed files to FTP server with retries and verification
        """
        try:
            from loaders.ftp_uploader import FTPUploader
            import time
            
            # Get FTP configuration
            ftp_config = self.config.get('ftp_config')
            if not ftp_config:
                raise ValueError("No FTP configuration found")
                
            # Get retry settings from Jetstream config
            jetstream_config = dataset_config.get('jetstream', {})
            max_retries = jetstream_config.get('retries', 3)
            
            # Create FTP uploader
            uploader = FTPUploader(self.logger, ftp_config)
            
            # Get upload folder from dataset config
            remote_folder = dataset_config.get('ftp_upload_folder')
            if not remote_folder:
                raise ValueError("No FTP upload folder configured in dataset")
                
            self.logger.info(f"Uploading {len(files)} files to FTP folder: {remote_folder}")
            
            # Upload each file with retries
            successful = []
            failed = []
            
            for file in files:
                file_name = file.name
                self.logger.info(f"Uploading {file_name} to {remote_folder}")
                
                for attempt in range(max_retries):
                    try:
                        if uploader.upload_file(file, remote_folder=remote_folder):
                            # Verify the upload
                            if self._verify_ftp_upload(uploader, remote_folder, file_name):
                                successful.append(file_name)
                                self.logger.info(f"✓ Successfully uploaded and verified: {file_name}")
                                break
                            else:
                                if attempt < max_retries - 1:
                                    self.logger.warning(f"Upload verification failed for {file_name}, retrying...")
                                    time.sleep(2 ** attempt)  # Exponential backoff
                                else:
                                    failed.append(file_name)
                                    self.logger.error(f"Upload verification failed for {file_name} after {max_retries} attempts")
                        else:
                            if attempt < max_retries - 1:
                                self.logger.warning(f"Upload attempt {attempt + 1} failed for {file_name}, retrying...")
                                time.sleep(2 ** attempt)
                            else:
                                failed.append(file_name)
                                self.logger.error(f"Upload failed for {file_name} after {max_retries} attempts")
                    except Exception as e:
                        if attempt < max_retries - 1:
                            self.logger.warning(f"Upload attempt {attempt + 1} failed for {file_name}: {e}, retrying...")
                            time.sleep(2 ** attempt)
                        else:
                            failed.append(file_name)
                            self.logger.error(f"Upload failed for {file_name}: {e} after {max_retries} attempts")
                
                if success:
                    successful.append(file_name)
                    self.logger.info(f"✓ Successfully uploaded to FTP: {file_name}")
                else:
                    failed.append(file_name)
                    self.logger.error(f"✗ Failed to upload to FTP: {file_name}")
            
            # Summary
            self.logger.info("=== FTP Upload Summary ===")
            self.logger.info(f"Total files: {len(files)}")
            self.logger.info(f"Successful: {len(successful)}")
            self.logger.info(f"Failed: {len(failed)}")
            
            # Handle files based on FTP action (copy/move)
            if dataset_config.get('filter_ftp_action') == 'move' and successful:
                self.logger.info("Cleaning up local files after successful upload...")
                for file in files:
                    if file.name in successful:
                        try:
                            file.unlink()
                            self.logger.info(f"Deleted local file: {file.name}")
                        except Exception as e:
                            self.logger.warning(f"Could not delete local file {file.name}: {e}")
            
            # Clean up temporary files if configured
            if jetstream_config.get('cleanup_temp', True):
                self.logger.info("Cleaning up temporary files...")
                temp_dir = Path(self.config.get('temp_dir', 'data/temp'))
                extract_dir = Path(self.config.get('extract_dir', 'data/extracted'))
                
                # Clean temp downloads
                self._cleanup_temp_files(temp_dir, "jetstream_*.part")
                # Clean extracted files if all uploads successful
                if not failed:
                    self._cleanup_temp_files(extract_dir)
            
            return len(successful) > 0
            
        except Exception as e:
            self.logger.error(f"FTP upload failed: {e}")
            return False

    def _process_and_copy_fips_files(self, files, dataset_config, filter_dir):
        """
        Match files against FIPS codes from config and copy to filter directory
        """
        filter_dir = Path(filter_dir)
        filter_dir.mkdir(parents=True, exist_ok=True)
        
        # Get FIPS codes from config
        jetstream_config = dataset_config.get('jetstream', {})
        expected_fips = jetstream_config.get('fips_codes', [])
        
        if not expected_fips:
            self.logger.warning("No FIPS codes configured in dataset config")
            return []
            
        processed_files = []
        self.logger.info(f"Looking for FIPS codes: {expected_fips}")
        
        for file in files:
            try:
                # Check if filename matches any FIPS code
                file_stem = file.stem
                if file_stem in expected_fips:
                    # Create new filename with fips prefix

                    #new_name = f"fips_{file_stem}.zip"
                    new_name = f"{file_stem}.zip"
                    target_path = filter_dir / new_name
                    
                    # Verify source file
                    if not self._verify_zip_file(file):
                        self.logger.warning(f"Source file corrupted: {file.name}")
                        continue
                    
                    # Copy file to filter directory
                    import shutil
                    shutil.copy2(file, target_path)
                    self.logger.info(f"✓ Copied {file.name} -> {new_name}")
                    processed_files.append(target_path)
                    
            except Exception as e:
                self.logger.error(f"Error processing file {file.name}: {e}")
                continue
        
        # Summary
        if processed_files:
            self.logger.info(f"Successfully processed {len(processed_files)} FIPS files:")
            for f in processed_files:
                self.logger.info(f"  ✓ {f.name}")
        else:
            self.logger.warning("No matching FIPS files found")
            
        return processed_files

    def _verify_zip_file(self, zip_path):
        """
        Verify if a ZIP file is valid and contains expected content
        """
        try:
            import zipfile
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Test ZIP file integrity
                test_result = zip_ref.testzip()
                if test_result is not None:
                    self.logger.warning(f"ZIP file {zip_path} is corrupted at {test_result}")
                    return False
                return True
        except Exception as e:
            self.logger.warning(f"Failed to verify ZIP file {zip_path}: {e}")
            return False

    def _get_jetstream_filename(self, url):
        """
        Extract the original filename from Jetstream URL or response headers
        """
        try:
            # First try to get filename from URL path
            path = urlparse(url).path
            if path and path.endswith('.zip'):
                return Path(path).name
            
            # If not in URL, make a HEAD request to get Content-Disposition
            headers = requests.head(url, allow_redirects=True).headers
            if 'Content-Disposition' in headers:
                import re
                matches = re.findall('filename=(.+)', headers['Content-Disposition'])
                if matches:
                    return matches[0].strip('"')
            
            # Make a GET request to see actual filename
            with requests.get(url, stream=True) as r:
                if 'Content-Disposition' in r.headers:
                    matches = re.findall('filename=(.+)', r.headers['Content-Disposition'])
                    if matches:
                        return matches[0].strip('"')
                
            # If no filename found, use URL ID
            url_id = url.split('/')[-1]
            return f"jetstream_{url_id}.zip"
            
        except Exception as e:
            self.logger.warning(f"Could not get filename from URL {url}: {e}")
            # Generate a filename based on URL hash
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            return f"jetstream_{url_hash}.zip"

    def _check_existing_files(self, url, download_dir, extract_dir, expected_fips):
        """
        Check for existing downloaded and extracted files.
        Returns:
            - downloaded_path: Path to existing downloaded file if valid
            - existing_fips: List of already processed FIPS files
            - missing_fips: List of FIPS codes that need processing
        """
        existing_fips = []
        missing_fips = set(expected_fips)
        downloaded_path = None

        # Get expected filename from Jetstream URL
        expected_filename = self._get_jetstream_filename(url)
        self.logger.info(f"Looking for downloaded file: {expected_filename}")

        # Check for exact filename match first
        file_path = download_dir / expected_filename
        if file_path.exists() and self._verify_zip_file(file_path):
            downloaded_path = file_path
            self.logger.info(f"Found existing valid download: {file_path.name}")
        else:
            # Check other zip files as fallback
            for file_path in download_dir.glob("*.zip"):
                if self._verify_zip_file(file_path):
                    downloaded_path = file_path
                    self.logger.info(f"Found existing valid download: {file_path.name}")
                    break

        # Check extracted FIPS files
        for file_path in extract_dir.glob("fips_*.zip"):
            if not self._verify_zip_file(file_path):
                self.logger.warning(f"Found corrupted FIPS file: {file_path.name}, will reprocess")
                continue

            # Extract FIPS code from filename (e.g., fips_06001.zip -> 06001)
            fips_code = file_path.stem.split('_')[1]
            if fips_code in missing_fips:
                existing_fips.append(file_path)
                missing_fips.remove(fips_code)
                self.logger.info(f"Found existing valid FIPS file: {file_path.name}")
        
        return downloaded_path, existing_fips, list(missing_fips)

    def _find_and_upload_fips_files(self, base_dir, dataset_config, expected_fips=None):
        """
        Find specific FIPS files in extracted folders and upload them to FTP.
        
        Args:
            base_dir: Base directory to search in
            dataset_config: Dataset configuration containing FTP settings
            expected_fips: List of FIPS codes to look for
        """
        processed_files = []
        base_dir = Path(base_dir)
        
        # Recursively search for FIPS code files
        for fips_code in expected_fips:
            self.logger.info(f"Searching for FIPS code: {fips_code}")
            # Look in all subdirectories for the FIPS file
            for found_file in base_dir.rglob(f"{fips_code}.zip"):
                try:
                    # Verify the file
                    if not self._verify_zip_file(found_file):
                        self.logger.warning(f"Found corrupted FIPS file: {found_file}")
                        continue
                    
                    # Create FIPS-prefixed name
                    new_name = f"fips_{fips_code}.zip"
                    renamed_path = found_file.parent / new_name
                    
                    # Rename if needed
                    if found_file.name != new_name:
                        found_file.rename(renamed_path)
                        self.logger.info(f"Renamed {found_file.name} to {new_name}")
                    else:
                        renamed_path = found_file
                    
                    processed_files.append(renamed_path)
                    self.logger.info(f"Found valid FIPS file: {renamed_path}")
                except Exception as e:
                    self.logger.error(f"Error processing FIPS file {found_file}: {e}")
        
        if processed_files:
            self.logger.info(f"Found {len(processed_files)} FIPS files to upload")
            try:
                # Upload to FTP
                from loaders.ftp_uploader import FTPUploader
                ftp_config = self.config.get('ftp_config')
                if not ftp_config:
                    raise ValueError("No FTP configuration found")
                
                uploader = FTPUploader(self.logger, ftp_config)
                remote_folder = dataset_config.get('ftp_upload_folder')
                if not remote_folder:
                    raise ValueError("No FTP upload folder configured")
                
                self.logger.info(f"Uploading {len(processed_files)} files to FTP folder: {remote_folder}")
                results = uploader.upload_multiple_files(processed_files, remote_folder=remote_folder)
                
                # Handle results
                for result in results:
                    path = result.get('path')
                    success = result.get('success', False)
                    if success:
                        self.logger.info(f"Successfully uploaded: {Path(path).name}")
                    else:
                        self.logger.error(f"Failed to upload: {Path(path).name}")
                
            except Exception as e:
                self.logger.error(f"Error during FTP upload: {e}")
        
        return processed_files

    def _process_fips_files(self, extracted_dir, expected_fips=None):
        """
        Process extracted files based on FIPS codes.
        Looks for files named like 06001.zip and organizes them by FIPS code.
        """
        fips_files = []
        extracted_dir = Path(extracted_dir)
        
        # Process any new files that need processing
        self.logger.info(f"Processing files in {extracted_dir} for FIPS codes: {expected_fips if expected_fips else 'all'}")
        
        # Process any new files
        for file_path in extracted_dir.glob("*.zip"):
            try:
                # Skip already processed files
                if file_path.name.startswith("fips_"):
                    if file_path not in fips_files:
                        fips_files.append(file_path)
                    continue

                # Check if filename matches FIPS pattern (5 digits)
                if file_path.stem.isdigit() and len(file_path.stem) == 5:
                    fips_code = file_path.stem
                    # Skip if not in expected FIPS codes
                    if expected_fips and fips_code not in expected_fips:
                        self.logger.debug(f"Skipping unexpected FIPS file: {file_path.name}")
                        continue

                    # Verify the file before processing
                    if not self._verify_zip_file(file_path):
                        self.logger.warning(f"Skipping corrupted file: {file_path.name}")
                        continue

                    # Create new filename with fips prefix
                    new_name = f"fips_{fips_code}.zip"
                    new_path = extracted_dir / new_name

                    # Skip if already exists and is valid
                    if new_path.exists():
                        if self._verify_zip_file(new_path):
                            self.logger.info(f"Valid FIPS file already exists: {new_name}")
                            if new_path not in fips_files:
                                fips_files.append(new_path)
                            continue
                        else:
                            self.logger.warning(f"Found corrupted FIPS file, will replace: {new_name}")
                            new_path.unlink()

                    # Rename the file
                    file_path.rename(new_path)
                    fips_files.append(new_path)
                    self.logger.info(f"Processed FIPS file: {file_path.name} -> {new_name}")

            except Exception as e:
                self.logger.error(f"Error processing file {file_path.name}: {e}")
                continue

        if fips_files:
            self.logger.info(f"Successfully processed FIPS files: {[f.name for f in fips_files]}")
        
        return fips_files

    def _download_with_progress(self, url, local_path):
        """Download file with progress tracking and resume capability."""
        temp_path = self._get_temp_path(url)
        file_name = self._get_jetstream_filename(url)
        final_path = local_path or (self.download_dir / file_name)
        self.logger.info(f"Downloading to: {final_path}")

        # Check if final file already exists
        if final_path.exists():
            self.logger.info(f"File already exists: {final_path}")
            return str(final_path)

        headers = {}
        mode = 'wb'
        
        # Check for partial download
        if temp_path.exists():
            existing_size = temp_path.stat().st_size
            headers['Range'] = f'bytes={existing_size}-'
            mode = 'ab'
            self.logger.info(f"Resuming download from byte {existing_size}")
        else:
            existing_size = 0
            
        try:
            with requests.get(url, stream=True, headers=headers) as response:
                response.raise_for_status()
                
                # Get total file size from Content-Range or Content-Length header
                if 'Content-Range' in response.headers:
                    total_size = int(response.headers['Content-Range'].split('/')[-1])
                else:
                    total_size = int(response.headers.get('Content-Length', 0))
                
                # Add existing size to total for resumed downloads
                downloaded_size = existing_size
                
                with open(temp_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if total_size:
                                progress = min(100.0, (downloaded_size / total_size) * 100)
                                self.logger.info(f"Download progress: {progress:.1f}% ({downloaded_size}/{total_size} bytes)")
                
                # Verify download completed
                if total_size and temp_path.stat().st_size != total_size:
                    raise Exception("Download incomplete - size mismatch")
                
                # Move temp file to final location
                temp_path.replace(final_path)
                self.logger.info(f"Download complete: {final_path}")
                return str(final_path)
                
        except Exception as e:
            self.logger.error(f"Download failed: {e}")
            # Keep partial file for resume
            if not temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise

    def _upload_to_ftp(self, files, dataset_config):
        """
        Upload processed FIPS files to FTP upload folder
        """
        from loaders.ftp_uploader import FTPUploader
        
        try:
            ftp_config = self.config.get('ftp_config')
            if not ftp_config:
                self.logger.warning("No FTP configuration found, skipping FTP upload")
                return []
                
            uploader = FTPUploader(self.logger, ftp_config)
            remote_folder = dataset_config.get('ftp_upload_folder')
            
            if not remote_folder:
                self.logger.warning("No FTP upload folder configured, skipping FTP upload")
                return []
                
            self.logger.info(f"Uploading {len(files)} FIPS files to FTP folder: {remote_folder}")
            results = uploader.upload_multiple_files(files, remote_folder=remote_folder)
            
            # Log upload results
            successful = [r['path'] for r in results if r['success']]
            failed = [r['path'] for r in results if not r['success']]
            
            if successful:
                self.logger.info(f"Successfully uploaded {len(successful)} files to FTP")
            if failed:
                self.logger.warning(f"Failed to upload {len(failed)} files to FTP")
                
            return successful
            
        except Exception as e:
            self.logger.error(f"FTP upload failed: {e}")
            return []

    def download_dataset(self, dataset_config):
        """
        Download files for dataset from Jetstream URLs, extract them,
        and process FIPS-coded files.
        
        Args:
            dataset_config: Dataset configuration containing Jetstream settings
            
        Returns:
            List of Path objects for processed files
        """
        jetstream_config = dataset_config.get('jetstream', {})
        if not jetstream_config:
            raise ValueError("No Jetstream configuration found in dataset config")
        
        # Get URLs and create extract directory
        urls = jetstream_config.get('urls', [])
        extract_dir = Path(self.config.get('extract_dir', 'data/extracted'))
        extract_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Using extract directory: {extract_dir}")

        # Ensure download directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Using download directory: {self.download_dir}")
        if not urls:
            self.logger.warning("No URLs configured for Parcel dataset")
            return []
            
        downloaded_files = []
        for url in urls:
            try:
                if not url.startswith(('http://', 'https://')):
                    self.logger.warning(f"Skipping non-HTTP URL: {url}")
                    continue
                
                self.logger.info(f"Downloading file from Jetstream: {url}")
                # Get expected FIPS codes from config
                expected_fips = jetstream_config.get('fips_codes', [])
                
                # Check existing files in both download and extract directories
                existing_download, existing_fips, missing_fips = self._check_existing_files(
                    url,
                    self.download_dir,
                    extract_dir,
                    expected_fips
                )
                
                # Add existing valid FIPS files to our result
                downloaded_files.extend(existing_fips)
                
                # If all FIPS files exist and are valid, we're done
                if existing_fips and not missing_fips:
                    self.logger.info("All required FIPS files already exist and are valid, skipping download")
                    return existing_fips
                
                # If we have a valid download but missing some extracted files
                if existing_download and missing_fips:
                    self.logger.info(f"Using existing download: {existing_download.name}")
                    local_path = str(existing_download)
                else:
                    # Download only if we need to
                    local_path = self._download_with_progress(url, None)
                
                if local_path:
                    # Extract the downloaded zip file
                    import zipfile
                    zip_path = Path(local_path)
                    
                    # Verify the downloaded file
                    if not self._verify_zip_file(zip_path):
                        raise Exception(f"Downloaded file {zip_path} is corrupted")
                    
                    self.logger.info(f"Extracting {zip_path} to {extract_dir}")
                    # Create a unique extraction directory based on the ZIP file name
                    zip_extract_dir = extract_dir / zip_path.stem
                    zip_extract_dir.mkdir(parents=True, exist_ok=True)
                    
                    self.logger.info(f"Extracting {zip_path} to {zip_extract_dir}")
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(zip_extract_dir)
                    
                    # List all extracted files
                    self.logger.info("=== Contents of extracted directory ===")
                    files = self._list_directory_contents(zip_extract_dir)
                    self.logger.info("=====================================")
                    
                    # Process FIPS files and copy to filter directory
                    filter_dir = Path(self.config.get('filter_dir', 'data/filtered'))
                    self.logger.info(f"Processing FIPS files to filter directory: {filter_dir}")
                    processed_files = self._process_and_copy_fips_files(files, dataset_config, filter_dir)
                    
                    # Upload processed files to FTP if configured
                    if processed_files and dataset_config.get('filter_ftp_upload', True):
                        self._upload_to_ftp(processed_files, dataset_config)
                    
                    return processed_files
                    
                    # Find and upload FIPS files
                    processed_files = self._find_and_upload_fips_files(
                        zip_extract_dir,
                        dataset_config,
                        missing_fips
                    )
                    
                    for file in processed_files:
                        if file not in downloaded_files:
                            downloaded_files.append(file)
                    
                    # Clean up original download if requested
                    if jetstream_config.get('cleanup_downloads', True):
                        zip_path.unlink()
                        self.logger.info(f"Cleaned up downloaded file: {zip_path}")
                    
            except Exception as e:
                self.logger.error(f"Failed to process {url}: {e}")
                continue
        
        if downloaded_files:
            self.logger.info(f"Successfully processed {len(downloaded_files)} FIPS files")
            
            # Upload processed files to FTP if configured
            if dataset_config.get('filter_ftp_upload', True):
                uploaded_files = self._upload_to_ftp(downloaded_files, dataset_config)
                
                # Handle files based on ftp_action (copy/move)
                if dataset_config.get('filter_ftp_action') == 'move':
                    # Remove local files that were successfully uploaded
                    for file_path in uploaded_files:
                        try:
                            Path(file_path).unlink()
                            self.logger.info(f"Removed local file after FTP upload: {file_path}")
                        except Exception as e:
                            self.logger.warning(f"Failed to remove local file {file_path}: {e}")
                else:
                    self.logger.info("Keeping local files (FTP action is 'copy')")
            else:
                self.logger.info("FTP upload is disabled for this dataset")
        else:
            self.logger.warning("No FIPS files were processed")
            
        return downloaded_files