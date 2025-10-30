import os
import ftplib
import socket
import time
import requests
from pathlib import Path
from urllib.parse import urlparse

class FTPDownloader:
    """Handles downloading files from ATTOM FTPS/FTP servers."""

    def __init__(self, logger, ftp_config, states=None):
        """
        Initialize the FTP downloader.

        Args:
            logger: ETLLogger instance for consistent logging
            ftp_config: dict containing FTP credentials and settings
            states: list of state codes to filter (e.g., ['CA', 'TX', 'FL'])
        """
        self.logger = logger
        self.ftp_config = ftp_config
        self.states = states or []
        self.download_dir = Path(ftp_config.get('download_dir', 'data/downloads'))
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        """Connect to FTP or FTPS server."""
        server = self.ftp_config.get('host')
        username = self.ftp_config.get('username')
        password = self.ftp_config.get('password')
        use_ftps = self.ftp_config.get('use_ftps', True)

        if not server or not username or not password:
            raise ValueError("Missing FTP configuration values")

        self.logger.info(f"Connecting to {'FTPS' if use_ftps else 'FTP'} server: {server}")

        if use_ftps:
            ftp = ftplib.FTP_TLS(server)
            ftp.login(username, password)
            ftp.prot_p()  # Secure data connection
        else:
            ftp = ftplib.FTP(server)
            ftp.login(username, password)

        self.logger.info("Connected successfully to FTP server")
        return ftp

    def _list_files_in_folder(self, ftp, folder_path):
        """
        List all files in an FTP folder.

        Args:
            ftp: FTP connection object
            folder_path: Path to the folder
        Returns:
            List of file names
        """
        try:
            ftp.cwd(folder_path)
            files = ftp.nlst()
            self.logger.info(f"Found {len(files)} files in {folder_path}")
            return files
        except Exception as e:
            self.logger.error(f"Error listing files in {folder_path}: {e}")
            return []

    def _get_parser_keyword(self, dataset_name):
        """
        Map dataset name to parser keyword in filename.
        
        Args:
            dataset_name: Name of the dataset (e.g., 'Assessor', 'AVM')
        Returns:
            Parser keyword used in filenames
        """
        parser_map = {
            'Assessor': 'TAXASSESSOR',
            'AVM': 'AVM',
            'Parcel': 'PARCEL',
            'PROPERTYTOBOUNDARYMATCH_PARCEL': 'PARCEL',
            'Recorder': 'RECORDER',
            'PreForeclosure': 'PREFORECLOSURE'
        }
        return parser_map.get(dataset_name, dataset_name.upper())

    def _filter_files_by_dataset(self, files, dataset_name):
        """
        Filter files by state codes and parser type.
        File pattern: PREFIX_STATECODE_PARSER_SERIES_SEQUENCE.zip
        Example: 1PARKPLACE_CA_RECORDER_0001_001.zip

        Args:
            files: List of file names
            dataset_name: Name of the dataset to filter for
        Returns:
            List of files matching state codes and parser type
        """
        if not self.states:
            return files

        parser_keyword = self._get_parser_keyword(dataset_name)
        filtered = []
        
        self.logger.info(f"Filtering files for dataset '{dataset_name}' (parser: {parser_keyword}) and states: {', '.join(self.states)}")

        for file in files:
            file_upper = file.upper()
            parts = file_upper.split('_')
            
            if len(parts) < 3:
                continue
            
            state_code = parts[1] if len(parts) > 1 else ''
            parser_type = parts[2] if len(parts) > 2 else ''
            
            if state_code in [s.upper() for s in self.states] and parser_type == parser_keyword:
                filtered.append(file)
                self.logger.info(f"Matched: {file} (state={state_code}, parser={parser_type})")

        self.logger.info(f"Filtered {len(filtered)} files out of {len(files)} for dataset {dataset_name}")
        return filtered

    def _download_http_file(self, url, local_path):
        """
        Download file via HTTP/HTTPS.
        
        Args:
            url: HTTP/HTTPS URL
            local_path: Local file path to save
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Downloading via HTTP: {url}")
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.logger.info(f"Downloaded via HTTP: {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"HTTP download failed for {url}: {e}")
            return False

    def download_dataset(self, dataset_config):
        """
        Download files for a dataset.
        Supports both direct URLs and FTP folder browsing with state/parser filtering.

        Args:
            dataset_config: dict with dataset configuration
        Returns:
            List of downloaded file paths
        """
        dataset_name = dataset_config.get('name')
        remote_urls = dataset_config.get('urls', [])
        ftp_folder = dataset_config.get('ftp_folder', '/Outgoing')
        downloaded_files = []

        if remote_urls:
            self.logger.info(f"Downloading from direct URLs for dataset: {dataset_name}")
            for url in remote_urls:
                file_name = os.path.basename(urlparse(url).path)
                local_path = self.download_dir / file_name
                
                if url.startswith(('http://', 'https://')):
                    if self._download_http_file(url, local_path):
                        downloaded_files.append(local_path)
                elif url.startswith('ftp://'):
                    parsed = urlparse(url)
                    ftp_path = parsed.path
                    self.logger.info(f"Downloading FTP file: {ftp_path}")
                    try:
                        ftp = self._connect()
                        ftp.set_pasv(True)
                        with open(local_path, "wb") as f:
                            ftp.retrbinary(f"RETR {ftp_path}", f.write)
                        ftp.quit()
                        self.logger.info(f"Downloaded: {local_path}")
                        downloaded_files.append(local_path)
                    except Exception as e:
                        self.logger.error(f"FTP download failed for {url}: {e}")
                else:
                    self.logger.warning(f"Unknown URL scheme: {url}")
            
            return downloaded_files

        self.logger.info(f"Browsing FTP folder for dataset: {dataset_name}")
        try:
            ftp = self._connect()
            ftp.set_pasv(True)
            
            all_files = self._list_files_in_folder(ftp, ftp_folder)
            filtered_files = self._filter_files_by_dataset(all_files, dataset_name)

            for file_name in filtered_files:
                local_path = self.download_dir / file_name
                self.logger.info(f"Downloading {ftp_folder}/{file_name} to {local_path}")

                try:
                    with open(local_path, "wb") as f:
                        ftp.retrbinary(f"RETR {file_name}", f.write)
                    self.logger.info(f"Downloaded: {local_path}")
                    downloaded_files.append(local_path)
                except Exception as e:
                    self.logger.error(f"Error downloading {file_name}: {e}")
                    continue

            ftp.quit()
            return downloaded_files

        except Exception as e:
            self.logger.error(f"FTP download failed for {dataset_name}: {e}")
            return []

    def download_test_files(self):
        """Test FTP connection."""
        self.logger.info("Testing FTP connection...")
        try:
            ftp = self._connect()
            ftp.set_pasv(True)
            ftp.cwd('/')
            files = ftp.nlst()
            self.logger.info(f"Files in root: {files[:5]} (showing up to 5)")
            ftp.quit()
        except Exception as e:
            self.logger.error(f"FTP test failed: {e}")
