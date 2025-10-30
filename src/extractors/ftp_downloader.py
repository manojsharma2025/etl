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

    def _filter_files_by_states(self, files):
        """
        Filter files by state codes.

        Args:
            files: List of file names
        Returns:
            List of files matching state codes
        """
        if not self.states:
            return files

        filtered = []
        for file in files:
            file_upper = file.upper()
            for state in self.states:
                if f"_{state.upper()}_" in file_upper or f"_{state.upper()}." in file_upper:
                    filtered.append(file)
                    self.logger.info(f"Matched file for state {state}: {file}")
                    break

        self.logger.info(f"Filtered {len(filtered)} files out of {len(files)} for states: {', '.join(self.states)}")
        return filtered

    def download_dataset(self, dataset_config):
        """
        Download files for a dataset.
        Supports both direct URLs and folder-based browsing with state filtering.

        Args:
            dataset_config: dict with dataset configuration
        Returns:
            List of downloaded file paths
        """
        dataset_name = dataset_config.get('name')
        remote_paths = dataset_config.get('urls', [])
        ftp_folder = dataset_config.get('ftp_folder', None)
        downloaded_files = []

        if not remote_paths and not ftp_folder:
            self.logger.warning(f"No URLs or FTP folder provided for dataset {dataset_name}")
            return []

        try:
            ftp = self._connect()
            ftp.set_pasv(True)

            if ftp_folder:
                self.logger.info(f"Browsing FTP folder: {ftp_folder}")
                all_files = self._list_files_in_folder(ftp, ftp_folder)
                filtered_files = self._filter_files_by_states(all_files)

                for file_name in filtered_files:
                    remote_file_path = f"{ftp_folder.rstrip('/')}/{file_name}"
                    local_path = self.download_dir / file_name
                    self.logger.info(f"Downloading {remote_file_path} to {local_path}")

                    try:
                        with open(local_path, "wb") as f:
                            ftp.retrbinary(f"RETR {remote_file_path}", f.write)
                        self.logger.info(f"Downloaded: {local_path}")
                        downloaded_files.append(local_path)
                    except Exception as e:
                        self.logger.error(f"Error downloading {remote_file_path}: {e}")
                        continue

            else:
                for remote_path in remote_paths:
                    if remote_path.startswith(('http://', 'https://')):
                        self.logger.info(f"Skipping HTTP URL (not FTP): {remote_path}")
                        continue

                    file_name = os.path.basename(remote_path)
                    local_path = self.download_dir / file_name
                    self.logger.info(f"Downloading {remote_path} to {local_path}")

                    try:
                        with open(local_path, "wb") as f:
                            ftp.retrbinary(f"RETR {remote_path}", f.write)
                        self.logger.info(f"Downloaded: {local_path}")
                        downloaded_files.append(local_path)
                    except Exception as e:
                        self.logger.error(f"Error downloading {remote_path}: {e}")
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
