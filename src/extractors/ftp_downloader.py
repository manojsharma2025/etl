import os
import ftplib
import socket
import time
from pathlib import Path

class FTPDownloader:
    """Handles downloading files from ATTOM FTPS/FTP servers."""

    def __init__(self, logger, ftp_config):
        """
        Initialize the FTP downloader.

        Args:
            logger: ETLLogger instance for consistent logging
            ftp_config: dict containing FTP credentials and settings
        """
        self.logger = logger
        self.ftp_config = ftp_config
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

    def download_dataset(self, dataset_config):
        """
        Download files for a dataset.

        Args:
            dataset_config: dict with dataset configuration
        Returns:
            List of downloaded file paths
        """
        dataset_name = dataset_config.get('name')
        remote_paths = dataset_config.get('urls', [])
        downloaded_files = []

        if not remote_paths:
            self.logger.warning(f"No URLs provided for dataset {dataset_name}")
            return []

        try:
            ftp = self._connect()
            ftp.set_pasv(True)

            for remote_path in remote_paths:
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
