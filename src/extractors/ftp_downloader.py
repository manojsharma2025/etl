import os
import ftplib
import socket
import time
import requests
from pathlib import Path
from urllib.parse import urlparse

class FTPDownloader:
    """Handles downloading files from ATTOM FTPS/FTP servers.

    Supports optional uploading of downloaded files to a DigitalOcean Spaces
    instance via a provided `spaces_uploader`. The download/save behaviour
    can be controlled with a `save_mode` setting: 'local' (default), 'both',
    or 'remote'. When 'both' the file is kept locally and uploaded; when
    'remote' the file can optionally be deleted locally after upload if
    `delete_local_after_upload` is True in dataset config.
    """

    def __init__(self, logger, ftp_config, states=None, spaces_uploader=None):
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
        # Optional Spaces uploader instance (instance of SpacesUploader)
        self.spaces_uploader = spaces_uploader

        # Default save mode can be provided in ftp_config, but datasets may override
        # Accepted values: 'local', 'both', 'remote'
        self.default_save_mode = ftp_config.get('save_mode', 'local')

    def _connect(self):
        """Connect to FTP or FTPS server."""
        server = self.ftp_config.get('host')
        username = self.ftp_config.get('username')
        password = self.ftp_config.get('password')
        use_ftps = self.ftp_config.get('use_ftps', True)

        if not server or not username or not password:
            raise ValueError("Missing FTP configuration values")

        self.logger.info(f"Connecting to {'FTPS' if use_ftps else 'FTP'} server: {server}")

        # Try FTPS first (with timeout). If FTPS handshake fails (network/SSL
        # issues), fall back to plain FTP to avoid hanging indefinitely.
        timeout = int(self.ftp_config.get('timeout', 30))

        if use_ftps:
            try:
                ftp = ftplib.FTP_TLS(host=server, timeout=timeout)
                ftp.login(username, password)
                ftp.prot_p()  # Secure data connection
                self.logger.info("Using FTPS (secure) connection")
                return ftp
            except Exception as e:
                self.logger.warning(f"FTPS connection failed ({e}), falling back to plain FTP")

        # Plain FTP fallback
        ftp = ftplib.FTP(host=server, timeout=timeout)
        ftp.login(username, password)
        self.logger.info("Using plain FTP connection")

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
            # Log a sample of returned file names to help debugging when root contains many files
            sample = files[:50]
            if sample:
                self.logger.info(f"Sample files: {', '.join(sample[:10])}{'...' if len(sample)>10 else ''}")
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

    def _filename_has_excluded_state(self, file_name, dataset_config):
        """
        Check if the provided filename contains any state listed in the
        dataset's `exclude_states` list. Matching is done against filename
        parts (case-insensitive) and returns True when an excluded state is found.
        """
        exclude_states = [s.upper() for s in dataset_config.get('exclude_states', [])]
        if not exclude_states:
            return False

        file_upper = file_name.upper()
        parts = file_upper.replace('.', '_').replace('-', '_').split('_')
        for p in parts:
            if p in exclude_states:
                return True
        return False

    def _filter_files_by_dataset(self, files, dataset_config):
        """
        Filter files by parser type and optionally by state codes.

        File pattern: PREFIX_STATECODE_PARSER_SERIES_SEQUENCE.zip
        Example: 1PARKPLACE_CA_RECORDER_0001_001.zip

        Dataset config may include:
          - 'ignore_states_for_download': bool (if True, match parser only)
          - 'parser_keyword': str (override parser keyword detection)

        Args:
            files: List of file names
            dataset_config: Dataset configuration dict (used to derive dataset name and flags)

        Returns:
            List of files matching parser (and optionally state codes)
        """
        dataset_name = dataset_config.get('name')
        ignore_states = bool(dataset_config.get('ignore_states_for_download', False))
        parser_keyword = (dataset_config.get('parser_keyword') or self._get_parser_keyword(dataset_name)).upper()

        # Prepare uppercase states list for comparisons
        states_upper = [s.upper() for s in self.states]
        # Per-dataset opt-in to accept parser-only files when no state token is present
        allow_parser_only = bool(dataset_config.get('allow_parser_only', False))

        filtered = []
        self.logger.info(f"Filtering files for dataset '{dataset_name}' (parser: {parser_keyword}) ignore_states={ignore_states}")

        for file in files:
            file_upper = file.upper()
            parts = file_upper.replace('.', '_').replace('-', '_').split('_')

            # Determine parser presence: either a dedicated part equals parser_keyword
            # or the parser keyword appears anywhere in the filename
            parser_in_parts = parser_keyword in parts
            parser_in_name = parser_keyword in file_upper

            # Determine state presence: check if any part equals any configured state
            state_found = None
            for p in parts:
                if p in states_upper:
                    state_found = p
                    break

            # Per-dataset exclusion list for states (e.g., exclude CA)
            exclude_states_upper = [s.upper() for s in dataset_config.get('exclude_states', [])]
            if state_found and state_found in exclude_states_upper:
                self.logger.info(f"Skipping {file} because state {state_found} is excluded for dataset {dataset_name}")
                continue

            # If ignoring states, require only parser match
            if ignore_states:
                if parser_in_parts or parser_in_name:
                    filtered.append(file)
                    self.logger.info(f"Matched (parser only): {file}")
                continue

            # Otherwise prefer parser+state match. If parser is present but no state
            # token is found, only accept the file when the dataset explicitly
            # allows parser-only matches via `allow_parser_only`.
            if (parser_in_parts or parser_in_name):
                if state_found:
                    filtered.append(file)
                    self.logger.info(f"Matched: {file} (state={state_found}, parser present)")
                elif allow_parser_only:
                    filtered.append(file)
                    self.logger.info(f"Matched (parser present, no state token, allow_parser_only=True): {file}")

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
        # Deprecated thin download — kept for compatibility but prefer the
        # resumable version implemented below.
        return self._download_http_file_with_retries(url, local_path)

    def _download_http_file_with_retries(self, url, local_path, retries=3, timeout=300):
        """Download an HTTP/HTTPS file with retries and resume support.

        Uses a temporary .part file and Range requests to resume interrupted
        downloads when supported by the server.
        """
        self.logger.info(f"Downloading via HTTP (resumable): {url}")
        # Create parent directory if it doesn't exist
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        temp_path = local_path.with_name(local_path.name + '.part')
        attempt = 0

        while attempt < retries:
            attempt += 1
            try:
                headers = {}
                mode = 'wb'
                existing_size = 0
                if temp_path.exists():
                    existing_size = temp_path.stat().st_size
                    if existing_size > 0:
                        headers['Range'] = f'bytes={existing_size}-'
                        mode = 'ab'

                with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
                    r.raise_for_status()
                    # If server returned 200 for a Range request, we should
                    # handle accordingly; we simply append when mode=='ab'.
                    with open(temp_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                # rename temp to final
                if temp_path.exists():
                    try:
                        temp_path.replace(local_path)
                        self.logger.info(f"Successfully downloaded via HTTP: {local_path} (size={local_path.stat().st_size} bytes)")
                        return True
                    except Exception as rename_error:
                        self.logger.error(f"Failed to rename temp file {temp_path} to {local_path}: {rename_error}")
                        return False
                else:
                    self.logger.error(f"Temp file missing after download: {temp_path}")
                    return False

            except Exception as e:
                self.logger.warning(f"HTTP download attempt {attempt}/{retries} failed for {url}: {e}")
                # wait a bit before retrying
                time.sleep(min(5 * attempt, 30))
                continue

        self.logger.error(f"HTTP download failed after {retries} attempts: {url}")
        # cleanup temp? keep for resume later
        return False

    def _upload_to_spaces(self, local_path, dataset_name, parser_keyword, make_public=False):
        """Upload a single local file to Spaces under outgoing/<parser>/<dataset>.

        Returns the public URL on success, or raises on failure.
        """
        if not self.spaces_uploader:
            raise RuntimeError("Spaces uploader not configured")
        local_file = Path(local_path)
        # Use outgoing/<parser>/<dataset> as requested (no leading slash)
        remote_prefix = f"outgoing/{parser_keyword}/{dataset_name}"
        remote_path = f"{remote_prefix}/{local_file.name}"

        self.logger.info(f"Uploading {local_file.name} to Spaces at {remote_path}")
        url = self.spaces_uploader.upload_file(local_file, remote_path, make_public=make_public)
        return url

    def _get_http_remote_size(self, url, timeout=30):
        try:
            r = requests.head(url, allow_redirects=True, timeout=timeout)
            if 'Content-Length' in r.headers:
                return int(r.headers.get('Content-Length'))
        except Exception:
            pass
        return None

    def _get_ftp_remote_size(self, remote_name, ftp_folder='/', timeout=None):
        try:
            ftp = self._connect()
            if timeout:
                try:
                    if hasattr(ftp, 'sock') and ftp.sock:
                        ftp.sock.settimeout(timeout)
                except Exception:
                    pass
            try:
                ftp.cwd(ftp_folder)
            except Exception:
                pass
            size = None
            try:
                size = ftp.size(remote_name)
            except Exception:
                # some servers may not support SIZE on directories; ignore
                size = None
            try:
                ftp.quit()
            except Exception:
                pass
            return size
        except Exception:
            return None

    def _download_ftp_file_with_retries(self, remote_name, local_path, ftp_folder='/', retries=3, blocksize=8192):
        """Download a file from FTP with resume and retries.

        remote_name: filename relative to current ftp cwd or absolute
        local_path: Path to save file locally
        ftp_folder: folder to cwd into before downloading (used on reconnect)
        """
        temp_path = local_path.with_name(local_path.name + '.part')
        attempt = 0

        # allow override of retries via config
        cfg_retries = int(self.ftp_config.get('retries', self.ftp_config.get('ftp_retries', retries)))
        use_retries = cfg_retries if cfg_retries and cfg_retries > 0 else retries

        # larger default blocksize for faster transfers
        blocksize = int(self.ftp_config.get('blocksize', blocksize))

        while attempt < use_retries:
            attempt += 1
            ftp = None
            try:
                ftp = self._connect()
                ftp.set_pasv(True)
                # ensure socket timeout is reasonable for long transfers
                sock_timeout = int(self.ftp_config.get('transfer_timeout', self.ftp_config.get('timeout', 60)))
                try:
                    if hasattr(ftp, 'sock') and ftp.sock:
                        ftp.sock.settimeout(sock_timeout)
                except Exception:
                    pass

                try:
                    ftp.cwd(ftp_folder)
                except Exception:
                    # ignore if cannot cwd
                    pass

                existing_size = temp_path.stat().st_size if temp_path.exists() else 0

                if existing_size > 0:
                    self.logger.info(f"Resuming FTP download for {remote_name} at byte {existing_size}")
                    mode = 'ab'
                    rest = existing_size
                else:
                    mode = 'wb'
                    rest = None

                bytes_written = existing_size

                with open(temp_path, mode) as f:
                    def callback(data):
                        nonlocal bytes_written
                        f.write(data)
                        bytes_written += len(data)
                        # log periodically
                        if bytes_written % (1024*1024) < len(data):
                            self.logger.info(f"Downloading {remote_name}: {bytes_written} bytes")

                    # Use retrbinary with rest to resume; many servers support REST
                    ftp.retrbinary(f"RETR {remote_name}", callback, blocksize, rest)

                # move temp to final
                temp_path.replace(local_path)
                if ftp:
                    try:
                        ftp.quit()
                    except Exception:
                        pass
                self.logger.info(f"Downloaded via FTP: {local_path}")
                return True

            except (socket.timeout, ftplib.error_temp, OSError) as e:
                self.logger.warning(f"FTP download attempt {attempt}/{use_retries} failed for {remote_name}: {e}")
                try:
                    if ftp:
                        ftp.quit()
                except Exception:
                    pass
                # exponential backoff (cap 60s)
                time.sleep(min(5 * attempt, 60))
                continue
            except Exception as e:
                self.logger.warning(f"FTP download attempt {attempt}/{use_retries} failed for {remote_name}: {e}")
                try:
                    if ftp:
                        ftp.quit()
                except Exception:
                    pass
                time.sleep(min(5 * attempt, 60))
                continue

        self.logger.error(f"FTP download failed after {use_retries} attempts: {remote_name}")
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
        ftp_folder = dataset_config.get('ftp_folder', '/') #/Outgoing
        downloaded_files = []

        # Precompute existing filenames in download directory so we strictly
        # perform filename-only checks (avoid size comparisons which can be
        # unreliable across OS/filesystems). This set contains only names
        # (not paths) and is used to skip downloads when a file with the same
        # filename already exists locally.
        try:
            # Store lowercase filenames for case-insensitive matching which is
            # more robust across platforms where case-sensitivity may differ.
            existing_names = {p.name.lower() for p in self.download_dir.iterdir() if p.is_file()}
        except Exception:
            existing_names = set()

        # Determine save mode for this dataset (dataset overrides ftp default)
        save_mode = dataset_config.get('save_mode', self.default_save_mode)
        delete_local_after_upload = dataset_config.get('delete_local_after_upload', False)

        if remote_urls:
            self.logger.info(f"Downloading from direct URLs for dataset: {dataset_name}")
            for url in remote_urls:
                file_name = os.path.basename(urlparse(url).path)
                local_path = self.download_dir / file_name

                # Strict filename-only skip: if a file with the same name
                # exists in the downloads directory (case-insensitive), skip downloading.
                if file_name.lower() in existing_names:
                    self.logger.info(f"Skipping download (filename exists): {local_path} (case-insensitive match)")
                    downloaded_files.append(local_path)
                    continue

                # Guard: if dataset produced filtered zips and those are uploaded
                # to an outgoing FTP folder, avoid re-downloading those filtered
                # zips back into the downloads directory. This prevents the
                # upload->download loop when outgoing and incoming folders overlap.
                filtered_prefix = dataset_config.get('filtered_zip_prefix')
                if filtered_prefix:
                    try:
                        if file_name.upper().startswith(filtered_prefix.upper()):
                            self.logger.info(f"Skipping download of filtered/outgoing file: {file_name} (matches filtered_zip_prefix)")
                            continue
                    except Exception:
                        # Be defensive: if something odd happens, fall through to normal behaviour
                        pass

                # Additional guard: skip files that are explicitly excluded by dataset
                if self._filename_has_excluded_state(file_name, dataset_config):
                    self.logger.info(f"Skipping download because filename contains excluded state for dataset {dataset_name}: {file_name}")
                    continue

                if url.startswith(('http://', 'https://')):
                    # proceed to download (resumable)
                    if self._download_http_file(url, local_path):
                        downloaded_files.append(local_path)
                        # Optionally upload to Spaces
                        if self.spaces_uploader and save_mode in ('both', 'remote'):
                            try:
                                parser_keyword = self._get_parser_keyword(dataset_name)
                                self._upload_to_spaces(local_path, dataset_name, parser_keyword)
                                if save_mode == 'remote' and delete_local_after_upload:
                                    try:
                                        local_path.unlink()
                                        self.logger.info(f"Deleted local file after upload: {local_path}")
                                    except Exception:
                                        self.logger.warning(f"Failed to delete local file: {local_path}")
                            except Exception as e:
                                self.logger.error(f"Upload to Spaces failed for {local_path}: {e}")
                elif url.startswith('ftp://'):
                    parsed = urlparse(url)
                    ftp_path = parsed.path
                    self.logger.info(f"Downloading FTP file: {ftp_path}")
                    try:
                        # Use the resumable FTP helper which will create its own
                        # connection and attempt retries. Split the ftp_path into
                        # folder and filename so the helper can cwd appropriately.
                        ftp_folder_for_download = os.path.dirname(ftp_path) or '/'
                        remote_name = os.path.basename(ftp_path)

                        # Additional guard: skip files that are explicitly excluded by dataset
                        if self._filename_has_excluded_state(file_name, dataset_config):
                            self.logger.info(f"Skipping download because filename contains excluded state for dataset {dataset_name}: {file_name}")
                            continue

                        # For FTP URLs, also check filename-only existence using
                        # the precomputed set (case-insensitive).
                        if file_name.lower() in existing_names:
                            self.logger.info(f"Skipping download (filename exists): {local_path} (case-insensitive match)")
                            downloaded_files.append(local_path)
                        else:
                            success = self._download_ftp_file_with_retries(remote_name, local_path, ftp_folder=ftp_folder_for_download)
                            if success:
                                downloaded_files.append(local_path)
                            else:
                                self.logger.error(f"Failed to download FTP file after retries: {ftp_path}")
                        # Optionally upload to Spaces
                        if self.spaces_uploader and save_mode in ('both', 'remote'):
                            try:
                                parser_keyword = self._get_parser_keyword(dataset_name)
                                self._upload_to_spaces(local_path, dataset_name, parser_keyword)
                                if save_mode == 'remote' and delete_local_after_upload:
                                    try:
                                        local_path.unlink()
                                        self.logger.info(f"Deleted local file after upload: {local_path}")
                                    except Exception:
                                        self.logger.warning(f"Failed to delete local file: {local_path}")
                            except Exception as e:
                                self.logger.error(f"Upload to Spaces failed for {local_path}: {e}")
                    except Exception as e:
                        self.logger.error(f"FTP download failed for {url}: {e}")
                else:
                    self.logger.warning(f"Unknown URL scheme: {url}")
            
            return downloaded_files

        self.logger.info(f"Browsing FTP folder for dataset: {dataset_name}")
        try:
            ftp = self._connect()
            ftp.set_pasv(True)
            
            # Try to list only files that include the parser keyword to avoid huge root listings
            try:
                ftp.cwd(ftp_folder)
            except Exception as e:
                self.logger.warning(f"Failed to change to FTP folder {ftp_folder}: {e}")

            dataset_name = dataset_config.get('name')
            parser_keyword = (dataset_config.get('parser_keyword') or self._get_parser_keyword(dataset_name)).upper()

            all_files = []
            try:
                # NLST pattern search (may be supported by the server) to find parser files quickly
                pattern = f"*{parser_keyword}*"
                self.logger.info(f"Listing FTP files with pattern: {pattern}")
                all_files = ftp.nlst(pattern)
                self.logger.info(f"Found {len(all_files)} files matching pattern {pattern}")
                if all_files:
                    # pass through filter for any additional checks (states, etc.)
                    filtered_files = self._filter_files_by_dataset(all_files, dataset_config)
                else:
                    # fallback to full listing if pattern returned nothing
                    self.logger.info("Pattern listing returned nothing; falling back to full listing")
                    all_files = self._list_files_in_folder(ftp, ftp_folder)
                    filtered_files = self._filter_files_by_dataset(all_files, dataset_config)
            except Exception as e:
                self.logger.warning(f"Pattern listing failed ({e}); falling back to full listing")
                all_files = self._list_files_in_folder(ftp, ftp_folder)
                filtered_files = self._filter_files_by_dataset(all_files, dataset_config)

            for file_name in filtered_files:
                local_path = self.download_dir / file_name
                self.logger.info(f"Downloading {ftp_folder}/{file_name} to {local_path}")

                try:
                    # Guard: avoid downloading filtered zip files that were
                    # produced by this pipeline and uploaded to the outgoing
                    # FTP folder. Use the dataset's filtered_zip_prefix config
                    # when present.
                    filtered_prefix = dataset_config.get('filtered_zip_prefix')
                    if filtered_prefix:
                        try:
                            if file_name.upper().startswith(filtered_prefix.upper()):
                                self.logger.info(f"Skipping download of filtered/outgoing file: {file_name} (matches filtered_zip_prefix)")
                                continue
                        except Exception:
                            pass
                    # Filename-only existence check (use precomputed set, case-insensitive).
                    # Additional guard: skip files that are explicitly excluded by dataset
                    if self._filename_has_excluded_state(file_name, dataset_config):
                        self.logger.info(f"Skipping download because filename contains excluded state for dataset {dataset_name}: {file_name}")
                        continue

                    if file_name.lower() in existing_names:
                        # NOTE: Previously we compared remote size vs local size and
                        # re-downloaded when they differed. To match current request
                        # behaviour, skip downloads when a file with the same name
                        # already exists locally (filename-only match).
                        #
                        # Uncomment the block below to re-enable size-based checks
                        # try:
                        #     remote_size = ftp.size(file_name)
                        #     local_size = local_path.stat().st_size
                        #     if remote_size is not None and remote_size == local_size:
                        #         self.logger.info(f"Skipping download, already exists and size matches: {local_path}")
                        #         downloaded_files.append(local_path)
                        #         continue
                        #     else:
                        #         self.logger.info(f"Local file exists but size differs (local={local_size} remote={remote_size}); re-downloading: {local_path}")
                        # except Exception as e:
                        #     self.logger.warning(f"Could not determine remote size for {file_name}: {e}")

                        self.logger.info(f"Skipping download (filename exists): {local_path} (case-insensitive match)")
                        downloaded_files.append(local_path)
                        continue

                    success = self._download_ftp_file_with_retries(file_name, local_path, ftp_folder=ftp_folder)
                    if success:
                        self.logger.info(f"Downloaded: {local_path}")
                        downloaded_files.append(local_path)
                    else:
                        self.logger.error(f"Failed to download: {file_name}")
                        continue
                    # Optionally upload to Spaces
                    if self.spaces_uploader and save_mode in ('both', 'remote'):
                        try:
                            parser_keyword = self._get_parser_keyword(dataset_name)
                            self._upload_to_spaces(local_path, dataset_name, parser_keyword)
                            if save_mode == 'remote' and delete_local_after_upload:
                                try:
                                    local_path.unlink()
                                    self.logger.info(f"Deleted local file after upload: {local_path}")
                                except Exception:
                                    self.logger.warning(f"Failed to delete local file: {local_path}")
                        except Exception as e:
                            self.logger.error(f"Upload to Spaces failed for {local_path}: {e}")
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
