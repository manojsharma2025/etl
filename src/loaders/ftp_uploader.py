import ftplib
import socket
import time
from pathlib import Path


class FTPUploader:
    """Simple FTP/FTPS uploader used to push filtered ZIPs back to an FTP outgoing folder.

    Uses ftplib.FTP or ftplib.FTP_TLS depending on `use_ftps` flag. Exposes
    upload_file and upload_multiple_files for integration with the ETL pipeline.
    """

    def __init__(self, logger, ftp_config: dict):
        self.logger = logger
        self.host = ftp_config.get('host')
        self.username = ftp_config.get('username')
        self.password = ftp_config.get('password')
        self.use_ftps = ftp_config.get('use_ftps', True)
        self.port = ftp_config.get('port') or (21)
        self.timeout = ftp_config.get('timeout', 30)
        # Retries/backoff configuration
        # ftp_config may include 'retries' and 'backoff_base' keys. If not
        # provided, sensible defaults are used.
        self.retries = int(ftp_config.get('retries', 3))
        self.backoff_base = float(ftp_config.get('backoff_base', 5.0))

        if not self.host:
            raise ValueError('FTP host must be provided for FTPUploader')

        self.logger.info(f"Initialized FTPUploader (host={self.host}, ftps={self.use_ftps})")

    def _connect(self):
        if self.use_ftps:
            ftp = ftplib.FTP_TLS()
        else:
            ftp = ftplib.FTP()

        ftp.connect(self.host, self.port, timeout=self.timeout)
        ftp.login(self.username, self.password)

        if self.use_ftps:
            # secure the data connection
            try:
                ftp.prot_p()
            except Exception:
                # Not all servers require/allow PROT P; ignore if it fails
                pass

        ftp.set_pasv(True)
        return ftp

    def _ensure_remote_dir(self, ftp, remote_dir: str):
        # Try to change into remote_dir. Do NOT create directories on the
        # remote server — the user requested the outgoing folder will already
        # exist. If cwd fails, log a warning and continue; upload will then
        # use the server's default working directory.
        try:
            ftp.cwd(remote_dir)
            return True
        except Exception as e:
            # Don't attempt to create the directory — just warn and proceed.
            self.logger.warning(f"Could not change into remote dir '{remote_dir}': {e}.\n" \
                                "Not creating directories per configuration; will attempt full-path STOR or upload to server default directory.")
            return False

    def upload_file(self, local_path: Path, remote_folder: str = '/', remote_name: str = None):
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        remote_name = remote_name or local_path.name

        attempt = 0
        last_exc = None
        # Build the exception tuple for except clauses safely
        except_types = ftplib.all_errors + (socket.error, RuntimeError)

        while attempt < max(1, self.retries):
            attempt += 1
            ftp = None
            try:
                ftp = self._connect()
                # change to root then ensure remote folder
                try:
                    ftp.cwd('/')
                except Exception:
                    pass

                did_cwd = False
                if remote_folder:
                    did_cwd = self._ensure_remote_dir(ftp, remote_folder)

                with open(local_path, 'rb') as f:
                    self.logger.info(f"Uploading {local_path.name} to FTP {self.host}:{remote_folder}/{remote_name} (attempt {attempt}/{self.retries})")
                    verified = False
                    if did_cwd:
                        # We are in the desired folder — store by name
                        ftp.storbinary(f'STOR {remote_name}', f)
                        # attempt a quick verification using SIZE
                        try:
                            size = ftp.size(remote_name)
                            if size is not None:
                                self.logger.info(f"Verified remote upload (size={size}): {remote_folder}/{remote_name}")
                                verified = True
                        except Exception:
                            pass
                    else:
                        # Try full-path STOR which some servers accept
                        full_path = f"{remote_folder.rstrip('/')}/{remote_name}" if remote_folder else remote_name
                        try:
                            ftp.storbinary(f'STOR {full_path}', f)
                            # verify by asking SIZE for the full path
                            try:
                                size = ftp.size(full_path)
                                if size is not None:
                                    self.logger.info(f"Verified remote upload (size={size}): {full_path}")
                                    verified = True
                            except Exception:
                                # if SIZE fails, try listing the remote folder
                                try:
                                    listing = ftp.nlst(remote_folder)
                                    found = any(remote_name == Path(l).name or remote_name == l for l in listing)
                                    if found:
                                        self.logger.info(f"Verified remote upload by NLST: {remote_folder}/{remote_name}")
                                        verified = True
                                except Exception:
                                    pass
                        except Exception:
                            # Fall back to storing in the server's current/default directory
                            f.seek(0)
                            ftp.storbinary(f'STOR {remote_name}', f)
                            try:
                                size = ftp.size(remote_name)
                                if size is not None:
                                    self.logger.info(f"Verified remote upload (size={size}): {remote_name} in default dir")
                                    verified = True
                            except Exception:
                                pass

                # Clean quit and return on success
                try:
                    if ftp:
                        ftp.quit()
                except Exception:
                    pass

                if verified:
                    return True
                else:
                    # If not verified, raise to trigger retry logic
                    raise RuntimeError('Upload completed but verification failed')

            except except_types as e:
                last_exc = e
                self.logger.warning(f"FTP upload attempt {attempt} failed for {local_path}: {e}")
                try:
                    if ftp:
                        ftp.quit()
                except Exception:
                    pass

                if attempt < self.retries:
                    backoff = self.backoff_base * (2 ** (attempt - 1))
                    self.logger.info(f"Retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                else:
                    self.logger.error(f"FTP upload failed after {attempt} attempts for {local_path}: {last_exc}")
                    return False

    def upload_multiple_files(self, paths, remote_folder: str = '/'):
        results = []
        for p in paths:
            ok = self.upload_file(Path(p), remote_folder=remote_folder)
            results.append({'path': str(p), 'success': ok})
        return results
