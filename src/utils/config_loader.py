import json
import os
import re
from pathlib import Path
from dotenv import load_dotenv


class ConfigLoader:
    """Load and manage configuration from JSON and environment variables"""
    
    def __init__(self, config_file="config/config.json"):
        from utils.logger import ETLLogger
        self.logger = ETLLogger()
        
        # Load environment variables
        env_file = Path('config/.env')
        if env_file.exists():
            self.logger.info(f"Loading environment from: {env_file}")
            load_dotenv(dotenv_path=env_file)
        else:
            self.logger.info("No .env file found in config/, trying default locations")
            load_dotenv()
        
        self.config_file = Path(config_file)
        self.config = self._load_config()
        
    def _load_config(self):
        """Load configuration from JSON file"""
        if not self.config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        
        with open(self.config_file, 'r') as f:
            config = json.load(f)
        
        # Always read credentials from .env file only, never from config.json
        config['spaces']['access_key'] = os.getenv('SPACES_ACCESS_KEY', '')
        config['spaces']['secret_key'] = os.getenv('SPACES_SECRET_KEY', '')
        
        # FTP Configuration from environment variables
        config['ftp'] = {
            'host': os.getenv('FTP_HOSTNAME', config['ftp'].get('host', '')),  # Using FTP_HOSTNAME as in .env
            'username': os.getenv('FTP_USERNAME', ''),
            'password': os.getenv('FTP_PASSWORD', ''),
            'use_ftps': os.getenv('FTP_USE_FTPS', 'true').lower() == 'true',
            'port': int(os.getenv('FTP_PORT', '21')),
            'timeout': int(os.getenv('FTP_TIMEOUT', '30')),
            'retries': int(os.getenv('FTP_RETRIES', '3')),
            'save_mode': config['ftp'].get('save_mode', 'local')
        }
        
        # Ensure we have the required FTP credentials
        if not all([config['ftp']['host'], config['ftp']['username'], config['ftp']['password']]):
            self.logger.warning("FTP credentials not fully configured in environment variables")
        
        return config
    
    def get_states(self):
        """Get list of states to filter"""
        return self.config.get('states', [])
    
    def get_datasets(self):
        """Get list of datasets to process"""
        return self.config.get('datasets', [])
    
    def validate_ftp_credentials(self):
        """
        Validate FTP credentials and connection settings.
        Returns tuple of (is_valid, messages)
        """
        ftp_config = self.config.get('ftp', {})
        messages = []
        required_fields = {
            'host': 'FTP_HOSTNAME',
            'username': 'FTP_USERNAME',
            'password': 'FTP_PASSWORD'
        }
        
        # Check required fields
        missing = []
        for field, env_var in required_fields.items():
            if not ftp_config.get(field):
                missing.append(f"{field} (env: {env_var})")
                
        if missing:
            messages.append(f"Missing required FTP credentials: {', '.join(missing)}")
        
        # Validate host format
        host = ftp_config.get('host', '')
        if host and not self._is_valid_hostname(host):
            messages.append(f"Invalid FTP host format: {host}")
        
        # Check FTPS settings
        if ftp_config.get('use_ftps'):
            messages.append("FTPS is enabled (using secure connection)")
        else:
            messages.append("Warning: FTPS is disabled (using unsecure connection)")
        
        # Validate port
        port = ftp_config.get('port', 21)
        if not isinstance(port, int) or port < 1 or port > 65535:
            messages.append(f"Invalid port number: {port}")
        
        # Add configuration summary
        messages.append("\nFTP Configuration Summary:")
        messages.append(f"Host: {host}")
        messages.append(f"Username: {ftp_config.get('username', '')}")
        messages.append(f"Port: {port}")
        messages.append(f"FTPS: {ftp_config.get('use_ftps', True)}")
        messages.append(f"Timeout: {ftp_config.get('timeout', 30)}s")
        messages.append(f"Retries: {ftp_config.get('retries', 3)}")
        
        is_valid = len(missing) == 0 and self._is_valid_hostname(host)
        return is_valid, messages

    def _is_valid_hostname(self, hostname):
        """Validate hostname format"""
        if not hostname:
            return False
        if len(hostname) > 255:
            return False
        if hostname[-1] == ".":
            hostname = hostname[:-1]
        allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
        return all(allowed.match(x) for x in hostname.split("."))

    def test_ftp_connection(self, ftp_config):
        """
        Test FTP connection with the provided configuration
        Returns tuple of (success, message)
        """
        import ftplib
        import socket
        
        try:
            self.logger.info(f"Testing FTP connection to {ftp_config['host']}...")
            
            # Create appropriate FTP connection
            if ftp_config.get('use_ftps', True):
                ftp = ftplib.FTP_TLS()
                self.logger.info("Using FTPS (secure) connection")
            else:
                ftp = ftplib.FTP()
                self.logger.warning("Using unsecure FTP connection")
            
            # Set timeout
            timeout = ftp_config.get('timeout', 30)
            ftp.timeout = timeout
            
            # Connect and login
            self.logger.info(f"Connecting to {ftp_config['host']}:{ftp_config.get('port', 21)}")
            ftp.connect(
                host=ftp_config['host'],
                port=ftp_config.get('port', 21),
                timeout=timeout
            )
            
            self.logger.info(f"Logging in as {ftp_config['username']}")
            ftp.login(
                user=ftp_config['username'],
                passwd=ftp_config['password']
            )
            
            if ftp_config.get('use_ftps', True):
                self.logger.info("Securing data connection with PROT P")
                ftp.prot_p()
            
            # Test listing directory
            self.logger.info("Testing directory listing...")
            ftp.nlst()
            
            # Clean disconnect
            ftp.quit()
            self.logger.info("✓ FTP connection test successful")
            return True, "Connection test successful"
            
        except ftplib.error_perm as e:
            error_msg = f"FTP permission error: {str(e)}"
            self.logger.error(error_msg)
            if "530" in str(e):
                error_msg += " (Invalid username/password)"
            return False, error_msg
            
        except socket.timeout:
            error_msg = f"Connection timed out after {timeout} seconds"
            self.logger.error(error_msg)
            return False, error_msg
            
        except socket.gaierror as e:
            error_msg = f"DNS lookup failed: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
            
        except ftplib.error_temp as e:
            error_msg = f"Temporary FTP error: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
            
        except Exception as e:
            error_msg = f"FTP connection failed: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
            
        finally:
            try:
                if 'ftp' in locals():
                    ftp.close()
            except:
                pass

    def get_ftp_config(self):
        """
        Get FTP configuration with validation and connection testing
        """
        # First validate credentials format
        is_valid, messages = self.validate_ftp_credentials()
        
        # Log validation results
        if is_valid:
            self.logger.info("FTP configuration validated successfully")
        else:
            self.logger.warning("FTP configuration validation failed")
        
        for msg in messages:
            if msg.startswith("Warning"):
                self.logger.warning(msg)
            elif "Missing" in msg or "Invalid" in msg:
                self.logger.error(msg)
            else:
                self.logger.info(msg)
        
        # If credentials are valid, test connection
        ftp_config = self.config.get('ftp', {})
        if is_valid:
            connection_ok, message = self.test_ftp_connection(ftp_config)
            if not connection_ok:
                self.logger.error(f"FTP connection test failed: {message}")
                self.logger.info("Will attempt to proceed with uploads anyway")
        
        return ftp_config
    
    def get_spaces_config(self):
        """Get DigitalOcean Spaces configuration"""
        return self.config.get('spaces', {})
    
    def get_schedule_time(self):
        """Get daily schedule time"""
        return self.config.get('schedule', {}).get('daily_time', '02:00')
    
    def get_working_directories(self):
        """Get working directory paths"""
        return self.config.get('directories', {})
    
    def get_state_code_column(self):
        """Get the column name for state code filtering"""
        return self.config.get('filter_column', 'SitusStateCode')
    
    def get_file_delimiter(self):
        """Get the file delimiter (default: tab)"""
        return self.config.get('file_delimiter', '\t')
        
    def get_dataset_zip_path(self, dataset_name):
        """Get custom ZIP path for a dataset if configured.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Path string if configured in dataset, None otherwise
        """
        for dataset in self.get_datasets():
            if dataset.get('name') == dataset_name:
                return dataset.get('zip_path')
