import json
import os
from pathlib import Path
from dotenv import load_dotenv


class ConfigLoader:
    """Load and manage configuration from JSON and environment variables"""
    
    def __init__(self, config_file="config/config.json"):
        env_file = Path('config/.env')
        if env_file.exists():
            load_dotenv(dotenv_path=env_file)
        else:
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
        
        config['ftp']['host'] = os.getenv('FTP_HOSTNAME', config['ftp'].get('host', ''))
        config['ftp']['username'] = os.getenv('FTP_USERNAME', '')
        config['ftp']['password'] = os.getenv('FTP_PASSWORD', '')
        config['ftp']['use_ftps'] = os.getenv('FTP_USE_FTPS', 'true').lower() == 'true'
        
        return config
    
    def get_states(self):
        """Get list of states to filter"""
        return self.config.get('states', [])
    
    def get_datasets(self):
        """Get list of datasets to process"""
        return self.config.get('datasets', [])
    
    def get_ftp_config(self):
        """Get FTP configuration"""
        return self.config.get('ftp', {})
    
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
