import sys
sys.path.insert(0, r'c:\attomdataapps\src')
from extractors.ftp_downloader import FTPDownloader
from utils.logger import ETLLogger
from utils.config_loader import ConfigLoader

logger = ETLLogger(log_dir='logs_test')
cfg_loader = ConfigLoader('config/config.json')
ftp_config = cfg_loader.get_ftp_config()
ftp_config['timeout'] = ftp_config.get('timeout', 15)

# Instantiate downloader (no states needed for listing)
downloader = FTPDownloader(logger, ftp_config, states=cfg_loader.get_states())

try:
    ftp = downloader._connect()
    print('PWD:', ftp.pwd())
    try:
        files = downloader._list_files_in_folder(ftp, '/')
        print('Listed files count:', len(files))
        print('Sample:', files[:20])
    except Exception as e:
        print('Listing exception:', e)
    ftp.quit()
except Exception as e:
    print('Error during listing:', e)
