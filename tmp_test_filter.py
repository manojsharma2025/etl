import sys
sys.path.insert(0, r'c:\attomdataapps\src')

from extractors.ftp_downloader import FTPDownloader
from utils.logger import ETLLogger

# Minimal logger that writes to console
logger = ETLLogger(log_dir='logs_test')

# Create downloader with some configured states
downloader = FTPDownloader(logger, {'download_dir': 'data/downloads'}, states=['TX', 'CA', 'FL'])

# Simulated FTP root filenames
files = [
    '1PARKPLACE_TAXASSESSOR_0018.zip',
    '1PARKPLACE_CA_RECORDER_0001_001.zip',
    'TAXASSESSOR_NATIONAL.zip',
    'SOMETHING_2018.zip',
    '2CITY_TX_TAXASSESSOR_0002.zip'
]

# Dataset config for Assessor (no explicit parser_keyword so it will use parser_map)
dataset_config = {
    'name': 'Assessor',
    'ftp_folder': '/',
    'ignore_states_for_download': False,
    'exclude_states': ['CA']
}

matched = downloader._filter_files_by_dataset(files, dataset_config)
print('Matched files:')
for m in matched:
    print(' -', m)

# Also test with ignore_states_for_download=True
print('\nWith ignore_states_for_download=True:')
dataset_config['ignore_states_for_download'] = True
matched2 = downloader._filter_files_by_dataset(files, dataset_config)
for m in matched2:
    print(' -', m)
