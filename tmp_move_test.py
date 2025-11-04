from pathlib import Path
import zipfile
import sys

# Ensure repo src on path
sys.path.insert(0, str(Path('.').resolve() / 'src'))
from etl_pipeline import AttomETLPipeline

# Create small test ZIP
downloads = Path('data/downloads')
downloads.mkdir(parents=True, exist_ok=True)
zip_path = downloads / '1PARKPLACE_TAXASSESSOR_TEST1.zip'

txt_name = '1PARKPLACE_TAXASSESSOR_TEST1.txt'
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
    content = 'id\tSitusStateCode\tval\n'
    content += '1\tTX\ta\n'
    content += '2\tCA\tb\n'
    z.writestr(txt_name, content)

print('Created test ZIP:', zip_path)

pipeline = AttomETLPipeline()
# monkeypatch downloader
pipeline.downloader.download_dataset = lambda cfg: [zip_path]

cfg = {
    'name': 'Assessor',
    'enabled': True,
    'ftp_folder': '/Outgoing',
    'filtered_zip_prefix': '1PP_FILTERED_',
    'exstates': ['TX'],
    'filter_ftp_upload': True,
    'post_process_filtered': 'move',
    'post_process_downloaded': 'move'
}

res = pipeline.process_dataset(cfg)
print('Result:', res)

print('\nProcessed dir contents:')
for p in sorted(Path('data/processed').glob('*')):
    print(p.name, p.stat().st_size)

print('\nFiltered dir contents:')
for p in sorted(Path('data/filtered').glob('*')):
    print(p.name, p.stat().st_size)
