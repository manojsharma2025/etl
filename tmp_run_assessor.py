from pathlib import Path
import sys

# Ensure src is on sys.path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from etl_pipeline import AttomETLPipeline
from utils.logger import ETLLogger

# Prepare
root = Path('.')
sample_zip = list((root / 'data' / 'downloads').glob('*TAXASSESSOR*.zip'))
if not sample_zip:
    print('No TAXASSESSOR sample zip found in data/downloads. Create one first.')
    raise SystemExit(1)

print('Found sample zips:', sample_zip)

# Initialize pipeline
pipeline = AttomETLPipeline()

# Find Assessor dataset from pipeline config
assessor_cfg = None
for ds in pipeline.datasets:
    if ds.get('name') == 'Assessor':
        assessor_cfg = ds
        break

if not assessor_cfg:
    print('Assessor dataset not found in config')
    raise SystemExit(1)

# Monkeypatch downloader to return our sample zip(s)
pipeline.downloader.download_dataset = lambda cfg: [Path(p) for p in sample_zip]

# Dummy uploader to avoid needing Spaces credentials
class DummyUploader:
    def __init__(self, logger):
        self.logger = logger
    def upload_multiple_files(self, file_paths):
        urls = []
        for p in file_paths:
            self.logger.info(f"(Dummy upload) Would upload: {p}")
            urls.append(f"file://{p}")
        return urls
    def upload_log_file(self, path):
        self.logger.info(f"(Dummy) upload log: {path}")
        return f"file://{path}"

pipeline.uploader = DummyUploader(pipeline.logger)

# Run processing for Assessor only
result = pipeline.process_dataset(assessor_cfg)
print('\nProcess result:')
print(result)
