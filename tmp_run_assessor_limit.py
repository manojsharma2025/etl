from pathlib import Path
import sys

# ensure src on path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from etl_pipeline import AttomETLPipeline

# locate sample TAXASSESSOR zips
root = Path('.')
sample_zips = sorted((root / 'data' / 'downloads').glob('*TAXASSESSOR*.zip'))
if not sample_zips:
    print('No TAXASSESSOR sample zip found in data/downloads. Create one first.')
    raise SystemExit(1)

N = 1
selected = sample_zips[:N]
print(f'Processing first {N} TAXASSESSOR zip(s):', selected)

pipeline = AttomETLPipeline()

# find assessor config
assessor_cfg = None
for ds in pipeline.datasets:
    if ds.get('name') == 'Assessor':
        assessor_cfg = ds
        break

if not assessor_cfg:
    print('Assessor dataset not found in config')
    raise SystemExit(1)

# monkeypatch downloader to return only selected files
pipeline.downloader.download_dataset = lambda cfg: [Path(p) for p in selected]

# dummy uploader to avoid needing spaces
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

# run
result = pipeline.process_dataset(assessor_cfg)
print('\nResult:')
print(result)

# list filtered outputs
from pathlib import Path
p = Path('data/filtered')
print('\nFiltered folder contents:')
if p.exists():
    for f in sorted(p.iterdir()):
        print(f.name, f.stat().st_size)
else:
    print('data/filtered does not exist')
