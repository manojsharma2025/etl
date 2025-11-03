import sys
from pathlib import Path

# Ensure repo root is on path
repo_root = Path(__file__).resolve().parent
# Ensure repo root and src directory are on path so imports like `utils` and `src` resolve
sys.path.insert(0, str(repo_root))
sys.path.insert(0, str(repo_root / 'src'))

from etl_pipeline import AttomETLPipeline
from datetime import datetime


def run_test():
    pipeline = AttomETLPipeline()

    # Use an existing ZIP in data/downloads; monkeypatch downloader to return it
    # Pick the first available ZIP in data/downloads for testing
    downloads = sorted(Path('data/downloads').glob('*.zip'))
    if not downloads:
        print('No ZIP files found in data/downloads')
        return
    zip_path = downloads[0]
    print('Using test ZIP:', zip_path)

    def fake_download(dataset_cfg):
        return [zip_path]

    pipeline.downloader.download_dataset = fake_download

    dataset_cfg = {
        'name': 'Assessor',
        'enabled': True,
        'filtered_zip_prefix': '1PP_FILTERED_',
        # provide per-dataset states if needed, otherwise pipeline global states used
        'exstates': ['CA', 'TX']
    }

    result = pipeline.process_dataset(dataset_cfg)
    print('Process result:', result)

    # List any filtered zips created
    filtered_dir = Path('data/filtered')
    if filtered_dir.exists():
        print('\nFiles in data/filtered:')
        for p in sorted(filtered_dir.iterdir()):
            print(p.name, p.stat().st_size)


if __name__ == '__main__':
    run_test()
