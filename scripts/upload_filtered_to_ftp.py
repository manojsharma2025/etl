from pathlib import Path
import sys

# Ensure src package is importable (same pattern as tmp_move_test.py)
sys.path.insert(0, str(Path('.').resolve() / 'src'))

from utils.config_loader import ConfigLoader
from utils.logger import ETLLogger
from loaders.ftp_uploader import FTPUploader


def main():
    cfg = ConfigLoader()
    logger = ETLLogger()

    ftp_cfg = cfg.get_ftp_config()
    uploader = FTPUploader(logger, ftp_cfg)

    # Target file patterns to try (preferred: data/filtered)
    preferred = Path('data/filtered/1PP_FILTERED_1PARKPLACE_TAXASSESSOR_TEST1.zip')
    fallback = Path('data/processed/1PP_FILTERED_1PARKPLACE_TAXASSESSOR_TEST1.zip')

    local_zip = None
    if preferred.exists():
        local_zip = preferred
    elif fallback.exists():
        local_zip = fallback
    else:
        # Try to find a close match in filtered or processed directories
        for d in (Path('data/filtered'), Path('data/processed')):
            if d.exists():
                for p in d.glob('1PP_FILTERED_1PARKPLACE_TAXASSESSOR_TEST1*.zip'):
                    local_zip = p
                    break
            if local_zip:
                break

    if not local_zip:
        print("Could not find any matching filtered ZIP in data/filtered or data/processed.")
        return 2

    # Try to use dataset-level ftp_folder if present in config.json for Assessor
    # Fallback to ftp config default '/'
    ftp_folder = '/'  
    try:
        # naive read of config to get dataset ftp_folder
        config_json = Path('config/config.json')
        if config_json.exists():
            import json
            j = json.loads(config_json.read_text())
            datasets = j.get('datasets', [])
            for d in datasets:
                if d.get('name') == 'Assessor':
                    ftp_folder = d.get('ftp_upload_folder', ftp_folder)
                    break
    except Exception:
        pass

    print(f"Uploading {local_zip} to FTP folder: {ftp_folder}")
    ok = uploader.upload_file(local_zip, remote_folder=ftp_folder)

    print('Upload result (verified):', ok)
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
