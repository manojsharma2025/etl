from pathlib import Path
import sys

sys.path.insert(0, str(Path('.').resolve() / 'src'))
from utils.config_loader import ConfigLoader
from utils.logger import ETLLogger
from loaders.ftp_uploader import FTPUploader


def main():
    cfg = ConfigLoader()
    logger = ETLLogger()

    datasets = cfg.get_datasets()
    # Find Assessor dataset by name
    assessor = next((d for d in datasets if d.get('name') == 'Assessor'), None)
    if not assessor:
        print('Assessor dataset not found in config')
        return 2

    if not assessor.get('filter_ftp_upload', False):
        print('Assessor is not configured for FTP uploads (filter_ftp_upload=false)')
        return 1

    ftp_cfg = cfg.get_ftp_config()
    uploader = FTPUploader(logger, ftp_cfg)

    ftp_folder = assessor.get('ftp_folder', '/')
    action = assessor.get('filter_ftp_action', 'copy')
    prefix = assessor.get('filtered_zip_prefix')

    search_dir = Path('data/filtered')
    if not search_dir.exists():
        print('No data/filtered directory present')
        return 2

    if prefix:
        files = sorted(search_dir.glob(f"{prefix}*.zip"))
    else:
        files = sorted(search_dir.glob("*.zip"))

    if not files:
        print('No filtered ZIPs found to upload')
        return 0

    print(f'Found {len(files)} files to upload. FTP action={action}.')

    for p in files:
        print(f'Uploading {p.name} -> {ftp_folder} (action={action})')
        ok = uploader.upload_file(p, remote_folder=ftp_folder)
        print('  Verified:', ok)
        if ok and action == 'move':
            try:
                p.unlink()
                print('  Removed local file after successful FTP upload:', p.name)
            except Exception as e:
                print('  Failed to remove local file:', e)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
