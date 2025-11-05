import requests
from pathlib import Path
import sys
import re

sys.path.insert(0, str(Path('.').resolve() / 'src'))
from utils.logger import ETLLogger  # your existing logger

def download_zip_file(download_url, output_dir):
    logger = ETLLogger()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting download: {download_url}")
    print(f"Downloading from {download_url}")

    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    # Try to extract real filename from headers
    content_disp = response.headers.get("content-disposition", "")
    filename = None
    if content_disp:
        match = re.search(r'filename="?(?P<name>[^";]+)"?', content_disp)
        if match:
            filename = match.group("name")

    # Fallback if header not found
    if not filename:
        filename = Path(download_url).name + ".zip"

    output_path = output_dir / filename

    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024 * 1024  # 1 MB

    downloaded = 0
    with open(output_path, 'wb') as file:
        for data in response.iter_content(block_size):
            downloaded += len(data)
            file.write(data)
            done = int(50 * downloaded / total_size) if total_size > 0 else 0
            percent = (downloaded / total_size * 100) if total_size > 0 else 0
            sys.stdout.write(
                f"\r[{'=' * done}{' ' * (50 - done)}] {percent:5.1f}% ({downloaded / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB)"
            )
            sys.stdout.flush()

    print(f"\n✅ Download complete: {output_path.name}")
    logger.info(f"Download complete: {output_path} ({total_size / (1024*1024):.2f} MB)")

    return output_path


if __name__ == "__main__":
    # For now, use direct download URL
    download_url = "https://depot.jetstream.pro/w74r-fkpb-tejp/download/37831b3e-33e2-579a-9cac-b36a5b18d7d7"
    output_dir = "data/downloads"

    download_zip_file(download_url, output_dir)
