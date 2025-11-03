from pathlib import Path
from src.utils.logger import ETLLogger
from src.extractors.extractor import Extractor
import zipfile

# Prepare paths
root = Path('.')
downloads = root / 'data' / 'downloads'
extracted = root / 'data' / 'extracted'
downloads.mkdir(parents=True, exist_ok=True)
extracted.mkdir(parents=True, exist_ok=True)

# Create a sample TXT with header and two rows
sample_txt_name = 'sample_assessor.txt'
header = ('[ATTOM ID]\tSitusStateCode\tSitusCounty\tPropertyJurisdictionName\n')
rows = [
    '1001\tCA\tLos Angeles\tSomeJurisdiction\n',
    '1002\tTX\tTravis\tOtherJurisdiction\n'
]

# Create a sample ZIP in downloads
zip_path = downloads / '1PARKPLACE_TAXASSESSOR_001.zip'
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
    z.writestr(sample_txt_name, header + ''.join(rows))

print(f"Created sample zip at: {zip_path}")

# Run extractor
logger = ETLLogger()
extractor = Extractor(logger, extracted)

try:
    extracted_files = extractor.move_and_extract(zip_path)
    print('Extracted files:')
    for f in extracted_files:
        print(' -', f)
except Exception as e:
    print('Extraction failed:', e)
