from pathlib import Path
from src.utils.logger import ETLLogger
from src.extractors.extractor import Extractor
from src.transformers.state_filter import StateFilter
import zipfile

# Setup
root = Path('.')
downloads = root / 'data' / 'downloads'
extracted = root / 'data' / 'extracted'
filtered = root / 'data' / 'filtered'
downloads.mkdir(parents=True, exist_ok=True)
extracted.mkdir(parents=True, exist_ok=True)
filtered.mkdir(parents=True, exist_ok=True)

# Create a sample TXT with header and two rows
sample_txt_name = 'sample_assessor.txt'
header = ('[ATTOM ID]\tSitusStateCode\tSitusCounty\tPropertyJurisdictionName\n')
rows = [
    '1001\tCA\tLos Angeles\tSomeJurisdiction\n',
    '1002\tTX\tTravis\tOtherJurisdiction\n',
    '1003\tNV\tClark\tOther\n'
]

# Create a sample ZIP in downloads
zip_path = downloads / '1PARKPLACE_TAXASSESSOR_001.zip'
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
    z.writestr(sample_txt_name, header + ''.join(rows))

print(f"Created sample zip at: {zip_path}")

# Components
logger = ETLLogger()
extractor = Extractor(logger, extracted)

filter_config = {
    'extracted_dir': str(extracted),
    'filtered_dir': str(filtered),
    'states': [],  # global empty; we'll pass exstates per-dataset
    'state_code_column': 'SitusStateCode',
    'delimiter': '\t'
}
state_filter = StateFilter(logger, filter_config)

# Dataset config with exstates
dataset_config = {'name': 'Assessor', 'exstates': ['CA', 'TX']}

# Run extractor (extract in-place)
extracted_files = extractor.move_and_extract(zip_path)
print('\nExtracted files:')
for f in extracted_files:
    print(' -', f)

# For each extracted file, run filter for dataset exstates
all_filtered = []
for f in extracted_files:
    if f.suffix.lower() in ['.txt', '.csv']:
        filtered_files = state_filter.filter_multiple_states(f, states=dataset_config.get('exstates'))
        all_filtered.extend(filtered_files)

print('\nFiltered files:')
for ff in all_filtered:
    print(' -', ff)

# Compress filtered files into a zip
if all_filtered:
    zip_name = f"{dataset_config['name']}_sample_filtered.zip"
    output_zip = filtered / zip_name
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as z:
        for file in all_filtered:
            z.write(file, file.name)
    print('\nCreated filtered ZIP:', output_zip)
else:
    print('\nNo filtered files created')
