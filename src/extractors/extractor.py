import zipfile
import shutil
from pathlib import Path


class Extractor:
    """Helper to move downloaded ZIPs into the extracted area and extract them.

    Behavior:
      - Moves the provided ZIP file into the configured extracted directory
      - Extracts the ZIP into a subfolder named after the ZIP (stem)
      - Returns a list of Path objects for the extracted files
    """

    def __init__(self, logger, extracted_dir: str | Path, config_loader=None):
        self.logger = logger
        self.extracted_dir = Path(extracted_dir)
        self.extracted_dir.mkdir(parents=True, exist_ok=True)
        self.config_loader = config_loader

    def move_and_extract(self, zip_path, dataset_name=None):
        """Move a ZIP file into the extracted dir and extract its contents.

        Args:
            zip_path: Path-like to the downloaded ZIP file (usually in downloads)
            dataset_name: Optional dataset name to look up custom ZIP path configuration

        Returns:
            List[Path] of extracted file paths
        """
        # Check for dataset-specific ZIP path configuration
        configured_path = None
        if dataset_name and self.config_loader:
            configured_path = self.config_loader.get_dataset_zip_path(dataset_name)
            if configured_path:
                self.logger.info(f"Using configured ZIP path for {dataset_name}: {configured_path}")
                zip_path = Path(configured_path) / Path(zip_path).name
        
        zip_path = Path(zip_path)
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")

        # Do NOT move the ZIP. Extract directly from the downloaded location
        # into the configured extracted directory under a per-zip subfolder.
        extract_folder = self.extracted_dir / zip_path.stem
        extract_folder.mkdir(parents=True, exist_ok=True)

        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                members = z.namelist()
                for member in members:
                    # Extract member into the designated folder
                    z.extract(member, extract_folder)
                    member_path = extract_folder / member
                    # If it's a file (not a directory), append
                    if member_path.exists() and member_path.is_file():
                        extracted_files.append(member_path)
                        self.logger.info(f"Extracted: {member_path}")

            return extracted_files

        except Exception as e:
            self.logger.error(f"Failed to extract {zip_path}: {e}")
            # Clean up the extract folder if it was created
            try:
                if extract_folder.exists():
                    shutil.rmtree(extract_folder)
            except Exception as cleanup_error:
                self.logger.warning(f"Failed to clean up extract folder {extract_folder}: {cleanup_error}")
            raise
