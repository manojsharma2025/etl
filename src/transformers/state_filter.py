import zipfile
import os
from pathlib import Path
import time


class StateFilter:
    """Memory-efficient streaming filter for large TXT files"""
    
    def __init__(self, logger, config):
        self.logger = logger
        self.config = config
        self.extracted_dir = Path(config.get('extracted_dir', 'data/extracted'))
        self.filtered_dir = Path(config.get('filtered_dir', 'data/filtered'))
        self.extracted_dir.mkdir(parents=True, exist_ok=True)
        self.filtered_dir.mkdir(parents=True, exist_ok=True)
        
        self.states = config.get('states', [])
        self.state_code_column = config.get('state_code_column', 'SitusStateCode')
        self.delimiter = config.get('delimiter', '\t')
    
    def extract_zip(self, zip_path):
        """
        Extract ZIP file to working directory
        
        Args:
            zip_path: Path to ZIP file
        
        Returns:
            List of extracted file paths
        """
        self.logger.info(f"Extracting ZIP file: {zip_path.name}")
        
        extracted_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                members = zip_ref.namelist()
                
                for member in members:
                    extract_path = self.extracted_dir / member
                    zip_ref.extract(member, self.extracted_dir)
                    extracted_files.append(extract_path)
                    self.logger.info(f"Extracted: {member}")
            
            return extracted_files
            
        except Exception as e:
            self.logger.error(f"Failed to extract {zip_path.name}: {e}")
            raise
    
    def filter_file_by_state(self, input_file, state_code):
        """
        Filter a single TXT file by state code using streaming (line-by-line)
        
        Args:
            input_file: Path to input TXT file
            state_code: State code to filter (e.g., 'CA', 'TX', 'FL')
        
        Returns:
            Path to filtered output file
        """
        start_time = time.time()
        
        output_filename = f"{input_file.stem}_filtered_{state_code}{input_file.suffix}"
        output_file = self.filtered_dir / output_filename
        
        self.logger.info(f"Filtering {input_file.name} for state: {state_code}")
        
        records_processed = 0
        records_kept = 0
        header_line = None
        state_column_index = None
        
        try:
            with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
                 open(output_file, 'w', encoding='utf-8') as outfile:
                
                for line_num, line in enumerate(infile, 1):
                    if line_num == 1:
                        header_line = line
                        outfile.write(line)
                        
                        headers = line.strip().split(self.delimiter)
                        try:
                            state_column_index = headers.index(self.state_code_column)
                        except ValueError:
                            self.logger.warning(f"Column '{self.state_code_column}' not found in {input_file.name}. Using fallback strategy.")
                            state_column_index = None
                        
                        continue
                    
                    records_processed += 1
                    
                    if state_column_index is not None:
                        columns = line.strip().split(self.delimiter)
                        
                        if len(columns) > state_column_index:
                            record_state = columns[state_column_index].strip()
                            
                            if record_state == state_code:
                                outfile.write(line)
                                records_kept += 1
                    else:
                        if f"{self.delimiter}{state_code}{self.delimiter}" in line or \
                           line.startswith(f"{state_code}{self.delimiter}") or \
                           line.endswith(f"{self.delimiter}{state_code}"):
                            outfile.write(line)
                            records_kept += 1
                    
                    if records_processed % 100000 == 0:
                        self.logger.log_filter_progress(input_file.name, records_processed, records_kept)
            
            duration = time.time() - start_time
            
            self.logger.info(f"Filtering complete: {input_file.name}")
            self.logger.info(f"Total processed: {records_processed}, Kept: {records_kept}, Duration: {duration:.2f}s")
            
            return output_file
            
        except Exception as e:
            self.logger.error(f"Failed to filter {input_file.name}: {e}")
            raise
    
    def filter_multiple_states(self, input_file):
        """
        Filter a TXT file for all configured states
        
        Args:
            input_file: Path to input TXT file
        
        Returns:
            List of filtered file paths (one per state)
        """
        filtered_files = []
        
        for state_code in self.states:
            try:
                filtered_file = self.filter_file_by_state(input_file, state_code)
                filtered_files.append(filtered_file)
            except Exception as e:
                self.logger.error(f"Failed to filter {input_file.name} for state {state_code}: {e}")
        
        return filtered_files
    
    def compress_to_zip(self, files, output_zip_name):
        """
        Compress filtered files into a ZIP archive
        
        Args:
            files: List of file paths to compress
            output_zip_name: Name of output ZIP file
        
        Returns:
            Path to created ZIP file
        """
        output_zip = self.filtered_dir / output_zip_name
        
        self.logger.info(f"Creating ZIP archive: {output_zip_name}")
        
        try:
            with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in files:
                    if file_path.exists():
                        zipf.write(file_path, file_path.name)
                        self.logger.debug(f"Added to ZIP: {file_path.name}")
            
            self.logger.info(f"ZIP archive created: {output_zip_name}")
            return output_zip
            
        except Exception as e:
            self.logger.error(f"Failed to create ZIP {output_zip_name}: {e}")
            raise
