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
    
    def filter_multiple_states(self, input_file, states=None):
        """
        Filter a TXT file for multiple states.

        Args:
            input_file: Path to input TXT file
            states: Optional list of state codes to filter for. If omitted,
                    uses the global states configured for this filter instance.

        Returns:
            List of filtered file paths (one per state)
        """
        states_to_use = states if states is not None else self.states

        if not states_to_use:
            self.logger.warning(f"No states configured for filtering {input_file.name}; skipping")
            return []

        # If only one state requested, reuse existing single-state method
        if len(states_to_use) == 1:
            try:
                return [self.filter_file_by_state(input_file, states_to_use[0])]
            except Exception as e:
                self.logger.error(f"Failed to filter {input_file.name} for state {states_to_use[0]}: {e}")
                return []

        # For multiple states, perform a single-pass combined filter that keeps
        # any record whose state code is in the provided list. This avoids
        # reading the large input file multiple times.
        states_set = {s.upper() for s in states_to_use}
        output_filename = f"{input_file.stem}_filtered_{'_'.join(states_set)}{input_file.suffix}"
        output_file = self.filtered_dir / output_filename

        start_time = time.time()
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

                    write_line = False
                    if state_column_index is not None:
                        columns = line.strip().split(self.delimiter)
                        if len(columns) > state_column_index:
                            record_state = columns[state_column_index].strip().upper()
                            if record_state in states_set:
                                write_line = True
                    else:
                        # Fallback: simple substring checks
                        for s in states_set:
                            if f"{self.delimiter}{s}{self.delimiter}" in line or \
                               line.startswith(f"{s}{self.delimiter}") or \
                               line.endswith(f"{self.delimiter}{s}"):
                                write_line = True
                                break

                    if write_line:
                        outfile.write(line)
                        records_kept += 1

                    if records_processed % 100000 == 0:
                        self.logger.log_filter_progress(input_file.name, records_processed, records_kept)

            duration = time.time() - start_time
            self.logger.info(f"Filtering complete (combined states): {input_file.name}")
            self.logger.info(f"Total processed: {records_processed}, Kept: {records_kept}, Duration: {duration:.2f}s")
            return [output_file]

        except Exception as e:
            self.logger.error(f"Failed to filter {input_file.name} for states {states_to_use}: {e}")
            return []
    
    def compress_to_zip(self, files, output_zip_name, inner_name_prefix=None):
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
            # Write to a temporary file first to avoid partial/locked ZIPs
            temp_zip = output_zip.with_suffix(output_zip.suffix + '.part')
            with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in files:
                    if file_path.exists():
                        # If an inner_name_prefix is provided, replace the
                        # filtered filename with the prefix + original stem.
                        # E.g., original filtered file:
                        #   1PARKPLACE_TAXASSESSOR_0022_filtered_TX_CA.txt
                        # becomes inside the ZIP:
                        #   1PP_FILTERED_1PARKPLACE_TAXASSESSOR_0022.txt
                        arcname = file_path.name
                        if inner_name_prefix:
                            # Derive base stem by removing any trailing
                            # "_filtered_..." suffix if present.
                            stem = file_path.stem
                            if '_filtered_' in stem:
                                base_stem = stem.split('_filtered_')[0]
                            else:
                                base_stem = stem
                            arcname = f"{inner_name_prefix}{base_stem}{file_path.suffix}"
                        zipf.write(file_path, arcname)
                        self.logger.debug(f"Added to ZIP: {arcname}")

            # Atomically move temp zip to final path
            temp_zip.replace(output_zip)
            self.logger.info(f"ZIP archive created: {output_zip_name}")
            return output_zip
            
        except Exception as e:
            self.logger.error(f"Failed to create ZIP {output_zip_name}: {e}")
            raise
