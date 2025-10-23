"""
Data handler for reading and writing DataFrames to Excel and CSV files with CSV as source of truth.
Implements a backup metadata system where CSV serves as the authoritative source.
"""
import pandas as pd
import os
import re
from typing import Optional, List, Tuple, Dict
import logging
from datetime import datetime
import hashlib
import json

logger = logging.getLogger(__name__)


class DataHandler:
    """
    A handler class for managing data files with CSV as the source of truth.
    Excel files are synchronized from CSV backups when manipulated.
    """
    
    def __init__(self, data_directory: str = "data"):
        """
        Initialize the Data handler.
        
        Args:
            data_directory (str): Directory where data files will be stored
        """
        self.data_directory = data_directory
        self.csv_backup_directory = os.path.join(data_directory, "backups")
        self.metadata_file = os.path.join(data_directory, "backups", "metadata.json")
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure the data and backup directories exist."""
        if not os.path.isabs(self.data_directory):
            # If relative path, make it relative to project root
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.data_directory = os.path.join(project_root, self.data_directory)
            self.csv_backup_directory = os.path.join(self.data_directory, "backups")
            self.metadata_file = os.path.join(self.data_directory, "backups", "metadata.json")
        
        os.makedirs(self.data_directory, exist_ok=True)
        os.makedirs(self.csv_backup_directory, exist_ok=True)
        logger.info(f"Data directory ensured: {self.data_directory}")
        logger.info(f"CSV backup directory ensured: {self.csv_backup_directory}")
    
    def _load_metadata(self) -> Dict:
        """Load metadata about files and their checksums."""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load metadata file: {e}")
        return {}
    
    def _save_metadata(self, metadata: Dict):
        """Save metadata about files and their checksums."""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metadata file: {e}")
    
    def _calculate_file_checksum(self, filepath: str) -> str:
        """Calculate MD5 checksum of a file."""
        if not os.path.exists(filepath):
            return ""
        
        hash_md5 = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate checksum for {filepath}: {e}")
            return ""
    
    def _dataframes_are_equal(self, df1: pd.DataFrame, df2: pd.DataFrame, unique_col: str) -> bool:
        """
        Compare two DataFrames to see if they contain the same data.
        Compares based on unique identifier and ignores created_date column.
        
        Args:
            df1, df2: DataFrames to compare
            unique_col: Column name to use as unique identifier
            
        Returns:
            bool: True if DataFrames contain same data
        """
        if df1.empty and df2.empty:
            return True
        
        if df1.empty or df2.empty:
            return False
        
        # Remove created_date column for comparison if it exists
        df1_compare = df1.copy()
        df2_compare = df2.copy()
        
        for df in [df1_compare, df2_compare]:
            if 'created_date' in df.columns:
                df.drop('created_date', axis=1, inplace=True)
        
        # Compare based on unique identifier
        if unique_col not in df1_compare.columns or unique_col not in df2_compare.columns:
            return False
        
        # Sort by unique column for comparison
        df1_sorted = df1_compare.sort_values(by=unique_col).reset_index(drop=True)
        df2_sorted = df2_compare.sort_values(by=unique_col).reset_index(drop=True)
        
        # Check if they have same shape and content
        if df1_sorted.shape != df2_sorted.shape:
            return False
        
        # Compare content (convert to string to handle type differences)
        for col in df1_sorted.columns:
            if col in df2_sorted.columns:
                if not df1_sorted[col].astype(str).equals(df2_sorted[col].astype(str)):
                    return False
        
        return True

    def _preserve_custom_columns(self, new_data: pd.DataFrame, existing_excel_data: pd.DataFrame) -> pd.DataFrame:
        """
        Preserve custom columns from existing Excel data that are not in the new data.
        
        Args:
            new_data: New data from URL (source of truth for core data)
            existing_excel_data: Current Excel data that may have custom columns
            
        Returns:
            pd.DataFrame: Combined data with custom columns preserved
        """
        if existing_excel_data.empty:
            return new_data
        
        # Find columns that exist in Excel but not in new data (custom columns)
        new_data_cols = set(new_data.columns)
        existing_cols = set(existing_excel_data.columns)
        custom_cols = existing_cols - new_data_cols
        
        if not custom_cols:
            return new_data
        
        logger.info(f"Found custom columns to preserve: {list(custom_cols)}")
        
        # Get unique identifier column
        unique_col = self._get_unique_column_name(new_data)
        
        # Create result DataFrame starting with new data
        result_df = new_data.copy()
        
        # Add custom columns with empty values initially
        for col in custom_cols:
            result_df[col] = ''
        
        # Merge custom column values for matching rows
        if unique_col in existing_excel_data.columns:
            for custom_col in custom_cols:
                # Create a mapping of unique_id -> custom_value
                custom_mapping = existing_excel_data.set_index(unique_col)[custom_col].to_dict()
                
                # Apply the mapping to result_df
                result_df[custom_col] = result_df[unique_col].map(custom_mapping).fillna('')
        
        logger.info(f"Preserved {len(custom_cols)} custom columns")
        return result_df

    def _detect_data_changes(self, new_data: pd.DataFrame, csv_backup: pd.DataFrame, unique_col: str) -> bool:
        """
        Detect if there are changes between new fetched data and CSV backup.
        
        Args:
            new_data: Newly fetched data from URL
            csv_backup: Data from CSV backup (source of truth)
            unique_col: Column to use as unique identifier
            
        Returns:
            bool: True if changes detected, False if no changes
        """
        # Remove created_date from comparison
        new_data_compare = new_data.copy()
        csv_backup_compare = csv_backup.copy()
        
        for df in [new_data_compare, csv_backup_compare]:
            if 'created_date' in df.columns:
                df.drop('created_date', axis=1, inplace=True)
        
        are_equal = self._dataframes_are_equal(new_data_compare, csv_backup_compare, unique_col)
        changes_detected = not are_equal
        
        if changes_detected:
            logger.info("Changes detected between new data and CSV backup")
        else:
            logger.info("No changes detected between new data and CSV backup")
        
        return changes_detected

    def _get_csv_backup_path(self, excel_filename: str, sheet_name: str = "data") -> str:
        """Get the CSV backup file path for an Excel file and sheet."""
        base_name = os.path.splitext(excel_filename)[0]
        csv_filename = f"{base_name}_{sheet_name}.csv"
        return os.path.join(self.csv_backup_directory, csv_filename)
    
    def _create_csv_backup(self, dataframe: pd.DataFrame, excel_filename: str, sheet_name: str = "data"):
        """Create a CSV backup of the dataframe."""
        csv_path = self._get_csv_backup_path(excel_filename, sheet_name)
        try:
            # Ensure all data is properly formatted as strings to preserve formatting
            df_backup = self._format_dataframe_for_csv(dataframe)
            df_backup.to_csv(csv_path, index=False, encoding='utf-8')
            logger.info(f"Created CSV backup: {csv_path}")
            return csv_path
        except Exception as e:
            logger.error(f"Failed to create CSV backup: {e}")
            raise
    
    def _load_csv_backup(self, excel_filename: str, sheet_name: str = "data") -> Optional[pd.DataFrame]:
        """Load data from CSV backup."""
        csv_path = self._get_csv_backup_path(excel_filename, sheet_name)
        if not os.path.exists(csv_path):
            return None
        
        try:
            # Read CSV with string dtypes to preserve formatting
            df = pd.read_csv(csv_path, dtype=str)
            # Fill NaN values with empty strings
            df = df.fillna('')
            logger.info(f"Loaded CSV backup from: {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load CSV backup from {csv_path}: {e}")
            return None
    
    def _format_second_column_value(self, val):
        """Helper function to format values in the second column consistently."""
        if pd.isna(val) or val == '' or val == 'nan':
            return ''
        str_val = str(val)
        # If it's a float ending in .0, remove the .0
        if str_val.endswith('.0'):
            str_val = str_val[:-2]
        return str_val
    
    def _format_dataframe_for_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame for CSV storage, ensuring all data is preserved as strings."""
        df_copy = df.copy()
        
        # Convert all columns to strings and handle NaN values
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna('').astype(str)
            
        # Ensure first two columns preserve formatting (especially important for IDs)
        if len(df_copy.columns) >= 1:
            df_copy.iloc[:, 0] = df_copy.iloc[:, 0].fillna('').astype(str)
            
        if len(df_copy.columns) >= 2:
            # Handle numeric values in second column
            df_copy.iloc[:, 1] = df_copy.iloc[:, 1].apply(self._format_second_column_value)
        
        return df_copy
    
    def _is_excel_manipulated(self, excel_filename: str) -> bool:
        """Check if Excel file has been manipulated by comparing checksum."""
        excel_path = os.path.join(self.data_directory, excel_filename)
        if not os.path.exists(excel_path):
            return False
        
        metadata = self._load_metadata()
        file_key = excel_filename
        
        if file_key not in metadata:
            return False
        
        current_checksum = self._calculate_file_checksum(excel_path)
        stored_checksum = metadata[file_key].get('checksum', '')
        
        is_manipulated = current_checksum != stored_checksum
        if is_manipulated:
            logger.warning(f"Excel file {excel_filename} appears to have been manipulated")
            logger.info(f"Stored checksum: {stored_checksum}")
            logger.info(f"Current checksum: {current_checksum}")
        
        return is_manipulated
    
    def _update_file_metadata(self, excel_filename: str, sheet_names: List[str]):
        """Update metadata for a file."""
        excel_path = os.path.join(self.data_directory, excel_filename)
        checksum = self._calculate_file_checksum(excel_path)
        
        metadata = self._load_metadata()
        metadata[excel_filename] = {
            'checksum': checksum,
            'last_updated': datetime.now().isoformat(),
            'sheet_names': sheet_names
        }
        self._save_metadata(metadata)
    
    def _restore_from_csv_backup(self, excel_filename: str, sheet_names: List[str] = None):
        """Restore Excel file from CSV backup."""
        if sheet_names is None:
            sheet_names = ["Data"]
        
        logger.info(f"Restoring {excel_filename} from CSV backup...")
        
        excel_path = os.path.join(self.data_directory, excel_filename)
        
        try:
            if len(sheet_names) == 1:
                # Single sheet
                df = self._load_csv_backup(excel_filename, sheet_names[0])
                if df is not None:
                    self._write_excel_direct(df, excel_filename, sheet_names[0])
                    logger.info(f"Restored {excel_filename} from CSV backup")
                else:
                    logger.warning(f"No CSV backup found for {excel_filename}")
            else:
                # Multiple sheets
                dataframes = []
                valid_sheets = []
                for sheet_name in sheet_names:
                    df = self._load_csv_backup(excel_filename, sheet_name)
                    if df is not None:
                        dataframes.append(df)
                        valid_sheets.append(sheet_name)
                
                if dataframes:
                    self._write_excel_multiple_sheets_direct(dataframes, excel_filename, valid_sheets)
                    logger.info(f"Restored {excel_filename} with {len(valid_sheets)} sheets from CSV backup")
                else:
                    logger.warning(f"No CSV backups found for any sheets in {excel_filename}")
        
        except Exception as e:
            logger.error(f"Failed to restore {excel_filename} from CSV backup: {e}")
            raise
    
    def read_excel(self, filename: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Read an Excel file and return a DataFrame. Checks for manipulation and restores from CSV if needed.
        
        Args:
            filename (str): Name of the Excel file
            sheet_name (str, optional): Name of the sheet to read. If None, reads the first sheet
            
        Returns:
            pd.DataFrame: DataFrame containing the Excel data
        """
        # Check if Excel file has been manipulated
        if self._is_excel_manipulated(filename):
            logger.info(f"Excel file {filename} has been manipulated. Restoring from CSV backup...")
            metadata = self._load_metadata()
            file_metadata = metadata.get(filename, {})
            sheet_names = file_metadata.get('sheet_names', [sheet_name or "Data"])
            self._restore_from_csv_backup(filename, sheet_names)
        
        filepath = os.path.join(self.data_directory, filename)
        
        try:
            if sheet_name:
                df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)
                logger.info(f"Successfully read sheet '{sheet_name}' from {filename}")
            else:
                df = pd.read_excel(filepath, dtype=str)
                logger.info(f"Successfully read {filename}")
            
            # Ensure all columns are strings to preserve formatting
            for col in df.columns:
                df[col] = df[col].fillna('').astype(str)
            
            logger.info(f"DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns")
            return df
        
        except FileNotFoundError:
            logger.error(f"Excel file not found: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Failed to read Excel file {filename}: {e}")
            raise
    
    def _write_excel_direct(self, dataframe: pd.DataFrame, filename: str, sheet_name: str = "data", index: bool = False) -> str:
        """Write DataFrame directly to Excel without incremental logic."""
        filepath = os.path.join(self.data_directory, filename)
        
        try:
            df_formatted = self._format_dataframe_for_excel(dataframe)
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df_formatted.to_excel(writer, sheet_name=sheet_name, index=index)
                self._apply_text_formatting(writer, sheet_name, df_formatted)
            
            return filepath
        
        except Exception as e:
            logger.error(f"Failed to write Excel file {filename}: {e}")
            raise
    
    def _write_excel_multiple_sheets_direct(self, dataframes: List[pd.DataFrame], filename: str, sheet_names: List[str], index: bool = False) -> str:
        """Write multiple DataFrames directly to Excel without incremental logic."""
        filepath = os.path.join(self.data_directory, filename)
        
        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for df, sheet_name in zip(dataframes, sheet_names):
                    df_formatted = self._format_dataframe_for_excel(df)
                    df_formatted.to_excel(writer, sheet_name=sheet_name, index=index)
                    self._apply_text_formatting(writer, sheet_name, df_formatted)
            
            return filepath
        
        except Exception as e:
            logger.error(f"Failed to write multi-sheet Excel file {filename}: {e}")
            raise
    
    def generate_filename(self, base_name: str, url: str = "", timestamp: bool = False) -> str:
        """
        Generate a distinct filename for the Excel file.
        
        Args:
            base_name (str): Base name for the file
            url (str): URL being scraped (used to create unique identifier) - optional
            timestamp (bool): Whether to include timestamp (default: False)
            
        Returns:
            str: Generated filename with .xlsx extension
        """
        clean_base_name = re.sub(r'[^\w\-_.]', '_', base_name)
        
        timestamp_part = ""
        if timestamp:
            timestamp_part = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        filename = f"{clean_base_name}{timestamp_part}.xlsx"
        logger.info(f"Generated filename: {filename}")
        return filename
    
    def _get_unique_column_name(self, df: pd.DataFrame) -> str:
        """Get the name of the first column which serves as the unique identifier."""
        if len(df.columns) < 1:
            raise ValueError("DataFrame must have at least 1 column to use first column as unique identifier")
        return df.columns[0]
    
    def _add_created_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add a 'created_date' column to the DataFrame with current date in yyyy-mm-dd format."""
        df_copy = df.copy()
        df_copy['created_date'] = datetime.now().strftime('%Y-%m-%d')
        return df_copy
    
    def _find_new_rows(self, new_df: pd.DataFrame, existing_df: pd.DataFrame, unique_col: str) -> pd.DataFrame:
        """Find rows in new_df that don't exist in existing_df based on unique column."""
        existing_values = set(existing_df[unique_col].values) if not existing_df.empty else set()
        new_rows = new_df[~new_df[unique_col].isin(existing_values)]
        logger.info(f"Found {len(new_rows)} new rows out of {len(new_df)} total rows")
        return new_rows
    
    def _format_dataframe_for_excel(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame for proper Excel output, ensuring string columns are preserved."""
        df_copy = df.copy()
        
        if len(df_copy.columns) >= 1:
            df_copy.iloc[:, 0] = df_copy.iloc[:, 0].fillna('').astype(str)
            
        if len(df_copy.columns) >= 2:
            df_copy.iloc[:, 1] = df_copy.iloc[:, 1].apply(self._format_second_column_value)
        
        return df_copy
    
    def _apply_text_formatting(self, writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame):
        """Apply text formatting to Excel columns to preserve string format."""
        try:
            
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            
            max_row = len(df) + 2
            
            if len(df.columns) >= 1:
                for row in range(2, max_row):
                    cell = worksheet[f'A{row}']
                    cell.number_format = '@'
                    
            if len(df.columns) >= 2:
                for row in range(2, max_row):
                    cell = worksheet[f'B{row}']
                    cell.number_format = '@'
            
            logger.info(f"Applied text formatting to first two columns in sheet '{sheet_name}'")
            
        except ImportError:
            logger.warning("openpyxl not available for advanced Excel formatting")
        except Exception as e:
            logger.warning(f"Failed to apply Excel text formatting: {e}")
    
    def write_excel_incremental(self, 
                               dataframe: pd.DataFrame, 
                               filename: str, 
                               sheet_name: str = "data",
                               index: bool = False) -> Tuple[str, int, int]:
        """
        Write a DataFrame to an Excel file incrementally with CSV backup as source of truth.
        Implements the three flows:
        1. Normal batch process: fetch -> store in CSV backup -> store in Excel
        2. Excel manipulation detected: fetch -> compare with CSV -> restore Excel from CSV + new data
        3. Custom columns preservation: maintain user-added columns during restoration
        
        Args:
            dataframe (pd.DataFrame): New DataFrame to write/append
            filename (str): Name of the Excel file
            sheet_name (str): Name of the sheet
            index (bool): Whether to write row names (index)
            
        Returns:
            Tuple[str, int, int]: Full path of the file, total rows, new rows added
        """
        filepath = os.path.join(self.data_directory, filename)
        
        try:
            unique_col = self._get_unique_column_name(dataframe)
            logger.info(f"Using column '{unique_col}' as unique identifier")
            
            # Add created_date to new data
            new_data = self._add_created_date_column(dataframe)
            
            # Load CSV backup (source of truth)
            csv_backup = self._load_csv_backup(filename, sheet_name)
            
            # Check if Excel file has been manipulated
            excel_was_manipulated = self._is_excel_manipulated(filename)
            
            # Load current Excel data to check for custom columns
            current_excel_data = None
            if os.path.exists(filepath) and excel_was_manipulated:
                try:
                    current_excel_data = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)
                    current_excel_data = current_excel_data.fillna('')
                    logger.info("Loaded current Excel data to check for custom columns")
                except Exception as e:
                    logger.warning(f"Could not read current Excel data: {e}")
                    current_excel_data = None
            
            # Flow 1: Normal batch process OR Flow 2: Excel manipulation detected
            if csv_backup is not None and not csv_backup.empty:
                # Ensure CSV backup has created_date column
                if 'created_date' not in csv_backup.columns:
                    if 'modified_time' in csv_backup.columns:
                        logger.info("Converting modified_time column to created_date format")
                        csv_backup['created_date'] = pd.to_datetime(csv_backup['modified_time']).dt.strftime('%Y-%m-%d')
                        csv_backup = csv_backup.drop('modified_time', axis=1)
                    else:
                        logger.info("Adding created_date column to CSV backup")
                        csv_backup['created_date'] = datetime.now().strftime('%Y-%m-%d')
                
                # Check if there are changes between new data and CSV backup
                changes_detected = self._detect_data_changes(new_data, csv_backup, unique_col)
                
                if changes_detected:
                    # Flow 1: Changes detected - update CSV backup and Excel
                    logger.info("Flow 1: Processing new data changes")
                    new_rows = self._find_new_rows(new_data, csv_backup, unique_col)
                    
                    if len(new_rows) > 0:
                        updated_csv_data = pd.concat([csv_backup, new_rows], ignore_index=True)
                        new_rows_count = len(new_rows)
                        logger.info(f"Added {new_rows_count} new rows to CSV backup")
                    else:
                        # Handle updates to existing rows
                        updated_csv_data = new_data.copy()
                        updated_csv_data['created_date'] = datetime.now().strftime('%Y-%m-%d')
                        new_rows_count = 0
                        logger.info("Updated existing data in CSV backup")
                    
                    # Update CSV backup first (source of truth)
                    self._create_csv_backup(updated_csv_data, filename, sheet_name)
                    
                    # Flow 3: Preserve custom columns if Excel was manipulated
                    if excel_was_manipulated and current_excel_data is not None:
                        logger.info("Flow 3: Preserving custom columns from manipulated Excel")
                        final_data = self._preserve_custom_columns(updated_csv_data, current_excel_data)
                    else:
                        final_data = updated_csv_data
                    
                    # Write to Excel
                    self._write_excel_direct(final_data, filename, sheet_name, index)
                    
                else:
                    # Flow 2: No changes detected - restore Excel from CSV backup
                    logger.info("Flow 2: No changes detected, updating Excel from CSV backup")
                    
                    # Flow 3: Preserve custom columns if Excel was manipulated
                    if excel_was_manipulated and current_excel_data is not None:
                        logger.info("Flow 3: Preserving custom columns during restoration")
                        final_data = self._preserve_custom_columns(csv_backup, current_excel_data)
                    else:
                        final_data = csv_backup
                    
                    # Write Excel from CSV backup
                    self._write_excel_direct(final_data, filename, sheet_name, index)
                    new_rows_count = 0
                
                updated_df = final_data
                
            else:
                # No CSV backup exists - create new file
                logger.info("No CSV backup found, creating new file")
                updated_df = new_data
                new_rows_count = len(new_data)
                
                # Create CSV backup (source of truth)
                self._create_csv_backup(updated_df, filename, sheet_name)
                
                # Write to Excel
                self._write_excel_direct(updated_df, filename, sheet_name, index)
            
            # Update metadata
            self._update_file_metadata(filename, [sheet_name])
            
            total_rows = len(updated_df)
            logger.info(f"Successfully updated {filename}")
            logger.info(f"Total rows: {total_rows}, New rows added: {new_rows_count}")
            
            return filepath, total_rows, new_rows_count
        
        except Exception as e:
            logger.error(f"Failed to write Excel file incrementally {filename}: {e}")
            raise
    
    def write_multiple_sheets_incremental(self, 
                                        dataframes: List[pd.DataFrame], 
                                        filename: str,
                                        sheet_names: Optional[List[str]] = None,
                                        index: bool = False) -> Tuple[str, List[Tuple[int, int]]]:
        """
        Write multiple DataFrames to different sheets incrementally with CSV backup.
        Implements the same three flows as write_excel_incremental for each sheet.
        
        Args:
            dataframes (List[pd.DataFrame]): List of DataFrames to write
            filename (str): Name of the Excel file
            sheet_names (List[str], optional): Names of the sheets
            index (bool): Whether to write row names (index)
            
        Returns:
            Tuple[str, List[Tuple[int, int]]]: Full path and list of (total_rows, new_rows) for each sheet
        """
        if sheet_names and len(sheet_names) != len(dataframes):
            raise ValueError("Number of sheet names must match number of DataFrames")
        
        if not sheet_names:
            sheet_names = [f"Sheet{i+1}" for i in range(len(dataframes))]
        
        filepath = os.path.join(self.data_directory, filename)
        results = []
        all_updated_dfs = []
        
        # Check if Excel file has been manipulated
        excel_was_manipulated = self._is_excel_manipulated(filename)
        
        try:
            # Process each sheet with the same logic as single sheet
            for df, sheet_name in zip(dataframes, sheet_names):
                unique_col = self._get_unique_column_name(df)
                new_data = self._add_created_date_column(df)
                
                # Load CSV backup for this sheet (source of truth)
                csv_backup = self._load_csv_backup(filename, sheet_name)
                
                # Load current Excel data for this sheet to check for custom columns
                current_excel_data = None
                if os.path.exists(filepath) and excel_was_manipulated:
                    try:
                        current_excel_data = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)
                        current_excel_data = current_excel_data.fillna('')
                    except Exception:
                        current_excel_data = None
                
                if csv_backup is not None and not csv_backup.empty:
                    # Ensure created_date column exists
                    if 'created_date' not in csv_backup.columns:
                        if 'modified_time' in csv_backup.columns:
                            csv_backup['created_date'] = pd.to_datetime(csv_backup['modified_time']).dt.strftime('%Y-%m-%d')
                            csv_backup = csv_backup.drop('modified_time', axis=1)
                        else:
                            csv_backup['created_date'] = datetime.now().strftime('%Y-%m-%d')
                    
                    # Check for changes
                    changes_detected = self._detect_data_changes(new_data, csv_backup, unique_col)
                    
                    if changes_detected:
                        # Flow 1: Process changes
                        new_rows = self._find_new_rows(new_data, csv_backup, unique_col)
                        if len(new_rows) > 0:
                            updated_csv_data = pd.concat([csv_backup, new_rows], ignore_index=True)
                            new_count = len(new_rows)
                        else:
                            updated_csv_data = new_data.copy()
                            updated_csv_data['created_date'] = datetime.now().strftime('%Y-%m-%d')
                            new_count = 0
                        
                        # Update CSV backup
                        self._create_csv_backup(updated_csv_data, filename, sheet_name)
                        
                        # Preserve custom columns if needed
                        if excel_was_manipulated and current_excel_data is not None:
                            updated_df = self._preserve_custom_columns(updated_csv_data, current_excel_data)
                        else:
                            updated_df = updated_csv_data
                    else:
                        # Flow 2: No changes - restore from CSV
                        if excel_was_manipulated and current_excel_data is not None:
                            updated_df = self._preserve_custom_columns(csv_backup, current_excel_data)
                        else:
                            updated_df = csv_backup
                        new_count = 0
                else:
                    # No CSV backup - create new
                    updated_df = new_data
                    new_count = len(new_data)
                    self._create_csv_backup(updated_df, filename, sheet_name)
                
                all_updated_dfs.append(updated_df)
                results.append((len(updated_df), new_count))
            
            # Write all sheets to Excel
            self._write_excel_multiple_sheets_direct(all_updated_dfs, filename, sheet_names, index)
            
            # Update metadata
            self._update_file_metadata(filename, sheet_names)
            
            logger.info(f"Successfully updated multi-sheet file {filename}")
            return filepath, results
        
        except Exception as e:
            logger.error(f"Failed to write multi-sheet Excel file incrementally {filename}: {e}")
            raise
    
    def list_excel_files(self) -> List[str]:
        """List all Excel files in the data directory."""
        try:
            files = [f for f in os.listdir(self.data_directory) 
                    if f.lower().endswith(('.xlsx', '.xls'))]
            logger.info(f"Found {len(files)} Excel files in {self.data_directory}")
            return files
        except Exception as e:
            logger.error(f"Failed to list Excel files: {e}")
            return []
    
    def list_csv_backups(self) -> List[str]:
        """List all CSV backup files."""
        try:
            files = [f for f in os.listdir(self.csv_backup_directory) 
                    if f.lower().endswith('.csv')]
            logger.info(f"Found {len(files)} CSV backup files in {self.csv_backup_directory}")
            return files
        except Exception as e:
            logger.error(f"Failed to list CSV backup files: {e}")
            return []
    
    def get_file_metadata(self, filename: str) -> Dict:
        """Get metadata for a specific file."""
        metadata = self._load_metadata()
        return metadata.get(filename, {})
    
    def verify_file_integrity(self, filename: str) -> bool:
        """Verify if an Excel file matches its stored checksum."""
        return not self._is_excel_manipulated(filename)
    
    def force_restore_from_backup(self, filename: str, sheet_names: List[str] = None):
        """Force restore an Excel file from CSV backup regardless of checksum."""
        if sheet_names is None:
            metadata = self._load_metadata()
            file_metadata = metadata.get(filename, {})
            sheet_names = file_metadata.get('sheet_names', ["Data"])
        
        self._restore_from_csv_backup(filename, sheet_names)
        # Update metadata with new checksum
        self._update_file_metadata(filename, sheet_names)
        logger.info(f"Force restored {filename} from CSV backup")