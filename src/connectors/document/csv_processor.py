# src/connectors/document/excel_reader.py

import os
import re
import csv
import pandas as pd
from typing import Dict, List, Optional, Union, Tuple


class ExcelCSVReader:
    """
    Excel and CSV file reader with advanced capabilities.
    Supports loading, processing and extracting data from spreadsheet files.
    """
    
    def __init__(self, filepath: Optional[str] = None):
        """
        Initialize the Excel/CSV reader.
        
        Args:
            filepath: Optional path to spreadsheet file
        """
        self.filepath = filepath
        self.data = None
        self.sheet_names = []
        self.current_sheet = None
        self.file_type = None
    
    def load_document(self, filepath: str, sheet_name: Optional[Union[str, int]] = 0) -> bool:
        """
        Load a spreadsheet document from the given filepath.
        
        Args:
            filepath: Path to the Excel or CSV file
            sheet_name: Sheet name or index (for Excel files)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.filepath = filepath
            self.data = None
            self.sheet_names = []
            
            # Check if file exists
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"File {filepath} not found")
            
            # Determine file type
            if filepath.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                return self._load_excel(filepath, sheet_name)
            elif filepath.lower().endswith('.csv'):
                return self._load_csv(filepath)
            else:
                raise ValueError(f"Unsupported file format for {filepath}")
                
        except Exception as e:
            print(f"Error loading document: {str(e)}")
            return False
    
    def _load_excel(self, filepath: str, sheet_name: Union[str, int] = 0) -> bool:
        """Load data from Excel file"""
        try:
            # Load the Excel file
            excel_file = pd.ExcelFile(filepath)
            self.sheet_names = excel_file.sheet_names
            
            # If sheet_name is an integer, make sure it's valid
            if isinstance(sheet_name, int) and (sheet_name < 0 or sheet_name >= len(self.sheet_names)):
                sheet_name = 0
            
            # If sheet_name is a string, make sure it exists
            if isinstance(sheet_name, str) and sheet_name not in self.sheet_names:
                sheet_name = self.sheet_names[0]
                
            # Load the specified sheet
            self.data = pd.read_excel(filepath, sheet_name=sheet_name)
            self.current_sheet = sheet_name
            self.file_type = 'excel'
            
            return True
        except Exception as e:
            print(f"Error loading Excel file: {str(e)}")
            return False
    
    def _load_csv(self, filepath: str) -> bool:
        """Load data from CSV file"""
        try:
            # First try to detect encoding and delimiter
            encoding, delimiter = self._detect_csv_params(filepath)
            
            # Load the CSV file
            self.data = pd.read_csv(
                filepath, 
                delimiter=delimiter, 
                encoding=encoding,
                on_bad_lines='skip'
            )
            self.current_sheet = None
            self.sheet_names = []
            self.file_type = 'csv'
            
            return True
        except Exception as e:
            print(f"Error loading CSV file: {str(e)}")
            
            # Fallback to default parameters
            try:
                self.data = pd.read_csv(filepath, on_bad_lines='skip')
                self.file_type = 'csv'
                return True
            except:
                return False
    
    def _detect_csv_params(self, filepath: str) -> Tuple[str, str]:
        """
        Detect CSV encoding and delimiter
        
        Args:
            filepath: Path to CSV file
            
        Returns:
            Tuple of (encoding, delimiter)
        """
        # Try to detect encoding
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        detected_encoding = 'utf-8'  # Default
        
        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    sample = f.read(1024)
                    if sample:
                        detected_encoding = encoding
                        break
            except:
                continue
                
        # Try to detect delimiter
        delimiters = [',', ';', '\t', '|']
        counts = {}
        
        try:
            with open(filepath, 'r', encoding=detected_encoding) as f:
                sample = f.read(1024)
                for delimiter in delimiters:
                    counts[delimiter] = sample.count(delimiter)
                
            # Get delimiter with highest count
            max_count = 0
            detected_delimiter = ','  # Default
            
            for delimiter, count in counts.items():
                if count > max_count:
                    max_count = count
                    detected_delimiter = delimiter
        except:
            detected_delimiter = ','
            
        return detected_encoding, detected_delimiter
    
    def get_sheet_names(self) -> List[str]:
        """
        Get available sheet names for Excel files.
        
        Returns:
            List[str]: List of sheet names
        """
        return self.sheet_names
    
    def change_sheet(self, sheet_name: Union[str, int]) -> bool:
        """
        Change the current sheet (Excel only).
        
        Args:
            sheet_name: Sheet name or index
            
        Returns:
            bool: True if successful, False otherwise
        """
        if self.file_type != 'excel':
            return False
            
        try:
            return self._load_excel(self.filepath, sheet_name)
        except:
            return False
    
    def get_data(self) -> pd.DataFrame:
        """
        Get the loaded data as a pandas DataFrame.
        
        Returns:
            pd.DataFrame: The loaded data
        """
        return self.data if self.data is not None else pd.DataFrame()
    
    def get_headers(self) -> List[str]:
        """
        Get column headers from the loaded data.
        
        Returns:
            List[str]: Column headers
        """
        if self.data is None:
            return []
        return list(self.data.columns)
    
    def get_row_count(self) -> int:
        """
        Get number of rows in the loaded data.
        
        Returns:
            int: Number of rows
        """
        if self.data is None:
            return 0
        return len(self.data)
    
    def sample_data(self, n: int = 5) -> pd.DataFrame:
        """
        Get a sample of rows from the data.
        
        Args:
            n: Number of rows to sample
            
        Returns:
            pd.DataFrame: Sample data
        """
        if self.data is None:
            return pd.DataFrame()
        return self.data.head(n)
    
    def to_csv(self, output_path: str) -> bool:
        """
        Export data to CSV.
        
        Args:
            output_path: Path to save CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if self.data is None:
            return False
            
        try:
            self.data.to_csv(output_path, index=False)
            return True
        except Exception as e:
            print(f"Error exporting to CSV: {str(e)}")
            return False
    
    def to_excel(self, output_path: str) -> bool:
        """
        Export data to Excel.
        
        Args:
            output_path: Path to save Excel file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if self.data is None:
            return False
            
        try:
            self.data.to_excel(output_path, index=False)
            return True
        except Exception as e:
            print(f"Error exporting to Excel: {str(e)}")
            return False
    
    def filter_data(self, column: str, value: str, operator: str = "==") -> pd.DataFrame:
        """
        Filter data based on a column value.
        
        Args:
            column: Column name to filter on
            value: Value to filter by
            operator: Comparison operator ("==", "!=", ">", "<", ">=", "<=", "contains")
            
        Returns:
            pd.DataFrame: Filtered data
        """
        if self.data is None or column not in self.data.columns:
            return pd.DataFrame()
            
        try:
            if operator == "==":
                return self.data[self.data[column] == value]
            elif operator == "!=":
                return self.data[self.data[column] != value]
            elif operator == ">":
                return self.data[self.data[column] > value]
            elif operator == "<":
                return self.data[self.data[column] < value]
            elif operator == ">=":
                return self.data[self.data[column] >= value]
            elif operator == "<=":
                return self.data[self.data[column] <= value]
            elif operator == "contains":
                return self.data[self.data[column].astype(str).str.contains(value, na=False)]
            else:
                return self.data
        except Exception as e:
            print(f"Error filtering data: {str(e)}")
            return pd.DataFrame()
    
    def extract_pattern_from_column(self, column: str, pattern: str) -> List[str]:
        """
        Extract text matching the given regex pattern from a specific column.
        
        Args:
            column: Column name to search in
            pattern: Regular expression pattern
            
        Returns:
            List[str]: List of matching strings
        """
        if self.data is None or column not in self.data.columns:
            return []
            
        matches = []
        for value in self.data[column].astype(str):
            matches.extend(re.findall(pattern, value))
            
        # Remove duplicates while preserving order
        unique_matches = []
        seen = set()
        for match in matches:
            if match not in seen:
                unique_matches.append(match)
                seen.add(match)
                
        return unique_matches
