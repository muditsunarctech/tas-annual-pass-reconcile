"""
File Processor Model - File Upload and Parsing Logic
Handles CSV and Excel file uploads, parses data, and filters for ANNUALPASS transactions.
"""

import os
import pandas as pd
import polars as pl
from typing import List, Dict, Optional, Tuple
from io import BytesIO


class FileProcessor:
    """
    Processes uploaded files (CSV, Excel) and extracts ANNUALPASS transactions.
    """
    
    # Supported file extensions
    SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".xlsb"}
    
    # Annual Pass filter values
    ANNUAL_PASS_VALUES = ["ANNUALPASS", "ANNUAL PASS", "ANNUAL_PASS"]
    
    # Common Plaza ID column headers
    PLAZA_ID_HEADERS = [
        "PlazaID", "Plaza ID", "Plaza Id", "plaza_id", 
        "conc_plaza_id", "ihmclplazacode"
    ]
    
    # Transaction reason columns
    REASON_CODE_HEADERS = [
        "acq_txn_reason", "acqtxnreason", "ReasonCode", 
        "TRC_VRC_REASON_CODE", "TransactionReasonCode"
    ]
    
    def __init__(self):
        """Initialize file processor."""
        pass
    
    @staticmethod
    def read_file(file_path: str) -> pd.DataFrame:
        """
        Read a single file (CSV or Excel) into a DataFrame.
        
        Args:
            file_path: Path to file
        
        Returns:
            pandas.DataFrame: File data
        """
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext == ".csv":
            return pd.read_csv(file_path)
        elif ext in [".xlsx", ".xls", ".xlsb"]:
            return pd.read_excel(file_path, engine='openpyxl' if ext == '.xlsx' else 'xlrd')
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
    
    @staticmethod
    def read_uploaded_file(uploaded_file) -> pd.DataFrame:
        """
        Read an uploaded Streamlit file object.
        
        Args:
            uploaded_file: Streamlit UploadedFile object
        
        Returns:
            pandas.DataFrame: File data
        """
        ext = os.path.splitext(uploaded_file.name)[1].lower()
        
        if ext == ".csv":
            return pd.read_csv(uploaded_file)
        elif ext in [".xlsx", ".xls", ".xlsb"]:
            return pd.read_excel(BytesIO(uploaded_file.read()), 
                               engine='openpyxl' if ext == '.xlsx' else 'xlrd')
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
    
    @classmethod
    def find_column(cls, columns: List[str], possible_names: List[str]) -> Optional[str]:
        """
        Find a column name from a list of possibilities.
        
        Args:
            columns: Available column names
            possible_names: List of possible column names to search for
        
        Returns:
            Matched column name or None
        """
        columns_lower = {c.lower(): c for c in columns}
        for name in possible_names:
            if name.lower() in columns_lower:
                return columns_lower[name.lower()]
        return None
    
    @classmethod
    def filter_annual_pass(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame for ANNUALPASS transactions.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Filtered DataFrame containing only ANNUALPASS records
        """
        # Find reason code column
        reason_col = cls.find_column(df.columns.tolist(), cls.REASON_CODE_HEADERS)
        
        if not reason_col:
            # If no reason column found, return all data
            # (user may have pre-filtered)
            return df
        
        # Filter for ANNUALPASS
        mask = df[reason_col].astype(str).str.upper().str.strip().isin(
            [v.upper() for v in cls.ANNUAL_PASS_VALUES]
        )
        
        return df[mask].copy()
    
    @classmethod
    def extract_plaza_id(cls, df: pd.DataFrame) -> Optional[str]:
        """
        Extract plaza ID from DataFrame.
        
        Args:
            df: DataFrame
        
        Returns:
            Plaza ID string or None
        """
        plaza_col = cls.find_column(df.columns.tolist(), cls.PLAZA_ID_HEADERS)
        
        if not plaza_col:
            return None
        
        # Get first non-null plaza ID
        plaza_series = df[plaza_col].dropna()
        if len(plaza_series) > 0:
            return str(plaza_series.iloc[0]).strip()
        
        return None
    
    @classmethod
    def normalize_columns(cls, df: pd.DataFrame, bank: str = None) -> pd.DataFrame:
        """
        Normalize column names to standard format.
        
        Args:
            df: Input DataFrame
            bank: Optional bank name for bank-specific mappings
        
        Returns:
            DataFrame with normalized column names
        """
        # Standard column mapping
        column_map = {
            # Plaza ID
            "conc_plaza_id": "PlazaID",
            "ihmclplazacode": "PlazaID",
            "Plaza Id": "PlazaID",
            "plaza_id": "PlazaID",
            
            # Vehicle Registration
            "conc_vrn_no": "Vehicle Reg. No.",
            "vrn": "Vehicle Reg. No.",
            "VRN": "Vehicle Reg. No.",
            "Vehicle Reg. No": "Vehicle Reg. No.",
            
            # Tag ID
            "conc_tag_id": "Tag ID",
            "tagid": "Tag ID",
            "TagID": "Tag ID",
            "Tag Id": "Tag ID",
            
            # Transaction Time
            "conc_txn_dt_processed": "Reader Read Time",
            "acqtxndateprocessed": "Reader Read Time",
            "TransactionDateTime": "Reader Read Time",
            "Reader Read Time": "Reader Read Time",
            
            # Trip Type
            "acq_txn_desc": "TripType",
            "triptype": "TripType",
            "TripType": "TripType",
        }
        
        # Rename columns
        df = df.rename(columns=column_map)
        
        return df
    
    @classmethod
    def process_file(cls, file_path: str, bank: str = None) -> pd.DataFrame:
        """
        Full file processing pipeline.
        
        Args:
            file_path: Path to file
            bank: Optional bank name
        
        Returns:
            Processed DataFrame with ANNUALPASS transactions
        """
        # Read file
        df = cls.read_file(file_path)
        
        # Filter for ANNUALPASS
        df = cls.filter_annual_pass(df)
        
        # Normalize columns
        df = cls.normalize_columns(df, bank)
        
        return df
    
    @classmethod
    def process_uploaded_files(cls, uploaded_files: List, bank: str = None) -> pd.DataFrame:
        """
        Process multiple uploaded files and combine into single DataFrame.
        
        Args:
            uploaded_files: List of Streamlit UploadedFile objects
            bank: Optional bank name
        
        Returns:
            Combined DataFrame
        """
        dfs = []
        
        for uploaded_file in uploaded_files:
            try:
                df = cls.read_uploaded_file(uploaded_file)
                df = cls.filter_annual_pass(df)
                df = cls.normalize_columns(df, bank)
                dfs.append(df)
            except Exception as e:
                print(f"Error processing {uploaded_file.name}: {e}")
                continue
        
        if not dfs:
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined = pd.concat(dfs, ignore_index=True)
        
        return combined


# Convenience functions
def process_file(file_path: str, bank: str = None) -> pd.DataFrame:
    """Process a single file."""
    return FileProcessor.process_file(file_path, bank)


def process_uploaded_files(uploaded_files: List, bank: str = None) -> pd.DataFrame:
    """Process multiple uploaded files."""
    return FileProcessor.process_uploaded_files(uploaded_files, bank)
