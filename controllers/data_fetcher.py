"""
Data Fetcher Controller - Fetch Data from Multiple Sources
Handles data retrieval from both database and file sources.
"""

import pandas as pd
from typing import Dict, List, Optional
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.database import RedshiftDatabase
from models.file_processor import FileProcessor


class DataFetcher:
    """
    Fetches data from database or file sources.
    """
    
    def __init__(self, source_type: str):
        """
        Initialize data fetch controller.
        
        Args:
            source_type: 'database' or 'file_upload'
        """
        self.source_type = source_type
        self.db = RedshiftDatabase() if source_type == 'database' else None
    
    def fetch(self, params: Dict) -> pd.DataFrame:
        """
        Fetch data based on source type.
        
        Args:
            params: Parameters dict containing:
                For database:
                    - bank: Bank name
                    - plaza_ids: List of plaza IDs
                    - start_date: Start datetime string
                    - end_date: End datetime string
                For files:
                    - uploaded_files: List of uploaded file objects
                    - bank: Optional bank name
        
        Returns:
            pd.DataFrame: Fetched transaction data
        """
        if self.source_type == 'database':
            return self._fetch_from_database(params)
        else:
            return self._fetch_from_files(params)
    
    def _fetch_from_database(self, params: Dict) -> pd.DataFrame:
        """
        Fetch data from Redshift database.
        
        Args:
            params: Database query parameters
        
        Returns:
            DataFrame of transactions
        """
        bank = params['bank']
        plaza_ids = params['plaza_ids']
        start_date = f"{params['start_date']} 00:00:00"
        end_date = f"{params['end_date']} 23:59:59"
        limit = params.get('limit')
        
        # Fetch from database
        df = self.db.fetch_transactions(
            bank=bank,
            plaza_ids=plaza_ids,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        
        return df
    
    def _fetch_from_files(self, params: Dict) -> pd.DataFrame:
        """
        Fetch data from uploaded files.
        
        Args:
            params: File parameters
        
        Returns:
            DataFrame of transactions
        """
        uploaded_files = params['uploaded_files']
        bank = params.get('bank')
        
        # Process files
        df = FileProcessor.process_uploaded_files(uploaded_files, bank)
        
        return df
    
    def cleanup(self):
        """Clean up resources."""
        if self.db:
            self.db.disconnect()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()


# Convenience function
def fetch_data(source_type: str, params: Dict) -> pd.DataFrame:
    """
    Fetch data from source.
    
    Args:
        source_type: 'database' or 'file_upload'
        params: Fetch parameters
    
    Returns:
        DataFrame
    """
    with DataFetcher(source_type) as fetcher:
        return fetcher.fetch(params)
