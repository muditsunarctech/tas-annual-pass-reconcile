"""
Database Model - Redshift Connection and Query Management
Handles all interactions with Amazon Redshift for reading source transaction data.
"""

import os
from typing import Optional, List
from dotenv import load_dotenv

# Import from config
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.db_config import get_connection, build_query, BANK_TABLE_MAP

load_dotenv()


class RedshiftDatabase:
    """Manages Redshift database connections and queries for transaction data."""
    
    def __init__(self):
        """Initialize database connection."""
        self.connection = None
    
    def connect(self):
        """Establish connection to Redshift."""
        if not self.connection:
            self.connection = get_connection()
        return self.connection
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def fetch_transactions(
        self,
        bank: str,
        plaza_ids: List[str],
        start_date: str,
        end_date: str,
        limit: Optional[int] = None
    ):
        """
        Fetch ANNUALPASS transactions from Redshift.
        
        Args:
            bank: Bank name (IDFC or ICICI)
            plaza_ids: List of plaza IDs to query
            start_date: Start date (YYYY-MM-DD HH:MM:SS format)
            end_date: End date (YYYY-MM-DD HH:MM:SS format)
            limit: Optional row limit for testing
        
        Returns:
            pandas.DataFrame: Transaction data
        """
        import pandas as pd
        
        # Build query using config
        query = build_query(bank, plaza_ids, start_date, end_date, limit)
        
        # Execute query
        conn = self.connect()
        df = pd.read_sql(query, conn)
        
        return df
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            bool: True if connection successful
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Convenience functions
def get_redshift_connection():
    """Get a Redshift connection."""
    return get_connection()


def query_transactions(bank: str, plaza_ids: List[str], start_date: str, end_date: str, limit: Optional[int] = None):
    """
    Query transactions from Redshift.
    
    Args:
        bank: Bank name
        plaza_ids: Plaza IDs
        start_date: Start date string
        end_date: End date string
        limit: Optional limit
    
    Returns:
        DataFrame: Transactions
    """
    db = RedshiftDatabase()
    try:
        return db.fetch_transactions(bank, plaza_ids, start_date, end_date, limit)
    finally:
        db.disconnect()
