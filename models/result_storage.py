"""
Result Storage Model - MySQL Connection and CRUD Operations
Handles storage and retrieval of reconciliation results in MySQL.
"""

import os
import uuid
from datetime import datetime
from typing import Optional, Dict, List
import pandas as pd
from dotenv import load_dotenv

try:
    import streamlit as st
except ImportError:
    st = None

load_dotenv()


def get_mysql_config():
    """Get MySQL configuration from Streamlit secrets or environment variables."""
    # Check for Streamlit secrets first
    if st is not None and hasattr(st, "secrets") and "mysql" in st.secrets:
        return st.secrets["mysql"]
    
    # Fallback to environment variables
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "database": os.getenv("MYSQL_DATABASE", "annual_pass_reconciler"),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
    }


class ResultStorage:
    """Manages MySQL storage for reconciliation results."""
    
    def __init__(self):
        """Initialize MySQL connection."""
        self.config = get_mysql_config()
        self.connection = None
    
    def connect(self):
        """Establish connection to MySQL."""
        if not self.connection:
            import mysql.connector
            self.connection = mysql.connector.connect(
                host=self.config["host"],
                port=self.config.get("port", 3306),
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"]
            )
        return self.connection
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def save_run(self, run_metadata: Dict) -> str:
        """
        Save reconciliation run metadata to MySQL.
        
        Args:
            run_metadata: Dictionary with run information:
                - data_source: 'database' or 'file_upload'
                - bank: IDFC or ICICI
                - project: Project name
                - plaza_ids: List of plaza IDs
                - start_date: Query start date
                - end_date: Query end date
                - total_transactions: Total ATP count
                - total_nap: Total NAP count
                - status: 'completed' or 'failed'
                - created_by: Optional user identifier
        
        Returns:
            str: Generated run_id (UUID)
        """
        run_id = str(uuid.uuid4())
        
        conn = self.connect()
        cursor = conn.cursor()
        
        query = """
        INSERT INTO reconciliation_runs (
            run_id, run_date, data_source, bank, project, plaza_ids,
            start_date, end_date, total_transactions, total_nap, status, created_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            run_id,
            datetime.now(),
            run_metadata.get('data_source'),
            run_metadata.get('bank'),
            run_metadata.get('project'),
            ','.join(run_metadata.get('plaza_ids', [])),
            run_metadata.get('start_date'),
            run_metadata.get('end_date'),
            run_metadata.get('total_transactions', 0),
            run_metadata.get('total_nap', 0),
            run_metadata.get('status', 'completed'),
            run_metadata.get('created_by')
        )
        
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        
        return run_id
    
    def save_transactions(self, run_id: str, transactions: pd.DataFrame):
        """
        Save transaction details with TripCount to MySQL.
        
        Args:
            run_id: Run ID from save_run()
            transactions: DataFrame with columns:
                - PlazaID, PlazaName, Vehicle Reg. No., Tag ID
                - Reader Read Time, TripCount, ReportDate
                - IsQualifiedNAP, TripType
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        query = """
        INSERT INTO reconciliation_transactions (
            run_id, plaza_id, plaza_name, vehicle_reg_no, tag_id,
            transaction_time, trip_count, report_date, is_qualified_nap, trip_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare data
        for _, row in transactions.iterrows():
            values = (
                run_id,
                row.get('PlazaID'),
                row.get('PlazaName'),
                row.get('Vehicle Reg. No.'),
                row.get('Tag ID'),
                row.get('Reader Read Time'),
                row.get('TripCount'),
                row.get('ReportDate'),
                1 if row.get('IsQualifiedNAP') else 0,
                row.get('TripType')
            )
            cursor.execute(query, values)
        
        conn.commit()
        cursor.close()
    
    def save_daily_summary(self, run_id: str, summary: pd.DataFrame):
        """
        Save daily ATP/NAP summary to MySQL.
        
        Args:
            run_id: Run ID from save_run()
            summary: DataFrame with columns:
                - ProjectName, PlazaID, PlazaName, ReportDate, ATP, NAP
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        query = """
        INSERT INTO reconciliation_daily_summary (
            run_id, project_name, plaza_id, plaza_name, report_date, atp, nap
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        for _, row in summary.iterrows():
            values = (
                run_id,
                row.get('ProjectName'),
                row.get('PlazaID'),
                row.get('PlazaName'),
                row.get('ReportDate'),
                row.get('ATP', 0),
                row.get('NAP', 0)
            )
            cursor.execute(query, values)
        
        conn.commit()
        cursor.close()
    
    def get_run_history(self, filters: Optional[Dict] = None) -> pd.DataFrame:
        """
        Retrieve past reconciliation runs from MySQL.
        
        Args:
            filters: Optional filters:
                - bank: Bank name
                - project: Project name
                - start_date: Filter by run_date >= start_date
                - end_date: Filter by run_date <= end_date
        
        Returns:
            DataFrame: Run history
        """
        conn = self.connect()
        
        query = "SELECT * FROM reconciliation_runs WHERE 1=1"
        params = []
        
        if filters:
            if 'bank' in filters and filters['bank'] != 'All':
                query += " AND bank = %s"
                params.append(filters['bank'])
            
            if 'project' in filters and filters['project'] != 'All':
                query += " AND project = %s"
                params.append(filters['project'])
            
            if 'start_date' in filters:
                query += " AND run_date >= %s"
                params.append(filters['start_date'])
            
            if 'end_date' in filters:
                query += " AND run_date <= %s"
                params.append(filters['end_date'])
        
        query += " ORDER BY run_date DESC"
        
        df = pd.read_sql(query, conn, params=params if params else None)
        return df
    
    def get_run_details(self, run_id: str) -> Dict:
        """
        Get full details of a specific run from MySQL.
        
        Args:
            run_id: Run ID
        
        Returns:
            Dict with keys: 'metadata', 'transactions', 'summary'
        """
        conn = self.connect()
        
        # Get metadata
        metadata_df = pd.read_sql(
            "SELECT * FROM reconciliation_runs WHERE run_id = %s",
            conn,
            params=(run_id,)
        )
        
        # Get transactions
        transactions_df = pd.read_sql(
            "SELECT * FROM reconciliation_transactions WHERE run_id = %s",
            conn,
            params=(run_id,)
        )
        
        # Get summary
        summary_df = pd.read_sql(
            "SELECT * FROM reconciliation_daily_summary WHERE run_id = %s",
            conn,
            params=(run_id,)
        )
        
        return {
            'metadata': metadata_df,
            'transactions': transactions_df,
            'summary': summary_df
        }
    
    def test_connection(self) -> bool:
        """
        Test MySQL connection.
        
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
            print(f"MySQL connection test failed: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
