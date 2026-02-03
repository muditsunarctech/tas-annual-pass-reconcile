"""
Data Consolidator Controller - Consolidate and Normalize Data
Groups data by project/plaza and adds metadata.
"""

import pandas as pd
from typing import Dict, List
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.plaza_config import get_plaza_info


class DataConsolidator:
    """
    Consolidates and normalizes transaction data.
    """
    
    @staticmethod
    def consolidate(df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """
        Consolidate transaction data.
        
        Steps:
        1. Add plaza metadata (Bank, PlazaName, ProjectName)
        2. Add source month from transaction dates
        3. Normalize column names
        4. Sort data
        
        Args:
            df: Input transaction DataFrame
            metadata: Metadata dict with 'bank' key
        
        Returns:
            Consolidated DataFrame
        """
        if df.empty:
            return df
        
        # Step 1: Add plaza metadata
        df = DataConsolidator._add_plaza_metadata(df, metadata.get('bank'))
        
        # Step 2: Add source month (if Reader Read Time exists)
        if 'Reader Read Time' in df.columns:
            df = DataConsolidator._add_source_month(df)
        
        # Step 3: Ensure required columns exist
        df = DataConsolidator._ensure_required_columns(df)
        
        # Step 4: Sort by plaza, vehicle, and time
        sort_cols = []
        if 'PlazaID' in df.columns:
            sort_cols.append('PlazaID')
        if 'Vehicle Reg. No.' in df.columns:
            sort_cols.append('Vehicle Reg. No.')
        if 'Reader Read Time' in df.columns:
            sort_cols.append('Reader Read Time')
        
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)
        
        return df
    
    @staticmethod
    def _add_plaza_metadata(df: pd.DataFrame, bank: str = None) -> pd.DataFrame:
        """
        Add plaza metadata (Bank, PlazaName, ProjectName) based on PlazaID.
        
        Args:
            df: DataFrame
            bank: Optional bank name
        
        Returns:
            DataFrame with added metadata
        """
        if 'PlazaID' not in df.columns:
            return df
        
        def get_metadata(plaza_id):
            info = get_plaza_info(str(plaza_id), bank)
            if info:
                return pd.Series({
                    'Bank': info['bank'],
                    'PlazaName': info['plaza'],
                    'ProjectName': info['project']
                })
            else:
                return pd.Series({
                    'Bank': bank or 'Unknown',
                    'PlazaName': 'Unknown',
                    'ProjectName': 'Unknown'
                })
        
        # Apply metadata enrichment
        plaza_meta = df['PlazaID'].apply(get_metadata)
        df['Bank'] = plaza_meta['Bank']
        df['PlazaName'] = plaza_meta['PlazaName']
        df['ProjectName'] = plaza_meta['ProjectName']
        
        return df
    
    @staticmethod
    def _add_source_month(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add SourceMonth column based on transaction timestamp.
        
        Args:
            df: DataFrame with Reader Read Time column
        
        Returns:
            DataFrame with SourceMonth column
        """
        if 'Reader Read Time' not in df.columns:
            return df
        
        # Ensure timestamp column is datetime
        df['Reader Read Time'] = pd.to_datetime(df['Reader Read Time'])
        
        # Extract month-year (e.g., "Jan-26")
        df['SourceMonth'] = df['Reader Read Time'].dt.strftime('%b-%y')
        
        return df
    
    @staticmethod
    def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure all required columns exist.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with required columns
        """
        required_cols = {
            'PlazaID': 'Unknown',
            'Vehicle Reg. No.': 'Unknown',
            'Tag ID': 'Unknown',
            'Reader Read Time': pd.NaT,
            'PlazaName': 'Unknown',
            'ProjectName': 'Unknown',
            'Bank': 'Unknown'
        }
        
        for col, default_value in required_cols.items():
            if col not in df.columns:
                df[col] = default_value
        
        return df
    
    @staticmethod
    def group_by_project(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Group DataFrame by ProjectName.
        
        Args:
            df: Consolidated DataFrame
        
        Returns:
            Dict of project_name: dataframe
        """
        if 'ProjectName' not in df.columns or df.empty:
            return {'Unknown': df}
        
        grouped = {}
        for project in df['ProjectName'].unique():
            grouped[project] = df[df['ProjectName'] == project].copy()
        
        return grouped


# Convenience function
def consolidate_data(df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
    """
    Consolidate transaction data.
    
    Args:
        df: Input DataFrame
        metadata: Metadata dict
    
    Returns:
        Consolidated DataFrame
    """
    return DataConsolidator.consolidate(df, metadata)
