"""
Reconciler Controller - Pipeline Orchestration
Coordinates the full reconciliation pipeline and manages result persistence.
"""

import os
import uuid
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Tuple
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.reconciliation import ReconciliationEngine
from models.result_storage import ResultStorage


class ReconcilerController:
    """
    Orchestrates the reconciliation pipeline.
    Handles data processing, reconciliation logic, and result persistence.
    """
    
    def __init__(self, save_to_db: bool = True):
        """
        Initialize controller.
        
        Args:
            save_to_db: Whether to save results to MySQL
        """
        self.save_to_db = save_to_db
        self.result_storage = ResultStorage() if save_to_db else None
        self.run_id = None
    
    def run_pipeline(
        self,
        df: pd.DataFrame,
        metadata: Dict,
        progress_callback: Optional[callable] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """
        Execute full reconciliation pipeline.
        
        Args:
            df: Input transaction DataFrame
            metadata: Run metadata dict with keys:
                - data_source: 'database' or 'file_upload'
                - bank: IDFC or ICICI
                - project: Project name
                - plaza_ids: List of plaza IDs
                - start_date: Start date
                - end_date: End date
            progress_callback: Optional callback for progress updates
        
        Returns:
            Tuple of (transactions_df, summary_df, results_dict)
        """
        try:
            # Generate run ID
            self.run_id = str(uuid.uuid4())
            
            if progress_callback:
                progress_callback(0.1, "Starting reconciliation...")
            
            # Step 1: Add plaza metadata if not present
            if 'PlazaName' not in df.columns or 'ProjectName' not in df.columns:
                df = self._add_plaza_metadata(df, metadata)
            
            if progress_callback:
                progress_callback(0.3, "Calculating TripCount...")
            
            # Step 2: Run reconciliation engine
            transactions_df, summary_df = ReconciliationEngine.reconcile(df)
            
            if progress_callback:
                progress_callback(0.7, "Generating summaries...")
            
            # Step 3: Calculate metrics
            total_transactions = len(transactions_df)
            total_nap = transactions_df['IsQualifiedNAP'].sum()
            
            results = {
                'run_id': self.run_id,
                'total_transactions': total_transactions,
                'total_nap': int(total_nap),
                'total_atp': total_transactions - int(total_nap),
                'summary_rows': len(summary_df),
                'status': 'completed'
            }
            
            if progress_callback:
                progress_callback(0.9, "Saving results...")
            
            # Step 4: Save to database if enabled
            if self.save_to_db and self.result_storage:
                self._save_results(
                    transactions_df,
                    summary_df,
                    metadata,
                    results
                )
            
            if progress_callback:
                progress_callback(1.0, "Complete!")
            
            return transactions_df, summary_df, results
            
        except Exception as e:
            # Log error and update status
            results = {
                'run_id': self.run_id,
                'status': 'failed',
                'error': str(e)
            }
            
            if self.save_to_db and self.result_storage:
                # Save failed run metadata
                run_meta = {
                    **metadata,
                    'total_transactions': 0,
                    'total_nap': 0,
                    'status': 'failed'
                }
                self.result_storage.save_run(run_meta)
            
            raise e
    
    def _add_plaza_metadata(self, df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """
        Add plaza metadata (PlazaName, ProjectName) to DataFrame.
        
        Args:
            df: Transaction DataFrame
            metadata: Run metadata
        
        Returns:
            DataFrame with added metadata columns
        """
        from config.plaza_config import get_plaza_info
        
        # Add metadata based on PlazaID
        def get_metadata(plaza_id):
            info = get_plaza_info(plaza_id, metadata.get('bank'))
            if info:
                return pd.Series({
                    'PlazaName': info['plaza'],
                    'ProjectName': info['project']
                })
            else:
                return pd.Series({
                    'PlazaName': 'Unknown',
                    'ProjectName': metadata.get('project', 'Unknown')
                })
        
        if 'PlazaID' in df.columns:
            plaza_meta = df['PlazaID'].apply(get_metadata)
            df['PlazaName'] = plaza_meta['PlazaName']
            df['ProjectName'] = plaza_meta['ProjectName']
        else:
            # Fallback: use metadata
            df['PlazaName'] = 'Unknown'
            df['ProjectName'] = metadata.get('project', 'Unknown')
        
        return df
    
    def _save_results(
        self,
        transactions_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        metadata: Dict,
        results: Dict
    ):
        """
        Save reconciliation results to MySQL.
        
        Args:
            transactions_df: Transactions with TripCount
            summary_df: Daily summary
            metadata: Run metadata
            results: Results dict
        """
        # Prepare run metadata
        run_meta = {
            'data_source': metadata.get('data_source'),
            'bank': metadata.get('bank'),
            'project': metadata.get('project'),
            'plaza_ids': metadata.get('plaza_ids', []),
            'start_date': metadata.get('start_date'),
            'end_date': metadata.get('end_date'),
            'total_transactions': results['total_transactions'],
            'total_nap': results['total_nap'],
            'status': results['status'],
            'created_by': metadata.get('created_by')
        }
        
        # Save run metadata
        run_id = self.result_storage.save_run(run_meta)
        
        # Update run_id in results
        results['run_id'] = run_id
        self.run_id = run_id
        
        # Save transactions
        self.result_storage.save_transactions(run_id, transactions_df)
        
        # Save daily summary
        self.result_storage.save_daily_summary(run_id, summary_df)
    
    def get_run_history(self, filters: Optional[Dict] = None) -> pd.DataFrame:
        """
        Get reconciliation run history from MySQL.
        
        Args:
            filters: Optional filters
        
        Returns:
            DataFrame of run history
        """
        if not self.result_storage:
            return pd.DataFrame()
        
        return self.result_storage.get_run_history(filters)
    
    def get_run_details(self, run_id: str) -> Dict:
        """
        Get details of a specific run from MySQL.
        
        Args:
            run_id: Run ID
        
        Returns:
            Dict with 'metadata', 'transactions', 'summary'
        """
        if not self.result_storage:
            return {}
        
        return self.result_storage.get_run_details(run_id)
    
    def cleanup(self):
        """Clean up resources."""
        if self.result_storage:
            self.result_storage.disconnect()


# Convenience function
def reconcile(
    df: pd.DataFrame,
    metadata: Dict,
    save_to_db: bool = True,
    progress_callback: Optional[callable] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Run reconciliation pipeline.
    
    Args:
        df: Input DataFrame
        metadata: Run metadata
        save_to_db: Whether to save to MySQL
        progress_callback: Progress callback
    
    Returns:
        Tuple of (transactions, summary, results)
    """
    controller = ReconcilerController(save_to_db=save_to_db)
    try:
        return controller.run_pipeline(df, metadata, progress_callback)
    finally:
        controller.cleanup()
