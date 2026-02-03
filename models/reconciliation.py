"""
Reconciliation Model - Core Business Logic
Handles TripCount calculation, ReportDate logic, and ATP/NAP determination.
"""

import pandas as pd
import warnings
from typing import Dict, Tuple


class ReconciliationEngine:
    """
    Core reconciliation logic for Annual Pass transactions.
    Calculates TripCount, ReportDate, and determines ATP/NAP qualification.
    """
    
    @staticmethod
    def calculate_trip_count(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate TripCount for each vehicle within 24-hour windows.
        
        Logic:
        - Group by PlazaID and Vehicle Reg. No.
        - For each transaction, count trips within a rolling 24-hour window
        - Window resets when next transaction is >24 hours after window start
        
        Args:
            df: DataFrame with columns: PlazaID, Vehicle Reg. No., Reader Read Time
        
        Returns:
            DataFrame with added TripCount column
        """
        def calc_tripcount(group):
            group = group.sort_values("Reader Read Time").copy()
            times = group["Reader Read Time"].values
            n = len(times)
            trip_counts = []
            window_start = None
            window_end = None
            trip_count = 0
            
            for i in range(n):
                t = pd.Timestamp(times[i])
                if window_start is None or t > window_end:
                    # Start new 24-hour window
                    window_start = t
                    window_end = window_start + pd.Timedelta(hours=24)
                    trip_count = 1
                else:
                    # Within current window
                    trip_count += 1
                trip_counts.append(trip_count)
            
            group["TripCount"] = trip_counts
            return group
        
        # Apply TripCount calculation
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            df = df.groupby(
                ["PlazaID", "Vehicle Reg. No."],
                sort=False,
                group_keys=False
            ).apply(calc_tripcount)
        
        return df.reset_index(drop=True)
    
    @staticmethod
    def calculate_report_date(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate ReportDate based on transaction timestamp.
        
        Logic:
        - If transaction occurs before 08:00 AM, attribute to previous day
        - Otherwise, attribute to same day
        
        Args:
            df: DataFrame with Reader Read Time column
        
        Returns:
            DataFrame with added ReportDate column
        """
        def report_date(ts):
            if pd.isna(ts):
                return None
            cutoff_time = pd.Timestamp("08:00").time()
            if ts.time() < cutoff_time:
                return (ts - pd.Timedelta(days=1)).date()
            else:
                return ts.date()
        
        df["ReportDate"] = df["Reader Read Time"].apply(report_date)
        return df
    
    @staticmethod
    def determine_nap_qualification(df: pd.DataFrame) -> pd.DataFrame:
        """
        Determine if a transaction qualifies as NAP (Not Annual Pass).
        
        Logic:
        - NAP if TripCount <= 2 (vehicle used pass 2 or fewer times in 24 hours)
        - ATP otherwise (valid Annual Pass usage)
        
        Args:
            df: DataFrame with TripCount column
        
        Returns:
            DataFrame with added IsQualifiedNAP column
        """
        df["IsQualifiedNAP"] = df["TripCount"] <= 2
        return df
    
    @staticmethod
    def generate_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate daily ATP/NAP summary by project and plaza.
        
        Args:
            df: DataFrame with ProjectName, PlazaID, PlazaName, ReportDate, IsQualifiedNAP
        
        Returns:
            DataFrame with columns: ProjectName, PlazaID, PlazaName, ReportDate, ATP, NAP
        """
        daily_summary = (
            df.groupby(["ProjectName", "PlazaID", "PlazaName", "ReportDate"])
            .agg(
                ATP=("Reader Read Time", "count"),  # Total count
                NAP=("IsQualifiedNAP", "sum")       # Count of NAP-qualified
            )
            .reset_index()
            .sort_values(["ProjectName", "PlazaID", "ReportDate"])
        )
        
        return daily_summary
    
    @classmethod
    def reconcile(cls, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Full reconciliation pipeline.
        
        Args:
            df: Input DataFrame with transaction data
        
        Returns:
            Tuple of (transactions_with_tripcount, daily_summary)
        """
        # Ensure required columns exist
        required_cols = ["PlazaID", "Vehicle Reg. No.", "Reader Read Time"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Ensure timestamp column is datetime
        df["Reader Read Time"] = pd.to_datetime(df["Reader Read Time"])
        
        # Sort by plaza, vehicle, and time
        df = df.sort_values(
            ["PlazaID", "Vehicle Reg. No.", "Reader Read Time"]
        ).reset_index(drop=True)
        
        # Apply reconciliation steps
        df = cls.calculate_trip_count(df)
        df = cls.calculate_report_date(df)
        df = cls.determine_nap_qualification(df)
        
        # Generate summary
        summary = cls.generate_daily_summary(df)
        
        return df, summary


# Convenience functions
def reconcile_transactions(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reconcile transactions.
    
    Args:
        df: Transaction DataFrame
    
    Returns:
        Tuple of (transactions, summary)
    """
    return ReconciliationEngine.reconcile(df)


def calculate_trip_count(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate TripCount for transactions."""
    return ReconciliationEngine.calculate_trip_count(df)


def generate_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Generate daily ATP/NAP summary."""
    return ReconciliationEngine.generate_daily_summary(df)
