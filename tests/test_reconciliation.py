"""
Test Reconciliation Logic
Verifies core business logic and file processing.
"""

import unittest
import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.reconciliation import ReconciliationEngine
from models.file_processor import FileProcessor

class TestReconciliation(unittest.TestCase):
    
    def setUp(self):
        # Create sample transaction data
        self.transactions = pd.DataFrame({
            "PlazaID": ["123", "123", "123", "123"],
            "Vehicle Reg. No.": ["MH01AB1234", "MH01AB1234", "MH01AB1234", "MH01AB1234"],
            "Reader Read Time": [
                "2023-01-01 10:00:00",  # Trip 1 (Start Window)
                "2023-01-01 14:00:00",  # Trip 2
                "2023-01-01 18:00:00",  # Trip 3 (Window End ~10:00 next day)
                "2023-01-02 11:00:00"   # Trip 1 (New Window)
            ]
        })
        self.transactions["Reader Read Time"] = pd.to_datetime(self.transactions["Reader Read Time"])
        
    def test_trip_count_logic(self):
        """Test TripCount calculation (24-hour rolling window)."""
        df = ReconciliationEngine.calculate_trip_count(self.transactions)
        
        # Access by iloc or column
        trip_counts = df["TripCount"].tolist()
        
        # Expected:
        # 1. 10:00 -> Trip 1 (Window Start)
        # 2. 14:00 -> Trip 2
        # 3. 18:00 -> Trip 3
        # 4. 11:00 next day -> Trip 1 (New Window, >24h from 10:00)
        self.assertEqual(trip_counts, [1, 2, 3, 1])

    def test_atp_qualification(self):
        """Test NAP vs ATP qualification."""
        # Add trip counts manually to test logic
        df = self.transactions.copy()
        df["TripCount"] = [1, 2, 3, 1]
        
        df = ReconciliationEngine.determine_nap_qualification(df)
        
        # Expected:
        # TripCount <= 2 -> IsQualifiedNAP = True (NAP)
        # TripCount > 2  -> IsQualifiedNAP = False (ATP)
        is_nap = df["IsQualifiedNAP"].tolist()
        self.assertEqual(is_nap, [True, True, False, True])

    def test_report_date(self):
        """Test ReportDate logic (cutoff at 8:00 AM)."""
        df = pd.DataFrame({
            "Reader Read Time": [
                pd.Timestamp("2023-01-02 07:59:00"), # Before 8 AM -> Prev Day
                pd.Timestamp("2023-01-02 08:00:00"), # At 8 AM -> Same Day
                pd.Timestamp("2023-01-02 08:01:00"), # After 8 AM -> Same Day
            ]
        })
        
        df = ReconciliationEngine.calculate_report_date(df)
        dates = df["ReportDate"].astype(str).tolist()
        
        self.assertEqual(dates[0], "2023-01-01")
        self.assertEqual(dates[1], "2023-01-02")
        self.assertEqual(dates[2], "2023-01-02")

if __name__ == '__main__':
    unittest.main()
