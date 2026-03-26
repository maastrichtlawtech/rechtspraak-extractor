import unittest
import sqlite3
import tempfile
import os
import pandas as pd
import warnings
from pathlib import Path

from rechtspraak_extractor.rechtspraak import get_rechtspraak
from rechtspraak_extractor.rechtspraak_metadata import get_rechtspraak_metadata

class TestSQLiteMetadataExtraction(unittest.TestCase):
    def setUp(self):
        # Ignore warning about pandas iteritems or other deprecations locally
        warnings.simplefilter(action='ignore', category=FutureWarning)
        
        # Use the actual local database instead of a temporary one
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "lido_metadata.db"))

    def test_sqlite_extraction(self):
        """Test retrieving metadata using the fast SQLite method."""
        # 1. Provide input dataframe with 'id' and 'link'
        test_ecli = "ECLI:NL:RBALK:2007:BB6697"
        df_input = pd.DataFrame([
            {
                "id": test_ecli,
                "link": f"http://deeplink.rechtspraak.nl/uitspraak?id={test_ecli}",
                "summary": "Sample summary"
            }
        ])

        # 2. Call the metadata extractor pointing to our actual SQLite database
        df_result = get_rechtspraak_metadata(
            save_file="n",              # Return DataFrame instead of writing CSV
            dataframe=df_input,         # Input data
            method="sqlite",            # Use the built SQLite approach
            sqlite_db_path=self.db_path,# Path to our database
            fallback_to_api=True        # We allow fallback to api since db might not be fully populated
        )

        # 3. Assertions
        self.assertIsNotNone(df_result)
        
        if df_result.empty:
            self.skipTest("API returned no data (likely HTTP 403 or DB unpopulated). Skipping assertion.")
            
        self.assertEqual(len(df_result), 1)

        # Confirm exact mappings and returned values
        result_row = df_result.iloc[0]
        self.assertEqual(result_row['ecli'], test_ecli)
        self.assertIn('instance', df_result.columns)
        self.assertIn('document_type', df_result.columns)

    def test_sqlite_missing_ecli_without_fallback(self):
        """Test how it handles an ECLI not in the database when fallback is False."""
        df_input = pd.DataFrame([
            {"id": "ECLI:NL:NOTFOUND:2026:2", "link": "http://fake-link.nl", "summary": "No metadata"}
        ])

        df_result = get_rechtspraak_metadata(
            save_file="n",
            dataframe=df_input,
            method="sqlite",
            sqlite_db_path=self.db_path,
            fallback_to_api=False
        )

        # Should be empty because it wasn't found in DB and we disabled fallback
        self.assertTrue(df_result.empty)

    def test_end_to_end_sqlite_workflow(self):
        """Test the full workflow: get_rechtspraak -> get_rechtspraak_metadata (sqlite) -> Save to CSV"""
        # Ensure data dir exists
        Path("data/raw").mkdir(parents=True, exist_ok=True)
        
        # 1. Download basic cases (get_rechtspraak)
        # We only snatch a tiny amount of recent cases for the test
        df = get_rechtspraak(sd="2025-04-01", ed="2025-04-08", save_file="n")
        
        # Note: If `get_rechtspraak` hits a 403 API rate limit during testing, `df` will be None.
        # This skips the remainder of the test properly instead of aggressively failing.
        if df is None or df.empty:
            self.skipTest("get_rechtspraak returned no data (likely HTTP 403 or API unavailable). Skipping integration test.")
            
        result = get_rechtspraak_metadata(
            save_file="y",
            dataframe=df,
            method="sqlite",
            sqlite_db_path=self.db_path,  # Use our mocked DB. Real ECLIs won't be inside it.
            fallback_to_api=True          # True tests if it correctly triggers the fallback mechanism to resolve the 5 cases
        )
        
        self.assertTrue(result)
        
        # Validate that the CSV was fully generated
        # The filename created internally has a prefix 'custom_rechtspraak_' and timestamp
        csv_files = list(Path("data/raw").glob("custom_rechtspraak_*_metadata.csv"))
        self.assertTrue(len(csv_files) > 0, "Wait, merged metadata CSV was not saved to disk.")

if __name__ == '__main__':
    unittest.main()
