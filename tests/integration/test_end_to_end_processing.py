# tests/integration/test_end_to_end_processing.py
import unittest
import tempfile
import json
import sqlite3
from pathlib import Path
import shutil
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.application import DataIngestionApplication

class TestEndToEndProcessing(unittest.TestCase):
    """Integration tests using real files and database operations"""
    
    def setUp(self):
        # Create temporary database
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()
        
        # Create temp dir and copy static test files into it
        src_dir = Path(__file__).parent / "integration_test_data"
        self.test_dir = Path(tempfile.mkdtemp())
        for file in src_dir.iterdir():
            shutil.copy(file, self.test_dir)
        
        # Initialize application
        self.app = DataIngestionApplication()
        
    def test_complete_processing_workflow(self):
        """Test the complete end-to-end processing workflow"""
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert basic processing results
        self.assertEqual(result['processed_files'], 3)  # customers, orders, edge_cases (empty.json has no data)
        self.assertEqual(result['total_records'], 4)     # 2 + 1 + 1
        self.assertEqual(len(result['errors']), 1)       # malformed.json error
        self.assertGreater(result['processing_time_seconds'], 0)
        
        # Verify database was created and populated
        self.assertTrue(Path(self.test_db.name).exists())
        
        # Connect to database and verify contents
        conn = sqlite3.connect(self.test_db.name)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        cursor = conn.cursor()
        
        # Verify table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='processed_data'")
        table_exists = cursor.fetchone() is not None
        self.assertTrue(table_exists)
        
        # Verify record count
        cursor.execute("SELECT COUNT(*) as count FROM processed_data")
        count = cursor.fetchone()['count']
        self.assertEqual(count, 4)
        
        # Verify some data was inserted (basic validation)
        cursor.execute("SELECT * FROM processed_data LIMIT 1")
        sample_record = cursor.fetchone()
        self.assertIsNotNone(sample_record)
        
        # Verify that _source_file metadata was added
        cursor.execute("SELECT _source_file FROM processed_data LIMIT 1")
        source_file_record = cursor.fetchone()
        self.assertIsNotNone(source_file_record)
        self.assertIsNotNone(source_file_record['_source_file'])
        
        conn.close()
        
    def test_schema_inference_across_files(self):
        """Test that schema inference works correctly across multiple files"""
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Verify schema includes fields from all files
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        
        # Get table schema
        cursor.execute("PRAGMA table_info(processed_data)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]  # Column name is at index 1
        
        # Verify presence of fields from different files
        expected_fields = {
            'customer_id', 'name', 'email',           # From customers.json
            'order_id', 'shipping_address', 'items',  # From orders.json
            'unicode_text', 'special_chars',          # From edge_cases.json
            '_source_file'                            # Metadata field
        }
        
        for field in expected_fields:
            self.assertIn(field, column_names, f"Field '{field}' missing from schema")
        
        conn.close()
        
    def test_error_handling_and_recovery(self):
        """Test that processing continues despite individual file errors"""
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert that processing continued despite malformed.json error
        self.assertEqual(len(result['errors']), 1)
        self.assertIn('malformed.json', result['errors'][0])
        
        # Verify that other files were still processed successfully
        self.assertEqual(result['processed_files'], 3)  # All valid files processed
        
        # Verify database contains data from valid files
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM processed_data")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 4)  # Records from valid files only
        conn.close()
        
    def tearDown(self):
        """Clean up test files and directories"""
        # Force close any remaining database connections
        import gc
        import sqlite3
        
        gc.collect()
        try:
            temp_conn = sqlite3.connect(self.test_db.name)
            temp_conn.close()
        except:
            pass
            
        # Clean up temporary directory
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)
        
        try:
            Path(self.test_db.name).unlink()
        except (FileNotFoundError, PermissionError):
            import time
            time.sleep(0.1)
            try:
                Path(self.test_db.name).unlink()
            except (FileNotFoundError, PermissionError):
                pass

if __name__ == "__main__":
    unittest.main()