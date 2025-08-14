# tests/unit/test_application.py
import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import sys
import os
import json
import shutil

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.application import DataIngestionApplication

class TestDataIngestionApplication(unittest.TestCase):
    
    def setUp(self):
        # Create application instance
        self.app = DataIngestionApplication()
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()
        
        # Store the source directory for copying specific files as needed
        self.src_dir = Path(__file__).parent / "unit_test_data"
        
    def tearDown(self):
        # Force close any remaining database connections
        import gc
        import sqlite3
        
        gc.collect()
        try:
            temp_conn = sqlite3.connect(self.test_db.name)
            temp_conn.close()
        except:
            pass
            
        # Clean up any temporary test directories
        if hasattr(self, 'test_dir') and self.test_dir.exists():
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
        
    def test_process_directory_successful_processing(self):
        """Test successful directory processing workflow"""
        # Create a clean temp directory for this specific test
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Copy specific test file to the test directory
        src_file = self.src_dir / "simple_data.json"
        shutil.copy(src_file, self.test_dir)
        
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert
        self.assertTrue(result['success'])
        self.assertEqual(result['processed_files'], 1)
        self.assertEqual(result['total_records'], 2)
        self.assertEqual(len(result['errors']), 0)
        
    def test_process_directory_with_processing_errors(self):
        """Test handling of file processing errors"""
        # Create a clean temp directory for this specific test
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Copy malformed test file to the test directory
        src_file = self.src_dir / "malformed.json"
        shutil.copy(src_file, self.test_dir)
        
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert
        self.assertFalse(result['success'])
        self.assertGreater(len(result['errors']), 0)
        
    def test_process_directory_empty_directory(self):
        """Test processing of directory with no JSON files"""
        # Create an empty temp directory for this specific test
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert
        self.assertFalse(result['success'])
        self.assertEqual(result.get('processed_files', 0), 0)
        self.assertIn('No JSON files found', result['message'])
        
    def test_process_directory_nonexistent_directory(self):
        """Test processing of non-existent directory"""
        # Act
        result = self.app.process_directory("/non/existent/directory", self.test_db.name)
        
        # Assert
        self.assertFalse(result['success'])
        self.assertIn('Processing failed', result['message'])
        
    def test_schema_inference_multiple_file_types(self):
        """Test schema inference across multiple files with different structures"""
        # Create a clean temp directory for this specific test
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Copy multiple test files to the test directory
        src_files = ["customers_orders.json", "orders_data.json"]
        for filename in src_files:
            src_file = self.src_dir / filename
            shutil.copy(src_file, self.test_dir)
        
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert
        self.assertTrue(result['success'])
        self.assertEqual(result['processed_files'], 2)
        self.assertEqual(result['total_records'], 2)
        
        # Verify database contains all fields
        preview = self.app.get_database_preview(self.test_db.name, "processed_data")
        self.assertEqual(len(preview), 2)
        
    def test_get_database_preview(self):
        """Test database preview functionality"""
        # Create a clean temp directory for this specific test
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Copy test file to the test directory  
        src_file = self.src_dir / "simple_data.json"
        shutil.copy(src_file, self.test_dir)
        
        # Process the data
        self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Act
        preview = self.app.get_database_preview(self.test_db.name, "processed_data", limit=5)
        
        # Assert
        self.assertEqual(len(preview), 2)
        self.assertEqual(preview[0]['name'], 'John')

if __name__ == "__main__":
    unittest.main()