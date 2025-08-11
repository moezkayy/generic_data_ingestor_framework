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
        self.test_dir = tempfile.mkdtemp()
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()
        
    def tearDown(self):
        # Clean up test files
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)
        # Don't delete database on Windows - it may be locked
        # This is acceptable for tests as temp files will be cleaned up by OS
        
    def test_process_directory_successful_processing(self):
        """Test successful directory processing workflow"""
        # Arrange - Create test JSON files
        test_data = [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ]
        
        test_file = Path(self.test_dir) / "test_data.json"
        with open(test_file, 'w') as f:
            json.dump(test_data, f)
        
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert
        self.assertTrue(result['success'])
        self.assertEqual(result['processed_files'], 1)
        self.assertEqual(result['total_records'], 2)
        self.assertEqual(len(result['errors']), 0)
        
    def test_process_directory_with_processing_errors(self):
        """Test handling of file processing errors"""
        # Arrange - Create malformed JSON file
        malformed_file = Path(self.test_dir) / "malformed.json"
        with open(malformed_file, 'w') as f:
            f.write('{"id": 1, "name": "Missing closing brace"')
        
        # Act
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert
        self.assertFalse(result['success'])
        self.assertGreater(len(result['errors']), 0)
        
    def test_process_directory_empty_directory(self):
        """Test processing of directory with no JSON files"""
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
        # Arrange - Create files with different structures
        customers_data = [{"id": 1, "name": "John", "email": "john@example.com"}]
        orders_data = [{"order_id": "ORD-001", "customer_id": 1, "total": 99.99}]
        
        customers_file = Path(self.test_dir) / "customers.json"
        orders_file = Path(self.test_dir) / "orders.json"
        
        with open(customers_file, 'w') as f:
            json.dump(customers_data, f)
        with open(orders_file, 'w') as f:
            json.dump(orders_data, f)
        
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
        # Arrange - Create test data and process it
        test_data = [{"id": 1, "name": "Test User"}]
        test_file = Path(self.test_dir) / "test.json"
        
        with open(test_file, 'w') as f:
            json.dump(test_data, f)
        
        # Process the data
        self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Act
        preview = self.app.get_database_preview(self.test_db.name, "processed_data", limit=5)
        
        # Assert
        self.assertEqual(len(preview), 1)
        self.assertEqual(preview[0]['name'], 'Test User')

if __name__ == "__main__":
    unittest.main()