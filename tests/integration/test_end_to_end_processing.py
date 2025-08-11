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
        # Create temporary directories and files
        self.test_dir = tempfile.mkdtemp()
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()
        
        # Create realistic test data files
        self.create_test_data_files()
        
        # Initialize application
        self.app = DataIngestionApplication()
        
    def create_test_data_files(self):
        """Create realistic test JSON files"""
        # Simple customer data
        customers = [
            {
                "customer_id": "CUST-001",
                "name": "John Doe",
                "email": "john.doe@example.com",
                "registration_date": "2024-01-15",
                "status": "active"
            },
            {
                "customer_id": "CUST-002", 
                "name": "Jane Smith",
                "email": "jane.smith@example.com",
                "registration_date": "2024-01-18",
                "status": "active"
            }
        ]
        
        # Complex order data with nesting
        orders = [
            {
                "order_id": "ORD-2024-001",
                "customer_id": "CUST-001",
                "order_date": "2024-01-20",
                "status": "shipped",
                "shipping_address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zip": "10001",
                    "coordinates": {
                        "latitude": 40.7128,
                        "longitude": -74.0060
                    }
                },
                "items": [
                    {
                        "product_id": "PROD-001",
                        "product_name": "Wireless Headphones",
                        "quantity": 1,
                        "unit_price": 99.99,
                        "total_price": 99.99
                    },
                    {
                        "product_id": "PROD-002",
                        "product_name": "Phone Case",
                        "quantity": 2,
                        "unit_price": 19.99,
                        "total_price": 39.98
                    }
                ],
                "payment": {
                    "method": "credit_card",
                    "card_last_four": "1234",
                    "transaction_id": "TXN-987654321"
                },
                "totals": {
                    "subtotal": 139.97,
                    "tax": 11.20,
                    "shipping": 9.99,
                    "total": 161.16
                }
            }
        ]
        
        # Edge case data
        edge_cases = [
            {
                "id": "edge_001",
                "null_field": None,
                "empty_string": "",
                "zero_number": 0,
                "false_boolean": False,
                "empty_array": [],
                "empty_object": {},
                "unicode_text": "Hello 世界! 🌍",
                "special_chars": "!@#$%^&*()_+-=[]{}|;':\",./<>?"
            }
        ]
        
        # Write test files
        with open(Path(self.test_dir) / "customers.json", 'w', encoding='utf-8') as f:
            json.dump(customers, f, ensure_ascii=False, indent=2)
            
        with open(Path(self.test_dir) / "orders.json", 'w', encoding='utf-8') as f:
            json.dump(orders, f, ensure_ascii=False, indent=2)
            
        with open(Path(self.test_dir) / "edge_cases.json", 'w', encoding='utf-8') as f:
            json.dump(edge_cases, f, ensure_ascii=False, indent=2)
            
        # Create malformed JSON file for error testing
        with open(Path(self.test_dir) / "malformed.json", 'w') as f:
            f.write('{"id": 1, "name": "Missing closing brace"')
            
        # Create empty JSON file
        with open(Path(self.test_dir) / "empty.json", 'w') as f:
            f.write('[]')
    
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
        # Clean up temporary directory
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)
        
        # Don't delete database on Windows - it may be locked
        # This is acceptable for tests as temp files will be cleaned up by OS

if __name__ == "__main__":
    unittest.main()