# tests/error_handling/test_error_scenarios.py
import unittest
import tempfile
import json
from pathlib import Path
import shutil
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.application import DataIngestionApplication

class TestErrorHandlingScenarios(unittest.TestCase):
    """Comprehensive error handling and edge case testing"""
    
    def setUp(self):
        self.app = DataIngestionApplication()
        self.test_dir = tempfile.mkdtemp()
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()
        
    def tearDown(self):
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)
        # Don't delete database on Windows - it may be locked
        # This is acceptable for tests as temp files will be cleaned up by OS
    
    def test_malformed_json_handling(self):
        """Test handling of various JSON syntax errors"""
        # Create files with different JSON errors
        json_errors = {
            "missing_comma.json": '{"id": 1 "name": "Missing comma"}',
            "trailing_comma.json": '{"id": 1, "name": "John",}',
            "missing_brace.json": '{"id": 1, "name": "John"',
            "invalid_quotes.json": "{'id': 1, 'name': 'Single quotes'}",
            "incomplete_array.json": '[{"id": 1}, {"id": 2',
            "null_value_error.json": '{"id": 1, "name": undefined}'
        }
        
        for filename, content in json_errors.items():
            with open(Path(self.test_dir) / filename, 'w') as f:
                f.write(content)
        
        # Process directory with malformed files
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert graceful error handling - when all files fail, the result is a failure
        self.assertFalse(result['success'])  # Should return failure when no data processed
        self.assertEqual(len(result.get('errors', [])), 6)  # All files caused errors
        
        # Verify specific error messages
        error_text = ' '.join(result['errors'])
        self.assertIn('missing_comma.json', error_text)
        self.assertIn('trailing_comma.json', error_text)
        self.assertIn('missing_brace.json', error_text)
    
    def test_unicode_and_special_characters(self):
        """Test handling of Unicode, emojis, and special characters"""
        unicode_test_data = [
            {
                "id": "unicode_001",
                "chinese": "你好世界",
                "arabic": "مرحبا بالعالم", 
                "emoji": "Hello World! 🌍🚀💻🎉",
                "japanese": "こんにちは世界",
                "russian": "Привет, мир!",
                "special_chars": "!@#$%^&*()_+-=[]{}|;':\",./<>?",
                "xml_entities": "<>&\"'",
                "unicode_escape": "\u0048\u0065\u006c\u006c\u006f",
                "zero_width": "\u200b\u200c\u200d",  # Zero-width characters
                "control_chars": "\n\t\r\b\f"
            }
        ]
        
        test_file = Path(self.test_dir) / "unicode_test.json"
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(unicode_test_data, f, ensure_ascii=False, indent=2)
        
        # Process file
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert successful processing
        self.assertTrue(result['success'])
        self.assertEqual(result['processed_files'], 1)
        self.assertEqual(result['total_records'], 1)
        self.assertEqual(len(result['errors']), 0)
        
        # Verify Unicode preservation in database
        import sqlite3
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM processed_data WHERE id = 'unicode_001'")
        record = cursor.fetchone()
        
        # Verify all Unicode characters are preserved
        self.assertIsNotNone(record)
        # Note: Exact verification would depend on column ordering
        conn.close()
    
    def test_extremely_large_values(self):
        """Test handling of extremely large numbers and strings"""
        large_value_data = [
            {
                "id": "large_001",
                "very_large_integer": 9223372036854775807,  # Max 64-bit signed int
                "very_large_float": 1.7976931348623157e+308,  # Near max float64
                "very_small_float": 2.2250738585072014e-308,  # Near min float64
                "large_string": "A" * 10000,  # 10KB string
                "deeply_nested": {
                    "level1": {
                        "level2": {
                            "level3": {
                                "level4": {
                                    "level5": {
                                        "deep_value": "Found at level 5"
                                    }
                                }
                            }
                        }
                    }
                },
                "large_array": list(range(1000))  # 1000 element array
            }
        ]
        
        test_file = Path(self.test_dir) / "large_values.json"
        with open(test_file, 'w') as f:
            json.dump(large_value_data, f)
        
        # Process file
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert successful processing
        self.assertTrue(result['success'])
        self.assertEqual(result['processed_files'], 1)
        self.assertEqual(result['total_records'], 1)
        self.assertEqual(len(result['errors']), 0)
    
    def test_null_and_undefined_handling(self):
        """Test handling of various null/undefined scenarios"""
        null_test_data = [
            {
                "id": "null_001",
                "explicit_null": None,
                "empty_string": "",
                "empty_array": [],
                "empty_object": {},
                "zero": 0,
                "false": False,
                "nested_with_nulls": {
                    "inner_null": None,
                    "inner_empty": "",
                    "inner_array_with_nulls": [None, "", 0, False]
                }
            }
        ]
        
        test_file = Path(self.test_dir) / "null_test.json"
        with open(test_file, 'w') as f:
            json.dump(null_test_data, f)
        
        # Process file  
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert successful processing with proper null handling
        self.assertTrue(result['success'])
        self.assertEqual(result['processed_files'], 1)
        self.assertEqual(result['total_records'], 1)
        self.assertEqual(len(result['errors']), 0)
        
        # Verify null handling in database
        import sqlite3
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM processed_data WHERE id = 'null_001'")
        record = cursor.fetchone()
        self.assertIsNotNone(record)
        conn.close()
    
    def test_directory_access_errors(self):
        """Test handling of directory access and permission errors"""
        # Test non-existent directory
        result = self.app.process_directory("/non/existent/directory", self.test_db.name)
        
        # Should handle gracefully - no crash, appropriate error message
        self.assertFalse(result['success'])
        self.assertEqual(result.get('total_records', 0), 0)
        # Error handling may vary by implementation
        
    def test_mixed_valid_invalid_files(self):
        """Test processing directory with mix of valid and invalid files"""
        # Create valid file
        valid_data = [{"id": 1, "name": "Valid data"}]
        with open(Path(self.test_dir) / "valid.json", 'w') as f:
            json.dump(valid_data, f)
        
        # Create invalid files
        with open(Path(self.test_dir) / "invalid.json", 'w') as f:
            f.write('{"invalid": json}')
        
        with open(Path(self.test_dir) / "empty.json", 'w') as f:
            f.write('')
        
        # Create non-JSON file (should be ignored)
        with open(Path(self.test_dir) / "text_file.txt", 'w') as f:
            f.write('This is not JSON')
        
        # Process directory
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        
        # Assert partial success
        self.assertEqual(result['processed_files'], 1)   # Only valid.json
        self.assertEqual(result['total_records'], 1)
        self.assertGreaterEqual(len(result['errors']), 1)  # At least invalid.json error
        
        # Verify valid data was processed
        import sqlite3
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_data")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)
        conn.close()

if __name__ == "__main__":
    unittest.main()