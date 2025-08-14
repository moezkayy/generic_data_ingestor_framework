# tests/unit/test_json_processor.py
import unittest
import json
from unittest.mock import patch, mock_open
from pathlib import Path
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from processors.json_processor import JSONProcessor

class TestJSONProcessor(unittest.TestCase):
    
    def setUp(self):
        self.processor = JSONProcessor()
        # Store the source directory for loading test data files
        self.src_dir = Path(__file__).parent / "unit_test_data"
    
    def test_process_data_simple_objects(self):
        """Test processing of simple flat JSON objects"""
        # Arrange - Load test data from file
        with open(self.src_dir / "simple_data.json", 'r') as f:
            input_data = json.load(f)
        
        # Act
        result = self.processor.process_data(input_data)
        
        # Assert
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "John")
        self.assertEqual(result[0]["email"], "john@example.com")
        
    def test_process_data_nested_objects(self):
        """Test preservation of nested objects as JSON strings"""
        # Arrange - Load test data from file
        with open(self.src_dir / "nested_data.json", 'r') as f:
            input_data = json.load(f)
        
        # Act
        result = self.processor.process_data(input_data)
        
        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["user_id"], "usr_001")
        
        # Verify nested objects are preserved as JSON
        profile = json.loads(result[0]["profile"])
        self.assertEqual(profile["name"], "John Doe")
        self.assertEqual(profile["address"]["city"], "New York")
        
        preferences = json.loads(result[0]["preferences"])
        self.assertEqual(preferences["theme"], "dark")
        self.assertTrue(preferences["notifications"]["email"])
        
    def test_process_data_arrays(self):
        """Test handling of array fields"""
        # Arrange - Load test data from file
        with open(self.src_dir / "array_data.json", 'r') as f:
            input_data = json.load(f)
        
        # Act
        result = self.processor.process_data(input_data)
        
        # Assert
        items = json.loads(result[0]["items"])
        tags = json.loads(result[0]["tags"])
        
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["product"], "Laptop")
        self.assertEqual(len(tags), 3)
        self.assertIn("electronics", tags)
        
    def test_process_data_null_values(self):
        """Test handling of null/None values"""
        # Arrange - Load test data from file
        with open(self.src_dir / "null_data.json", 'r') as f:
            input_data = json.load(f)
        
        # Act
        result = self.processor.process_data(input_data)
        
        # Assert
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "John")
        self.assertEqual(result[0]["middle_name"], "")  # None -> empty string
        self.assertEqual(result[0]["address"], "")      # None -> empty string
        self.assertEqual(result[0]["tags"], "")         # Empty array -> empty string
        
    def test_process_data_empty_input(self):
        """Test handling of empty input"""
        # Act
        result = self.processor.process_data([])
        
        # Assert
        self.assertEqual(result, [])
        
    def test_process_data_non_dict_items(self):
        """Test handling of non-dictionary items"""
        # Arrange
        input_data = ["string", 123, None]
        
        # Act
        result = self.processor.process_data(input_data)
        
        # Assert - non-dict items should be filtered out
        self.assertEqual(result, [])
        
    def test_flatten_json_data_simple(self):
        """Test flattening of simple nested objects"""
        # Arrange
        data = {"user": {"name": "John", "age": 30}, "city": "NYC"}
        
        # Act
        result = self.processor.flatten_json_data(data)
        
        # Assert
        expected_keys = {"user.name", "user.age", "city"}
        self.assertTrue(expected_keys.issubset(set(result.keys())))
        self.assertEqual(result["user.name"], "John")
        self.assertEqual(result["user.age"], 30)
        
    def test_flatten_json_data_array(self):
        """Test flattening of data containing arrays"""
        # Arrange
        data = [{"id": 1, "profile": {"name": "John"}}, {"id": 2, "profile": {"name": "Jane"}}]
        
        # Act
        result = self.processor.flatten_json_data(data)
        
        # Assert
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIn("profile.name", result[0])
        
    def test_get_processing_statistics(self):
        """Test getting processing statistics"""
        # Act
        stats = self.processor.get_processing_statistics()
        
        # Assert
        self.assertIsInstance(stats, dict)
        self.assertIn('files_processed', stats)
        self.assertIn('files_successful', stats)
        self.assertIn('total_records', stats)
        
    def test_reset_statistics(self):
        """Test resetting processing statistics"""
        # Arrange - modify stats
        self.processor.processing_stats['files_processed'] = 5
        
        # Act
        self.processor.reset_statistics()
        stats = self.processor.get_processing_statistics()
        
        # Assert
        self.assertEqual(stats['files_processed'], 0)

if __name__ == "__main__":
    unittest.main()