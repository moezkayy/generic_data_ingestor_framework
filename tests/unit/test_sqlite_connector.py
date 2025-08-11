# tests/unit/test_sqlite_connector.py
import unittest
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from connectors.sqlite_connector import SQLiteConnector

class TestSQLiteConnector(unittest.TestCase):
    
    def setUp(self):
        # Create temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        
        self.connection_params = {'database': self.temp_db.name}
        self.connector = SQLiteConnector(self.connection_params)
        
    def tearDown(self):
        if self.connector.connection:
            self.connector.disconnect()
        # Don't delete database on Windows - it may be locked
        # This is acceptable for tests as temp files will be cleaned up by OS
        
    def test_connection_success(self):
        """Test successful database connection"""
        # Act
        result = self.connector.connect()
        
        # Assert
        self.assertTrue(result)
        self.assertIsNotNone(self.connector.connection)
        
    def test_get_connection_info(self):
        """Test getting connection info"""
        # Act
        self.connector.connect()
        info = self.connector.get_connection_info()
        
        # Assert
        self.assertEqual(info['db_type'], 'sqlite')
        self.assertEqual(info['database'], self.temp_db.name)
        self.assertTrue(info['connected'])
        
    def test_create_table_success(self):
        """Test successful table creation"""
        # Arrange
        self.connector.connect()
        schema = [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'TEXT', 'nullable': True},
            {'name': 'email', 'type': 'TEXT', 'nullable': True}
        ]
        
        # Act
        result = self.connector.create_table('test_table', schema)
        
        # Assert
        self.assertTrue(result)
        
        # Verify table exists
        table_exists = self.connector.table_exists('test_table')
        self.assertTrue(table_exists)
        
    def test_table_exists(self):
        """Test table existence checking"""
        # Arrange
        self.connector.connect()
        
        # Act - check non-existent table
        exists_before = self.connector.table_exists('nonexistent_table')
        
        # Create table
        schema = [{'name': 'id', 'type': 'INTEGER'}]
        self.connector.create_table('test_table', schema)
        
        # Act - check existing table
        exists_after = self.connector.table_exists('test_table')
        
        # Assert
        self.assertFalse(exists_before)
        self.assertTrue(exists_after)
        
    def test_insert_data_batch(self):
        """Test batch data insertion"""
        # Arrange
        self.connector.connect()
        schema = [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'TEXT', 'nullable': True}
        ]
        self.connector.create_table('test_table', schema)
        
        test_data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            {'id': 3, 'name': 'Charlie'}
        ]
        
        # Act
        inserted_count = self.connector.insert_data('test_table', test_data)
        
        # Assert
        self.assertEqual(inserted_count, 3)
        
        # Verify data was inserted
        results = self.connector.execute_query("SELECT COUNT(*) as count FROM test_table")
        count = results[0]['count']
        self.assertEqual(count, 3)
        
    def test_insert_data_large_batch(self):
        """Test insertion of large dataset with batching"""
        # Arrange
        self.connector.connect()
        schema = [{'name': 'id', 'type': 'INTEGER'}, {'name': 'value', 'type': 'TEXT'}]
        self.connector.create_table('large_table', schema)
        
        # Generate 150 records (test batching with batch_size=100)
        large_dataset = [{'id': i, 'value': f'value_{i}'} for i in range(150)]
        
        # Act
        inserted_count = self.connector.insert_data('large_table', large_dataset, batch_size=100)
        
        # Assert
        self.assertEqual(inserted_count, 150)
        
        # Verify all data inserted
        results = self.connector.execute_query("SELECT COUNT(*) as count FROM large_table")
        count = results[0]['count']
        self.assertEqual(count, 150)
        
    def test_execute_query_select(self):
        """Test query execution with results"""
        # Arrange
        self.connector.connect()
        schema = [{'name': 'id', 'type': 'INTEGER'}, {'name': 'name', 'type': 'TEXT'}]
        self.connector.create_table('query_table', schema)
        
        test_data = [{'id': 1, 'name': 'Test User'}]
        self.connector.insert_data('query_table', test_data)
        
        # Act
        results = self.connector.execute_query("SELECT * FROM query_table WHERE id = ?", (1,))
        
        # Assert
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], 1)
        self.assertEqual(results[0]['name'], 'Test User')
        
    def test_execute_query_invalid_sql(self):
        """Test handling of invalid SQL queries"""
        # Arrange
        self.connector.connect()
        
        # Act - Invalid SQL should return empty list instead of raising exception
        results = self.connector.execute_query("INVALID SQL QUERY")
        
        # Assert
        self.assertEqual(results, [])
        
    def test_disconnect(self):
        """Test database disconnection"""
        # Arrange
        self.connector.connect()
        self.assertIsNotNone(self.connector.connection)
        
        # Act
        result = self.connector.disconnect()
        
        # Assert
        self.assertTrue(result)
        self.assertIsNone(self.connector.connection)
        
    def test_insert_empty_data(self):
        """Test inserting empty data"""
        # Arrange
        self.connector.connect()
        schema = [{'name': 'id', 'type': 'INTEGER'}]
        self.connector.create_table('empty_table', schema)
        
        # Act
        inserted_count = self.connector.insert_data('empty_table', [])
        
        # Assert
        self.assertEqual(inserted_count, 0)

if __name__ == "__main__":
    unittest.main()