"""
Test helper utilities and fixtures.

This module provides common utilities and helper functions for tests.
"""

import os
import time
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List
from contextlib import contextmanager

from src.connectors.database_connector import DatabaseConnector

# Import connectors conditionally
try:
    from src.connectors.sqlite_connector import SQLiteConnector
except ImportError:
    SQLiteConnector = None

try:
    from src.connectors.postgresql_connector import PostgreSQLConnector
except ImportError:
    PostgreSQLConnector = None

try:
    from src.connectors.mysql_connector import MySQLConnector
except ImportError:
    MySQLConnector = None


class TestDatabaseManager:
    """Manager for test database connections and operations."""
    
    @staticmethod
    def create_test_sqlite_connector(db_path: Optional[str] = None):
        """Create a SQLite connector for testing."""
        if SQLiteConnector is None:
            raise ImportError("SQLite connector not available")
            
        if db_path is None:
            db_path = ':memory:'
        
        params = {
            'database': db_path,
            'timeout': 30,
            'check_same_thread': False,
            'create_if_not_exists': True
        }
        return SQLiteConnector(params)
    
    @staticmethod
    def create_test_postgresql_connector():
        """Create a PostgreSQL connector for testing."""
        if PostgreSQLConnector is None:
            raise ImportError("PostgreSQL connector not available")
            
        params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'test_db'),
            'username': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
            'connection_pool_size': 3,
            'connection_timeout': 30
        }
        return PostgreSQLConnector(params)
    
    @staticmethod
    def create_test_mysql_connector():
        """Create a MySQL connector for testing."""
        if MySQLConnector is None:
            raise ImportError("MySQL connector not available")
            
        params = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', '3306')),
            'database': os.getenv('MYSQL_DB', 'test_db'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', 'mysql'),
            'connection_pool_size': 3,
            'connection_timeout': 30
        }
        return MySQLConnector(params)
    
    @staticmethod
    def is_database_available(db_type: str) -> bool:
        """Check if a database type is available for testing."""
        try:
            if db_type.lower() == 'sqlite':
                if SQLiteConnector is None:
                    return False
                connector = TestDatabaseManager.create_test_sqlite_connector()
                with connector:
                    connector.execute_query("SELECT 1")
                return True
            elif db_type.lower() == 'postgresql':
                if PostgreSQLConnector is None:
                    return False
                connector = TestDatabaseManager.create_test_postgresql_connector()
                with connector:
                    connector.execute_query("SELECT 1")
                return True
            elif db_type.lower() == 'mysql':
                if MySQLConnector is None:
                    return False
                connector = TestDatabaseManager.create_test_mysql_connector()
                with connector:
                    connector.execute_query("SELECT 1")
                return True
            else:
                return False
        except Exception:
            return False


class TestDataGenerator:
    """Generator for test data and schemas."""
    
    @staticmethod
    def create_sample_schema(include_constraints: bool = True) -> List[Dict[str, Any]]:
        """Create a sample table schema for testing."""
        schema = [
            {
                'name': 'id',
                'type': 'integer',
                'primary_key': True,
                'auto_increment': True,
                'nullable': False
            },
            {
                'name': 'name',
                'type': 'varchar',
                'nullable': False
            },
            {
                'name': 'email',
                'type': 'varchar',
                'nullable': True
            },
            {
                'name': 'age',
                'type': 'integer',
                'nullable': True
            },
            {
                'name': 'is_active',
                'type': 'boolean',
                'nullable': False,
                'default': True
            },
            {
                'name': 'created_at',
                'type': 'timestamp',
                'nullable': False,
                'default': 'CURRENT_TIMESTAMP'
            }
        ]
        
        if include_constraints:
            # Add unique constraint to email
            schema[2]['unique'] = True
        
        return schema
    
    @staticmethod
    def create_sample_data(count: int = 3) -> List[Dict[str, Any]]:
        """Create sample data for testing."""
        data = []
        for i in range(count):
            data.append({
                'name': f'User {i + 1}',
                'email': f'user{i + 1}@example.com',
                'age': 25 + i * 5,
                'is_active': i % 2 == 0
            })
        return data
    
    @staticmethod
    def create_large_dataset(count: int = 1000) -> List[Dict[str, Any]]:
        """Create a large dataset for performance testing."""
        data = []
        for i in range(count):
            data.append({
                'name': f'User {i}',
                'email': f'user{i}@example.com',
                'age': 20 + (i % 60),
                'is_active': i % 3 != 0,
                'category': f'Category {i % 10}',
                'score': (i * 7) % 100,
                'description': f'This is a description for user {i} with some sample text data.'
            })
        return data


class TestFileManager:
    """Manager for test file operations."""
    
    @staticmethod
    @contextmanager
    def temporary_file(suffix: str = '.tmp', content: str = None):
        """Create a temporary file for testing."""
        with tempfile.NamedTemporaryFile(mode='w+', suffix=suffix, delete=False) as temp_file:
            if content:
                temp_file.write(content)
                temp_file.flush()
            temp_path = temp_file.name
        
        try:
            yield temp_path
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
    
    @staticmethod
    @contextmanager
    def temporary_directory():
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir


class TestTimer:
    """Timer utility for performance testing."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
    
    def start(self):
        """Start the timer."""
        self.start_time = time.time()
    
    def stop(self):
        """Stop the timer."""
        self.end_time = time.time()
    
    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None or self.end_time is None:
            return 0.0
        return self.end_time - self.start_time
    
    @contextmanager
    def measure(self):
        """Context manager to measure execution time."""
        self.start()
        try:
            yield self
        finally:
            self.stop()


class TestAssertions:
    """Custom assertions for database testing."""
    
    @staticmethod
    def assert_schema_matches(actual_schema: List[Dict], expected_columns: List[str]):
        """Assert that schema contains expected columns."""
        actual_columns = [col.get('column_name', col.get('name', '')) for col in actual_schema]
        
        for expected_col in expected_columns:
            assert expected_col in actual_columns, f"Column '{expected_col}' not found in schema"
    
    @staticmethod
    def assert_data_integrity(original_data: List[Dict], retrieved_data: List[Dict], 
                             key_field: str = 'id'):
        """Assert data integrity between original and retrieved data."""
        assert len(original_data) <= len(retrieved_data), "Retrieved data has fewer records than expected"
        
        # Create lookup for comparison (handling auto-generated IDs)
        if key_field != 'id':
            original_lookup = {item[key_field]: item for item in original_data}
            for row in retrieved_data:
                if row[key_field] in original_lookup:
                    original = original_lookup[row[key_field]]
                    for key, value in original.items():
                        if key in row:
                            # Handle boolean conversion for different databases
                            if isinstance(value, bool):
                                assert bool(row[key]) == value, f"Mismatch in {key}: {row[key]} != {value}"
                            else:
                                assert row[key] == value, f"Mismatch in {key}: {row[key]} != {value}"
    
    @staticmethod
    def assert_performance_within_bounds(elapsed_time: float, max_time: float, 
                                       operation: str = "Operation"):
        """Assert that operation completed within time bounds."""
        assert elapsed_time <= max_time, f"{operation} took {elapsed_time:.3f}s, expected <= {max_time:.3f}s"
    
    @staticmethod
    def assert_connection_info_valid(info: Dict[str, Any], db_type: str):
        """Assert that connection info contains expected fields."""
        required_fields = ['db_type', 'is_connected']
        
        for field in required_fields:
            assert field in info, f"Missing required field: {field}"
        
        assert info['db_type'] == db_type, f"Expected db_type {db_type}, got {info['db_type']}"
        
        if info['is_connected']:
            # When connected, should have additional info
            if db_type == 'postgresql':
                extended_fields = ['host', 'port', 'database', 'username']
            elif db_type == 'mysql':
                extended_fields = ['host', 'port', 'database', 'user']
            elif db_type == 'sqlite':
                extended_fields = ['database_path']
            else:
                extended_fields = []
            
            for field in extended_fields:
                assert field in info, f"Missing extended field for {db_type}: {field}"


class TestCleanup:
    """Utilities for test cleanup operations."""
    
    @staticmethod
    def cleanup_test_tables(connector: DatabaseConnector, table_prefix: str = 'test_'):
        """Clean up test tables with specified prefix."""
        if not connector.is_connected:
            return
        
        try:
            # Get list of tables (implementation varies by database type)
            if hasattr(connector, 'get_table_list'):
                tables = connector.get_table_list()
            else:
                # Fallback for databases without get_table_list method
                tables = []
            
            # Drop tables with test prefix
            for table in tables:
                if table.startswith(table_prefix):
                    try:
                        connector.drop_table(table, if_exists=True)
                    except Exception:
                        pass  # Ignore individual table cleanup errors
                        
        except Exception:
            pass  # Ignore overall cleanup errors
    
    @staticmethod
    def cleanup_database_objects(connector: DatabaseConnector, objects: List[str]):
        """Clean up specific database objects."""
        if not connector.is_connected:
            return
        
        for obj_name in objects:
            try:
                if connector.table_exists(obj_name):
                    connector.drop_table(obj_name, if_exists=True)
            except Exception:
                pass  # Ignore cleanup errors


class DatabaseTestSuite:
    """Test suite runner for database operations."""
    
    def __init__(self, connector: DatabaseConnector):
        self.connector = connector
        self.cleanup = TestCleanup()
        self.data_gen = TestDataGenerator()
        self.assertions = TestAssertions()
    
    def run_basic_functionality_tests(self) -> Dict[str, bool]:
        """Run basic functionality tests."""
        results = {}
        
        with self.connector:
            try:
                # Test 1: Connection
                results['connection'] = self.connector.is_connected
                
                # Test 2: Simple query
                result = self.connector.execute_query("SELECT 1 as test")
                results['simple_query'] = len(result) == 1 and result[0].get('test') == 1
                
                # Test 3: Table operations
                schema = self.data_gen.create_sample_schema()
                table_name = 'test_basic_functionality'
                
                try:
                    # Clean up first
                    if self.connector.table_exists(table_name):
                        self.connector.drop_table(table_name)
                    
                    # Create table
                    self.connector.create_table(table_name, schema)
                    results['table_creation'] = self.connector.table_exists(table_name)
                    
                    # Insert data
                    data = self.data_gen.create_sample_data(1)
                    insert_sql = self._build_insert_sql(table_name, data[0])
                    insert_result = self.connector.execute_query(insert_sql, list(data[0].values()))
                    results['data_insertion'] = insert_result == 1
                    
                    # Query data
                    select_result = self.connector.execute_query(f"SELECT * FROM {table_name}")
                    results['data_retrieval'] = len(select_result) == 1
                    
                    # Clean up
                    self.connector.drop_table(table_name)
                    results['table_deletion'] = not self.connector.table_exists(table_name)
                    
                except Exception as e:
                    results['table_operations_error'] = str(e)
                    
            except Exception as e:
                results['test_suite_error'] = str(e)
        
        return results
    
    def _build_insert_sql(self, table_name: str, data: Dict[str, Any]) -> str:
        """Build INSERT SQL statement."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' if 'sqlite' in str(type(self.connector)).lower() else '%s'] * len(data))
        return f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"