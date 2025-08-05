"""
Integration tests for SQLite connector.

These tests use actual SQLite databases to verify real-world functionality.
"""

import pytest
import tempfile
import os
from pathlib import Path
from typing import Dict, Any, List

from src.connectors.sqlite_connector import SQLiteConnector


@pytest.mark.integration
@pytest.mark.sqlite
class TestSQLiteIntegration:
    """Integration tests for SQLite connector."""
    
    @pytest.fixture
    def temp_db_path(self) -> str:
        """Create a temporary SQLite database file."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_file:
            yield temp_file.name
        
        # Cleanup
        try:
            os.unlink(temp_file.name)
        except OSError:
            pass
    
    @pytest.fixture
    def connector(self, temp_db_path: str) -> SQLiteConnector:
        """Create SQLite connector with temporary database."""
        params = {
            'database': temp_db_path,
            'timeout': 30,
            'check_same_thread': False,
            'create_if_not_exists': True
        }
        connector = SQLiteConnector(params)
        yield connector
        
        # Cleanup
        if connector.is_connected:
            connector.disconnect()
    
    @pytest.fixture
    def memory_connector(self) -> SQLiteConnector:
        """Create SQLite connector with in-memory database."""
        params = {
            'database': ':memory:',
            'timeout': 30,
            'check_same_thread': False
        }
        connector = SQLiteConnector(params)
        yield connector
        
        # Cleanup
        if connector.is_connected:
            connector.disconnect()
    
    @pytest.fixture
    def sample_schema(self) -> List[Dict[str, Any]]:
        """Sample table schema for testing."""
        return [
            {
                'name': 'id',
                'type': 'integer',
                'primary_key': True,
                'auto_increment': True,
                'nullable': False
            },
            {
                'name': 'name',
                'type': 'text',
                'nullable': False
            },
            {
                'name': 'email',
                'type': 'text',
                'nullable': True,
                'unique': True
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
    
    @pytest.fixture
    def sample_data(self) -> List[Dict[str, Any]]:
        """Sample data for testing."""
        return [
            {
                'name': 'John Doe',
                'email': 'john@example.com',
                'age': 30,
                'is_active': True
            },
            {
                'name': 'Jane Smith',
                'email': 'jane@example.com',
                'age': 25,
                'is_active': False
            },
            {
                'name': 'Bob Johnson',
                'email': 'bob@example.com',
                'age': 35,
                'is_active': True
            }
        ]
    
    def test_connection_lifecycle(self, connector):
        """Test connection and disconnection lifecycle."""
        # Initially not connected
        assert not connector.is_connected
        
        # Connect
        result = connector.connect()
        assert result is True
        assert connector.is_connected
        
        # Connection info
        info = connector.get_connection_info()
        assert info['db_type'] == 'sqlite'
        assert info['is_connected'] is True
        assert 'sqlite_version' in info
        
        # Disconnect
        result = connector.disconnect()
        assert result is True
        assert not connector.is_connected
    
    def test_context_manager(self, connector):
        """Test context manager functionality."""
        assert not connector.is_connected
        
        with connector:
            assert connector.is_connected
            
            # Execute a simple query
            result = connector.execute_query("SELECT 1 as test")
            assert len(result) == 1
            assert result[0]['test'] == 1
            
        assert not connector.is_connected
    
    def test_memory_database(self, memory_connector):
        """Test in-memory database functionality."""
        with memory_connector:
            # Create a simple table
            create_sql = """
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """
            memory_connector.execute_query(create_sql)
            
            # Insert data
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
            result = memory_connector.execute_query(insert_sql, ['Test Name'])
            assert result == 1
            
            # Query data
            select_sql = "SELECT * FROM test_table"
            results = memory_connector.execute_query(select_sql)
            assert len(results) == 1
            assert results[0]['name'] == 'Test Name'
    
    def test_table_operations(self, connector, sample_schema):
        """Test table creation, existence check, and schema retrieval."""
        with connector:
            table_name = 'test_users'
            
            # Table shouldn't exist initially
            assert not connector.table_exists(table_name)
            
            # Create table
            success = connector.create_table(table_name, sample_schema)
            assert success is True
            
            # Table should now exist
            assert connector.table_exists(table_name)
            
            # Get table schema
            schema = connector.get_table_schema(table_name)
            assert len(schema) == len(sample_schema)
            
            # Verify key columns exist
            column_names = [col['column_name'] for col in schema]
            assert 'id' in column_names
            assert 'name' in column_names
            assert 'email' in column_names
            
            # Drop table
            drop_success = connector.drop_table(table_name)
            assert drop_success is True
            
            # Table shouldn't exist after dropping
            assert not connector.table_exists(table_name)
    
    def test_data_insertion_and_retrieval(self, connector, sample_schema, sample_data):
        """Test data insertion and retrieval operations."""
        with connector:
            table_name = 'test_users'
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            # Insert single row
            insert_sql = """
                INSERT INTO test_users (name, email, age, is_active) 
                VALUES (?, ?, ?, ?)
            """
            params = ['Single User', 'single@example.com', 28, True]
            result = connector.execute_query(insert_sql, params)
            assert result == 1
            
            # Insert multiple rows using batch
            batch_params = [
                [data['name'], data['email'], data['age'], data['is_active']]
                for data in sample_data
            ]
            batch_result = connector.execute_batch(insert_sql, batch_params)
            assert batch_result == len(sample_data)
            
            # Query all data
            select_sql = "SELECT * FROM test_users ORDER BY id"
            results = connector.execute_query(select_sql)
            assert len(results) == len(sample_data) + 1  # +1 for single insert
            
            # Verify data integrity
            first_user = results[0]
            assert first_user['name'] == 'Single User'
            assert first_user['email'] == 'single@example.com'
            assert first_user['age'] == 28
            assert first_user['is_active'] == 1  # SQLite stores boolean as integer
            
            # Query with conditions
            active_users_sql = "SELECT * FROM test_users WHERE is_active = ?"
            active_users = connector.execute_query(active_users_sql, [1])
            active_count = len([u for u in results if u['is_active'] == 1])
            assert len(active_users) == active_count
    
    def test_transaction_management(self, connector, sample_schema):
        """Test transaction begin, commit, and rollback."""
        with connector:
            table_name = 'test_transactions'
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            # Test successful transaction
            connector.begin_transaction()
            
            insert_sql = "INSERT INTO test_transactions (name, email) VALUES (?, ?)"
            connector.execute_query(insert_sql, ['User 1', 'user1@example.com'])
            connector.execute_query(insert_sql, ['User 2', 'user2@example.com'])
            
            # Commit transaction
            connector.commit_transaction()
            
            # Verify data was committed
            count_sql = "SELECT COUNT(*) as count FROM test_transactions"
            result = connector.execute_query(count_sql)
            assert result[0]['count'] == 2
            
            # Test rollback
            connector.begin_transaction()
            
            connector.execute_query(insert_sql, ['User 3', 'user3@example.com'])
            
            # Rollback transaction
            connector.rollback_transaction()
            
            # Verify data was rolled back
            result = connector.execute_query(count_sql)
            assert result[0]['count'] == 2  # Should still be 2, not 3
    
    def test_error_handling(self, connector, sample_schema):
        """Test error handling for various scenarios."""
        with connector:
            table_name = 'test_errors'
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            # Test duplicate key error (email is unique)
            insert_sql = "INSERT INTO test_errors (name, email) VALUES (?, ?)"
            connector.execute_query(insert_sql, ['User 1', 'test@example.com'])
            
            # This should raise an integrity error
            with pytest.raises(Exception):  # SQLiteQueryError with IntegrityError
                connector.execute_query(insert_sql, ['User 2', 'test@example.com'])
            
            # Test syntax error
            with pytest.raises(Exception):  # SQLiteQueryError with ProgrammingError
                connector.execute_query("INVALID SQL SYNTAX")
            
            # Test query on non-existent table
            with pytest.raises(Exception):
                connector.execute_query("SELECT * FROM non_existent_table")
    
    def test_pragma_settings(self, connector):
        """Test PRAGMA settings application."""
        with connector:
            # Check some PRAGMA settings
            pragma_checks = [
                ('journal_mode', 'WAL'),
                ('synchronous', 'NORMAL'),
                ('foreign_keys', 'ON')
            ]
            
            for pragma, expected_value in pragma_checks:
                result = connector.execute_query(f"PRAGMA {pragma}")
                if result and len(result) > 0:
                    # PRAGMA results come back as list with single item
                    actual_value = list(result[0].values())[0]
                    assert str(actual_value).upper() == expected_value.upper()
    
    def test_database_operations(self, connector):
        """Test database-specific operations like VACUUM and ANALYZE."""
        with connector:
            # Test VACUUM
            vacuum_result = connector.vacuum_database()
            assert vacuum_result is True
            
            # Test ANALYZE
            analyze_result = connector.analyze_database()
            assert analyze_result is True
    
    def test_database_size_info(self, connector, sample_schema, sample_data):
        """Test database size information retrieval."""
        with connector:
            # Get initial size
            size_info = connector.get_database_size()
            assert size_info['database_path'] == connector.db_path
            assert size_info['is_memory_database'] is False
            assert isinstance(size_info['file_size_bytes'], int)
            assert isinstance(size_info['file_size_mb'], float)
            
            # Create table and add data
            connector.create_table('size_test', sample_schema)
            
            insert_sql = "INSERT INTO size_test (name, email, age) VALUES (?, ?, ?)"
            for data in sample_data:
                connector.execute_query(insert_sql, [data['name'], data['email'], data['age']])
            
            # Get size after adding data
            new_size_info = connector.get_database_size()
            assert new_size_info['file_size_bytes'] >= size_info['file_size_bytes']
    
    def test_backup_operation(self, connector, sample_schema, sample_data):
        """Test database backup functionality."""
        with connector:
            # Create table and add data
            table_name = 'backup_test'
            connector.create_table(table_name, sample_schema)
            
            insert_sql = f"INSERT INTO {table_name} (name, email, age) VALUES (?, ?, ?)"
            for data in sample_data:
                connector.execute_query(insert_sql, [data['name'], data['email'], data['age']])
            
            # Create backup
            with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as backup_file:
                backup_path = backup_file.name
            
            try:
                backup_result = connector.backup_database(backup_path)
                assert backup_result is True
                
                # Verify backup file exists and has content
                assert os.path.exists(backup_path)
                assert os.path.getsize(backup_path) > 0
                
                # Verify backup content by connecting to it
                backup_params = {
                    'database': backup_path,
                    'timeout': 30,
                    'check_same_thread': False
                }
                backup_connector = SQLiteConnector(backup_params)
                
                with backup_connector:
                    # Check if table exists in backup
                    assert backup_connector.table_exists(table_name)
                    
                    # Check if data exists in backup
                    backup_data = backup_connector.execute_query(f"SELECT * FROM {table_name}")
                    assert len(backup_data) == len(sample_data)
                    
            finally:
                # Cleanup backup file
                try:
                    os.unlink(backup_path)
                except OSError:
                    pass
    
    def test_schema_caching(self, connector, sample_schema):
        """Test schema caching functionality."""
        with connector:
            table_name = 'cache_test'
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            # Clear cache to start fresh
            connector.clear_schema_cache()
            
            # First call should query database
            schema1 = connector.get_table_schema(table_name)
            
            # Second call should use cache
            schema2 = connector.get_table_schema(table_name)
            
            # Results should be identical
            assert schema1 == schema2
            
            # Check cache stats
            stats = connector.get_schema_cache_stats()
            assert stats['enabled'] is True
            assert stats['total_entries'] > 0
            
            # Test cache invalidation
            connector._invalidate_table_cache(table_name)
            
            # Next call should query database again
            schema3 = connector.get_table_schema(table_name)
            assert schema3 == schema1
    
    def test_connection_info_details(self, connector):
        """Test detailed connection information."""
        with connector:
            info = connector.get_connection_info()
            
            # Basic info
            assert info['db_type'] == 'sqlite'
            assert info['is_connected'] is True
            assert info['database_path'] == connector.db_path
            
            # Extended info
            assert 'sqlite_version' in info
            assert 'database_size_bytes' in info
            assert 'database_size_mb' in info
            assert 'current_pragma_values' in info
            
            # PRAGMA values
            pragma_values = info['current_pragma_values']
            assert 'journal_mode' in pragma_values
            assert 'synchronous' in pragma_values
            assert 'foreign_keys' in pragma_values
            assert 'cache_size' in pragma_values
    
    def test_concurrent_access(self, temp_db_path):
        """Test concurrent access to the same database file."""
        # Create two connectors to the same database
        params = {
            'database': temp_db_path,
            'timeout': 30,
            'check_same_thread': False,
            'create_if_not_exists': True
        }
        
        connector1 = SQLiteConnector(params)
        connector2 = SQLiteConnector(params)
        
        try:
            with connector1, connector2:
                # Create table from first connection
                schema = [
                    {'name': 'id', 'type': 'integer', 'primary_key': True},
                    {'name': 'data', 'type': 'text', 'nullable': False}
                ]
                connector1.create_table('concurrent_test', schema)
                
                # Insert data from first connection
                connector1.execute_query(
                    "INSERT INTO concurrent_test (data) VALUES (?)",
                    ['Data from connector 1']
                )
                
                # Read from second connection
                results = connector2.execute_query("SELECT * FROM concurrent_test")
                assert len(results) == 1
                assert results[0]['data'] == 'Data from connector 1'
                
                # Insert data from second connection
                connector2.execute_query(
                    "INSERT INTO concurrent_test (data) VALUES (?)",
                    ['Data from connector 2']
                )
                
                # Read all from first connection
                all_results = connector1.execute_query("SELECT * FROM concurrent_test ORDER BY id")
                assert len(all_results) == 2
                assert all_results[0]['data'] == 'Data from connector 1'
                assert all_results[1]['data'] == 'Data from connector 2'
                
        finally:
            # Cleanup
            if connector1.is_connected:
                connector1.disconnect()
            if connector2.is_connected:
                connector2.disconnect()