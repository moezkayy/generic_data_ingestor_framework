"""
Unit tests for SQLite connector using mocks.

These tests cover all functionality of the SQLite connector without
requiring actual SQLite database files.
"""

import pytest
import os
import time
from unittest.mock import Mock, MagicMock, patch, mock_open
from typing import Dict, Any

from src.connectors.sqlite_connector import (
    SQLiteConnector,
    SQLiteConnectionError,
    SQLiteQueryError,
    SQLiteTransactionError
)


class TestSQLiteConnector:
    """Test cases for SQLite connector."""
    
    @pytest.fixture
    def connector(self, sample_sqlite_params):
        """Create a SQLite connector for testing."""
        return SQLiteConnector(sample_sqlite_params)
    
    @pytest.fixture
    def mock_sqlite3(self):
        """Mock sqlite3 module."""
        with patch('src.connectors.sqlite_connector.sqlite3') as mock_sqlite:
            yield mock_sqlite
    
    def test_initialization(self, sample_sqlite_params):
        """Test SQLite connector initialization."""
        connector = SQLiteConnector(sample_sqlite_params)
        
        assert connector.connection_params == sample_sqlite_params
        assert connector.connection is None
        assert connector.is_connected is False
        assert connector.in_transaction is False
        assert connector.connection_lock is not None
        
    def test_initialization_missing_database(self):
        """Test initialization with missing database parameter."""
        with pytest.raises(ValueError, match="Missing required SQLite parameter: database"):
            SQLiteConnector({})
            
    def test_initialization_default_values(self, temp_db_file):
        """Test initialization with default values."""
        params = {'database': temp_db_file}
        connector = SQLiteConnector(params)
        
        assert connector.connection_params['timeout'] == 30
        assert connector.connection_params['check_same_thread'] is False
        assert connector.connection_params['create_if_not_exists'] is True
        
    def test_initialization_invalid_timeout(self, temp_db_file):
        """Test initialization with invalid timeout."""
        params = {
            'database': temp_db_file,
            'timeout': 'invalid'
        }
        with pytest.raises(ValueError, match="Invalid timeout value"):
            SQLiteConnector(params)
            
    @patch('src.connectors.sqlite_connector.Path.mkdir')
    def test_resolve_database_path_absolute(self, mock_mkdir, temp_db_file):
        """Test database path resolution with absolute path."""
        params = {'database': temp_db_file}
        connector = SQLiteConnector(params)
        
        assert connector.db_path == temp_db_file
        
    @patch('src.connectors.sqlite_connector.Path.cwd')
    @patch('src.connectors.sqlite_connector.Path.mkdir')
    def test_resolve_database_path_relative(self, mock_mkdir, mock_cwd):
        """Test database path resolution with relative path."""
        mock_cwd.return_value = '/home/user'
        params = {'database': 'test.db'}
        
        with patch('src.connectors.sqlite_connector.Path.is_absolute', return_value=False):
            connector = SQLiteConnector(params)
            
        assert '/home/user/test.db' in connector.db_path
        
    def test_resolve_database_path_memory(self):
        """Test database path resolution for in-memory database."""
        params = {'database': ':memory:'}
        connector = SQLiteConnector(params)
        
        assert connector.db_path == ':memory:'
        
    @patch('src.connectors.sqlite_connector.Path.mkdir')
    def test_resolve_database_path_create_directory_error(self, mock_mkdir):
        """Test database path resolution with directory creation error."""
        mock_mkdir.side_effect = OSError("Permission denied")
        params = {'database': '/invalid/path/test.db'}
        
        with pytest.raises(ValueError, match="Cannot create directory"):
            SQLiteConnector(params)
            
    def test_get_pragma_settings_default(self, connector):
        """Test default PRAGMA settings."""
        pragma_settings = connector.pragma_settings
        
        expected_settings = {
            'journal_mode': 'WAL',
            'synchronous': 'NORMAL',
            'foreign_keys': 'ON',
            'temp_store': 'MEMORY',
            'cache_size': '-64000',
            'mmap_size': '268435456'
        }
        
        for key, value in expected_settings.items():
            assert pragma_settings[key] == value
            
    def test_get_pragma_settings_custom(self, temp_db_file):
        """Test custom PRAGMA settings."""
        custom_pragmas = {
            'journal_mode': 'DELETE',
            'cache_size': '-32000',
            'custom_pragma': 'custom_value'
        }
        params = {
            'database': temp_db_file,
            'pragma_settings': custom_pragmas
        }
        connector = SQLiteConnector(params)
        
        assert connector.pragma_settings['journal_mode'] == 'DELETE'
        assert connector.pragma_settings['cache_size'] == '-32000'
        assert connector.pragma_settings['custom_pragma'] == 'custom_value'
        assert connector.pragma_settings['synchronous'] == 'NORMAL'  # Default preserved
        
    @patch('src.connectors.sqlite_connector.sqlite3.connect')
    @patch('src.connectors.sqlite_connector.os.path.exists')
    def test_connect_success_existing_file(self, mock_exists, mock_connect, connector):
        """Test successful connection to existing database file."""
        mock_exists.return_value = True
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.object(connector, '_apply_pragma_settings'):
            result = connector.connect()
            
        assert result is True
        assert connector.is_connected is True
        assert connector.connection == mock_connection
        
        mock_connect.assert_called_once_with(
            connector.db_path,
            timeout=connector.connection_params['timeout'],
            check_same_thread=connector.connection_params['check_same_thread']
        )
        
        # Verify connection test
        mock_cursor.execute.assert_called_with("SELECT 1")
        mock_cursor.fetchone.assert_called_once()
        
    @patch('src.connectors.sqlite_connector.sqlite3.connect')
    @patch('src.connectors.sqlite_connector.os.path.exists')
    def test_connect_success_create_new_file(self, mock_exists, mock_connect, connector):
        """Test successful connection creating new database file."""
        mock_exists.return_value = False
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        connector.connection_params['create_if_not_exists'] = True
        
        with patch.object(connector, '_apply_pragma_settings'):
            result = connector.connect()
            
        assert result is True
        assert connector.is_connected is True
        
    @patch('src.connectors.sqlite_connector.os.path.exists')
    def test_connect_file_not_exists_no_create(self, mock_exists, connector):
        """Test connection failure when file doesn't exist and create is disabled."""
        mock_exists.return_value = False
        connector.connection_params['create_if_not_exists'] = False
        connector.db_path = '/path/to/nonexistent.db'
        
        with pytest.raises(SQLiteConnectionError, match="Database file does not exist"):
            connector.connect()
            
    def test_connect_already_connected(self, connector):
        """Test connect when already connected."""
        connector.is_connected = True
        connector.connection = MagicMock()
        
        result = connector.connect()
        
        assert result is True
        
    def test_connect_memory_database(self):
        """Test connection to in-memory database."""
        params = {'database': ':memory:'}
        connector = SQLiteConnector(params)
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        with patch('src.connectors.sqlite_connector.sqlite3.connect', return_value=mock_connection):
            mock_connection.cursor.return_value = mock_cursor
            with patch.object(connector, '_apply_pragma_settings'):
                result = connector.connect()
                
        assert result is True
        assert connector.is_connected is True
        
    @patch('src.connectors.sqlite_connector.sqlite3.connect')
    def test_connect_sqlite_error(self, mock_connect, connector):
        """Test connection with SQLite error."""
        from src.connectors.sqlite_connector import SQLiteError
        
        mock_connect.side_effect = SQLiteError("Database locked")
        
        with pytest.raises(SQLiteConnectionError, match="Failed to connect"):
            connector.connect()
            
    @patch('src.connectors.sqlite_connector.sqlite3.connect')
    def test_connect_unexpected_error(self, mock_connect, connector):
        """Test connection with unexpected error."""
        mock_connect.side_effect = Exception("Unexpected error")
        
        with pytest.raises(SQLiteConnectionError, match="Unexpected error"):
            connector.connect()
            
    def test_apply_pragma_settings(self, connector):
        """Test PRAGMA settings application."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        connector.connection = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        connector._apply_pragma_settings()
        
        # Verify PRAGMA commands were executed
        expected_calls = []
        for pragma, value in connector.pragma_settings.items():
            expected_calls.append(call(f"PRAGMA {pragma} = {value}"))
            
        mock_cursor.execute.assert_has_calls(expected_calls, any_order=True)
        
    def test_apply_pragma_settings_error(self, connector):
        """Test PRAGMA settings application with error."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        connector.connection = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("PRAGMA error")
        
        # Should not raise exception, just log warning
        connector._apply_pragma_settings()
        
    def test_disconnect_success(self, connector):
        """Test successful disconnection."""
        mock_connection = MagicMock()
        connector.connection = mock_connection
        connector.is_connected = True
        connector.in_transaction = True
        
        with patch.object(connector, 'rollback_transaction') as mock_rollback:
            result = connector.disconnect()
            
        assert result is True
        assert connector.is_connected is False
        assert connector.connection is None
        mock_rollback.assert_called_once()
        mock_connection.close.assert_called_once()
        
    def test_disconnect_with_error(self, connector):
        """Test disconnection with error."""
        mock_connection = MagicMock()
        mock_connection.close.side_effect = Exception("Close error")
        connector.connection = mock_connection
        connector.is_connected = True
        
        result = connector.disconnect()
        
        assert result is False
        
    def test_get_connection_context_manager(self, connector):
        """Test connection context manager."""
        mock_connection = MagicMock()
        connector.connection = mock_connection
        connector.is_connected = True
        
        with connector._get_connection() as conn:
            assert conn == mock_connection
            
    def test_get_connection_not_connected(self, connector):
        """Test connection context manager when not connected."""
        with pytest.raises(SQLiteConnectionError, match="Not connected"):
            with connector._get_connection():
                pass
                
    def test_execute_query_select(self, connector):
        """Test SELECT query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock sqlite3.Row objects
        class MockRow:
            def __init__(self, data):
                self._data = data
            def keys(self):
                return self._data.keys()
            def __getitem__(self, key):
                return self._data[key]
                
        mock_rows = [MockRow({'id': 1, 'name': 'test1'}), MockRow({'id': 2, 'name': 'test2'})]
        mock_cursor.fetchall.return_value = mock_rows
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            # Mock dict conversion
            with patch('builtins.dict', side_effect=lambda x: dict(x._data)):
                result = connector.execute_query("SELECT * FROM test", ['param1'])
                
        expected_result = [{'id': 1, 'name': 'test1'}, {'id': 2, 'name': 'test2'}]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ['param1'])
        mock_cursor.fetchall.assert_called_once()
        
    def test_execute_query_insert(self, connector):
        """Test INSERT query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        
        connector.in_transaction = False
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            result = connector.execute_query("INSERT INTO test VALUES (?)", ['value'])
            
        assert result == 5
        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (?)", ['value'])
        mock_connection.commit.assert_called_once()
        
    def test_execute_query_in_transaction(self, connector):
        """Test query execution within transaction."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        
        connector.in_transaction = True
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            result = connector.execute_query("INSERT INTO test VALUES (?)", ['value'])
            
        assert result == 3
        # Should not commit when in transaction
        mock_connection.commit.assert_not_called()
        
    def test_execute_query_empty(self, connector):
        """Test execution of empty query."""
        with pytest.raises(SQLiteQueryError, match="Query cannot be empty"):
            connector.execute_query("")
            
        with pytest.raises(SQLiteQueryError, match="Query cannot be empty"):
            connector.execute_query("   ")
            
    def test_execute_query_programming_error(self, connector):
        """Test query execution with programming error."""
        from src.connectors.sqlite_connector import ProgrammingError
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = ProgrammingError("Syntax error")
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            with pytest.raises(SQLiteQueryError, match="SQL syntax error"):
                connector.execute_query("INVALID SQL")
                
    def test_execute_query_integrity_error(self, connector):
        """Test query execution with integrity error."""
        from src.connectors.sqlite_connector import IntegrityError
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = IntegrityError("UNIQUE constraint failed")
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            with pytest.raises(SQLiteQueryError, match="Database integrity error"):
                connector.execute_query("INSERT INTO test VALUES (?)", ['duplicate'])
                
    def test_begin_transaction_success(self, connector):
        """Test successful transaction begin."""
        with patch.object(connector, 'execute_query') as mock_execute:
            result = connector.begin_transaction()
            
        assert result is True
        assert connector.in_transaction is True
        mock_execute.assert_called_once_with("BEGIN TRANSACTION")
        
    def test_begin_transaction_already_in_transaction(self, connector):
        """Test begin transaction when already in transaction."""
        connector.in_transaction = True
        
        result = connector.begin_transaction()
        
        assert result is True
        
    def test_begin_transaction_error(self, connector):
        """Test begin transaction with error."""
        with patch.object(connector, 'execute_query', side_effect=Exception("Transaction error")):
            with pytest.raises(SQLiteTransactionError, match="Failed to start transaction"):
                connector.begin_transaction()
                
    def test_commit_transaction_success(self, connector):
        """Test successful transaction commit."""
        mock_connection = MagicMock()
        connector.connection = mock_connection
        connector.in_transaction = True
        
        result = connector.commit_transaction()
        
        assert result is True
        assert connector.in_transaction is False
        mock_connection.commit.assert_called_once()
        
    def test_commit_transaction_not_in_transaction(self, connector):
        """Test commit when not in transaction."""
        connector.in_transaction = False
        
        result = connector.commit_transaction()
        
        assert result is True
        
    def test_commit_transaction_error(self, connector):
        """Test commit transaction with error."""
        mock_connection = MagicMock()
        mock_connection.commit.side_effect = Exception("Commit error")
        connector.connection = mock_connection
        connector.in_transaction = True
        
        with pytest.raises(SQLiteTransactionError, match="Failed to commit transaction"):
            connector.commit_transaction()
            
    def test_rollback_transaction_success(self, connector):
        """Test successful transaction rollback."""
        mock_connection = MagicMock()
        connector.connection = mock_connection
        connector.in_transaction = True
        
        result = connector.rollback_transaction()
        
        assert result is True
        assert connector.in_transaction is False
        mock_connection.rollback.assert_called_once()
        
    def test_rollback_transaction_not_in_transaction(self, connector):
        """Test rollback when not in transaction."""
        connector.in_transaction = False
        
        result = connector.rollback_transaction()
        
        assert result is True
        
    def test_rollback_transaction_error(self, connector):
        """Test rollback transaction with error."""
        mock_connection = MagicMock()
        mock_connection.rollback.side_effect = Exception("Rollback error")
        connector.connection = mock_connection
        connector.in_transaction = True
        
        with pytest.raises(SQLiteTransactionError, match="Failed to rollback transaction"):
            connector.rollback_transaction()
            
    def test_get_connection_info_basic(self, connector):
        """Test basic connection info."""
        info = connector.get_connection_info()
        
        expected_keys = [
            'db_type', 'database_path', 'is_connected', 'in_transaction',
            'timeout', 'check_same_thread', 'pragma_settings'
        ]
        
        for key in expected_keys:
            assert key in info
            
        assert info['db_type'] == 'sqlite'
        assert info['database_path'] == connector.db_path
        assert info['is_connected'] is False
        
    @patch('src.connectors.sqlite_connector.os.path.getsize')
    def test_get_connection_info_extended(self, mock_getsize, connector):
        """Test extended connection info when connected."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        connector.is_connected = True
        connector.connection = mock_connection
        connector.db_path = '/path/to/test.db'
        
        # Mock file size
        mock_getsize.return_value = 1024 * 1024  # 1MB
        
        # Mock SQLite version and PRAGMA results
        mock_cursor.fetchone.side_effect = [
            ('3.36.0',),  # sqlite_version
            ('WAL',),  # journal_mode
            ('NORMAL',),  # synchronous
            ('1',),  # foreign_keys
            ('-64000',)  # cache_size
        ]
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            info = connector.get_connection_info()
            
        assert info['sqlite_version'] == '3.36.0'
        assert info['database_size_bytes'] == 1024 * 1024
        assert info['database_size_mb'] == 1.0
        assert info['current_pragma_values']['journal_mode'] == 'WAL'
        
    def test_get_connection_info_memory_database(self):
        """Test connection info for in-memory database."""
        params = {'database': ':memory:'}
        connector = SQLiteConnector(params)
        
        info = connector.get_connection_info()
        
        assert info['database_path'] == ':memory:'
        assert info['database_size_bytes'] is None
        assert info['database_size_mb'] is None
        
    def test_execute_batch(self, connector):
        """Test batch query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 10
        
        params_list = [['value1'], ['value2'], ['value3']]
        connector.in_transaction = False
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            result = connector.execute_batch("INSERT INTO test VALUES (?)", params_list)
            
        assert result == 10
        mock_cursor.executemany.assert_called_once_with("INSERT INTO test VALUES (?)", params_list)
        mock_connection.commit.assert_called_once()
        
    def test_execute_batch_empty_query(self, connector):
        """Test batch execution with empty query."""
        with pytest.raises(SQLiteQueryError, match="Query cannot be empty"):
            connector.execute_batch("", [])
            
    def test_execute_batch_empty_params(self, connector):
        """Test batch execution with empty parameters."""
        with pytest.raises(SQLiteQueryError, match="Parameters list cannot be empty"):
            connector.execute_batch("INSERT INTO test VALUES (?)", [])
            
    def test_table_exists_impl_true(self, connector):
        """Test table existence check returning True."""
        mock_result = [{'count': 1}]
        
        with patch.object(connector, 'execute_query', return_value=mock_result):
            result = connector._table_exists_impl('test_table')
            
        assert result is True
        
    def test_table_exists_impl_false(self, connector):
        """Test table existence check returning False."""
        mock_result = [{'count': 0}]
        
        with patch.object(connector, 'execute_query', return_value=mock_result):
            result = connector._table_exists_impl('test_table')
            
        assert result is False
        
    def test_get_table_schema_impl(self, connector):
        """Test table schema retrieval."""
        mock_schema = [
            {
                'cid': 0,
                'name': 'id',
                'type': 'INTEGER',
                'notnull': 1,
                'dflt_value': None,
                'pk': 1
            },
            {
                'cid': 1,
                'name': 'name',
                'type': 'TEXT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0
            }
        ]
        
        with patch.object(connector, 'execute_query', return_value=mock_schema):
            result = connector._get_table_schema_impl('test_table')
            
        expected_result = [
            {
                'column_name': 'id',
                'data_type': 'INTEGER',
                'is_nullable': 'NO',
                'column_default': None,
                'is_primary_key': True,
                'ordinal_position': 1
            },
            {
                'column_name': 'name',
                'data_type': 'TEXT',
                'is_nullable': 'YES',
                'column_default': None,
                'is_primary_key': False,
                'ordinal_position': 2
            }
        ]
        
        assert result == expected_result
        
    def test_vacuum_database(self, connector):
        """Test VACUUM operation."""
        with patch.object(connector, 'execute_query') as mock_execute:
            result = connector.vacuum_database()
            
        assert result is True
        mock_execute.assert_called_once_with("VACUUM")
        
    def test_vacuum_database_error(self, connector):
        """Test VACUUM operation with error."""
        with patch.object(connector, 'execute_query', side_effect=Exception("VACUUM error")):
            with pytest.raises(SQLiteQueryError, match="VACUUM operation failed"):
                connector.vacuum_database()
                
    def test_analyze_database(self, connector):
        """Test ANALYZE operation."""
        with patch.object(connector, 'execute_query') as mock_execute:
            result = connector.analyze_database()
            
        assert result is True
        mock_execute.assert_called_once_with("ANALYZE")
        
    def test_analyze_database_error(self, connector):
        """Test ANALYZE operation with error."""
        with patch.object(connector, 'execute_query', side_effect=Exception("ANALYZE error")):
            with pytest.raises(SQLiteQueryError, match="ANALYZE operation failed"):
                connector.analyze_database()
                
    @patch('src.connectors.sqlite_connector.os.path.getsize')
    @patch('src.connectors.sqlite_connector.os.path.exists')
    def test_get_database_size_file(self, mock_exists, mock_getsize, connector):
        """Test database size calculation for file database."""
        mock_exists.return_value = True
        mock_getsize.return_value = 2048
        connector.is_connected = True
        
        mock_page_results = [{'page_count': 100}, {'page_size': 4096}]
        
        with patch.object(connector, 'execute_query', side_effect=mock_page_results):
            size_info = connector.get_database_size()
            
        assert size_info['database_path'] == connector.db_path
        assert size_info['is_memory_database'] is False
        assert size_info['file_size_bytes'] == 2048
        assert size_info['file_size_mb'] == round(2048 / (1024 * 1024), 2)
        assert size_info['page_count'] == 100
        assert size_info['page_size'] == 4096
        assert size_info['calculated_size_bytes'] == 409600
        
    def test_get_database_size_memory(self):
        """Test database size calculation for memory database."""
        params = {'database': ':memory:'}
        connector = SQLiteConnector(params)
        
        size_info = connector.get_database_size()
        
        assert size_info['database_path'] == ':memory:'
        assert size_info['is_memory_database'] is True
        assert size_info['file_size_bytes'] is None
        assert size_info['file_size_mb'] is None
        
    def test_map_data_type(self, connector):
        """Test data type mapping."""
        assert connector._map_data_type('string') == 'TEXT'
        assert connector._map_data_type('int') == 'INTEGER'
        assert connector._map_data_type('float') == 'REAL'
        assert connector._map_data_type('boolean') == 'INTEGER'
        assert connector._map_data_type('date') == 'TEXT'
        assert connector._map_data_type('blob') == 'BLOB'
        assert connector._map_data_type('unknown_type') == 'TEXT'
        
    def test_format_default_value(self, connector):
        """Test default value formatting."""
        # String values
        assert connector._format_default_value('test', 'string') == "'test'"
        assert connector._format_default_value("value's", 'varchar') == "'value''s'"
        
        # Boolean values
        assert connector._format_default_value(True, 'boolean') == '1'
        assert connector._format_default_value(False, 'bool') == '0'
        
        # Numeric values
        assert connector._format_default_value(42, 'int') == '42'
        assert connector._format_default_value(3.14, 'float') == '3.14'
        
        # Date/time values
        assert connector._format_default_value('2023-01-01', 'date') == "'2023-01-01'"
        
        # JSON values
        json_data = {'key': 'value'}
        result = connector._format_default_value(json_data, 'json')
        assert result == "'{\"key\": \"value\"}'"
        
        # NULL value
        assert connector._format_default_value(None, 'any') == 'NULL'
        
    def test_backup_database_memory_error(self):
        """Test backup of memory database (should fail)."""
        params = {'database': ':memory:'}
        connector = SQLiteConnector(params)
        
        with pytest.raises(SQLiteQueryError, match="Cannot backup in-memory database"):
            connector.backup_database('/path/to/backup.db')
            
    @patch('src.connectors.sqlite_connector.os.makedirs')
    @patch('src.connectors.sqlite_connector.sqlite3.connect')
    def test_backup_database_success(self, mock_connect, mock_makedirs, connector):
        """Test successful database backup."""
        mock_source_conn = MagicMock()
        mock_backup_conn = MagicMock()
        
        mock_connect.return_value = mock_backup_conn
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_source_conn
            
            result = connector.backup_database('/path/to/backup.db')
            
        assert result is True
        mock_makedirs.assert_called_once_with('/path/to', exist_ok=True)
        mock_connect.assert_called_once_with('/path/to/backup.db')
        mock_source_conn.backup.assert_called_once_with(mock_backup_conn)
        mock_backup_conn.close.assert_called_once()
        
    @patch('src.connectors.sqlite_connector.sqlite3.connect')
    def test_backup_database_error(self, mock_connect, connector):
        """Test database backup with error."""
        mock_connect.side_effect = Exception("Backup error")
        
        with pytest.raises(SQLiteQueryError, match="Backup operation failed"):
            connector.backup_database('/path/to/backup.db')