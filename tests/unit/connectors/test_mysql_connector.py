"""
Unit tests for MySQL connector using mocks.

These tests cover all functionality of the MySQL connector without
requiring an actual MySQL database connection.
"""

import pytest
import time
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any

from src.connectors.mysql_connector import (
    MySQLConnector,
    MySQLConnectionError,
    MySQLQueryError,
    MySQLTransactionError
)


class TestMySQLConnector:
    """Test cases for MySQL connector."""
    
    @pytest.fixture
    def connector(self, sample_mysql_params):
        """Create a MySQL connector for testing."""
        return MySQLConnector(sample_mysql_params)
    
    @pytest.fixture
    def mock_mysql_connector(self):
        """Mock mysql.connector module."""
        with patch('src.connectors.mysql_connector.mysql.connector') as mock_mysql:
            yield mock_mysql
    
    def test_initialization(self, sample_mysql_params):
        """Test MySQL connector initialization."""
        connector = MySQLConnector(sample_mysql_params)
        
        assert connector.connection_params == sample_mysql_params
        assert connector.connection_pool is None
        assert connector.current_connection is None
        assert connector.is_connected is False
        assert connector.in_transaction is False
        assert connector.max_retries == sample_mysql_params.get('max_retries', 3)
        assert connector.retry_delay == sample_mysql_params.get('retry_delay', 1)
        
    def test_initialization_missing_required_params(self):
        """Test initialization with missing required parameters."""
        with pytest.raises(ValueError, match="Missing required MySQL parameter: host"):
            MySQLConnector({'database': 'test'})
            
        with pytest.raises(ValueError, match="Missing required MySQL parameter: database"):
            MySQLConnector({'host': 'localhost'})
            
        with pytest.raises(ValueError, match="Missing required MySQL parameter: user"):
            MySQLConnector({'host': 'localhost', 'database': 'test'})
    
    def test_initialization_default_values(self):
        """Test initialization with default values."""
        params = {
            'host': 'localhost',
            'database': 'test',
            'user': 'user'
        }
        connector = MySQLConnector(params)
        
        assert connector.connection_params['port'] == 3306
        assert connector.connection_params['connection_pool_size'] == 5
        assert connector.connection_params['connection_timeout'] == 30
        
    def test_initialization_invalid_port(self):
        """Test initialization with invalid port."""
        params = {
            'host': 'localhost',
            'database': 'test',
            'user': 'user',
            'port': 70000
        }
        with pytest.raises(ValueError, match="Invalid port number"):
            MySQLConnector(params)
            
    def test_build_connection_config(self, connector):
        """Test connection configuration building."""
        config = connector.connection_config
        
        assert config['host'] == 'localhost'
        assert config['port'] == 3306
        assert config['database'] == 'test_db'
        assert config['user'] == 'test_user'
        assert config['connection_timeout'] == 30
        assert config['autocommit'] is False
        assert config['charset'] == 'utf8mb4'
        assert config['collation'] == 'utf8mb4_unicode_ci'
        
    def test_build_connection_config_with_ssl(self):
        """Test connection configuration with SSL."""
        params = {
            'host': 'localhost',
            'database': 'test',
            'user': 'user',
            'ssl_ca': '/path/to/ca.pem',
            'ssl_cert': '/path/to/cert.pem',
            'ssl_key': '/path/to/key.pem'
        }
        connector = MySQLConnector(params)
        config = connector.connection_config
        
        assert config['ssl_disabled'] is False
        assert config['ssl_ca'] == '/path/to/ca.pem'
        assert config['ssl_cert'] == '/path/to/cert.pem'
        assert config['ssl_key'] == '/path/to/key.pem'
        
    def test_build_connection_config_ssl_disabled(self):
        """Test connection configuration with SSL disabled."""
        params = {
            'host': 'localhost',
            'database': 'test',
            'user': 'user',
            'ssl_disabled': True
        }
        connector = MySQLConnector(params)
        config = connector.connection_config
        
        assert config['ssl_disabled'] is True
        
    @patch('src.connectors.mysql_connector.mysql.connector.pooling.MySQLConnectionPool')
    def test_connect_success(self, mock_pool_class, connector):
        """Test successful connection."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        mock_pool_class.return_value = mock_pool
        mock_pool.get_connection.return_value = mock_connection
        
        result = connector.connect()
        
        assert result is True
        assert connector.is_connected is True
        assert connector.connection_pool == mock_pool
        
        # Verify pool creation
        mock_pool_class.assert_called_once()
        call_kwargs = mock_pool_class.call_args[1]
        assert call_kwargs['pool_size'] == connector.connection_params['connection_pool_size']
        assert 'mysql_pool_' in call_kwargs['pool_name']
        
        # Verify connection test
        mock_pool.get_connection.assert_called()
        mock_connection.ping.assert_called_once_with(reconnect=True, attempts=1, delay=0)
        mock_connection.close.assert_called_once()
        
    def test_connect_already_connected(self, connector):
        """Test connect when already connected."""
        connector.is_connected = True
        connector.connection_pool = MagicMock()
        
        result = connector.connect()
        
        assert result is True
        
    @patch('src.connectors.mysql_connector.mysql.connector.pooling.MySQLConnectionPool')
    @patch('src.connectors.mysql_connector.time.sleep')
    def test_connect_with_retries(self, mock_sleep, mock_pool_class, connector):
        """Test connection with retry logic."""
        from src.connectors.mysql_connector import MySQLError
        
        # First two attempts fail, third succeeds
        mock_pool_class.side_effect = [
            MySQLError("Connection failed"),
            MySQLError("Connection failed"),
            MagicMock()
        ]
        
        connector.max_retries = 3
        result = connector.connect()
        
        assert result is True
        assert mock_pool_class.call_count == 3
        assert mock_sleep.call_count == 2
        
    @patch('src.connectors.mysql_connector.mysql.connector.pooling.MySQLConnectionPool')
    def test_connect_failure_all_retries(self, mock_pool_class, connector):
        """Test connection failure after all retries."""
        from src.connectors.mysql_connector import MySQLError
        
        mock_pool_class.side_effect = MySQLError("Connection failed")
        connector.max_retries = 2
        
        with pytest.raises(MySQLConnectionError, match="Failed to connect"):
            connector.connect()
            
        assert mock_pool_class.call_count == 2
        
    def test_disconnect_success(self, connector):
        """Test successful disconnection."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        connector.connection_pool = mock_pool
        connector.current_connection = mock_connection
        connector.in_transaction = True
        connector.is_connected = True
        
        # Mock getting connections until pool is empty
        mock_pool.get_connection.side_effect = [
            MagicMock(), MagicMock(), Exception("Pool empty")
        ]
        
        with patch.object(connector, 'rollback_transaction') as mock_rollback:
            result = connector.disconnect()
            
        assert result is True
        assert connector.is_connected is False
        assert connector.connection_pool is None
        assert connector.current_connection is None
        mock_rollback.assert_called_once()
        
    def test_disconnect_with_error(self, connector):
        """Test disconnection with error."""
        mock_connection = MagicMock()
        mock_connection.close.side_effect = Exception("Disconnect error")
        
        connector.current_connection = mock_connection
        connector.is_connected = True
        
        result = connector.disconnect()
        
        assert result is False
        
    def test_get_connection_context_manager(self, connector):
        """Test connection context manager."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        connector.connection_pool = mock_pool
        connector.is_connected = True
        mock_pool.get_connection.return_value = mock_connection
        
        with connector._get_connection() as conn:
            assert conn == mock_connection
            
        mock_pool.get_connection.assert_called_once()
        mock_connection.close.assert_called_once()
        
    def test_get_connection_not_connected(self, connector):
        """Test connection context manager when not connected."""
        with pytest.raises(MySQLConnectionError, match="Not connected"):
            with connector._get_connection():
                pass
                
    def test_get_connection_with_exception(self, connector):
        """Test connection context manager with exception."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        connector.connection_pool = mock_pool
        connector.is_connected = True
        mock_pool.get_connection.return_value = mock_connection
        
        with pytest.raises(ValueError):
            with connector._get_connection() as conn:
                raise ValueError("Test error")
                
        mock_connection.close.assert_called_once()
        
    def test_execute_query_select(self, connector):
        """Test SELECT query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock results
        mock_results = [{'id': 1, 'name': 'test1'}, {'id': 2, 'name': 'test2'}]
        mock_cursor.fetchall.return_value = mock_results
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            result = connector.execute_query("SELECT * FROM test", ['param1'])
            
        assert result == mock_results
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ['param1'])
        mock_cursor.fetchall.assert_called_once()
        
    def test_execute_query_insert(self, connector):
        """Test INSERT query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            result = connector.execute_query("INSERT INTO test VALUES (%s)", ['value'])
            
        assert result == 5
        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (%s)", ['value'])
        mock_connection.commit.assert_called_once()
        
    def test_execute_query_empty(self, connector):
        """Test execution of empty query."""
        with pytest.raises(MySQLQueryError, match="Query cannot be empty"):
            connector.execute_query("")
            
        with pytest.raises(MySQLQueryError, match="Query cannot be empty"):
            connector.execute_query("   ")
            
    def test_execute_query_programming_error(self, connector):
        """Test query execution with programming error."""
        from src.connectors.mysql_connector import ProgrammingError
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = ProgrammingError("Syntax error")
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            with pytest.raises(MySQLQueryError, match="SQL syntax error"):
                connector.execute_query("INVALID SQL")
                
    def test_begin_transaction_success(self, connector):
        """Test successful transaction begin."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        connector.connection_pool = mock_pool
        mock_pool.get_connection.return_value = mock_connection
        
        result = connector.begin_transaction()
        
        assert result is True
        assert connector.in_transaction is True
        assert connector.current_connection == mock_connection
        mock_connection.start_transaction.assert_called_once()
        
    def test_begin_transaction_already_in_transaction(self, connector):
        """Test begin transaction when already in transaction."""
        connector.in_transaction = True
        
        result = connector.begin_transaction()
        
        assert result is True
        
    def test_begin_transaction_error(self, connector):
        """Test begin transaction with error."""
        mock_pool = MagicMock()
        mock_pool.get_connection.side_effect = Exception("Connection error")
        
        connector.connection_pool = mock_pool
        
        with pytest.raises(MySQLTransactionError, match="Failed to start transaction"):
            connector.begin_transaction()
            
    def test_commit_transaction_success(self, connector):
        """Test successful transaction commit."""
        mock_connection = MagicMock()
        
        connector.current_connection = mock_connection
        connector.in_transaction = True
        
        result = connector.commit_transaction()
        
        assert result is True
        assert connector.in_transaction is False
        assert connector.current_connection is None
        mock_connection.commit.assert_called_once()
        mock_connection.close.assert_called_once()
        
    def test_commit_transaction_not_in_transaction(self, connector):
        """Test commit when not in transaction."""
        connector.in_transaction = False
        
        result = connector.commit_transaction()
        
        assert result is True
        
    def test_commit_transaction_error(self, connector):
        """Test commit transaction with error."""
        mock_connection = MagicMock()
        mock_connection.commit.side_effect = Exception("Commit error")
        
        connector.current_connection = mock_connection
        connector.in_transaction = True
        
        with pytest.raises(MySQLTransactionError, match="Failed to commit transaction"):
            connector.commit_transaction()
            
    def test_rollback_transaction_success(self, connector):
        """Test successful transaction rollback."""
        mock_connection = MagicMock()
        
        connector.current_connection = mock_connection
        connector.in_transaction = True
        
        result = connector.rollback_transaction()
        
        assert result is True
        assert connector.in_transaction is False 
        assert connector.current_connection is None
        mock_connection.rollback.assert_called_once()
        mock_connection.close.assert_called_once()
        
    def test_rollback_transaction_not_in_transaction(self, connector):
        """Test rollback when not in transaction."""
        connector.in_transaction = False
        
        result = connector.rollback_transaction()
        
        assert result is True
        
    def test_rollback_transaction_error(self, connector):
        """Test rollback transaction with error."""
        mock_connection = MagicMock()
        mock_connection.rollback.side_effect = Exception("Rollback error")
        
        connector.current_connection = mock_connection
        connector.in_transaction = True
        
        with pytest.raises(MySQLTransactionError, match="Failed to rollback transaction"):
            connector.rollback_transaction()
            
    def test_get_connection_info_basic(self, connector):
        """Test basic connection info."""
        info = connector.get_connection_info()
        
        expected_keys = [
            'db_type', 'host', 'port', 'database', 'user',
            'is_connected', 'connection_pool_size', 'ssl_disabled', 'in_transaction'
        ]
        
        for key in expected_keys:
            assert key in info
            
        assert info['db_type'] == 'mysql'
        assert info['host'] == connector.connection_params['host']
        assert info['is_connected'] is False
        
    def test_get_connection_info_extended(self, connector):
        """Test extended connection info when connected."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        connector.is_connected = True
        connector.connection_pool = MagicMock()
        
        # Mock server information
        mock_cursor.fetchone.side_effect = [
            ('8.0.25',),  # version
            ('test_db',),  # current_db
            ('mysql', 42),  # threads_connected
            ('utf8mb4',)  # character_set
        ]
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            info = connector.get_connection_info()
            
        assert info['server_version'] == '8.0.25'
        assert info['current_database'] == 'test_db'
        assert info['active_connections'] == 42
        assert info['character_set'] == 'utf8mb4'
        
    def test_execute_batch(self, connector):
        """Test batch query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 10
        
        params_list = [['value1'], ['value2'], ['value3']]
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value = mock_cursor
            
            result = connector.execute_batch("INSERT INTO test VALUES (%s)", params_list)
            
        assert result == 10
        mock_cursor.executemany.assert_called_once_with("INSERT INTO test VALUES (%s)", params_list)
        mock_connection.commit.assert_called_once()
        
    def test_execute_batch_empty_query(self, connector):
        """Test batch execution with empty query."""
        with pytest.raises(MySQLQueryError, match="Query cannot be empty"):
            connector.execute_batch("", [])
            
    def test_execute_batch_empty_params(self, connector):
        """Test batch execution with empty parameters."""
        with pytest.raises(MySQLQueryError, match="Parameters list cannot be empty"):
            connector.execute_batch("INSERT INTO test VALUES (%s)", [])
            
    def test_table_exists_impl_true(self, connector):
        """Test table existence check returning True."""
        mock_result = [{'count': 1}]
        
        with patch.object(connector, 'execute_query', return_value=mock_result):
            result = connector._table_exists_impl('test_table', 'test_schema')
            
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
                'column_name': 'id',
                'data_type': 'int',
                'is_nullable': 'NO',
                'column_default': None,
                'character_maximum_length': None,
                'numeric_precision': 10,
                'numeric_scale': 0,
                'column_key': 'PRI',
                'extra': 'auto_increment'
            },
            {
                'column_name': 'name',
                'data_type': 'varchar',
                'is_nullable': 'YES',
                'column_default': None,
                'character_maximum_length': 255,
                'numeric_precision': None,
                'numeric_scale': None,
                'column_key': '',
                'extra': ''
            }
        ]
        
        with patch.object(connector, 'execute_query', return_value=mock_schema):
            result = connector._get_table_schema_impl('test_table', 'test_schema')
            
        assert result == mock_schema
        
    def test_get_table_schema_impl_error(self, connector):
        """Test table schema retrieval with error."""
        with patch.object(connector, 'execute_query', side_effect=Exception("DB Error")):
            with pytest.raises(MySQLQueryError, match="Failed to get table schema"):
                connector._get_table_schema_impl('test_table')
                
    def test_map_data_type(self, connector):
        """Test data type mapping."""
        assert connector._map_data_type('string') == 'TEXT'
        assert connector._map_data_type('int') == 'INT'
        assert connector._map_data_type('varchar(255)') == 'VARCHAR(255)'
        assert connector._map_data_type('boolean') == 'BOOLEAN'
        assert connector._map_data_type('unknown_type') == 'UNKNOWN_TYPE'
        
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
        assert connector._format_default_value(3.14, 'decimal') == '3.14'
        
        # Date/time values
        assert connector._format_default_value('NOW()', 'timestamp') == 'NOW()'
        assert connector._format_default_value('2023-01-01', 'date') == "'2023-01-01'"
        
        # JSON values
        json_data = {'key': 'value'}
        result = connector._format_default_value(json_data, 'json')
        assert result == "'{\"key\": \"value\"}'"
        
        # NULL value
        assert connector._format_default_value(None, 'any') == 'NULL'