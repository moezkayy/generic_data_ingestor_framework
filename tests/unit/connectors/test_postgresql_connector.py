"""
Unit tests for PostgreSQL connector using mocks.

These tests cover all functionality of the PostgreSQL connector without
requiring an actual PostgreSQL database connection.
"""

import pytest
import time
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any

from src.connectors.postgresql_connector import (
    PostgreSQLConnector,
    PostgreSQLConnectionError,
    PostgreSQLQueryError,
    PostgreSQLTransactionError
)


class TestPostgreSQLConnector:
    """Test cases for PostgreSQL connector."""
    
    @pytest.fixture
    def connector(self, sample_connection_params):
        """Create a PostgreSQL connector for testing."""
        return PostgreSQLConnector(sample_connection_params)
    
    @pytest.fixture
    def mock_psycopg2_pool(self):
        """Mock psycopg2 connection pool."""
        with patch('src.connectors.postgresql_connector.psycopg2.pool') as mock_pool:
            yield mock_pool
    
    @pytest.fixture
    def mock_psycopg2_extras(self):
        """Mock psycopg2 extras."""
        with patch('src.connectors.postgresql_connector.psycopg2.extras') as mock_extras:
            yield mock_extras
    
    def test_initialization(self, sample_connection_params):
        """Test PostgreSQL connector initialization."""
        connector = PostgreSQLConnector(sample_connection_params)
        
        assert connector.connection_params == sample_connection_params
        assert connector.connection_pool is None
        assert connector.current_connection is None
        assert connector.is_connected is False
        assert connector.in_transaction is False
        assert connector.max_retries == sample_connection_params.get('max_retries', 3)
        assert connector.retry_delay == sample_connection_params.get('retry_delay', 1)
        
    def test_initialization_missing_required_params(self):
        """Test initialization with missing required parameters."""
        with pytest.raises(ValueError, match="Missing required PostgreSQL parameter: host"):
            PostgreSQLConnector({'database': 'test'})
            
        with pytest.raises(ValueError, match="Missing required PostgreSQL parameter: database"):
            PostgreSQLConnector({'host': 'localhost'})
            
        with pytest.raises(ValueError, match="Missing required PostgreSQL parameter: username"):
            PostgreSQLConnector({'host': 'localhost', 'database': 'test'})
    
    def test_initialization_default_values(self):
        """Test initialization with default values."""
        params = {
            'host': 'localhost',
            'database': 'test',
            'username': 'user'
        }
        connector = PostgreSQLConnector(params)
        
        assert connector.connection_params['port'] == 5432
        assert connector.connection_params['connection_pool_size'] == 5
        assert connector.connection_params['connection_timeout'] == 30
        
    def test_initialization_invalid_port(self):
        """Test initialization with invalid port."""
        params = {
            'host': 'localhost',
            'database': 'test',
            'username': 'user',
            'port': 'invalid'
        }
        with pytest.raises(ValueError, match="Invalid port number"):
            PostgreSQLConnector(params)
            
    def test_build_connection_string(self, connector):
        """Test connection string building."""
        conn_string = connector.connection_string
        
        assert 'host=localhost' in conn_string
        assert 'port=5432' in conn_string
        assert 'dbname=test_db' in conn_string
        assert 'user=test_user' in conn_string
        assert 'password=test_pass' in conn_string
        assert 'connect_timeout=30' in conn_string
        assert 'sslmode=disable' in conn_string
        
    def test_build_connection_string_with_ssl(self):
        """Test connection string with SSL enabled."""
        params = {
            'host': 'localhost',
            'database': 'test',
            'username': 'user',
            'ssl_enabled': True,
            'ssl_ca_cert': '/path/to/ca.crt',
            'ssl_client_cert': '/path/to/client.crt',
            'ssl_client_key': '/path/to/client.key'
        }
        connector = PostgreSQLConnector(params)
        conn_string = connector.connection_string
        
        assert 'sslmode=require' in conn_string
        assert 'sslcert=/path/to/client.crt' in conn_string
        assert 'sslkey=/path/to/client.key' in conn_string
        
    @patch('src.connectors.postgresql_connector.psycopg2.pool.ThreadedConnectionPool')
    def test_connect_success(self, mock_pool_class, connector):
        """Test successful connection."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        mock_pool_class.return_value = mock_pool
        mock_pool.getconn.return_value = mock_connection
        
        result = connector.connect()
        
        assert result is True
        assert connector.is_connected is True
        assert connector.connection_pool == mock_pool
        
        # Verify pool creation
        mock_pool_class.assert_called_once_with(
            minconn=1,
            maxconn=connector.connection_params['connection_pool_size'],
            dsn=connector.connection_string
        )
        
        # Verify connection test
        mock_pool.getconn.assert_called()
        mock_connection.close.assert_called()
        mock_pool.putconn.assert_called_with(mock_connection)
        
    def test_connect_already_connected(self, connector):
        """Test connect when already connected."""
        connector.is_connected = True
        connector.connection_pool = MagicMock()
        
        result = connector.connect()
        
        assert result is True
        
    @patch('src.connectors.postgresql_connector.psycopg2.pool.ThreadedConnectionPool')
    @patch('src.connectors.postgresql_connector.time.sleep')
    def test_connect_with_retries(self, mock_sleep, mock_pool_class, connector):
        """Test connection with retry logic."""
        from src.connectors.postgresql_connector import OperationalError
        
        # First two attempts fail, third succeeds
        mock_pool_class.side_effect = [
            OperationalError("Connection failed"),
            OperationalError("Connection failed"),
            MagicMock()
        ]
        
        connector.max_retries = 3
        result = connector.connect()
        
        assert result is True
        assert mock_pool_class.call_count == 3
        assert mock_sleep.call_count == 2
        
    @patch('src.connectors.postgresql_connector.psycopg2.pool.ThreadedConnectionPool')
    def test_connect_failure_all_retries(self, mock_pool_class, connector):
        """Test connection failure after all retries."""
        from src.connectors.postgresql_connector import OperationalError
        
        mock_pool_class.side_effect = OperationalError("Connection failed")
        connector.max_retries = 2
        
        with pytest.raises(PostgreSQLConnectionError, match="Failed to connect"):
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
        
        with patch.object(connector, 'rollback_transaction') as mock_rollback:
            result = connector.disconnect()
            
        assert result is True
        assert connector.is_connected is False
        assert connector.connection_pool is None
        assert connector.current_connection is None
        mock_rollback.assert_called_once()
        mock_pool.closeall.assert_called_once()
        
    def test_disconnect_with_error(self, connector):
        """Test disconnection with error."""
        mock_pool = MagicMock()
        mock_pool.closeall.side_effect = Exception("Disconnect error")
        
        connector.connection_pool = mock_pool
        connector.is_connected = True
        
        result = connector.disconnect()
        
        assert result is False
        
    def test_get_connection_context_manager(self, connector):
        """Test connection context manager."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        connector.connection_pool = mock_pool
        connector.is_connected = True
        mock_pool.getconn.return_value = mock_connection
        
        with connector._get_connection() as conn:
            assert conn == mock_connection
            
        mock_pool.getconn.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_connection)
        
    def test_get_connection_not_connected(self, connector):
        """Test connection context manager when not connected."""
        with pytest.raises(PostgreSQLConnectionError, match="Not connected"):
            with connector._get_connection():
                pass
                
    def test_get_connection_with_exception(self, connector):
        """Test connection context manager with exception."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        connector.connection_pool = mock_pool
        connector.is_connected = True
        mock_pool.getconn.return_value = mock_connection
        
        with pytest.raises(ValueError):
            with connector._get_connection() as conn:
                raise ValueError("Test error")
                
        mock_connection.rollback.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_connection)
        
    @patch('src.connectors.postgresql_connector.psycopg2.extras.RealDictCursor')
    def test_execute_query_select(self, mock_cursor_class, connector):
        """Test SELECT query execution."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_cursor_class.return_value = mock_cursor
        
        # Mock results
        mock_row1 = {'id': 1, 'name': 'test1'}
        mock_row2 = {'id': 2, 'name': 'test2'}
        mock_cursor.fetchall.return_value = [mock_row1, mock_row2]
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = connector.execute_query("SELECT * FROM test", ['param1'])
            
        assert result == [mock_row1, mock_row2]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ['param1'])
        mock_cursor.fetchall.assert_called_once()
        
    @patch('src.connectors.postgresql_connector.psycopg2.extras.RealDictCursor')
    def test_execute_query_insert(self, mock_cursor_class, connector):
        """Test INSERT query execution."""
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_cursor_class.return_value = mock_cursor
        mock_cursor.rowcount = 5
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = connector.execute_query("INSERT INTO test VALUES (%s)", ['value'])
            
        assert result == 5
        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (%s)", ['value'])
        mock_connection.commit.assert_called_once()
        
    def test_execute_query_empty(self, connector):
        """Test execution of empty query."""
        with pytest.raises(PostgreSQLQueryError, match="Query cannot be empty"):
            connector.execute_query("")
            
        with pytest.raises(PostgreSQLQueryError, match="Query cannot be empty"):
            connector.execute_query("   ")
            
    @patch('src.connectors.postgresql_connector.psycopg2.extras.RealDictCursor')
    def test_execute_query_programming_error(self, mock_cursor_class, connector):
        """Test query execution with programming error."""
        from src.connectors.postgresql_connector import ProgrammingError
        
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_cursor_class.return_value = mock_cursor
        mock_cursor.execute.side_effect = ProgrammingError("Syntax error")
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            
            with pytest.raises(PostgreSQLQueryError, match="SQL syntax error"):
                connector.execute_query("INVALID SQL")
                
    def test_begin_transaction_success(self, connector):
        """Test successful transaction begin."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        
        connector.connection_pool = mock_pool
        mock_pool.getconn.return_value = mock_connection
        
        result = connector.begin_transaction()
        
        assert result is True
        assert connector.in_transaction is True
        assert connector.current_connection == mock_connection
        mock_connection.autocommit = False
        
    def test_begin_transaction_already_in_transaction(self, connector):
        """Test begin transaction when already in transaction."""
        connector.in_transaction = True
        
        result = connector.begin_transaction()
        
        assert result is True
        
    def test_begin_transaction_error(self, connector):
        """Test begin transaction with error."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = Exception("Connection error")
        
        connector.connection_pool = mock_pool
        
        with pytest.raises(PostgreSQLTransactionError, match="Failed to start transaction"):
            connector.begin_transaction()
            
    def test_commit_transaction_success(self, connector):
        """Test successful transaction commit."""
        mock_connection = MagicMock()
        
        connector.current_connection = mock_connection
        connector.in_transaction = True
        connector.connection_pool = MagicMock()
        
        result = connector.commit_transaction()
        
        assert result is True
        assert connector.in_transaction is False
        assert connector.current_connection is None
        mock_connection.commit.assert_called_once()
        mock_connection.autocommit = True
        
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
        
        with pytest.raises(PostgreSQLTransactionError, match="Failed to commit transaction"):
            connector.commit_transaction()
            
    def test_rollback_transaction_success(self, connector):
        """Test successful transaction rollback."""
        mock_connection = MagicMock()
        
        connector.current_connection = mock_connection
        connector.in_transaction = True
        connector.connection_pool = MagicMock()
        
        result = connector.rollback_transaction()
        
        assert result is True
        assert connector.in_transaction is False 
        assert connector.current_connection is None
        mock_connection.rollback.assert_called_once()
        mock_connection.autocommit = True
        
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
        
        with pytest.raises(PostgreSQLTransactionError, match="Failed to rollback transaction"):
            connector.rollback_transaction()
            
    def test_get_connection_info_basic(self, connector):
        """Test basic connection info."""
        info = connector.get_connection_info()
        
        expected_keys = [
            'db_type', 'host', 'port', 'database', 'username',
            'is_connected', 'connection_pool_size', 'ssl_enabled', 'in_transaction'
        ]
        
        for key in expected_keys:
            assert key in info
            
        assert info['db_type'] == 'postgresql'
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
            ('public',),  # current_schema
            (5,)  # active_connections
        ]
        mock_connection.get_parameter_status.side_effect = [
            'PostgreSQL 13.0',  # server_version
            'UTF8',  # server_encoding
            'UTF8'   # client_encoding
        ]
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_connection
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            
            info = connector.get_connection_info()
            
        assert info['server_version'] == 'PostgreSQL 13.0'
        assert info['server_encoding'] == 'UTF8'
        assert info['client_encoding'] == 'UTF8'
        assert info['current_schema'] == 'public'
        assert info['active_connections'] == 5
        
    def test_execute_batch(self, connector):
        """Test batch query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 10
        
        params_list = [['value1'], ['value2'], ['value3']]
        
        with patch.object(connector, '_get_connection') as mock_get_conn:
            with patch('src.connectors.postgresql_connector.psycopg2.extras.execute_batch') as mock_execute_batch:
                mock_get_conn.return_value.__enter__.return_value = mock_connection
                mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
                
                result = connector.execute_batch("INSERT INTO test VALUES (%s)", params_list)
                
        assert result == 10
        mock_execute_batch.assert_called_once_with(mock_cursor, "INSERT INTO test VALUES (%s)", params_list)
        mock_connection.commit.assert_called_once()
        
    def test_execute_batch_empty_query(self, connector):
        """Test batch execution with empty query."""
        with pytest.raises(PostgreSQLQueryError, match="Query cannot be empty"):
            connector.execute_batch("", [])
            
    def test_execute_batch_empty_params(self, connector):
        """Test batch execution with empty parameters."""
        with pytest.raises(PostgreSQLQueryError, match="Parameters list cannot be empty"):
            connector.execute_batch("INSERT INTO test VALUES (%s)", [])
            
    def test_table_exists_impl_true(self, connector):
        """Test table existence check returning True."""
        mock_result = [{'exists': True}]
        
        with patch.object(connector, 'execute_query', return_value=mock_result):
            result = connector._table_exists_impl('test_table', 'test_schema')
            
        assert result is True
        
    def test_table_exists_impl_false(self, connector):
        """Test table existence check returning False."""
        mock_result = [{'exists': False}]
        
        with patch.object(connector, 'execute_query', return_value=mock_result):
            result = connector._table_exists_impl('test_table')
            
        assert result is False
        
    def test_get_table_schema_impl(self, connector):
        """Test table schema retrieval."""
        mock_schema = [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO',
                'column_default': 'nextval(\'test_seq\')',
                'character_maximum_length': None,
                'numeric_precision': 32,
                'numeric_scale': 0
            },
            {
                'column_name': 'name',
                'data_type': 'varchar',
                'is_nullable': 'YES',
                'column_default': None,
                'character_maximum_length': 255,
                'numeric_precision': None,
                'numeric_scale': None
            }
        ]
        
        with patch.object(connector, 'execute_query', return_value=mock_schema):
            result = connector._get_table_schema_impl('test_table', 'test_schema')
            
        assert result == mock_schema
        
    def test_get_table_schema_impl_error(self, connector):
        """Test table schema retrieval with error."""
        with patch.object(connector, 'execute_query', side_effect=Exception("DB Error")):
            with pytest.raises(PostgreSQLQueryError, match="Failed to get table schema"):
                connector._get_table_schema_impl('test_table')