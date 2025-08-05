"""
Unit tests for the base DatabaseConnector class.

These tests use mocks to test the abstract base class functionality
without requiring actual database connections.
"""

import pytest
import time
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any, List

from src.connectors.database_connector import DatabaseConnector


class ConcreteDatabaseConnector(DatabaseConnector):
    """Concrete implementation of DatabaseConnector for testing."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.mock_connection = Mock()
        self.connect_called = False
        self.disconnect_called = False
        
    def connect(self) -> bool:
        self.connect_called = True
        self.is_connected = True
        return True
        
    def disconnect(self) -> bool:
        self.disconnect_called = True
        self.is_connected = False
        return True
        
    def execute_query(self, query: str, params=None):
        return [{'result': 'mock'}]
        
    def begin_transaction(self) -> bool:
        return True
        
    def commit_transaction(self) -> bool:
        return True
        
    def rollback_transaction(self) -> bool:
        return True
        
    def get_connection_info(self) -> Dict[str, Any]:
        return {'db_type': 'mock', 'is_connected': self.is_connected}
        
    def _table_exists_impl(self, table_name: str, schema_name: str = None) -> bool:
        return table_name == 'existing_table'
        
    def _get_table_schema_impl(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        if table_name == 'test_table':
            return [
                {'column_name': 'id', 'data_type': 'integer'},
                {'column_name': 'name', 'data_type': 'varchar'}
            ]
        return []


class TestDatabaseConnector:
    """Test cases for the DatabaseConnector base class."""
    
    @pytest.fixture
    def connector(self, sample_connection_params):
        """Create a concrete database connector for testing."""
        return ConcreteDatabaseConnector(sample_connection_params)
    
    def test_initialization(self, sample_connection_params):
        """Test connector initialization."""
        connector = ConcreteDatabaseConnector(sample_connection_params)
        
        assert connector.connection_params == sample_connection_params
        assert connector.connection is None
        assert connector.is_connected is False
        assert connector.logger is not None
        assert connector.logger.name == 'data_ingestion.database_connector'
        
    def test_cache_initialization(self, sample_connection_params):
        """Test that caching is properly initialized."""
        connector = ConcreteDatabaseConnector(sample_connection_params)
        
        assert hasattr(connector, '_schema_cache')
        assert hasattr(connector, '_schema_cache_lock')
        assert hasattr(connector, '_cache_ttl')
        assert hasattr(connector, '_cache_enabled')
        assert connector._cache_enabled is True
        assert connector._cache_ttl == 300  # Default 5 minutes
        
    def test_cache_ttl_custom(self):
        """Test custom cache TTL setting."""
        params = {'schema_cache_ttl': 600, 'enable_schema_cache': False}
        connector = ConcreteDatabaseConnector(params)
        
        assert connector._cache_ttl == 600
        assert connector._cache_enabled is False
        
    def test_context_manager(self, connector):
        """Test context manager functionality."""
        with connector as conn:
            assert conn.connect_called
            assert conn.is_connected
            
        assert conn.disconnect_called
        assert not conn.is_connected
        
    def test_context_manager_with_exception(self, connector):
        """Test context manager with exception."""
        try:
            with connector as conn:
                assert conn.connect_called
                raise ValueError("Test exception")
        except ValueError:
            pass
            
        assert connector.disconnect_called
        
    def test_connection_test_success(self, connector):
        """Test successful connection test."""
        success, error = connector.test_connection()
        
        assert success is True
        assert error is None
        assert connector.connect_called
        
    def test_connection_test_failure(self, connector):
        """Test failed connection test."""
        # Make connect raise an exception
        def failing_connect():
            raise ConnectionError("Connection failed")
            
        connector.connect = failing_connect
        
        success, error = connector.test_connection()
        
        assert success is False
        assert error is not None
        assert "Connection test failed" in error
        
    def test_table_exists_with_cache_miss(self, connector):
        """Test table existence check with cache miss."""
        with patch.object(connector, '_get_cached_table_exists', return_value=None):
            with patch.object(connector, '_cache_table_exists') as mock_cache:
                result = connector.table_exists('existing_table')
                
                assert result is True
                mock_cache.assert_called_once_with('existing_table', True, None)
                
    def test_table_exists_with_cache_hit(self, connector):
        """Test table existence check with cache hit."""
        with patch.object(connector, '_get_cached_table_exists', return_value=True):
            with patch.object(connector, '_table_exists_impl') as mock_impl:
                result = connector.table_exists('test_table')
                
                assert result is True
                mock_impl.assert_not_called()
                
    def test_table_exists_cache_miss_false(self, connector):
        """Test table existence check returning False."""
        with patch.object(connector, '_get_cached_table_exists', return_value=None):
            result = connector.table_exists('nonexistent_table')
            assert result is False
            
    def test_get_table_schema_with_cache_miss(self, connector):
        """Test schema retrieval with cache miss."""
        with patch.object(connector, '_get_cached_table_schema', return_value=None):
            with patch.object(connector, '_cache_table_schema') as mock_cache:
                result = connector.get_table_schema('test_table')
                
                expected_schema = [
                    {'column_name': 'id', 'data_type': 'integer'},
                    {'column_name': 'name', 'data_type': 'varchar'}
                ]
                assert result == expected_schema
                mock_cache.assert_called_once_with('test_table', expected_schema, None)
                
    def test_get_table_schema_with_cache_hit(self, connector):
        """Test schema retrieval with cache hit."""
        cached_schema = [{'column_name': 'cached', 'data_type': 'text'}]
        
        with patch.object(connector, '_get_cached_table_schema', return_value=cached_schema):
            with patch.object(connector, '_get_table_schema_impl') as mock_impl:
                result = connector.get_table_schema('test_table')
                
                assert result == cached_schema
                mock_impl.assert_not_called()
                
    def test_cache_key_generation(self, connector):
        """Test cache key generation."""
        # Test without schema
        key = connector._get_cache_key('table1')
        assert key == 'table1'
        
        # Test with schema
        key = connector._get_cache_key('table1', 'schema1')
        assert key == 'schema1.table1'
        
    def test_cache_validity(self, connector):
        """Test cache validity checking."""
        current_time = time.time()
        
        # Valid cache entry
        valid_entry = {'timestamp': current_time - 100, 'data': 'test'}
        assert connector._is_cache_valid(valid_entry) is True
        
        # Expired cache entry
        expired_entry = {'timestamp': current_time - 400, 'data': 'test'}
        assert connector._is_cache_valid(expired_entry) is False
        
        # Cache disabled
        connector._cache_enabled = False
        assert connector._is_cache_valid(valid_entry) is False
        
    def test_cache_table_exists(self, connector):
        """Test caching table existence."""
        connector._cache_table_exists('test_table', True, 'schema1')
        
        cached_result = connector._get_cached_table_exists('test_table', 'schema1')
        assert cached_result is True
        
    def test_cache_table_exists_disabled(self, connector):
        """Test caching when disabled."""
        connector._cache_enabled = False
        connector._cache_table_exists('test_table', True, 'schema1')
        
        cached_result = connector._get_cached_table_exists('test_table', 'schema1')
        assert cached_result is None
        
    def test_cache_table_schema(self, connector):
        """Test caching table schema."""
        schema = [{'column_name': 'id', 'data_type': 'integer'}]
        connector._cache_table_schema('test_table', schema, 'schema1')
        
        cached_schema = connector._get_cached_table_schema('test_table', 'schema1')
        assert cached_schema == schema
        
    def test_invalidate_table_cache(self, connector):
        """Test cache invalidation."""
        # Cache some data
        connector._cache_table_exists('test_table', True, 'schema1')
        schema = [{'column_name': 'id', 'data_type': 'integer'}]
        connector._cache_table_schema('test_table', schema, 'schema1')
        
        # Verify data is cached
        assert connector._get_cached_table_exists('test_table', 'schema1') is True
        assert connector._get_cached_table_schema('test_table', 'schema1') == schema
        
        # Invalidate cache
        connector._invalidate_table_cache('test_table', 'schema1')
        
        # Verify cache is cleared
        assert connector._get_cached_table_exists('test_table', 'schema1') is None
        assert connector._get_cached_table_schema('test_table', 'schema1') is None
        
    def test_clear_schema_cache(self, connector):
        """Test clearing entire schema cache."""
        # Cache some data
        connector._cache_table_exists('table1', True)
        connector._cache_table_exists('table2', False)
        
        # Verify cache has data
        assert len(connector._schema_cache) > 0
        
        # Clear cache
        connector.clear_schema_cache()
        
        # Verify cache is empty
        assert len(connector._schema_cache) == 0
        
    def test_get_schema_cache_stats(self, connector):
        """Test cache statistics."""
        # Add some cache entries
        current_time = time.time()
        connector._schema_cache['test1'] = {
            'exists': True,
            'timestamp': current_time - 100,
            'type': 'table_existence'
        }
        connector._schema_cache['test2_schema'] = {
            'schema': [{'col': 'test'}],
            'timestamp': current_time - 400,  # Expired
            'type': 'table_schema'
        }
        
        stats = connector.get_schema_cache_stats()
        
        assert stats['enabled'] is True
        assert stats['cache_ttl_seconds'] == 300
        assert stats['total_entries'] == 2
        assert stats['valid_entries'] == 1
        assert stats['expired_entries'] == 1
        assert stats['table_existence_entries'] == 1
        assert stats['table_schema_entries'] == 1
        
    def test_cleanup_expired_cache(self, connector):
        """Test cleanup of expired cache entries."""
        current_time = time.time()
        
        # Add valid and expired entries
        connector._schema_cache['valid'] = {
            'exists': True,
            'timestamp': current_time - 100,
            'type': 'table_existence'
        }
        connector._schema_cache['expired'] = {
            'exists': True,
            'timestamp': current_time - 400,  # Expired
            'type': 'table_existence'
        }
        
        assert len(connector._schema_cache) == 2
        
        removed_count = connector.cleanup_expired_cache()
        
        assert removed_count == 1
        assert len(connector._schema_cache) == 1
        assert 'valid' in connector._schema_cache
        assert 'expired' not in connector._schema_cache
        
    def test_cleanup_expired_cache_disabled(self, connector):
        """Test cleanup when caching is disabled."""
        connector._cache_enabled = False
        
        removed_count = connector.cleanup_expired_cache()
        assert removed_count == 0