"""
Abstract Database Connector for the Generic Data Ingestor Framework.

This module provides an abstract base class for database connections,
defining a standard interface for all database connector implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Tuple
import logging
import time
import threading
from collections import defaultdict


class DatabaseConnector(ABC):
    """
    Abstract base class for database connections.
    
    This class defines the standard interface that all database connector
    implementations must follow. It provides methods for connecting to a database,
    executing queries, and managing transactions.
    
    Attributes:
        logger: Logger instance for the connector
        connection_params: Dictionary containing connection parameters
        connection: The active database connection object
        is_connected: Boolean indicating if the connection is active
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the database connector with connection parameters.
        
        Args:
            connection_params: Dictionary containing connection parameters such as
                              host, port, database name, username, password, etc.
        """
        self.logger = logging.getLogger('data_ingestion.database_connector')
        self.connection_params = connection_params
        self.connection = None
        self.is_connected = False
        
        # Schema caching attributes
        self._schema_cache = {}
        self._schema_cache_lock = threading.RLock()
        self._cache_ttl = connection_params.get('schema_cache_ttl', 300)  # 5 minutes default
        self._cache_enabled = connection_params.get('enable_schema_cache', True)
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish a connection to the database.
        
        Returns:
            bool: True if connection was successful, False otherwise
        
        Raises:
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Close the database connection.
        
        Returns:
            bool: True if disconnection was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Union[List, Dict]] = None) -> Any:
        """
        Execute a query on the database.
        
        Args:
            query: SQL query string to execute
            params: Parameters to bind to the query
            
        Returns:
            Query results
            
        Raises:
            ConnectionError: If not connected to the database
            QueryError: If query execution fails
        """
        pass
    
    @abstractmethod
    def begin_transaction(self) -> bool:
        """
        Begin a database transaction.
        
        Returns:
            bool: True if transaction was successfully started, False otherwise
        """
        pass
    
    @abstractmethod
    def commit_transaction(self) -> bool:
        """
        Commit the current transaction.
        
        Returns:
            bool: True if transaction was successfully committed, False otherwise
        """
        pass
    
    @abstractmethod
    def rollback_transaction(self) -> bool:
        """
        Rollback the current transaction.
        
        Returns:
            bool: True if transaction was successfully rolled back, False otherwise
        """
        pass
    
    @abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current database connection.
        
        Returns:
            Dict containing connection information such as database type,
            server version, connection status, etc.
        """
        pass
    
    @abstractmethod
    def _table_exists_impl(self, table_name: str, schema_name: str = None) -> bool:
        """
        Implementation-specific method to check if a table exists.
        This method should be implemented by concrete connector classes.
        
        Args:
            table_name: Name of the table to check
            schema_name: Schema name (optional)
            
        Returns:
            bool: True if table exists, False otherwise
        """
        pass
    
    @abstractmethod
    def _get_table_schema_impl(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Implementation-specific method to get table schema.
        This method should be implemented by concrete connector classes.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            
        Returns:
            List of dictionaries containing column information
        """
        pass
    
    def table_exists(self, table_name: str, schema_name: str = None) -> bool:
        """
        Check if a table exists in the database with caching support.
        
        Args:
            table_name: Name of the table to check
            schema_name: Schema name (optional)
            
        Returns:
            bool: True if table exists, False otherwise
        """
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        self.logger.debug(f"DB_TABLE_EXISTS: Checking existence of table '{full_table_name}'")
        
        # Try to get from cache first
        cached_result = self._get_cached_table_exists(table_name, schema_name)
        if cached_result is not None:
            self.logger.debug(f"DB_TABLE_EXISTS: Found cached result for table '{full_table_name}': {cached_result}")
            return cached_result
        
        # Cache miss - call implementation-specific method
        self.logger.debug(f"DB_TABLE_EXISTS: Cache miss, querying database for table '{full_table_name}'")
        exists = self._table_exists_impl(table_name, schema_name)
        
        # Cache the result
        self._cache_table_exists(table_name, exists, schema_name)
        
        self.logger.info(f"DB_TABLE_EXISTS: Table '{full_table_name}' exists: {exists}")
        return exists
    
    def get_table_schema(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Get table schema with caching support.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            
        Returns:
            List of dictionaries containing column information
        """
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        self.logger.debug(f"DB_SCHEMA_RETRIEVAL: Retrieving schema for table '{full_table_name}'")
        
        # Try to get from cache first
        cached_schema = self._get_cached_table_schema(table_name, schema_name)
        if cached_schema is not None:
            self.logger.debug(f"DB_SCHEMA_RETRIEVAL: Found cached schema for table '{full_table_name}' with {len(cached_schema)} columns")
            return cached_schema
        
        # Cache miss - call implementation-specific method
        self.logger.debug(f"DB_SCHEMA_RETRIEVAL: Cache miss, querying database schema for table '{full_table_name}'")
        schema = self._get_table_schema_impl(table_name, schema_name)
        
        # Cache the result
        self._cache_table_schema(table_name, schema, schema_name)
        
        column_count = len(schema)
        column_names = [col.get('column_name', 'unknown') for col in schema]
        self.logger.info(f"DB_SCHEMA_RETRIEVAL: Retrieved schema for table '{full_table_name}' - {column_count} columns: {column_names}")
        return schema
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Test the database connection.
        
        Returns:
            Tuple containing:
                - bool: True if connection test was successful, False otherwise
                - Optional[str]: Error message if connection test failed, None otherwise
        """
        self.logger.info("DB_CONNECTION_TEST: Starting database connection test")
        try:
            if not self.is_connected:
                self.logger.info("DB_CONNECTION_TEST: Connection not established, attempting to connect")
                self.connect()
            
            # Get connection info to verify connection is working
            connection_info = self.get_connection_info()
            db_type = connection_info.get('db_type', 'unknown')
            
            self.logger.info(f"DB_CONNECTION_TEST: Connection test successful for {db_type} database")
            return True, None
        except Exception as e:
            error_message = f"Connection test failed: {str(e)}"
            self.logger.error(f"DB_CONNECTION_TEST: {error_message}")
            return False, error_message
    
    def __enter__(self):
        """
        Context manager entry point - connect to the database.
        
        Returns:
            Self for use in with statement
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point - disconnect from the database.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        self.disconnect()
    
    def _get_cache_key(self, table_name: str, schema_name: str = None) -> str:
        """
        Generate a cache key for a table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            
        Returns:
            str: Cache key for the table
        """
        if schema_name:
            return f"{schema_name}.{table_name}"
        return table_name
    
    def _is_cache_valid(self, cache_entry: Dict[str, Any]) -> bool:
        """
        Check if a cache entry is still valid based on TTL.
        
        Args:
            cache_entry: Cache entry containing timestamp and data
            
        Returns:
            bool: True if cache entry is valid, False otherwise
        """
        if not self._cache_enabled:
            return False
        
        current_time = time.time()
        return (current_time - cache_entry['timestamp']) < self._cache_ttl
    
    def _cache_table_exists(self, table_name: str, exists: bool, schema_name: str = None) -> None:
        """
        Cache the result of a table existence check.
        
        Args:
            table_name: Name of the table
            exists: Whether the table exists
            schema_name: Schema name (optional)
        """
        if not self._cache_enabled:
            return
        
        cache_key = self._get_cache_key(table_name, schema_name)
        
        with self._schema_cache_lock:
            self._schema_cache[cache_key] = {
                'exists': exists,
                'timestamp': time.time(),
                'type': 'table_existence'
            }
            
        self.logger.debug(f"Cached table existence for {cache_key}: {exists}")
    
    def _get_cached_table_exists(self, table_name: str, schema_name: str = None) -> Optional[bool]:
        """
        Get cached table existence result.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            
        Returns:
            Optional[bool]: True/False if cached and valid, None if not cached or expired
        """
        if not self._cache_enabled:
            return None
        
        cache_key = self._get_cache_key(table_name, schema_name)
        
        with self._schema_cache_lock:
            if cache_key in self._schema_cache:
                cache_entry = self._schema_cache[cache_key]
                if (cache_entry.get('type') == 'table_existence' and 
                    self._is_cache_valid(cache_entry)):
                    self.logger.debug(f"Cache hit for table existence {cache_key}: {cache_entry['exists']}")
                    return cache_entry['exists']
                else:
                    # Remove expired entry
                    del self._schema_cache[cache_key]
                    self.logger.debug(f"Cache expired for table existence {cache_key}")
        
        return None
    
    def _cache_table_schema(self, table_name: str, schema: List[Dict[str, Any]], schema_name: str = None) -> None:
        """
        Cache the schema of a table.
        
        Args:
            table_name: Name of the table
            schema: Table schema information
            schema_name: Schema name (optional)
        """
        if not self._cache_enabled:
            return
        
        cache_key = self._get_cache_key(table_name, schema_name)
        
        with self._schema_cache_lock:
            self._schema_cache[cache_key + "_schema"] = {
                'schema': schema,
                'timestamp': time.time(),
                'type': 'table_schema'
            }
            
        self.logger.debug(f"Cached table schema for {cache_key}")
    
    def _get_cached_table_schema(self, table_name: str, schema_name: str = None) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached table schema.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            
        Returns:
            Optional[List[Dict[str, Any]]]: Cached schema if valid, None otherwise
        """
        if not self._cache_enabled:
            return None
        
        cache_key = self._get_cache_key(table_name, schema_name) + "_schema"
        
        with self._schema_cache_lock:
            if cache_key in self._schema_cache:
                cache_entry = self._schema_cache[cache_key]
                if (cache_entry.get('type') == 'table_schema' and 
                    self._is_cache_valid(cache_entry)):
                    self.logger.debug(f"Cache hit for table schema {cache_key}")
                    return cache_entry['schema']
                else:
                    # Remove expired entry
                    del self._schema_cache[cache_key]
                    self.logger.debug(f"Cache expired for table schema {cache_key}")
        
        return None
    
    def _invalidate_table_cache(self, table_name: str, schema_name: str = None) -> None:
        """
        Invalidate cached data for a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
        """
        if not self._cache_enabled:
            return
        
        cache_key = self._get_cache_key(table_name, schema_name)
        
        with self._schema_cache_lock:
            # Remove table existence cache
            if cache_key in self._schema_cache:
                del self._schema_cache[cache_key]
                self.logger.debug(f"Invalidated table existence cache for {cache_key}")
            
            # Remove table schema cache
            schema_key = cache_key + "_schema"
            if schema_key in self._schema_cache:
                del self._schema_cache[schema_key]
                self.logger.debug(f"Invalidated table schema cache for {cache_key}")
    
    def clear_schema_cache(self) -> None:
        """
        Clear all cached schema information.
        """
        with self._schema_cache_lock:
            cache_size = len(self._schema_cache)
            self._schema_cache.clear()
            self.logger.info(f"Cleared schema cache ({cache_size} entries)")
    
    def get_schema_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the schema cache.
        
        Returns:
            Dict containing cache statistics
        """
        with self._schema_cache_lock:
            total_entries = len(self._schema_cache)
            valid_entries = 0
            expired_entries = 0
            table_existence_entries = 0
            table_schema_entries = 0
            
            current_time = time.time()
            
            for cache_entry in self._schema_cache.values():
                if self._is_cache_valid(cache_entry):
                    valid_entries += 1
                else:
                    expired_entries += 1
                
                entry_type = cache_entry.get('type', 'unknown')
                if entry_type == 'table_existence':
                    table_existence_entries += 1
                elif entry_type == 'table_schema':
                    table_schema_entries += 1
            
            return {
                'enabled': self._cache_enabled,
                'cache_ttl_seconds': self._cache_ttl,
                'total_entries': total_entries,
                'valid_entries': valid_entries,
                'expired_entries': expired_entries,
                'table_existence_entries': table_existence_entries,
                'table_schema_entries': table_schema_entries
            }
    
    def cleanup_expired_cache(self) -> int:
        """
        Remove expired entries from the cache.
        
        Returns:
            int: Number of entries removed
        """
        if not self._cache_enabled:
            return 0
        
        removed_count = 0
        
        with self._schema_cache_lock:
            expired_keys = []
            
            for cache_key, cache_entry in self._schema_cache.items():
                if not self._is_cache_valid(cache_entry):
                    expired_keys.append(cache_key)
            
            for key in expired_keys:
                del self._schema_cache[key]
                removed_count += 1
            
            if removed_count > 0:
                self.logger.debug(f"Cleaned up {removed_count} expired cache entries")
        
        return removed_count