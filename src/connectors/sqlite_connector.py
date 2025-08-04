"""
SQLite Database Connector for the Generic Data Ingestor Framework.

This module provides a concrete implementation of the DatabaseConnector abstract base class
for SQLite databases, with file-based database handling and simplified connection logic
optimized for local development and testing usage.
"""

import sqlite3
from sqlite3 import Error as SQLiteError, IntegrityError, OperationalError, ProgrammingError
from typing import Any, Dict, List, Optional, Union, Tuple
import logging
import time
import os
from pathlib import Path
from contextlib import contextmanager
import threading

from .database_connector import DatabaseConnector


class SQLiteConnectionError(Exception):
    """Custom exception for SQLite connection errors."""
    pass


class SQLiteQueryError(Exception):
    """Custom exception for SQLite query execution errors."""
    pass


class SQLiteTransactionError(Exception):
    """Custom exception for SQLite transaction errors."""
    pass


class SQLiteConnector(DatabaseConnector):
    """
    SQLite database connector implementation.
    
    This class provides a concrete implementation of the DatabaseConnector abstract base class
    specifically for SQLite databases. It includes file-based database handling, simplified
    connection management, and comprehensive error handling optimized for local development
    and testing scenarios.
    
    Attributes:
        db_path: Path to the SQLite database file
        connection: The current active SQLite connection
        in_transaction: Boolean indicating if a transaction is currently active
        connection_lock: Thread lock for connection safety
        pragma_settings: Dictionary of SQLite PRAGMA settings
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the SQLite connector with connection parameters.
        
        Args:
            connection_params: Dictionary containing SQLite connection parameters
                Required: database (path to SQLite file)
                Optional: timeout (default: 30), check_same_thread (default: False),
                         pragma_settings, create_if_not_exists (default: True)
        """
        super().__init__(connection_params)
        self.connection = None
        self.in_transaction = False
        self.connection_lock = threading.RLock()
        
        # Validate SQLite-specific parameters
        self._validate_connection_params()
        
        # Set up database path and pragma settings
        self.db_path = self._resolve_database_path()
        self.pragma_settings = self._get_pragma_settings()
        
        self.logger.info(f"SQLite connector initialized for database: {self.db_path}")
    
    def _validate_connection_params(self) -> None:
        """
        Validate SQLite-specific connection parameters.
        
        Raises:
            ValueError: If required parameters are missing or invalid
        """
        if not self.connection_params.get('database'):
            raise ValueError("Missing required SQLite parameter: database (file path)")
        
        # Set default values
        if 'timeout' not in self.connection_params:
            self.connection_params['timeout'] = 30
        
        if 'check_same_thread' not in self.connection_params:
            self.connection_params['check_same_thread'] = False
        
        if 'create_if_not_exists' not in self.connection_params:
            self.connection_params['create_if_not_exists'] = True
        
        # Validate timeout
        timeout = self.connection_params['timeout']
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError(f"Invalid timeout value: {timeout}. Must be a positive number")
    
    def _resolve_database_path(self) -> str:
        """
        Resolve and validate the database file path.
        
        Returns:
            str: Absolute path to the SQLite database file
            
        Raises:
            ValueError: If database path is invalid
        """
        db_path = self.connection_params['database']
        
        # Handle special cases
        if db_path == ':memory:':
            return db_path
        
        # Convert to Path object for easier handling
        path = Path(db_path)
        
        # Make path absolute
        if not path.is_absolute():
            path = Path.cwd() / path
        
        # Create parent directories if they don't exist
        if self.connection_params.get('create_if_not_exists', True):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise ValueError(f"Cannot create directory for database: {e}")
        
        return str(path)
    
    def _get_pragma_settings(self) -> Dict[str, Any]:
        """
        Get PRAGMA settings for SQLite optimization.
        
        Returns:
            Dict containing PRAGMA settings
        """
        default_pragmas = {
            'journal_mode': 'WAL',  # Write-Ahead Logging for better concurrency
            'synchronous': 'NORMAL',  # Balance between safety and performance
            'foreign_keys': 'ON',  # Enable foreign key constraints
            'temp_store': 'MEMORY',  # Store temporary tables in memory
            'cache_size': '-64000',  # 64MB cache size (negative means KB)
            'mmap_size': '268435456',  # 256MB memory-mapped I/O
        }
        
        # Override with user-provided settings
        user_pragmas = self.connection_params.get('pragma_settings', {})
        default_pragmas.update(user_pragmas)
        
        return default_pragmas
    
    def connect(self) -> bool:
        """
        Establish a connection to the SQLite database.
        
        Returns:
            bool: True if connection was successful, False otherwise
        
        Raises:
            SQLiteConnectionError: If connection fails
        """
        if self.is_connected and self.connection:
            self.logger.info("Already connected to SQLite database")
            return True
        
        try:
            with self.connection_lock:
                self.logger.info(f"Connecting to SQLite database: {self.db_path}")
                
                # Check if database file exists (except for :memory:)
                if self.db_path != ':memory:':
                    if not os.path.exists(self.db_path):
                        if not self.connection_params.get('create_if_not_exists', True):
                            raise SQLiteConnectionError(f"Database file does not exist: {self.db_path}")
                        else:
                            self.logger.info(f"Creating new SQLite database: {self.db_path}")
                
                # Create connection
                self.connection = sqlite3.connect(
                    self.db_path,
                    timeout=self.connection_params['timeout'],
                    check_same_thread=self.connection_params['check_same_thread']
                )
                
                # Set row factory for dictionary-style results
                self.connection.row_factory = sqlite3.Row
                
                # Apply PRAGMA settings
                self._apply_pragma_settings()
                
                # Test the connection
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
                self.is_connected = True
                self.logger.info("Successfully connected to SQLite database")
                return True
                
        except SQLiteError as e:
            error_msg = f"Failed to connect to SQLite database: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during SQLite connection: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteConnectionError(error_msg)
    
    def _apply_pragma_settings(self) -> None:
        """Apply PRAGMA settings to optimize SQLite performance."""
        try:
            cursor = self.connection.cursor()
            for pragma, value in self.pragma_settings.items():
                pragma_sql = f"PRAGMA {pragma} = {value}"
                cursor.execute(pragma_sql)
                self.logger.debug(f"Applied PRAGMA: {pragma_sql}")
        except Exception as e:
            self.logger.warning(f"Failed to apply some PRAGMA settings: {str(e)}")
    
    def disconnect(self) -> bool:
        """
        Close the SQLite database connection.
        
        Returns:
            bool: True if disconnection was successful, False otherwise
        """
        try:
            with self.connection_lock:
                if self.in_transaction:
                    self.rollback_transaction()
                
                if self.connection:
                    self.connection.close()
                    self.connection = None
                
                self.is_connected = False
                self.logger.info("Successfully disconnected from SQLite database")
                return True
                
        except Exception as e:
            self.logger.error(f"Error during disconnection: {str(e)}")
            return False
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager to get the SQLite connection with thread safety.
        
        Yields:
            sqlite3.Connection: The SQLite connection object
        
        Raises:
            SQLiteConnectionError: If not connected
        """
        if not self.is_connected or not self.connection:
            raise SQLiteConnectionError("Not connected to database. Call connect() first.")
        
        with self.connection_lock:
            try:
                yield self.connection
            except Exception as e:
                # SQLite doesn't have separate connections, so we just re-raise
                raise
    
    def execute_query(self, query: str, params: Optional[Union[List, Dict]] = None) -> Any:
        """
        Execute a query on the SQLite database.
        
        Args:
            query: SQL query string to execute
            params: Parameters to bind to the query
            
        Returns:
            Query results for SELECT queries, row count for INSERT/UPDATE/DELETE
            
        Raises:
            SQLiteConnectionError: If not connected to the database
            SQLiteQueryError: If query execution fails
        """
        if not query or not query.strip():
            raise SQLiteQueryError("Query cannot be empty")
        
        query_type = query.strip().upper().split()[0]
        
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()
                
                self.logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
                
                start_time = time.time()
                cursor.execute(query, params or [])
                execution_time = time.time() - start_time
                
                if query_type in ['SELECT', 'PRAGMA']:
                    # Convert sqlite3.Row objects to dictionaries
                    results = [dict(row) for row in cursor.fetchall()]
                    self.logger.debug(f"Query executed successfully in {execution_time:.3f}s, returned {len(results)} rows")
                    return results
                else:
                    rowcount = cursor.rowcount
                    if not self.in_transaction:
                        connection.commit()
                    self.logger.debug(f"Query executed successfully in {execution_time:.3f}s, affected {rowcount} rows")
                    return rowcount
                    
        except ProgrammingError as e:
            error_msg = f"SQL syntax error: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
            
        except IntegrityError as e:
            error_msg = f"Database integrity error: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
            
        except OperationalError as e:
            error_msg = f"SQLite operational error: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
            
        except SQLiteError as e:
            error_msg = f"SQLite error during query execution: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
            
        except Exception as e:
            error_msg = f"Unexpected error during query execution: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def begin_transaction(self) -> bool:
        """
        Begin a database transaction.
        
        Returns:
            bool: True if transaction was successfully started, False otherwise
        
        Raises:
            SQLiteTransactionError: If transaction cannot be started
        """
        try:
            with self.connection_lock:
                if self.in_transaction:
                    self.logger.warning("Transaction already in progress")
                    return True
                
                self.execute_query("BEGIN TRANSACTION")
                self.in_transaction = True
                self.logger.debug("Transaction started successfully")
                return True
                
        except Exception as e:
            error_msg = f"Failed to start transaction: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteTransactionError(error_msg)
    
    def commit_transaction(self) -> bool:
        """
        Commit the current transaction.
        
        Returns:
            bool: True if transaction was successfully committed, False otherwise
        
        Raises:
            SQLiteTransactionError: If transaction cannot be committed
        """
        try:
            with self.connection_lock:
                if not self.in_transaction:
                    self.logger.warning("No transaction in progress")
                    return True
                
                self.connection.commit()
                self.in_transaction = False
                self.logger.debug("Transaction committed successfully")
                return True
                
        except Exception as e:
            error_msg = f"Failed to commit transaction: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteTransactionError(error_msg)
    
    def rollback_transaction(self) -> bool:
        """
        Rollback the current transaction.
        
        Returns:
            bool: True if transaction was successfully rolled back, False otherwise
        
        Raises:
            SQLiteTransactionError: If transaction cannot be rolled back
        """
        try:
            with self.connection_lock:
                if not self.in_transaction:
                    self.logger.warning("No transaction in progress")
                    return True
                
                self.connection.rollback()
                self.in_transaction = False
                self.logger.debug("Transaction rolled back successfully")
                return True
                
        except Exception as e:
            error_msg = f"Failed to rollback transaction: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteTransactionError(error_msg)
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current SQLite database connection.
        
        Returns:
            Dict containing connection information
        """
        info = {
            "db_type": "sqlite",
            "database_path": self.db_path,
            "is_connected": self.is_connected,
            "in_transaction": self.in_transaction,
            "timeout": self.connection_params['timeout'],
            "check_same_thread": self.connection_params['check_same_thread'],
            "pragma_settings": self.pragma_settings
        }
        
        if self.is_connected and self.connection:
            try:
                with self._get_connection() as connection:
                    cursor = connection.cursor()
                    
                    # Get SQLite version
                    cursor.execute("SELECT sqlite_version()")
                    version_result = cursor.fetchone()
                    info["sqlite_version"] = version_result[0] if version_result else "Unknown"
                    
                    # Get database file size (if not in-memory)
                    if self.db_path != ':memory:':
                        try:
                            file_size = os.path.getsize(self.db_path)
                            info["database_size_bytes"] = file_size
                            info["database_size_mb"] = round(file_size / (1024 * 1024), 2)
                        except OSError:
                            info["database_size_bytes"] = 0
                            info["database_size_mb"] = 0.0
                    else:
                        info["database_size_bytes"] = None
                        info["database_size_mb"] = None
                    
                    # Get current PRAGMA values
                    pragma_values = {}
                    for pragma in ['journal_mode', 'synchronous', 'foreign_keys', 'cache_size']:
                        try:
                            cursor.execute(f"PRAGMA {pragma}")
                            result = cursor.fetchone()
                            pragma_values[pragma] = result[0] if result else None
                        except:
                            pragma_values[pragma] = None
                    
                    info["current_pragma_values"] = pragma_values
                    
            except Exception as e:
                self.logger.warning(f"Could not retrieve extended connection info: {str(e)}")
        
        return info
    
    def execute_batch(self, query: str, params_list: List[Union[List, Dict]]) -> int:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string to execute
            params_list: List of parameter sets to execute the query with
            
        Returns:
            Total number of affected rows
            
        Raises:
            SQLiteQueryError: If batch execution fails
        """
        if not query or not query.strip():
            raise SQLiteQueryError("Query cannot be empty")
        
        if not params_list:
            raise SQLiteQueryError("Parameters list cannot be empty")
        
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()
                self.logger.debug(f"Executing batch query with {len(params_list)} parameter sets")
                
                start_time = time.time()
                cursor.executemany(query, params_list)
                execution_time = time.time() - start_time
                
                rowcount = cursor.rowcount
                if not self.in_transaction:
                    connection.commit()
                
                self.logger.debug(f"Batch query executed successfully in {execution_time:.3f}s, affected {rowcount} rows")
                return rowcount
                
        except Exception as e:
            error_msg = f"Batch execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def table_exists(self, table_name: str, schema_name: str = None) -> bool:
        """
        Check if a table exists in the SQLite database.
        
        Args:
            table_name: Name of the table to check
            schema_name: Schema name (not used in SQLite, included for compatibility)
            
        Returns:
            bool: True if table exists, False otherwise
            
        Raises:
            SQLiteQueryError: If existence check fails
        """
        try:
            query = """
                SELECT COUNT(*) as count 
                FROM sqlite_master 
                WHERE type='table' AND name=?
            """
            
            result = self.execute_query(query, [table_name])
            exists = result[0]['count'] > 0 if result else False
            
            self.logger.debug(f"Table existence check for {table_name}: {exists}")
            return exists
            
        except Exception as e:
            error_msg = f"Failed to check table existence for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def get_table_schema(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Get schema information for a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (not used in SQLite, included for compatibility)
            
        Returns:
            List of dictionaries containing column information
            
        Raises:
            SQLiteQueryError: If schema retrieval fails
        """
        try:
            query = "PRAGMA table_info(?)"
            result = self.execute_query(query, [table_name])
            
            # Convert SQLite PRAGMA output to standard format
            schema_info = []
            for row in result:
                column_info = {
                    'column_name': row['name'],
                    'data_type': row['type'],
                    'is_nullable': 'YES' if not row['notnull'] else 'NO',
                    'column_default': row['dflt_value'],
                    'is_primary_key': bool(row['pk']),
                    'ordinal_position': row['cid'] + 1  # SQLite uses 0-based indexing
                }
                schema_info.append(column_info)
            
            return schema_info
            
        except Exception as e:
            error_msg = f"Failed to get table schema for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def vacuum_database(self) -> bool:
        """
        Perform VACUUM operation to reclaim space and optimize database.
        
        Returns:
            bool: True if VACUUM was successful
            
        Raises:
            SQLiteQueryError: If VACUUM operation fails
        """
        try:
            self.logger.info("Starting VACUUM operation on SQLite database")
            start_time = time.time()
            
            self.execute_query("VACUUM")
            
            execution_time = time.time() - start_time
            self.logger.info(f"VACUUM operation completed successfully in {execution_time:.2f}s")
            return True
            
        except Exception as e:
            error_msg = f"VACUUM operation failed: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def analyze_database(self) -> bool:
        """
        Perform ANALYZE operation to update query planner statistics.
        
        Returns:
            bool: True if ANALYZE was successful
            
        Raises:
            SQLiteQueryError: If ANALYZE operation fails
        """
        try:
            self.logger.info("Starting ANALYZE operation on SQLite database")
            start_time = time.time()
            
            self.execute_query("ANALYZE")
            
            execution_time = time.time() - start_time
            self.logger.info(f"ANALYZE operation completed successfully in {execution_time:.2f}s")
            return True
            
        except Exception as e:
            error_msg = f"ANALYZE operation failed: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def get_database_size(self) -> Dict[str, Any]:
        """
        Get database size information.
        
        Returns:
            Dict containing size information
        """
        size_info = {
            "database_path": self.db_path,
            "is_memory_database": self.db_path == ':memory:'
        }
        
        if self.db_path == ':memory:':
            size_info.update({
                "file_size_bytes": None,
                "file_size_mb": None,
                "page_count": None,
                "page_size": None
            })
        else:
            try:
                # Get file size
                if os.path.exists(self.db_path):
                    file_size = os.path.getsize(self.db_path)
                    size_info.update({
                        "file_size_bytes": file_size,
                        "file_size_mb": round(file_size / (1024 * 1024), 2)
                    })
                else:
                    size_info.update({
                        "file_size_bytes": 0,
                        "file_size_mb": 0.0
                    })
                
                # Get SQLite internal size info
                if self.is_connected:
                    page_count_result = self.execute_query("PRAGMA page_count")
                    page_size_result = self.execute_query("PRAGMA page_size")
                    
                    page_count = page_count_result[0].get('page_count', 0) if page_count_result else 0
                    page_size = page_size_result[0].get('page_size', 0) if page_size_result else 0
                    
                    size_info.update({
                        "page_count": page_count,
                        "page_size": page_size,
                        "calculated_size_bytes": page_count * page_size
                    })
                
            except Exception as e:
                self.logger.warning(f"Could not retrieve database size info: {str(e)}")
                size_info.update({
                    "file_size_bytes": None,
                    "file_size_mb": None,
                    "page_count": None,
                    "page_size": None
                })
        
        return size_info
    
    def schema_to_ddl(self, table_name: str, schema: List[Dict[str, Any]], 
                      if_not_exists: bool = True) -> str:
        """
        Convert a schema definition to SQLite DDL (Data Definition Language).
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions with keys: name, type, nullable, default, etc.
            if_not_exists: Whether to include IF NOT EXISTS clause
            
        Returns:
            str: SQLite DDL CREATE TABLE statement
            
        Raises:
            ValueError: If schema is invalid or empty
        """
        if not schema or not isinstance(schema, list):
            raise ValueError("Schema must be a non-empty list")
        
        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be empty")
        
        # Start building DDL
        ddl_parts = ["CREATE TABLE"]
        
        if if_not_exists:
            ddl_parts.append("IF NOT EXISTS")
        
        ddl_parts.append(f'"{table_name}"')
        ddl_parts.append("(")
        
        # Process columns
        column_definitions = []
        primary_keys = []
        
        for i, column in enumerate(schema):
            if not isinstance(column, dict):
                raise ValueError(f"Column {i} must be a dictionary")
            
            if 'name' not in column:
                raise ValueError(f"Column {i} missing required 'name' field")
            
            if 'type' not in column:
                raise ValueError(f"Column {i} missing required 'type' field")
            
            col_name = column['name'].strip()
            if not col_name:
                raise ValueError(f"Column {i} name cannot be empty")
            
            # Build column definition
            col_def = [f'"{col_name}"']
            
            # Map data type
            sqlite_type = self._map_data_type(column['type'])
            col_def.append(sqlite_type)
            
            # Handle constraints
            if column.get('primary_key', False):
                col_def.append("PRIMARY KEY")
                primary_keys.append(col_name)
                
                # Handle auto-increment for integer primary keys
                if column.get('auto_increment', False) and sqlite_type.upper() == 'INTEGER':
                    col_def.append("AUTOINCREMENT")
            
            if not column.get('nullable', True) and not column.get('primary_key', False):
                col_def.append("NOT NULL")
            
            if column.get('unique', False) and not column.get('primary_key', False):
                col_def.append("UNIQUE")
            
            if 'default' in column and column['default'] is not None:
                default_value = self._format_default_value(column['default'], column['type'])
                col_def.append(f"DEFAULT {default_value}")
            
            column_definitions.append(" ".join(col_def))
        
        # Combine all parts
        ddl = " ".join(ddl_parts[:3]) + "\n(\n    " + ",\n    ".join(column_definitions) + "\n)"
        
        self.logger.debug(f"Generated DDL for table {table_name}: {ddl}")
        return ddl
    
    def _map_data_type(self, data_type: str) -> str:
        """
        Map generic data types to SQLite-specific types.
        
        Args:
            data_type: Generic data type string
            
        Returns:
            str: SQLite-specific data type
        """
        # Normalize input
        data_type = str(data_type).lower().strip()
        
        # SQLite type mappings (SQLite has flexible typing)
        type_mappings = {
            # String types
            'string': 'TEXT',
            'str': 'TEXT',
            'text': 'TEXT',
            'varchar': 'TEXT',
            'char': 'TEXT',
            
            # Numeric types (SQLite uses type affinity)
            'int': 'INTEGER',
            'integer': 'INTEGER',
            'bigint': 'INTEGER',
            'smallint': 'INTEGER',
            'tinyint': 'INTEGER',
            'float': 'REAL',
            'double': 'REAL',
            'decimal': 'REAL',
            'numeric': 'REAL',
            
            # Boolean type (stored as INTEGER in SQLite)
            'bool': 'INTEGER',
            'boolean': 'INTEGER',
            
            # Date/time types (stored as TEXT, REAL, or INTEGER in SQLite)
            'date': 'TEXT',
            'time': 'TEXT',
            'datetime': 'TEXT',
            'timestamp': 'TEXT',
            
            # Binary type
            'binary': 'BLOB',
            'blob': 'BLOB',
            
            # JSON (stored as TEXT in SQLite)
            'json': 'TEXT',
        }
        
        # Return mapped type or original if not found
        return type_mappings.get(data_type, 'TEXT')  # Default to TEXT for unknown types
    
    def _format_default_value(self, default_value: Any, data_type: str) -> str:
        """
        Format default value for SQLite DDL.
        
        Args:
            default_value: The default value
            data_type: The column data type
            
        Returns:
            str: Formatted default value for DDL
        """
        if default_value is None:
            return "NULL"
        
        data_type = str(data_type).lower()
        
        # Handle string/text types
        if data_type in ['string', 'str', 'text', 'varchar', 'char', 'date', 'time', 'datetime', 'timestamp']:
            # Escape single quotes and wrap in quotes
            escaped_value = str(default_value).replace("'", "''")
            return f"'{escaped_value}'"
        
        # Handle boolean types (stored as 0/1 in SQLite)
        if data_type in ['bool', 'boolean']:
            return '1' if default_value else '0'
        
        # Handle numeric types
        if data_type in ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'float', 'double', 'decimal', 'numeric']:
            return str(default_value)
        
        # Handle JSON type
        if data_type == 'json':
            if isinstance(default_value, (dict, list)):
                import json
                return f"'{json.dumps(default_value)}'"
            else:
                return f"'{default_value}'"
        
        # Default: treat as string
        return f"'{default_value}'"
    
    def create_table(self, table_name: str, schema: List[Dict[str, Any]], 
                     schema_name: str = None, if_not_exists: bool = True,
                     drop_if_exists: bool = False) -> bool:
        """
        Create a table in the SQLite database based on a schema definition.
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions
            schema_name: Schema name (not used in SQLite, included for compatibility)
            if_not_exists: Whether to use IF NOT EXISTS clause (ignored if drop_if_exists=True)
            drop_if_exists: Whether to drop the table if it already exists
            
        Returns:
            bool: True if table was created successfully, False otherwise
            
        Raises:
            SQLiteQueryError: If table creation fails
            ValueError: If schema is invalid
        """
        try:
            # Check if table exists
            table_exists = self.table_exists(table_name)
            
            if table_exists:
                if drop_if_exists:
                    self.logger.info(f"Dropping existing table {table_name}")
                    drop_ddl = f'DROP TABLE "{table_name}"'
                    self.execute_query(drop_ddl)
                elif not if_not_exists:
                    raise SQLiteQueryError(f"Table {table_name} already exists")
                else:
                    self.logger.info(f"Table {table_name} already exists, skipping creation")
                    return True
            
            # Generate DDL
            use_if_not_exists = if_not_exists and not drop_if_exists
            ddl = self.schema_to_ddl(table_name, schema, use_if_not_exists)
            
            # Execute DDL
            self.logger.info(f"Creating table {table_name}")
            self.execute_query(ddl)
            
            # Verify table was created
            if self.table_exists(table_name):
                self.logger.info(f"Successfully created table {table_name}")
                return True
            else:
                raise SQLiteQueryError(f"Table {table_name} was not created")
            
        except Exception as e:
            error_msg = f"Failed to create table {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def drop_table(self, table_name: str, schema_name: str = None, 
                   if_exists: bool = True, cascade: bool = False) -> bool:
        """
        Drop a table from the SQLite database.
        
        Args:
            table_name: Name of the table to drop
            schema_name: Schema name (not used in SQLite, included for compatibility)
            if_exists: Whether to use IF EXISTS clause
            cascade: Whether to use CASCADE option (not supported in SQLite)
            
        Returns:
            bool: True if table was dropped successfully, False otherwise
            
        Raises:
            SQLiteQueryError: If table drop fails
        """
        try:
            # Build DROP statement
            drop_parts = ["DROP TABLE"]
            
            if if_exists:
                drop_parts.append("IF EXISTS")
            
            drop_parts.append(f'"{table_name}"')
            
            # Note: SQLite doesn't support CASCADE
            if cascade:
                self.logger.warning("CASCADE option is not supported in SQLite")
            
            drop_ddl = " ".join(drop_parts)
            
            self.logger.info(f"Dropping table {table_name}")
            self.execute_query(drop_ddl)
            
            self.logger.info(f"Successfully dropped table {table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to drop table {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def get_table_list(self) -> List[str]:
        """
        Get a list of all tables in the SQLite database.
        
        Returns:
            List of table names
            
        Raises:
            SQLiteQueryError: If table list retrieval fails
        """
        try:
            query = """
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """
            
            result = self.execute_query(query)
            table_names = [row['name'] for row in result]
            
            self.logger.debug(f"Found {len(table_names)} tables: {table_names}")
            return table_names
            
        except Exception as e:
            error_msg = f"Failed to get table list: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)
    
    def backup_database(self, backup_path: str) -> bool:
        """
        Create a backup of the SQLite database.
        
        Args:
            backup_path: Path where the backup should be saved
            
        Returns:
            bool: True if backup was successful
            
        Raises:
            SQLiteQueryError: If backup operation fails
        """
        if self.db_path == ':memory:':
            raise SQLiteQueryError("Cannot backup in-memory database")
        
        try:
            self.logger.info(f"Creating backup of database to: {backup_path}")
            
            # Ensure backup directory exists
            backup_dir = os.path.dirname(backup_path)
            if backup_dir:
                os.makedirs(backup_dir, exist_ok=True)
            
            with self._get_connection() as source:
                # Create backup connection
                backup_conn = sqlite3.connect(backup_path)
                try:
                    # Perform backup using SQLite's backup API
                    source.backup(backup_conn)
                    self.logger.info(f"Successfully created backup: {backup_path}")
                    return True
                finally:
                    backup_conn.close()
                    
        except Exception as e:
            error_msg = f"Backup operation failed: {str(e)}"
            self.logger.error(error_msg)
            raise SQLiteQueryError(error_msg)