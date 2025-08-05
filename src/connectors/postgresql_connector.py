"""
PostgreSQL Database Connector for the Generic Data Ingestor Framework.

This module provides a concrete implementation of the DatabaseConnector abstract base class
for PostgreSQL databases, with comprehensive connection management and error handling.
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2 import sql, OperationalError, DatabaseError, IntegrityError, ProgrammingError
from typing import Any, Dict, List, Optional, Union, Tuple
import logging
import time
from contextlib import contextmanager

from .database_connector import DatabaseConnector


class PostgreSQLConnectionError(Exception):
    """Custom exception for PostgreSQL connection errors."""
    pass


class PostgreSQLQueryError(Exception):
    """Custom exception for PostgreSQL query execution errors."""
    pass


class PostgreSQLTransactionError(Exception):
    """Custom exception for PostgreSQL transaction errors."""
    pass


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL database connector implementation.
    
    This class provides a concrete implementation of the DatabaseConnector abstract base class
    specifically for PostgreSQL databases. It includes connection pooling, comprehensive error
    handling, transaction management, and SSL support.
    
    Attributes:
        connection_pool: psycopg2 connection pool for managing multiple connections
        current_connection: The current active connection from the pool
        in_transaction: Boolean indicating if a transaction is currently active
        max_retries: Maximum number of retry attempts for failed operations
        retry_delay: Delay in seconds between retry attempts
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the PostgreSQL connector with connection parameters.
        
        Args:
            connection_params: Dictionary containing PostgreSQL connection parameters
                Required: host, database, username
                Optional: port (default: 5432), password, schema, connection_timeout,
                         connection_pool_size, ssl_enabled, ssl_ca_cert, ssl_client_cert,
                         ssl_client_key, additional_options
        """
        super().__init__(connection_params)
        self.connection_pool = None
        self.current_connection = None
        self.in_transaction = False
        self.max_retries = connection_params.get('max_retries', 3)
        self.retry_delay = connection_params.get('retry_delay', 1)
        
        self.logger.info(f"PostgreSQL connector initialized for database: {self.connection_params.get('database')}")
    
    def _setup_connection_params(self) -> None:
        """
        Setup and validate PostgreSQL connection parameters.
        
        Raises:
            ValueError: If required parameters are missing or invalid
        """
        self._validate_connection_params()
    
    def _validate_connection_params(self) -> None:
        """
        Validate PostgreSQL-specific connection parameters.
        
        Raises:
            ValueError: If required parameters are missing or invalid
        """
        required_params = ['host', 'database', 'username']
        for param in required_params:
            if not self.connection_params.get(param):
                raise ValueError(f"Missing required PostgreSQL parameter: {param}")
        
        # Set default port if not provided
        if 'port' not in self.connection_params:
            self.connection_params['port'] = 5432
        
        # Validate port
        port = self.connection_params['port']
        if not isinstance(port, int) or port <= 0 or port > 65535:
            raise ValueError(f"Invalid port number: {port}. Must be between 1 and 65535")
        
        # Set default connection pool size
        if 'connection_pool_size' not in self.connection_params:
            self.connection_params['connection_pool_size'] = 5
        
        # Set default connection timeout
        if 'connection_timeout' not in self.connection_params:
            self.connection_params['connection_timeout'] = 30
    
    def _build_connection_config(self) -> Dict[str, Any]:
        """
        Build PostgreSQL connection configuration from parameters.
        
        Returns:
            Dict containing the finalized connection configuration
        """
        return {
            'connection_string': self._build_connection_string(),
            'pool_size': self.connection_params['connection_pool_size'],
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay
        }
    
    def _build_connection_string(self) -> str:
        """
        Build PostgreSQL connection string from parameters.
        
        Returns:
            PostgreSQL connection string
        """
        conn_params = []
        
        # Basic connection parameters
        conn_params.append(f"host={self.connection_params['host']}")
        conn_params.append(f"port={self.connection_params['port']}")
        conn_params.append(f"dbname={self.connection_params['database']}")
        conn_params.append(f"user={self.connection_params['username']}")
        
        if self.connection_params.get('password'):
            conn_params.append(f"password={self.connection_params['password']}")
        
        # Connection timeout
        conn_params.append(f"connect_timeout={self.connection_params['connection_timeout']}")
        
        # SSL configuration
        if self.connection_params.get('ssl_enabled', False):
            conn_params.append("sslmode=require")
            
            if self.connection_params.get('ssl_ca_cert'):
                conn_params.append(f"sslcert={self.connection_params['ssl_ca_cert']}")
            
            if self.connection_params.get('ssl_client_cert'):
                conn_params.append(f"sslcert={self.connection_params['ssl_client_cert']}")
            
            if self.connection_params.get('ssl_client_key'):
                conn_params.append(f"sslkey={self.connection_params['ssl_client_key']}")
        else:
            conn_params.append("sslmode=disable")
        
        # Additional options
        additional_options = self.connection_params.get('additional_options', {})
        for key, value in additional_options.items():
            conn_params.append(f"{key}={value}")
        
        return " ".join(conn_params)
    
    def _establish_connection(self) -> Any:
        """
        Establish the PostgreSQL database connection pool.
        
        Returns:
            psycopg2 connection pool
            
        Raises:
            PostgreSQLConnectionError: If connection establishment fails
        """
        config = self._build_connection_config()
        connection_string = config['connection_string']
        pool_size = config['pool_size']
        max_retries = config['max_retries']
        retry_delay = config['retry_delay']
        
        host = self.connection_params.get('host')
        port = self.connection_params.get('port')
        database = self.connection_params.get('database')
        username = self.connection_params.get('username')
        
        self.logger.info(f"DB_CONNECTION: Initiating PostgreSQL connection to {host}:{port}/{database} as {username} with pool size {pool_size}")
        
        last_error = None
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"DB_CONNECTION: Connection attempt {attempt + 1}/{max_retries} to PostgreSQL database")
                
                # Create connection pool
                connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=pool_size,
                    dsn=connection_string
                )
                
                # Test the connection by getting one from the pool
                test_conn = connection_pool.getconn()
                test_conn.close()
                connection_pool.putconn(test_conn)
                
                self.connection_pool = connection_pool
                self.logger.info(f"DB_CONNECTION: Successfully established PostgreSQL connection pool (size: {pool_size})")
                return connection_pool
                
            except OperationalError as e:
                last_error = e
                self.logger.warning(f"DB_CONNECTION: Connection attempt {attempt + 1} failed with operational error: {str(e)}")
                
                if attempt < max_retries - 1:
                    self.logger.debug(f"DB_CONNECTION: Waiting {retry_delay}s before retry")
                    time.sleep(retry_delay)
                
            except Exception as e:
                last_error = e
                self.logger.error(f"DB_CONNECTION: Unexpected error during connection attempt {attempt + 1}: {str(e)}")
                break
        
        error_msg = f"Failed to connect to PostgreSQL database after {max_retries} attempts: {str(last_error)}"
        self.logger.error(f"DB_CONNECTION: {error_msg}")
        raise PostgreSQLConnectionError(error_msg)
    
    # Connection method is now handled by the base class
    
    def _cleanup_connection(self) -> None:
        """
        Perform PostgreSQL-specific cleanup operations.
        """
        try:
            if self.current_connection:
                if self.in_transaction:
                    self.rollback_transaction()
                self.connection_pool.putconn(self.current_connection)
                self.current_connection = None
        except Exception as e:
            self.logger.warning(f"Error during connection cleanup: {str(e)}")
    
    def _close_connection(self) -> None:
        """
        Close the PostgreSQL connection pool.
        
        Raises:
            Exception: If connection closure fails
        """
        if self.connection_pool:
            self.connection_pool.closeall()
            self.connection_pool = None
    
    # Disconnect method is now handled by the base class
    
    def _prepare_query(self, query: str, params: Optional[Union[List, Dict]] = None) -> Tuple[str, Any]:
        """
        Prepare and validate a PostgreSQL query before execution.
        
        Args:
            query: SQL query string to prepare
            params: Parameters to bind to the query
            
        Returns:
            Tuple of (prepared_query, prepared_params)
            
        Raises:
            PostgreSQLQueryError: If query or parameters are invalid
        """
        if not query or not query.strip():
            raise PostgreSQLQueryError("Query cannot be empty")
        
        # PostgreSQL uses %s placeholders - no special preparation needed
        return query, params
    
    def _execute_prepared_query(self, prepared_query: str, prepared_params: Any) -> Any:
        """
        Execute a prepared query on the PostgreSQL connection.
        
        Args:
            prepared_query: The prepared SQL query
            prepared_params: The prepared parameters
            
        Returns:
            Raw query results from psycopg2
            
        Raises:
            PostgreSQLQueryError: If query execution fails
        """
        try:
            with self._get_connection() as connection:
                with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    query_preview = prepared_query[:100] + ('...' if len(prepared_query) > 100 else '')
                    param_count = len(prepared_params) if prepared_params else 0
                    query_type = prepared_query.strip().upper().split()[0]
                    
                    self.logger.debug(f"DB_QUERY_EXECUTE: Executing {query_type} query with {param_count} parameters: {query_preview}")
                    
                    start_time = time.time()
                    cursor.execute(prepared_query, prepared_params)
                    execution_time = time.time() - start_time
                    
                    # Return raw results with metadata
                    return {
                        'cursor': cursor,
                        'query_type': query_type,
                        'execution_time': execution_time,
                        'connection': connection
                    }
                    
        except (ProgrammingError, IntegrityError, DatabaseError) as e:
            query_type = prepared_query.strip().upper().split()[0] if prepared_query.strip() else "UNKNOWN"
            error_msg = f"Database error during {query_type} query execution: {str(e)}"
            self.logger.error(f"DB_QUERY_EXECUTE: {error_msg}")
            raise PostgreSQLQueryError(error_msg)
            
        except Exception as e:
            query_type = prepared_query.strip().upper().split()[0] if prepared_query.strip() else "UNKNOWN"
            error_msg = f"Unexpected error during {query_type} query execution: {str(e)}"
            self.logger.error(f"DB_QUERY_EXECUTE: {error_msg}")
            raise PostgreSQLQueryError(error_msg)
    
    def _process_query_results(self, raw_results: Any, query_type: str) -> Any:
        """
        Process raw PostgreSQL query results into the expected format.
        
        Args:
            raw_results: Raw results from _execute_prepared_query
            query_type: Type of query (SELECT, INSERT, UPDATE, etc.)
            
        Returns:
            Processed query results
        """
        cursor = raw_results['cursor']
        execution_time = raw_results['execution_time']
        connection = raw_results['connection']
        
        if query_type in ['SELECT', 'WITH']:
            results = cursor.fetchall()
            result_count = len(results)
            self.logger.info(f"DB_QUERY_EXECUTE: {query_type} query completed in {execution_time:.3f}s, returned {result_count} rows")
            return [dict(row) for row in results]
        else:
            rowcount = cursor.rowcount
            connection.commit()
            self.logger.info(f"DB_QUERY_EXECUTE: {query_type} query completed in {execution_time:.3f}s, affected {rowcount} rows")
            return rowcount
    
    # Execute query method is now handled by the base class
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager to get a connection from the pool.
        
        Yields:
            psycopg2 connection object
        
        Raises:
            PostgreSQLConnectionError: If unable to get connection from pool
        """
        if not self.is_connected or not self.connection_pool:
            raise PostgreSQLConnectionError("Not connected to database. Call connect() first.")
        
        connection = None
        try:
            connection = self.connection_pool.getconn()
            if connection is None:
                raise PostgreSQLConnectionError("Unable to get connection from pool")
            
            yield connection
            
        except Exception as e:
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            raise
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    # Execute query method is now handled by the base class
    
    def begin_transaction(self) -> bool:
        """
        Begin a database transaction.
        
        Returns:
            bool: True if transaction was successfully started, False otherwise
        
        Raises:
            PostgreSQLTransactionError: If transaction cannot be started
        """
        try:
            if self.in_transaction:
                self.logger.warning("Transaction already in progress")
                return True
            
            if not self.current_connection:
                self.current_connection = self.connection_pool.getconn()
            
            self.current_connection.autocommit = False
            self.in_transaction = True
            self.logger.debug("Transaction started successfully")
            return True
            
        except Exception as e:
            error_msg = f"Failed to start transaction: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLTransactionError(error_msg)
    
    def commit_transaction(self) -> bool:
        """
        Commit the current transaction.
        
        Returns:
            bool: True if transaction was successfully committed, False otherwise
        
        Raises:
            PostgreSQLTransactionError: If transaction cannot be committed
        """
        try:
            if not self.in_transaction:
                self.logger.warning("No transaction in progress")
                return True
            
            if self.current_connection:
                self.current_connection.commit()
                self.current_connection.autocommit = True
                self.connection_pool.putconn(self.current_connection)
                self.current_connection = None
            
            self.in_transaction = False
            self.logger.debug("Transaction committed successfully")
            return True
            
        except Exception as e:
            error_msg = f"Failed to commit transaction: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLTransactionError(error_msg)
    
    def rollback_transaction(self) -> bool:
        """
        Rollback the current transaction.
        
        Returns:
            bool: True if transaction was successfully rolled back, False otherwise
        
        Raises:
            PostgreSQLTransactionError: If transaction cannot be rolled back
        """
        try:
            if not self.in_transaction:
                self.logger.warning("No transaction in progress")
                return True
            
            if self.current_connection:
                self.current_connection.rollback()
                self.current_connection.autocommit = True
                self.connection_pool.putconn(self.current_connection)
                self.current_connection = None
            
            self.in_transaction = False
            self.logger.debug("Transaction rolled back successfully")
            return True
            
        except Exception as e:
            error_msg = f"Failed to rollback transaction: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLTransactionError(error_msg)
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current PostgreSQL database connection.
        
        Returns:
            Dict containing connection information such as database type,
            server version, connection status, etc.
        """
        info = {
            "db_type": "postgresql",
            "host": self.connection_params['host'],
            "port": self.connection_params['port'],
            "database": self.connection_params['database'],
            "username": self.connection_params['username'],
            "is_connected": self.is_connected,
            "connection_pool_size": self.connection_params['connection_pool_size'],
            "ssl_enabled": self.connection_params.get('ssl_enabled', False),
            "in_transaction": self.in_transaction
        }
        
        if self.is_connected and self.connection_pool:
            try:
                with self._get_connection() as connection:
                    # Get PostgreSQL server version
                    info["server_version"] = connection.get_parameter_status("server_version")
                    info["server_encoding"] = connection.get_parameter_status("server_encoding")
                    info["client_encoding"] = connection.get_parameter_status("client_encoding")
                    
                    # Get current schema
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT current_schema()")
                        info["current_schema"] = cursor.fetchone()[0]
                        
                        # Get connection count
                        cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = %s", 
                                     (self.connection_params['database'],))
                        info["active_connections"] = cursor.fetchone()[0]
                        
            except Exception as e:
                self.logger.warning(f"Could not retrieve extended connection info: {str(e)}")
        
        return info
    
    def execute_batch(self, query: str, params_list: List[Union[List, Dict]]) -> int:
        """
        Execute a query multiple times with different parameters for better performance.
        
        Args:
            query: SQL query string to execute
            params_list: List of parameter sets to execute the query with
            
        Returns:
            Total number of affected rows
            
        Raises:
            PostgreSQLQueryError: If batch execution fails
        """
        if not query or not query.strip():
            raise PostgreSQLQueryError("Query cannot be empty")
        
        if not params_list:
            raise PostgreSQLQueryError("Parameters list cannot be empty")
        
        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    self.logger.debug(f"Executing batch query with {len(params_list)} parameter sets")
                    
                    start_time = time.time()
                    psycopg2.extras.execute_batch(cursor, query, params_list)
                    execution_time = time.time() - start_time
                    
                    rowcount = cursor.rowcount
                    connection.commit()
                    
                    self.logger.debug(f"Batch query executed successfully in {execution_time:.3f}s, affected {rowcount} rows")
                    return rowcount
                    
        except Exception as e:
            error_msg = f"Batch execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def _get_table_schema_impl(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Get schema information for a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to current schema)
            
        Returns:
            List of dictionaries containing column information
            
        Raises:
            PostgreSQLQueryError: If schema retrieval fails
        """
        try:
            query = """
                SELECT column_name, data_type, is_nullable, column_default,
                       character_maximum_length, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_name = %s
            """
            params = [table_name]
            
            if schema_name:
                query += " AND table_schema = %s"
                params.append(schema_name)
            else:
                query += " AND table_schema = current_schema()"
            
            query += " ORDER BY ordinal_position"
            
            return self.execute_query(query, params)
            
        except Exception as e:
            error_msg = f"Failed to get table schema for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def _table_exists_impl(self, table_name: str, schema_name: str = None) -> bool:
        """
        Check if a table exists in the PostgreSQL database.
        
        Args:
            table_name: Name of the table to check
            schema_name: Schema name (defaults to current schema)
            
        Returns:
            bool: True if table exists, False otherwise
            
        Raises:
            PostgreSQLQueryError: If existence check fails
        """
        try:
            query = """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s
            """
            params = [table_name]
            
            if schema_name:
                query += " AND table_schema = %s"
                params.append(schema_name)
            else:
                query += " AND table_schema = current_schema()"
            
            query += ")"
            
            result = self.execute_query(query, params)
            exists = result[0]['exists'] if result else False
            
            self.logger.debug(f"Table existence check for {table_name}: {exists}")
            return exists
            
        except Exception as e:
            error_msg = f"Failed to check table existence for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def schema_to_ddl(self, table_name: str, schema: List[Dict[str, Any]], 
                      schema_name: str = None, if_not_exists: bool = True) -> str:
        """
        Convert a schema definition to PostgreSQL DDL (Data Definition Language).
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions with keys: name, type, nullable, default, etc.
            schema_name: Schema name to create table in (optional)
            if_not_exists: Whether to include IF NOT EXISTS clause
            
        Returns:
            str: PostgreSQL DDL CREATE TABLE statement
            
        Raises:
            ValueError: If schema is invalid or empty
        """
        if not schema or not isinstance(schema, list):
            raise ValueError("Schema must be a non-empty list")
        
        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be empty")
        
        # Build table name with schema if provided
        full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
        
        # Start building DDL
        ddl_parts = ["CREATE TABLE"]
        
        if if_not_exists:
            ddl_parts.append("IF NOT EXISTS")
        
        ddl_parts.append(full_table_name)
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
            pg_type = self._map_data_type(column['type'])
            col_def.append(pg_type)
            
            # Handle constraints
            if not column.get('nullable', True):
                col_def.append("NOT NULL")
            
            if 'default' in column and column['default'] is not None:
                default_value = self._format_default_value(column['default'], column['type'])
                col_def.append(f"DEFAULT {default_value}")
            
            if column.get('unique', False):
                col_def.append("UNIQUE")
            
            if column.get('primary_key', False):
                primary_keys.append(col_name)
            
            column_definitions.append(" ".join(col_def))
        
        # Add primary key constraint if specified
        if primary_keys:
            pk_cols = ', '.join(f'"{pk}"' for pk in primary_keys)
            column_definitions.append(f"PRIMARY KEY ({pk_cols})")
        
        # Combine all parts
        ddl_parts.append(",\n    ".join(column_definitions))
        ddl_parts.append(")")
        
        ddl = " ".join(ddl_parts[:3]) + "\n(\n    " + ",\n    ".join(column_definitions) + "\n)"
        
        self.logger.debug(f"Generated DDL for table {table_name}: {ddl}")
        return ddl
    
    def _map_data_type(self, data_type: str) -> str:
        """
        Map generic data types to PostgreSQL-specific types.
        
        Args:
            data_type: Generic data type string
            
        Returns:
            str: PostgreSQL-specific data type
        """
        # Normalize input
        data_type = str(data_type).lower().strip()
        
        # PostgreSQL type mappings
        type_mappings = {
            # String types
            'string': 'TEXT',
            'str': 'TEXT',
            'text': 'TEXT',
            'varchar': 'VARCHAR',
            'char': 'CHAR',
            
            # Numeric types
            'int': 'INTEGER',
            'integer': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            'decimal': 'DECIMAL',
            'numeric': 'NUMERIC',
            'money': 'MONEY',
            
            # Boolean type
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            
            # Date/time types
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'timestamptz': 'TIMESTAMPTZ',
            
            # JSON types
            'json': 'JSON',
            'jsonb': 'JSONB',
            
            # Binary types
            'binary': 'BYTEA',
            'blob': 'BYTEA',
            
            # UUID type
            'uuid': 'UUID',
            
            # Array types
            'array': 'TEXT[]',
        }
        
        # Handle parameterized types (e.g., VARCHAR(255))
        if '(' in data_type:
            base_type = data_type.split('(')[0]
            if base_type in type_mappings:
                # For parameterized types, preserve the parameters
                return data_type.upper()
        
        # Return mapped type or original if not found
        return type_mappings.get(data_type, data_type.upper())
    
    def _format_default_value(self, default_value: Any, data_type: str) -> str:
        """
        Format default value for PostgreSQL DDL.
        
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
        if data_type in ['string', 'str', 'text', 'varchar', 'char']:
            # Escape single quotes and wrap in quotes
            escaped_value = str(default_value).replace("'", "''")
            return f"'{escaped_value}'"
        
        # Handle boolean types
        if data_type in ['bool', 'boolean']:
            return 'TRUE' if default_value else 'FALSE'
        
        # Handle numeric types
        if data_type in ['int', 'integer', 'bigint', 'smallint', 'float', 'double', 'decimal', 'numeric']:
            return str(default_value)
        
        # Handle date/time types
        if data_type in ['date', 'time', 'datetime', 'timestamp']:
            if str(default_value).upper() in ['NOW()', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME']:
                return str(default_value).upper()
            else:
                return f"'{default_value}'"
        
        # Handle JSON types
        if data_type in ['json', 'jsonb']:
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
        Create a table in the PostgreSQL database based on a schema definition.
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions
            schema_name: Schema name to create table in (optional)
            if_not_exists: Whether to use IF NOT EXISTS clause (ignored if drop_if_exists=True)
            drop_if_exists: Whether to drop the table if it already exists
            
        Returns:
            bool: True if table was created successfully, False otherwise
            
        Raises:
            PostgreSQLQueryError: If table creation fails
            ValueError: If schema is invalid
        """
        try:
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            column_count = len(schema)
            column_names = [col.get('name', 'unknown') for col in schema]
            
            self.logger.info(f"DB_SCHEMA_CREATE: Starting table creation for '{full_table_name}' with {column_count} columns, drop_if_exists={drop_if_exists}, if_not_exists={if_not_exists}")
            self.logger.debug(f"DB_SCHEMA_CREATE: Table columns: {column_names}")
            
            # Check if table exists
            table_exists = self._table_exists_impl(table_name, schema_name)
            
            if table_exists:
                if drop_if_exists:
                    self.logger.info(f"DB_SCHEMA_CREATE: Dropping existing table {full_table_name}")
                    drop_ddl = f"DROP TABLE {full_table_name}"
                    self.execute_query(drop_ddl)
                    self.logger.info(f"DB_SCHEMA_CREATE: Existing table {full_table_name} dropped successfully")
                elif not if_not_exists:
                    raise PostgreSQLQueryError(f"Table {full_table_name} already exists")
                else:
                    self.logger.info(f"DB_SCHEMA_CREATE: Table {full_table_name} already exists, skipping creation")
                    return True
            
            # Generate DDL
            use_if_not_exists = if_not_exists and not drop_if_exists
            ddl = self.schema_to_ddl(table_name, schema, schema_name, use_if_not_exists)
            
            # Execute DDL
            self.logger.info(f"DB_SCHEMA_CREATE: Executing CREATE TABLE DDL for {full_table_name}")
            start_time = time.time()
            self.execute_query(ddl)
            creation_time = time.time() - start_time
            
            # Invalidate cache since table structure may have changed
            self._invalidate_table_cache(table_name, schema_name)
            
            # Verify table was created
            if self._table_exists_impl(table_name, schema_name):
                self.logger.info(f"DB_SCHEMA_CREATE: Successfully created table {full_table_name} in {creation_time:.3f}s")
                # Cache the fact that table now exists
                self._cache_table_exists(table_name, True, schema_name)
                return True
            else:
                raise PostgreSQLQueryError(f"Table {full_table_name} was not created")
            
        except Exception as e:
            error_msg = f"Failed to create table {table_name}: {str(e)}"
            self.logger.error(f"DB_SCHEMA_CREATE: {error_msg}")
            raise PostgreSQLQueryError(error_msg)
    
    def drop_table(self, table_name: str, schema_name: str = None, 
                   if_exists: bool = True, cascade: bool = False) -> bool:
        """
        Drop a table from the PostgreSQL database.
        
        Args:
            table_name: Name of the table to drop
            schema_name: Schema name (optional)
            if_exists: Whether to use IF EXISTS clause
            cascade: Whether to use CASCADE option
            
        Returns:
            bool: True if table was dropped successfully, False otherwise
            
        Raises:
            PostgreSQLQueryError: If table drop fails
        """
        try:
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            
            # Build DROP statement
            drop_parts = ["DROP TABLE"]
            
            if if_exists:
                drop_parts.append("IF EXISTS")
            
            drop_parts.append(full_table_name)
            
            if cascade:
                drop_parts.append("CASCADE")
            
            drop_ddl = " ".join(drop_parts)
            
            self.logger.info(f"Dropping table {full_table_name}")
            self.execute_query(drop_ddl)
            
            # Invalidate cache since table no longer exists
            self._invalidate_table_cache(table_name, schema_name)
            
            self.logger.info(f"Successfully dropped table {full_table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to drop table {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]], 
                    schema_name: str = None, batch_size: int = 1000,
                    on_conflict: str = 'error') -> int:
        """
        Insert data into a PostgreSQL table using batch operations.
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the data to insert
            schema_name: Schema name (optional)
            batch_size: Number of rows to insert per batch
            on_conflict: How to handle conflicts ('error', 'ignore', 'update')
            
        Returns:
            int: Number of rows successfully inserted
            
        Raises:
            PostgreSQLQueryError: If data insertion fails
            ValueError: If data is invalid or empty
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        if not isinstance(data, list):
            raise ValueError("Data must be a list of dictionaries")
        
        if batch_size <= 0:
            raise ValueError("Batch size must be positive")
        
        if on_conflict not in ['error', 'ignore', 'update']:
            raise ValueError("on_conflict must be 'error', 'ignore', or 'update'")
        
        try:
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            data_count = len(data)
            
            self.logger.info(f"DB_INSERT: Starting insert operation for table '{full_table_name}' with {data_count} rows, batch_size={batch_size}, on_conflict={on_conflict}")
            
            # Validate table exists
            if not self.table_exists(table_name, schema_name):
                raise PostgreSQLQueryError(f"Table {full_table_name} does not exist")
            
            # Get table schema for validation
            table_schema = self.get_table_schema(table_name, schema_name)
            column_info = {col['column_name']: col for col in table_schema}
            column_names = list(column_info.keys())
            
            self.logger.debug(f"DB_INSERT: Target table schema has {len(column_names)} columns: {column_names}")
            
            # Validate and preprocess data
            start_preprocessing = time.time()
            processed_data = self._preprocess_data(data, column_info)
            preprocessing_time = time.time() - start_preprocessing
            
            self.logger.debug(f"DB_INSERT: Data preprocessing completed in {preprocessing_time:.3f}s")
            
            total_inserted = 0
            total_batches = (len(processed_data) + batch_size - 1) // batch_size
            
            self.logger.info(f"DB_INSERT: Processing {len(processed_data)} rows into {full_table_name} in {total_batches} batches")
            
            # Process data in batches
            start_insert_time = time.time()
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(processed_data))
                batch_data = processed_data[start_idx:end_idx]
                
                try:
                    batch_start_time = time.time()
                    rows_inserted = self._insert_batch(
                        full_table_name, batch_data, column_info, on_conflict
                    )
                    batch_time = time.time() - batch_start_time
                    total_inserted += rows_inserted
                    
                    self.logger.debug(f"DB_INSERT: Batch {batch_num + 1}/{total_batches} completed in {batch_time:.3f}s - inserted {rows_inserted} rows")
                    
                except Exception as e:
                    if on_conflict == 'error':
                        self.logger.error(f"DB_INSERT: Batch {batch_num + 1} failed with error strategy - {str(e)}")
                        raise
                    else:
                        self.logger.warning(f"DB_INSERT: Batch {batch_num + 1} failed with {on_conflict} strategy, continuing - {str(e)}")
                        continue
            
            total_insert_time = time.time() - start_insert_time
            rows_per_second = total_inserted / total_insert_time if total_insert_time > 0 else 0
            
            self.logger.info(f"DB_INSERT: Successfully inserted {total_inserted} rows into {full_table_name} in {total_insert_time:.3f}s ({rows_per_second:.1f} rows/sec)")
            return total_inserted
            
        except Exception as e:
            error_msg = f"Failed to insert data into {table_name}: {str(e)}"
            self.logger.error(f"DB_INSERT: {error_msg}")
            raise PostgreSQLQueryError(error_msg)
    
    def _preprocess_data(self, data: List[Dict[str, Any]], 
                        column_info: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Preprocess and validate data before insertion.
        
        Args:
            data: List of data dictionaries
            column_info: Column information from table schema
            
        Returns:
            List of preprocessed data dictionaries
            
        Raises:
            ValueError: If data validation fails
        """
        processed_data = []
        
        for row_idx, row in enumerate(data):
            if not isinstance(row, dict):
                raise ValueError(f"Row {row_idx} must be a dictionary")
            
            processed_row = {}
            
            # Process each column in the row
            for col_name, col_value in row.items():
                if col_name not in column_info:
                    self.logger.warning(f"Column '{col_name}' not found in table schema, skipping")
                    continue
                
                col_def = column_info[col_name]
                
                try:
                    # Validate and convert data type
                    processed_value = self._validate_and_convert_value(
                        col_value, col_def, col_name, row_idx
                    )
                    processed_row[col_name] = processed_value
                    
                except Exception as e:
                    error_msg = f"Data validation failed for row {row_idx}, column '{col_name}': {str(e)}"
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
            
            # Check for required columns
            for col_name, col_def in column_info.items():
                if (col_def['is_nullable'] == 'NO' and 
                    col_def['column_default'] is None and 
                    col_name not in processed_row):
                    raise ValueError(f"Required column '{col_name}' missing in row {row_idx}")
            
            processed_data.append(processed_row)
        
        return processed_data
    
    def _validate_and_convert_value(self, value: Any, column_def: Dict[str, Any], 
                                   col_name: str, row_idx: int) -> Any:
        """
        Validate and convert a value according to column definition.
        
        Args:
            value: The value to validate and convert
            column_def: Column definition from table schema
            col_name: Column name for error reporting
            row_idx: Row index for error reporting
            
        Returns:
            Converted and validated value
            
        Raises:
            ValueError: If validation fails
        """
        # Handle NULL values
        if value is None:
            if column_def['is_nullable'] == 'NO' and column_def['column_default'] is None:
                raise ValueError(f"NULL value not allowed for non-nullable column '{col_name}'")
            return None
        
        data_type = column_def['data_type'].lower()
        
        try:
            # String types
            if data_type in ['text', 'character varying', 'varchar', 'character', 'char']:
                converted_value = str(value)
                max_length = column_def.get('character_maximum_length')
                if max_length and len(converted_value) > max_length:
                    raise ValueError(f"String too long (max {max_length} characters)")
                return converted_value
            
            # Integer types
            elif data_type in ['integer', 'bigint', 'smallint']:
                if isinstance(value, bool):
                    raise ValueError("Boolean cannot be converted to integer")
                return int(value)
            
            # Numeric types
            elif data_type in ['numeric', 'decimal']:
                return float(value)
            
            # Float types
            elif data_type in ['real', 'double precision']:
                return float(value)
            
            # Boolean type
            elif data_type == 'boolean':
                if isinstance(value, str):
                    if value.lower() in ['true', 't', '1', 'yes', 'y', 'on']:
                        return True
                    elif value.lower() in ['false', 'f', '0', 'no', 'n', 'off']:
                        return False
                    else:
                        raise ValueError(f"Invalid boolean value: {value}")
                return bool(value)
            
            # Date and time types
            elif data_type in ['date', 'timestamp', 'timestamp without time zone', 
                             'timestamp with time zone', 'time', 'time without time zone']:
                if isinstance(value, str):
                    # Let PostgreSQL handle date/time parsing
                    return value
                else:
                    return str(value)
            
            # JSON types
            elif data_type in ['json', 'jsonb']:
                if isinstance(value, (dict, list)):
                    import json
                    return json.dumps(value)
                elif isinstance(value, str):
                    # Validate JSON string
                    import json
                    json.loads(value)  # This will raise exception if invalid
                    return value
                else:
                    raise ValueError(f"Invalid JSON value type: {type(value)}")
            
            # UUID type
            elif data_type == 'uuid':
                import uuid
                if isinstance(value, str):
                    # Validate UUID format
                    uuid.UUID(value)
                    return value
                else:
                    return str(value)
            
            # Array types
            elif data_type.endswith('[]'):
                if not isinstance(value, list):
                    raise ValueError(f"Array column requires list value")
                return value
            
            # Binary types
            elif data_type == 'bytea':
                if isinstance(value, bytes):
                    return value
                elif isinstance(value, str):
                    return value.encode('utf-8')
                else:
                    raise ValueError(f"Invalid binary value type: {type(value)}")
            
            # Default: return as string
            else:
                return str(value)
                
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert value '{value}' to {data_type}: {str(e)}")
    
    def _insert_batch(self, table_name: str, batch_data: List[Dict[str, Any]], 
                     column_info: Dict[str, Dict[str, Any]], on_conflict: str) -> int:
        """
        Insert a batch of data into the table.
        
        Args:
            table_name: Full table name (with schema if applicable)
            batch_data: List of data dictionaries for this batch
            column_info: Column information from table schema
            on_conflict: How to handle conflicts
            
        Returns:
            int: Number of rows inserted
            
        Raises:
            PostgreSQLQueryError: If batch insertion fails
        """
        if not batch_data:
            return 0
        
        try:
            # Get all unique column names from the batch
            all_columns = set()
            for row in batch_data:
                all_columns.update(row.keys())
            
            # Sort columns for consistent ordering
            columns = sorted(all_columns)
            
            # Build INSERT statement
            column_list = ', '.join(f'"{col}"' for col in columns)
            placeholders = ', '.join(['%s'] * len(columns))
            
            base_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
            
            # Add conflict handling
            if on_conflict == 'ignore':
                query = f"{base_query} ON CONFLICT DO NOTHING"
            elif on_conflict == 'update':
                # Simple UPDATE SET strategy - update all non-key columns
                update_columns = [col for col in columns if not column_info[col].get('is_primary_key', False)]
                if update_columns:
                    update_set = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)
                    query = f"{base_query} ON CONFLICT DO UPDATE SET {update_set}"
                else:
                    query = f"{base_query} ON CONFLICT DO NOTHING"
            else:
                query = base_query
            
            # Prepare batch parameters
            batch_params = []
            for row in batch_data:
                row_params = []
                for col in columns:
                    row_params.append(row.get(col))
                batch_params.append(row_params)
            
            # Execute batch insert
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_batch(cursor, query, batch_params)
                    rows_affected = cursor.rowcount
                    connection.commit()
                    
                    return rows_affected
                    
        except Exception as e:
            error_msg = f"Batch insertion failed: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def upsert_data(self, table_name: str, data: List[Dict[str, Any]], 
                    conflict_columns: List[str], schema_name: str = None,
                    batch_size: int = 1000) -> int:
        """
        Insert or update data using PostgreSQL's ON CONFLICT functionality.
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the data to upsert
            conflict_columns: List of column names that define uniqueness
            schema_name: Schema name (optional)
            batch_size: Number of rows to process per batch
            
        Returns:
            int: Number of rows processed (inserted or updated)
            
        Raises:
            PostgreSQLQueryError: If upsert operation fails
            ValueError: If parameters are invalid
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        if not conflict_columns:
            raise ValueError("Conflict columns must be specified")
        
        try:
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            data_count = len(data)
            
            self.logger.info(f"DB_UPSERT: Starting upsert operation for table '{full_table_name}' with {data_count} rows, batch_size={batch_size}")
            self.logger.info(f"DB_UPSERT: Conflict columns: {conflict_columns}")
            
            # Validate table exists
            if not self.table_exists(table_name, schema_name):
                raise PostgreSQLQueryError(f"Table {full_table_name} does not exist")
            
            # Get table schema
            table_schema = self.get_table_schema(table_name, schema_name)
            column_info = {col['column_name']: col for col in table_schema}
            
            # Validate conflict columns exist
            for col in conflict_columns:
                if col not in column_info:
                    raise ValueError(f"Conflict column '{col}' does not exist in table")
            
            self.logger.debug(f"DB_UPSERT: Validated conflict columns exist in table schema")
            
            # Preprocess data
            start_preprocessing = time.time()
            processed_data = self._preprocess_data(data, column_info)
            preprocessing_time = time.time() - start_preprocessing
            
            self.logger.debug(f"DB_UPSERT: Data preprocessing completed in {preprocessing_time:.3f}s")
            
            total_processed = 0
            total_batches = (len(processed_data) + batch_size - 1) // batch_size
            
            self.logger.info(f"DB_UPSERT: Processing {len(processed_data)} rows into {full_table_name} in {total_batches} batches")
            
            # Process in batches
            start_upsert_time = time.time()
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(processed_data))
                batch_data = processed_data[start_idx:end_idx]
                
                batch_start_time = time.time()
                rows_processed = self._upsert_batch(
                    full_table_name, batch_data, conflict_columns, column_info
                )
                batch_time = time.time() - batch_start_time
                total_processed += rows_processed
                
                self.logger.debug(f"DB_UPSERT: Batch {batch_num + 1}/{total_batches} completed in {batch_time:.3f}s - processed {rows_processed} rows")
            
            total_upsert_time = time.time() - start_upsert_time
            rows_per_second = total_processed / total_upsert_time if total_upsert_time > 0 else 0
            
            self.logger.info(f"DB_UPSERT: Successfully processed {total_processed} rows in {full_table_name} in {total_upsert_time:.3f}s ({rows_per_second:.1f} rows/sec)")
            return total_processed
            
        except Exception as e:
            error_msg = f"Failed to upsert data into {table_name}: {str(e)}"
            self.logger.error(f"DB_UPSERT: {error_msg}")
            raise PostgreSQLQueryError(error_msg)
    
    def _upsert_batch(self, table_name: str, batch_data: List[Dict[str, Any]], 
                     conflict_columns: List[str], 
                     column_info: Dict[str, Dict[str, Any]]) -> int:
        """
        Perform upsert operation on a batch of data.
        
        Args:
            table_name: Full table name
            batch_data: Batch of data to upsert
            conflict_columns: Columns that define uniqueness
            column_info: Column information from table schema
            
        Returns:
            int: Number of rows processed
        """
        if not batch_data:
            return 0
        
        try:
            # Get all columns from batch
            all_columns = set()
            for row in batch_data:
                all_columns.update(row.keys())
            
            columns = sorted(all_columns)
            
            # Build upsert query
            column_list = ', '.join(f'"{col}"' for col in columns)
            placeholders = ', '.join(['%s'] * len(columns))
            conflict_list = ', '.join(f'"{col}"' for col in conflict_columns)
            
            # Update columns (exclude conflict columns from updates)
            update_columns = [col for col in columns if col not in conflict_columns]
            update_set = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)
            
            if update_set:
                query = f"""
                    INSERT INTO {table_name} ({column_list}) 
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_list}) 
                    DO UPDATE SET {update_set}
                """
            else:
                query = f"""
                    INSERT INTO {table_name} ({column_list}) 
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_list}) 
                    DO NOTHING
                """
            
            # Prepare batch parameters
            batch_params = []
            for row in batch_data:
                row_params = [row.get(col) for col in columns]
                batch_params.append(row_params)
            
            # Execute upsert
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_batch(cursor, query, batch_params)
                    rows_affected = cursor.rowcount
                    connection.commit()
                    
                    return rows_affected
                    
        except Exception as e:
            error_msg = f"Batch upsert failed: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def truncate_table(self, table_name: str, schema_name: str = None, 
                       cascade: bool = False, restart_identity: bool = False) -> bool:
        """
        Truncate a PostgreSQL table, removing all data while preserving structure.
        
        Args:
            table_name: Name of the table to truncate
            schema_name: Schema name (optional)
            cascade: Whether to truncate tables that have foreign-key references
            restart_identity: Whether to restart identity columns
            
        Returns:
            bool: True if truncation was successful
            
        Raises:
            PostgreSQLQueryError: If truncation fails
        """
        try:
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            
            # Validate table exists
            if not self.table_exists(table_name, schema_name):
                raise PostgreSQLQueryError(f"Table {full_table_name} does not exist")
            
            # Build TRUNCATE statement
            truncate_parts = ["TRUNCATE TABLE", full_table_name]
            
            if restart_identity:
                truncate_parts.append("RESTART IDENTITY")
            
            if cascade:
                truncate_parts.append("CASCADE")
            
            truncate_query = " ".join(truncate_parts)
            
            self.logger.info(f"Truncating table {full_table_name}")
            self.execute_query(truncate_query)
            
            self.logger.info(f"Successfully truncated table {full_table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to truncate table {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def replace_data(self, table_name: str, data: List[Dict[str, Any]], 
                     schema_name: str = None, batch_size: int = 1000,
                     use_transaction: bool = True, backup_strategy: str = 'truncate') -> int:
        """
        Replace all data in a PostgreSQL table using atomic operations.
        
        This method provides several strategies for replacing data:
        - 'truncate': Truncate table and insert new data (fastest)
        - 'backup': Create backup table, replace data, drop backup on success
        - 'temp': Use temporary table for atomic replacement
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the new data
            schema_name: Schema name (optional)  
            batch_size: Number of rows to process per batch
            use_transaction: Whether to wrap operation in a transaction
            backup_strategy: Strategy for atomic replacement ('truncate', 'backup', 'temp')
            
        Returns:
            int: Number of rows inserted
            
        Raises:
            PostgreSQLQueryError: If replace operation fails
            ValueError: If parameters are invalid
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        if not isinstance(data, list):
            raise ValueError("Data must be a list of dictionaries")
        
        if backup_strategy not in ['truncate', 'backup', 'temp']:
            raise ValueError("backup_strategy must be 'truncate', 'backup', or 'temp'")
        
        try:
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            
            # Validate table exists
            if not self.table_exists(table_name, schema_name):
                raise PostgreSQLQueryError(f"Table {full_table_name} does not exist")
            
            self.logger.info(f"Starting replace operation for {full_table_name} with {len(data)} rows using {backup_strategy} strategy")
            
            if backup_strategy == 'truncate':
                return self._replace_with_truncate(table_name, data, schema_name, batch_size, use_transaction)
            elif backup_strategy == 'backup':
                return self._replace_with_backup(table_name, data, schema_name, batch_size, use_transaction)
            elif backup_strategy == 'temp':
                return self._replace_with_temp_table(table_name, data, schema_name, batch_size, use_transaction)
                
        except Exception as e:
            error_msg = f"Failed to replace data in {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def _replace_with_truncate(self, table_name: str, data: List[Dict[str, Any]], 
                              schema_name: str, batch_size: int, use_transaction: bool) -> int:
        """
        Replace data using TRUNCATE strategy (fastest but less safe).
        
        Args:
            table_name: Name of the target table
            data: Data to insert
            schema_name: Schema name
            batch_size: Batch size for insertion
            use_transaction: Whether to use transaction
            
        Returns:
            int: Number of rows inserted
        """
        full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
        
        if use_transaction:
            # Use transaction for atomicity
            transaction_started = False
            try:
                if not self.in_transaction:
                    self.begin_transaction()
                    transaction_started = True
                
                # Truncate table
                self.truncate_table(table_name, schema_name, restart_identity=True)
                
                # Insert new data
                rows_inserted = self.insert_data(table_name, data, schema_name, batch_size, on_conflict='error')
                
                if transaction_started:
                    self.commit_transaction()
                
                self.logger.info(f"Successfully replaced data in {full_table_name} using truncate strategy")
                return rows_inserted
                
            except Exception as e:
                if transaction_started and self.in_transaction:
                    self.rollback_transaction()
                    self.logger.error(f"Transaction rolled back due to error: {str(e)}")
                raise
        else:
            # Non-transactional approach (less safe)
            self.truncate_table(table_name, schema_name, restart_identity=True)
            rows_inserted = self.insert_data(table_name, data, schema_name, batch_size, on_conflict='error')
            
            self.logger.info(f"Successfully replaced data in {full_table_name} using truncate strategy (non-transactional)")
            return rows_inserted
    
    def _replace_with_backup(self, table_name: str, data: List[Dict[str, Any]], 
                            schema_name: str, batch_size: int, use_transaction: bool) -> int:
        """
        Replace data using backup table strategy (safer but slower).
        
        Args:
            table_name: Name of the target table
            data: Data to insert
            schema_name: Schema name
            batch_size: Batch size for insertion
            use_transaction: Whether to use transaction
            
        Returns:
            int: Number of rows inserted
        """
        import time
        
        full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
        backup_table_name = f"{table_name}_backup_{int(time.time())}"
        full_backup_name = f'"{schema_name}"."{backup_table_name}"' if schema_name else f'"{backup_table_name}"'
        
        transaction_started = False
        backup_created = False
        
        try:
            if use_transaction and not self.in_transaction:
                self.begin_transaction()
                transaction_started = True
            
            # Create backup table
            self.logger.info(f"Creating backup table {full_backup_name}")
            create_backup_query = f"CREATE TABLE {full_backup_name} AS SELECT * FROM {full_table_name}"
            self.execute_query(create_backup_query)
            backup_created = True
            
            # Truncate original table
            self.truncate_table(table_name, schema_name, restart_identity=True)
            
            # Insert new data
            rows_inserted = self.insert_data(table_name, data, schema_name, batch_size, on_conflict='error')
            
            # Drop backup table on success
            self.drop_table(backup_table_name, schema_name, if_exists=True)
            backup_created = False
            
            if transaction_started:
                self.commit_transaction()
            
            self.logger.info(f"Successfully replaced data in {full_table_name} using backup strategy")
            return rows_inserted
            
        except Exception as e:
            # Restore from backup if operation failed
            if backup_created:
                try:
                    self.logger.warning(f"Restoring data from backup table {full_backup_name}")
                    
                    # Truncate original table and restore from backup
                    self.truncate_table(table_name, schema_name)
                    restore_query = f"INSERT INTO {full_table_name} SELECT * FROM {full_backup_name}"
                    self.execute_query(restore_query)
                    
                    # Drop backup table
                    self.drop_table(backup_table_name, schema_name, if_exists=True)
                    
                    self.logger.info(f"Successfully restored data from backup")
                    
                except Exception as restore_error:
                    self.logger.error(f"Failed to restore from backup: {str(restore_error)}")
            
            if transaction_started and self.in_transaction:
                self.rollback_transaction()
                self.logger.error(f"Transaction rolled back due to error: {str(e)}")
            
            raise
    
    def _replace_with_temp_table(self, table_name: str, data: List[Dict[str, Any]], 
                                schema_name: str, batch_size: int, use_transaction: bool) -> int:
        """
        Replace data using temporary table strategy (most atomic).
        
        Args:
            table_name: Name of the target table
            data: Data to insert  
            schema_name: Schema name
            batch_size: Batch size for insertion
            use_transaction: Whether to use transaction
            
        Returns:
            int: Number of rows inserted
        """
        import time
        
        full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
        temp_table_name = f"{table_name}_temp_{int(time.time())}"
        full_temp_name = f'"{schema_name}"."{temp_table_name}"' if schema_name else f'"{temp_table_name}"'
        
        transaction_started = False
        temp_created = False
        
        try:
            if use_transaction and not self.in_transaction:
                self.begin_transaction()
                transaction_started = True
            
            # Get original table schema
            table_schema = self.get_table_schema(table_name, schema_name)
            
            # Create temporary table with same structure
            self.logger.info(f"Creating temporary table {full_temp_name}")
            create_temp_query = f"CREATE TABLE {full_temp_name} (LIKE {full_table_name} INCLUDING ALL)"
            self.execute_query(create_temp_query)
            temp_created = True
            
            # Insert new data into temporary table
            rows_inserted = self.insert_data(temp_table_name, data, schema_name, batch_size, on_conflict='error')
            
            # Atomic swap: rename tables
            self.logger.info(f"Performing atomic table swap")
            
            # Drop original table and rename temp table
            swap_queries = [
                f"DROP TABLE {full_table_name}",
                f"ALTER TABLE {full_temp_name} RENAME TO \"{table_name}\""
            ]
            
            for query in swap_queries:
                self.execute_query(query)
            
            temp_created = False  # Table was renamed, no longer temp
            
            if transaction_started:
                self.commit_transaction()
            
            self.logger.info(f"Successfully replaced data in {full_table_name} using temp table strategy")
            return rows_inserted
            
        except Exception as e:
            # Clean up temporary table if it exists
            if temp_created:
                try:
                    self.drop_table(temp_table_name, schema_name, if_exists=True)
                    self.logger.info(f"Cleaned up temporary table {full_temp_name}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Failed to clean up temporary table: {str(cleanup_error)}")
            
            if transaction_started and self.in_transaction:
                self.rollback_transaction()
                self.logger.error(f"Transaction rolled back due to error: {str(e)}")
            
            raise
    
    def replace_data_with_validation(self, table_name: str, data: List[Dict[str, Any]], 
                                   schema_name: str = None, batch_size: int = 1000,
                                   backup_strategy: str = 'backup', 
                                   validation_sample_size: int = 100) -> Dict[str, Any]:
        """
        Replace data with pre-validation and detailed reporting.
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the new data
            schema_name: Schema name (optional)
            batch_size: Number of rows to process per batch
            backup_strategy: Strategy for replacement ('truncate', 'backup', 'temp')
            validation_sample_size: Number of rows to validate before full replacement
            
        Returns:
            Dict containing operation results and statistics
            
        Raises:
            PostgreSQLQueryError: If replace operation fails
            ValueError: If validation fails
        """
        try:
            import time
            
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            
            # Get current row count
            count_query = f"SELECT COUNT(*) as count FROM {full_table_name}"
            current_count = self.execute_query(count_query)[0]['count']
            
            # Get table schema for validation
            table_schema = self.get_table_schema(table_name, schema_name)
            column_info = {col['column_name']: col for col in table_schema}
            
            # Validate sample data
            validation_data = data[:validation_sample_size]
            self.logger.info(f"Validating {len(validation_data)} sample rows before replacement")
            
            try:
                self._preprocess_data(validation_data, column_info)
                self.logger.info("Data validation passed")
            except Exception as e:
                raise ValueError(f"Data validation failed: {str(e)}")
            
            # Perform replacement
            start_time = time.time()
            rows_inserted = self.replace_data(
                table_name, data, schema_name, batch_size, 
                use_transaction=True, backup_strategy=backup_strategy
            )
            end_time = time.time()
            
            # Return operation statistics
            result = {
                'success': True,
                'table_name': full_table_name,
                'strategy': backup_strategy,
                'original_row_count': current_count,
                'new_row_count': rows_inserted,
                'rows_changed': rows_inserted - current_count,
                'execution_time_seconds': round(end_time - start_time, 2),
                'batch_size': batch_size,
                'validation_sample_size': validation_sample_size
            }
            
            self.logger.info(f"Replace operation completed successfully: {result}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to replace data with validation in {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def get_primary_key_columns(self, table_name: str, schema_name: str = None) -> List[str]:
        """
        Get the primary key columns for a PostgreSQL table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to current schema)
            
        Returns:
            List of primary key column names
            
        Raises:
            PostgreSQLQueryError: If primary key detection fails
        """
        try:
            query = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)
            """
            
            # Build table identifier
            if schema_name:
                table_identifier = f'"{schema_name}"."{table_name}"'
            else:
                table_identifier = f'"{table_name}"'
            
            result = self.execute_query(query, [table_identifier])
            primary_keys = [row['attname'] for row in result]
            
            self.logger.debug(f"Primary key columns for {table_identifier}: {primary_keys}")
            return primary_keys
            
        except Exception as e:
            error_msg = f"Failed to get primary key columns for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def get_unique_constraints(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Get unique constraints for a PostgreSQL table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (defaults to current schema)
            
        Returns:
            List of dictionaries containing constraint information
            
        Raises:
            PostgreSQLQueryError: If constraint detection fails
        """
        try:
            query = """
                SELECT 
                    con.conname AS constraint_name,
                    con.contype AS constraint_type,
                    array_agg(att.attname ORDER BY array_position(con.conkey, att.attnum)) AS columns
                FROM pg_constraint con
                JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
                WHERE con.conrelid = %s::regclass 
                AND con.contype IN ('p', 'u')  -- primary key or unique
                GROUP BY con.conname, con.contype
                ORDER BY con.conname
            """
            
            # Build table identifier
            if schema_name:
                table_identifier = f'"{schema_name}"."{table_name}"'
            else:
                table_identifier = f'"{table_name}"'
            
            result = self.execute_query(query, [table_identifier])
            
            constraints = []
            for row in result:
                constraints.append({
                    'constraint_name': row['constraint_name'],
                    'constraint_type': 'primary_key' if row['constraint_type'] == 'p' else 'unique',
                    'columns': row['columns']
                })
            
            self.logger.debug(f"Unique constraints for {table_identifier}: {constraints}")
            return constraints
            
        except Exception as e:
            error_msg = f"Failed to get unique constraints for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def detect_conflict_columns(self, table_name: str, data: List[Dict[str, Any]], 
                               schema_name: str = None, prefer_primary_key: bool = True) -> List[str]:
        """
        Automatically detect appropriate conflict columns for upsert operations.
        
        Args:
            table_name: Name of the table
            data: Sample data to analyze for common columns
            schema_name: Schema name (optional)
            prefer_primary_key: Whether to prefer primary key over other unique constraints
            
        Returns:
            List of column names suitable for conflict resolution
            
        Raises:
            PostgreSQLQueryError: If no suitable conflict columns are found
        """
        try:
            # Get unique constraints
            constraints = self.get_unique_constraints(table_name, schema_name)
            
            if not constraints:
                raise PostgreSQLQueryError(f"No unique constraints found for table {table_name}")
            
            # Get columns present in the data
            data_columns = set()
            for row in data:
                data_columns.update(row.keys())
            
            # Find suitable constraints
            suitable_constraints = []
            
            for constraint in constraints:
                constraint_columns = set(constraint['columns'])
                
                # Check if all constraint columns are present in data
                if constraint_columns.issubset(data_columns):
                    suitable_constraints.append(constraint)
            
            if not suitable_constraints:
                available_columns = [c['columns'] for c in constraints]
                raise PostgreSQLQueryError(
                    f"No suitable conflict columns found. Available constraints: {available_columns}, "
                    f"Data columns: {list(data_columns)}"
                )
            
            # Prefer primary key if available and requested
            if prefer_primary_key:
                for constraint in suitable_constraints:
                    if constraint['constraint_type'] == 'primary_key':
                        self.logger.info(f"Using primary key columns for conflict resolution: {constraint['columns']}")
                        return constraint['columns']
            
            # Use the first suitable constraint
            selected_constraint = suitable_constraints[0]
            self.logger.info(f"Using {selected_constraint['constraint_type']} columns for conflict resolution: {selected_constraint['columns']}")
            return selected_constraint['columns']
            
        except Exception as e:
            error_msg = f"Failed to detect conflict columns for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def upsert_data_optimized(self, table_name: str, data: List[Dict[str, Any]], 
                             schema_name: str = None, batch_size: int = 1000,
                             conflict_columns: Optional[List[str]] = None,
                             update_columns: Optional[List[str]] = None,
                             auto_detect_conflicts: bool = True) -> Dict[str, Any]:
        """
        Optimized upsert operation with automatic conflict detection and performance optimization.
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the data to upsert
            schema_name: Schema name (optional)
            batch_size: Number of rows to process per batch
            conflict_columns: Specific columns for conflict resolution (auto-detected if None)
            update_columns: Specific columns to update on conflict (all non-conflict columns if None)
            auto_detect_conflicts: Whether to automatically detect conflict columns
            
        Returns:
            Dict containing operation results and statistics
            
        Raises:
            PostgreSQLQueryError: If upsert operation fails
            ValueError: If parameters are invalid
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        if not isinstance(data, list):
            raise ValueError("Data must be a list of dictionaries")
        
        try:
            import time
            
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            
            # Validate table exists
            if not self.table_exists(table_name, schema_name):
                raise PostgreSQLQueryError(f"Table {full_table_name} does not exist")
            
            # Auto-detect conflict columns if not provided
            if conflict_columns is None and auto_detect_conflicts:
                conflict_columns = self.detect_conflict_columns(table_name, data, schema_name)
            elif conflict_columns is None:
                raise ValueError("conflict_columns must be provided when auto_detect_conflicts=False")
            
            # Get table schema for validation
            table_schema = self.get_table_schema(table_name, schema_name)
            column_info = {col['column_name']: col for col in table_schema}
            
            # Validate conflict columns exist
            for col in conflict_columns:
                if col not in column_info:
                    raise ValueError(f"Conflict column '{col}' does not exist in table")
            
            # Determine update columns
            if update_columns is None:
                # Get all columns from data, excluding conflict columns
                all_data_columns = set()
                for row in data:
                    all_data_columns.update(row.keys())
                update_columns = [col for col in all_data_columns if col not in conflict_columns]
            
            # Validate update columns
            for col in update_columns:
                if col not in column_info:
                    self.logger.warning(f"Update column '{col}' not found in table schema, will be ignored")
            
            update_columns = [col for col in update_columns if col in column_info]
            
            # Preprocess data
            processed_data = self._preprocess_data(data, column_info)
            
            # Perform optimized upsert
            start_time = time.time()
            
            total_processed = 0
            total_batches = (len(processed_data) + batch_size - 1) // batch_size
            
            self.logger.info(f"Starting optimized upsert for {full_table_name} with {len(processed_data)} rows")
            self.logger.info(f"Conflict columns: {conflict_columns}")
            self.logger.info(f"Update columns: {update_columns}")
            
            # Process in batches
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(processed_data))
                batch_data = processed_data[start_idx:end_idx]
                
                rows_processed = self._upsert_batch_optimized(
                    full_table_name, batch_data, conflict_columns, update_columns, column_info
                )
                total_processed += rows_processed
                
                self.logger.debug(f"Batch {batch_num + 1}/{total_batches}: processed {rows_processed} rows")
            
            end_time = time.time()
            
            # Return operation statistics
            result = {
                'success': True,
                'table_name': full_table_name,
                'operation': 'upsert_optimized',
                'rows_processed': total_processed,
                'conflict_columns': conflict_columns,
                'update_columns': update_columns,
                'batch_count': total_batches,
                'batch_size': batch_size,
                'execution_time_seconds': round(end_time - start_time, 2),
                'rows_per_second': round(total_processed / (end_time - start_time), 2) if (end_time - start_time) > 0 else 0
            }
            
            self.logger.info(f"Optimized upsert completed successfully: {result}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to perform optimized upsert on {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def _upsert_batch_optimized(self, table_name: str, batch_data: List[Dict[str, Any]], 
                               conflict_columns: List[str], update_columns: List[str],
                               column_info: Dict[str, Dict[str, Any]]) -> int:
        """
        Perform optimized upsert operation on a batch of data.
        
        Args:
            table_name: Full table name
            batch_data: Batch of data to upsert
            conflict_columns: Columns that define uniqueness
            update_columns: Columns to update on conflict
            column_info: Column information from table schema
            
        Returns:
            int: Number of rows processed
        """
        if not batch_data:
            return 0
        
        try:
            # Get all columns from batch
            all_columns = set()
            for row in batch_data:
                all_columns.update(row.keys())
            
            # Filter to only include columns that exist in the table
            columns = sorted([col for col in all_columns if col in column_info])
            
            # Build optimized upsert query
            column_list = ', '.join(f'"{col}"' for col in columns)
            placeholders = ', '.join(['%s'] * len(columns))
            conflict_list = ', '.join(f'"{col}"' for col in conflict_columns)
            
            # Build UPDATE SET clause for non-conflict columns
            valid_update_columns = [col for col in update_columns if col in columns]
            
            if valid_update_columns:
                update_set = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in valid_update_columns)
                query = f"""
                    INSERT INTO {table_name} ({column_list}) 
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_list}) 
                    DO UPDATE SET {update_set}
                """
            else:
                # No columns to update, just ignore conflicts
                query = f"""
                    INSERT INTO {table_name} ({column_list}) 
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_list}) 
                    DO NOTHING
                """
            
            # Prepare batch parameters
            batch_params = []
            for row in batch_data:
                row_params = [row.get(col) for col in columns]
                batch_params.append(row_params)
            
            # Execute optimized upsert
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_batch(
                        cursor, query, batch_params, 
                        page_size=min(len(batch_params), 1000)  # Optimize page size
                    )
                    rows_affected = cursor.rowcount
                    connection.commit()
                    
                    return rows_affected
                    
        except Exception as e:
            error_msg = f"Optimized batch upsert failed: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def bulk_upsert_with_staging(self, table_name: str, data: List[Dict[str, Any]], 
                                schema_name: str = None, conflict_columns: Optional[List[str]] = None,
                                use_copy: bool = True) -> Dict[str, Any]:
        """
        High-performance bulk upsert using staging table and COPY operations.
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the data to upsert
            schema_name: Schema name (optional)
            conflict_columns: Specific columns for conflict resolution
            use_copy: Whether to use COPY for initial data loading (fastest)
            
        Returns:
            Dict containing operation results and statistics
            
        Raises:
            PostgreSQLQueryError: If bulk upsert operation fails
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        try:
            import time
            import io
            import csv
            
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            staging_table_name = f"{table_name}_staging_{int(time.time())}"
            full_staging_name = f'"{schema_name}"."{staging_table_name}"' if schema_name else f'"{staging_table_name}"'
            
            # Auto-detect conflict columns if not provided
            if conflict_columns is None:
                conflict_columns = self.detect_conflict_columns(table_name, data, schema_name)
            
            # Get table schema
            table_schema = self.get_table_schema(table_name, schema_name)
            column_info = {col['column_name']: col for col in table_schema}
            
            staging_created = False
            start_time = time.time()
            
            try:
                # Create staging table with same structure
                self.logger.info(f"Creating staging table {full_staging_name}")
                create_staging_query = f"CREATE TEMP TABLE {staging_table_name} (LIKE {full_table_name})"
                self.execute_query(create_staging_query)
                staging_created = True
                
                # Preprocess data
                processed_data = self._preprocess_data(data, column_info)
                
                if use_copy and len(processed_data) > 1000:
                    # Use COPY for large datasets (most efficient)
                    self._bulk_copy_to_staging(staging_table_name, processed_data, column_info)
                else:
                    # Use regular batch insert for smaller datasets
                    self.insert_data(staging_table_name, processed_data, None, batch_size=5000, on_conflict='error')
                
                # Perform upsert from staging to main table
                rows_affected = self._upsert_from_staging(
                    full_table_name, full_staging_name, conflict_columns, column_info
                )
                
                # Drop staging table
                self.execute_query(f"DROP TABLE {staging_table_name}")
                staging_created = False
                
                end_time = time.time()
                
                # Return operation statistics
                result = {
                    'success': True,
                    'table_name': full_table_name,
                    'operation': 'bulk_upsert_staging',
                    'rows_processed': rows_affected,
                    'conflict_columns': conflict_columns,
                    'use_copy': use_copy,
                    'execution_time_seconds': round(end_time - start_time, 2),
                    'rows_per_second': round(rows_affected / (end_time - start_time), 2) if (end_time - start_time) > 0 else 0
                }
                
                self.logger.info(f"Bulk upsert with staging completed successfully: {result}")
                return result
                
            except Exception as e:
                # Clean up staging table if it exists
                if staging_created:
                    try:
                        self.execute_query(f"DROP TABLE IF EXISTS {staging_table_name}")
                        self.logger.info(f"Cleaned up staging table {full_staging_name}")
                    except Exception as cleanup_error:
                        self.logger.warning(f"Failed to clean up staging table: {str(cleanup_error)}")
                raise
                
        except Exception as e:
            error_msg = f"Failed to perform bulk upsert with staging on {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def _bulk_copy_to_staging(self, staging_table_name: str, data: List[Dict[str, Any]], 
                             column_info: Dict[str, Dict[str, Any]]) -> None:
        """
        Use PostgreSQL COPY to bulk load data into staging table.
        
        Args:
            staging_table_name: Name of the staging table
            data: Preprocessed data to load
            column_info: Column information from table schema
        """
        try:
            import io
            import csv
            
            # Get all columns present in data
            all_columns = set()
            for row in data:
                all_columns.update(row.keys())
            
            # Filter and sort columns
            columns = sorted([col for col in all_columns if col in column_info])
            
            # Create CSV data in memory
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            
            for row in data:
                csv_row = [row.get(col) for col in columns]
                writer.writerow(csv_row)
            
            csv_buffer.seek(0)
            
            # Use COPY to load data
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    column_list = ', '.join(f'"{col}"' for col in columns)
                    copy_query = f"COPY {staging_table_name} ({column_list}) FROM STDIN WITH CSV"
                    
                    cursor.copy_expert(copy_query, csv_buffer)
                    connection.commit()
                    
                    self.logger.info(f"Bulk copied {len(data)} rows to staging table using COPY")
                    
        except Exception as e:
            error_msg = f"Failed to bulk copy data to staging table: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
    def _upsert_from_staging(self, main_table: str, staging_table: str, 
                            conflict_columns: List[str], 
                            column_info: Dict[str, Dict[str, Any]]) -> int:
        """
        Perform upsert from staging table to main table.
        
        Args:
            main_table: Full name of the main table
            staging_table: Full name of the staging table
            conflict_columns: Columns for conflict resolution
            column_info: Column information from table schema
            
        Returns:
            int: Number of rows affected
        """
        try:
            # Get columns from staging table
            staging_columns_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{staging_table.split('.')[-1].strip('"')}'
                ORDER BY ordinal_position
            """
            
            staging_cols_result = self.execute_query(staging_columns_query)
            staging_columns = [row['column_name'] for row in staging_cols_result]
            
            # Filter columns that exist in both tables
            common_columns = [col for col in staging_columns if col in column_info]
            
            # Build upsert query from staging
            column_list = ', '.join(f'"{col}"' for col in common_columns)
            conflict_list = ', '.join(f'"{col}"' for col in conflict_columns)
            
            # Update columns (exclude conflict columns)
            update_columns = [col for col in common_columns if col not in conflict_columns]
            update_set = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)
            
            if update_set:
                upsert_query = f"""
                    INSERT INTO {main_table} ({column_list})
                    SELECT {column_list} FROM {staging_table}
                    ON CONFLICT ({conflict_list})
                    DO UPDATE SET {update_set}
                """
            else:
                upsert_query = f"""
                    INSERT INTO {main_table} ({column_list})
                    SELECT {column_list} FROM {staging_table}
                    ON CONFLICT ({conflict_list})
                    DO NOTHING
                """
            
            # Execute upsert
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(upsert_query)
                    rows_affected = cursor.rowcount
                    connection.commit()
                    
                    self.logger.info(f"Upserted {rows_affected} rows from staging to main table")
                    return rows_affected
                    
        except Exception as e:
            error_msg = f"Failed to upsert from staging table: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)