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
        
        # Validate PostgreSQL-specific parameters
        self._validate_connection_params()
        
        # Build connection string
        self.connection_string = self._build_connection_string()
        
        self.logger.info(f"PostgreSQL connector initialized for database: {self.connection_params.get('database')}")
    
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
    
    def connect(self) -> bool:
        """
        Establish a connection pool to the PostgreSQL database.
        
        Returns:
            bool: True if connection was successful, False otherwise
        
        Raises:
            PostgreSQLConnectionError: If connection fails after all retry attempts
        """
        if self.is_connected and self.connection_pool:
            self.logger.info("Already connected to PostgreSQL database")
            return True
        
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting to connect to PostgreSQL database (attempt {attempt + 1}/{self.max_retries})")
                
                # Create connection pool
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=self.connection_params['connection_pool_size'],
                    dsn=self.connection_string
                )
                
                # Test the connection by getting one from the pool
                test_conn = self.connection_pool.getconn()
                test_conn.close()
                self.connection_pool.putconn(test_conn)
                
                self.is_connected = True
                self.logger.info("Successfully connected to PostgreSQL database")
                return True
                
            except OperationalError as e:
                last_error = e
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                
            except Exception as e:
                last_error = e
                self.logger.error(f"Unexpected error during connection attempt {attempt + 1}: {str(e)}")
                break
        
        error_msg = f"Failed to connect to PostgreSQL database after {self.max_retries} attempts: {str(last_error)}"
        self.logger.error(error_msg)
        raise PostgreSQLConnectionError(error_msg)
    
    def disconnect(self) -> bool:
        """
        Close all connections in the connection pool.
        
        Returns:
            bool: True if disconnection was successful, False otherwise
        """
        try:
            if self.current_connection:
                if self.in_transaction:
                    self.rollback_transaction()
                self.connection_pool.putconn(self.current_connection)
                self.current_connection = None
            
            if self.connection_pool:
                self.connection_pool.closeall()
                self.connection_pool = None
            
            self.is_connected = False
            self.logger.info("Successfully disconnected from PostgreSQL database")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during disconnection: {str(e)}")
            return False
    
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
    
    def execute_query(self, query: str, params: Optional[Union[List, Dict]] = None) -> Any:
        """
        Execute a query on the PostgreSQL database.
        
        Args:
            query: SQL query string to execute
            params: Parameters to bind to the query
            
        Returns:
            Query results for SELECT queries, row count for INSERT/UPDATE/DELETE
            
        Raises:
            PostgreSQLConnectionError: If not connected to the database
            PostgreSQLQueryError: If query execution fails
        """
        if not query or not query.strip():
            raise PostgreSQLQueryError("Query cannot be empty")
        
        query_type = query.strip().upper().split()[0]
        
        try:
            with self._get_connection() as connection:
                with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    self.logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
                    
                    start_time = time.time()
                    cursor.execute(query, params)
                    execution_time = time.time() - start_time
                    
                    if query_type in ['SELECT', 'WITH']:
                        results = cursor.fetchall()
                        self.logger.debug(f"Query executed successfully in {execution_time:.3f}s, returned {len(results)} rows")
                        return [dict(row) for row in results]
                    else:
                        rowcount = cursor.rowcount
                        connection.commit()
                        self.logger.debug(f"Query executed successfully in {execution_time:.3f}s, affected {rowcount} rows")
                        return rowcount
                        
        except ProgrammingError as e:
            error_msg = f"SQL syntax error: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
            
        except IntegrityError as e:
            error_msg = f"Database integrity error: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
            
        except DatabaseError as e:
            error_msg = f"Database error during query execution: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
            
        except Exception as e:
            error_msg = f"Unexpected error during query execution: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)
    
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
    
    def get_table_schema(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
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
    
    def table_exists(self, table_name: str, schema_name: str = None) -> bool:
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
            
            # Check if table exists
            table_exists = self.table_exists(table_name, schema_name)
            
            if table_exists:
                if drop_if_exists:
                    self.logger.info(f"Dropping existing table {full_table_name}")
                    drop_ddl = f"DROP TABLE {full_table_name}"
                    self.execute_query(drop_ddl)
                elif not if_not_exists:
                    raise PostgreSQLQueryError(f"Table {full_table_name} already exists")
                else:
                    self.logger.info(f"Table {full_table_name} already exists, skipping creation")
                    return True
            
            # Generate DDL
            use_if_not_exists = if_not_exists and not drop_if_exists
            ddl = self.schema_to_ddl(table_name, schema, schema_name, use_if_not_exists)
            
            # Execute DDL
            self.logger.info(f"Creating table {full_table_name}")
            self.execute_query(ddl)
            
            # Verify table was created
            if self.table_exists(table_name, schema_name):
                self.logger.info(f"Successfully created table {full_table_name}")
                return True
            else:
                raise PostgreSQLQueryError(f"Table {full_table_name} was not created")
            
        except Exception as e:
            error_msg = f"Failed to create table {table_name}: {str(e)}"
            self.logger.error(error_msg)
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
            
            self.logger.info(f"Successfully dropped table {full_table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to drop table {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise PostgreSQLQueryError(error_msg)