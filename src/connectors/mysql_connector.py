"""
MySQL Database Connector for the Generic Data Ingestor Framework.

This module provides a concrete implementation of the DatabaseConnector abstract base class
for MySQL databases, with comprehensive connection management and error handling.
"""

import mysql.connector
import mysql.connector.pooling
from mysql.connector import Error as MySQLError, IntegrityError, ProgrammingError, OperationalError
from typing import Any, Dict, List, Optional, Union, Tuple
import logging
import time
from contextlib import contextmanager

from .database_connector import DatabaseConnector


class MySQLConnectionError(Exception):
    """Custom exception for MySQL connection errors."""
    pass


class MySQLQueryError(Exception):
    """Custom exception for MySQL query execution errors."""
    pass


class MySQLTransactionError(Exception):
    """Custom exception for MySQL transaction errors."""
    pass


class MySQLConnector(DatabaseConnector):
    """
    MySQL database connector implementation.
    
    This class provides a concrete implementation of the DatabaseConnector abstract base class
    specifically for MySQL databases. It includes connection pooling, comprehensive error
    handling, transaction management, and SSL support.
    
    Attributes:
        connection_pool: mysql.connector connection pool for managing multiple connections
        current_connection: The current active connection from the pool
        in_transaction: Boolean indicating if a transaction is currently active
        max_retries: Maximum number of retry attempts for failed operations
        retry_delay: Delay in seconds between retry attempts
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the MySQL connector with connection parameters.
        
        Args:
            connection_params: Dictionary containing MySQL connection parameters
                Required: host, database, user
                Optional: port (default: 3306), password, connection_timeout,
                         connection_pool_size, ssl_disabled, ssl_ca, ssl_cert,
                         ssl_key, additional_options
        """
        super().__init__(connection_params)
        self.connection_pool = None
        self.current_connection = None
        self.in_transaction = False
        self.max_retries = connection_params.get('max_retries', 3)
        self.retry_delay = connection_params.get('retry_delay', 1)
        
        # Validate MySQL-specific parameters
        self._validate_connection_params()
        
        # Build connection configuration
        self.connection_config = self._build_connection_config()
        
        self.logger.info(f"MySQL connector initialized for database: {self.connection_params.get('database')}")
    
    def _validate_connection_params(self) -> None:
        """
        Validate MySQL-specific connection parameters.
        
        Raises:
            ValueError: If required parameters are missing or invalid
        """
        required_params = ['host', 'database', 'user']
        for param in required_params:
            if not self.connection_params.get(param):
                raise ValueError(f"Missing required MySQL parameter: {param}")
        
        # Set default port if not provided
        if 'port' not in self.connection_params:
            self.connection_params['port'] = 3306
        
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
        Build MySQL connection configuration from parameters.
        
        Returns:
            MySQL connection configuration dictionary
        """
        config = {
            'host': self.connection_params['host'],
            'port': self.connection_params['port'],
            'database': self.connection_params['database'],
            'user': self.connection_params['user'],
            'connection_timeout': self.connection_params['connection_timeout'],
            'autocommit': False,  # We'll manage transactions manually
            'raise_on_warnings': True,
            'use_unicode': True,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        }
        
        # Add password if provided
        if self.connection_params.get('password'):
            config['password'] = self.connection_params['password']
        
        # SSL configuration
        if not self.connection_params.get('ssl_disabled', False):
            config['ssl_disabled'] = False
            
            if self.connection_params.get('ssl_ca'):
                config['ssl_ca'] = self.connection_params['ssl_ca']
            
            if self.connection_params.get('ssl_cert'):
                config['ssl_cert'] = self.connection_params['ssl_cert']
            
            if self.connection_params.get('ssl_key'):
                config['ssl_key'] = self.connection_params['ssl_key']
        else:
            config['ssl_disabled'] = True
        
        # Additional options
        additional_options = self.connection_params.get('additional_options', {})
        config.update(additional_options)
        
        return config
    
    def connect(self) -> bool:
        """
        Establish a connection pool to the MySQL database.
        
        Returns:
            bool: True if connection was successful, False otherwise
        
        Raises:
            MySQLConnectionError: If connection fails after all retry attempts
        """
        if self.is_connected and self.connection_pool:
            self.logger.info("Already connected to MySQL database")
            return True
        
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting to connect to MySQL database (attempt {attempt + 1}/{self.max_retries})")
                
                # Create connection pool
                pool_config = self.connection_config.copy()
                pool_config.update({
                    'pool_name': f"mysql_pool_{id(self)}",
                    'pool_size': self.connection_params['connection_pool_size'],
                    'pool_reset_session': True
                })
                
                self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
                
                # Test the connection by getting one from the pool
                test_conn = self.connection_pool.get_connection()
                test_conn.ping(reconnect=True, attempts=1, delay=0)
                test_conn.close()
                
                self.is_connected = True
                self.logger.info("Successfully connected to MySQL database")
                return True
                
            except MySQLError as e:
                last_error = e
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                
            except Exception as e:
                last_error = e
                self.logger.error(f"Unexpected error during connection attempt {attempt + 1}: {str(e)}")
                break
        
        error_msg = f"Failed to connect to MySQL database after {self.max_retries} attempts: {str(last_error)}"
        self.logger.error(error_msg)
        raise MySQLConnectionError(error_msg)
    
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
                self.current_connection.close()
                self.current_connection = None
            
            if self.connection_pool:
                # Close all connections in the pool
                try:
                    # Get all connections and close them
                    while True:
                        conn = self.connection_pool.get_connection()
                        conn.close()
                except:
                    # Expected when pool is empty
                    pass
                self.connection_pool = None
            
            self.is_connected = False
            self.logger.info("Successfully disconnected from MySQL database")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during disconnection: {str(e)}")
            return False
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager to get a connection from the pool.
        
        Yields:
            mysql.connector connection object
        
        Raises:
            MySQLConnectionError: If unable to get connection from pool
        """
        if not self.is_connected or not self.connection_pool:
            raise MySQLConnectionError("Not connected to database. Call connect() first.")
        
        connection = None
        try:
            connection = self.connection_pool.get_connection()
            connection.ping(reconnect=True, attempts=1, delay=0)
            
            yield connection
            
        except MySQLError as e:
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            raise MySQLConnectionError(f"Connection error: {str(e)}")
        except Exception as e:
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: Optional[Union[List, Dict]] = None) -> Any:
        """
        Execute a query on the MySQL database.
        
        Args:
            query: SQL query string to execute
            params: Parameters to bind to the query
            
        Returns:
            Query results for SELECT queries, row count for INSERT/UPDATE/DELETE
            
        Raises:
            MySQLConnectionError: If not connected to the database
            MySQLQueryError: If query execution fails
        """
        if not query or not query.strip():
            raise MySQLQueryError("Query cannot be empty")
        
        query_type = query.strip().upper().split()[0]
        
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor(dictionary=True, buffered=True)
                
                self.logger.debug(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
                
                start_time = time.time()
                cursor.execute(query, params)
                execution_time = time.time() - start_time
                
                if query_type in ['SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN']:
                    results = cursor.fetchall()
                    self.logger.debug(f"Query executed successfully in {execution_time:.3f}s, returned {len(results)} rows")
                    return results
                else:
                    rowcount = cursor.rowcount
                    connection.commit()
                    self.logger.debug(f"Query executed successfully in {execution_time:.3f}s, affected {rowcount} rows")
                    return rowcount
                    
        except ProgrammingError as e:
            error_msg = f"SQL syntax error: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
            
        except IntegrityError as e:
            error_msg = f"Database integrity error: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
            
        except MySQLError as e:
            error_msg = f"MySQL error during query execution: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
            
        except Exception as e:
            error_msg = f"Unexpected error during query execution: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
    
    def begin_transaction(self) -> bool:
        """
        Begin a database transaction.
        
        Returns:
            bool: True if transaction was successfully started, False otherwise
        
        Raises:
            MySQLTransactionError: If transaction cannot be started
        """
        try:
            if self.in_transaction:
                self.logger.warning("Transaction already in progress")
                return True
            
            if not self.current_connection:
                self.current_connection = self.connection_pool.get_connection()
            
            self.current_connection.start_transaction()
            self.in_transaction = True
            self.logger.debug("Transaction started successfully")
            return True
            
        except Exception as e:
            error_msg = f"Failed to start transaction: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLTransactionError(error_msg)
    
    def commit_transaction(self) -> bool:
        """
        Commit the current transaction.
        
        Returns:
            bool: True if transaction was successfully committed, False otherwise
        
        Raises:
            MySQLTransactionError: If transaction cannot be committed
        """
        try:
            if not self.in_transaction:
                self.logger.warning("No transaction in progress")
                return True
            
            if self.current_connection:
                self.current_connection.commit()
                self.current_connection.close()
                self.current_connection = None
            
            self.in_transaction = False
            self.logger.debug("Transaction committed successfully")
            return True
            
        except Exception as e:
            error_msg = f"Failed to commit transaction: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLTransactionError(error_msg)
    
    def rollback_transaction(self) -> bool:
        """
        Rollback the current transaction.
        
        Returns:
            bool: True if transaction was successfully rolled back, False otherwise
        
        Raises:
            MySQLTransactionError: If transaction cannot be rolled back
        """
        try:
            if not self.in_transaction:
                self.logger.warning("No transaction in progress")
                return True
            
            if self.current_connection:
                self.current_connection.rollback()
                self.current_connection.close()
                self.current_connection = None
            
            self.in_transaction = False
            self.logger.debug("Transaction rolled back successfully")
            return True
            
        except Exception as e:
            error_msg = f"Failed to rollback transaction: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLTransactionError(error_msg)
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current MySQL database connection.
        
        Returns:
            Dict containing connection information such as database type,
            server version, connection status, etc.
        """
        info = {
            "db_type": "mysql",
            "host": self.connection_params['host'],
            "port": self.connection_params['port'],
            "database": self.connection_params['database'],
            "user": self.connection_params['user'],
            "is_connected": self.is_connected,
            "connection_pool_size": self.connection_params['connection_pool_size'],
            "ssl_disabled": self.connection_params.get('ssl_disabled', False),
            "in_transaction": self.in_transaction
        }
        
        if self.is_connected and self.connection_pool:
            try:
                with self._get_connection() as connection:
                    # Get MySQL server version
                    cursor = connection.cursor()
                    cursor.execute("SELECT VERSION() AS version")
                    version_result = cursor.fetchone()
                    info["server_version"] = version_result[0] if version_result else "Unknown"
                    
                    # Get current database
                    cursor.execute("SELECT DATABASE() AS current_db")
                    db_result = cursor.fetchone()
                    info["current_database"] = db_result[0] if db_result else "None"
                    
                    # Get connection count
                    cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
                    threads_result = cursor.fetchone()
                    info["active_connections"] = int(threads_result[1]) if threads_result else 0
                    
                    # Get character set
                    cursor.execute("SHOW VARIABLES LIKE 'character_set_database'")
                    charset_result = cursor.fetchone()
                    info["character_set"] = charset_result[1] if charset_result else "Unknown"
                    
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
            MySQLQueryError: If batch execution fails
        """
        if not query or not query.strip():
            raise MySQLQueryError("Query cannot be empty")
        
        if not params_list:
            raise MySQLQueryError("Parameters list cannot be empty")
        
        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()
                self.logger.debug(f"Executing batch query with {len(params_list)} parameter sets")
                
                start_time = time.time()
                cursor.executemany(query, params_list)
                execution_time = time.time() - start_time
                
                rowcount = cursor.rowcount
                connection.commit()
                
                self.logger.debug(f"Batch query executed successfully in {execution_time:.3f}s, affected {rowcount} rows")
                return rowcount
                
        except Exception as e:
            error_msg = f"Batch execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
    
    def get_table_schema(self, table_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Get schema information for a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (database name in MySQL, defaults to current database)
            
        Returns:
            List of dictionaries containing column information
            
        Raises:
            MySQLQueryError: If schema retrieval fails
        """
        try:
            query = """
                SELECT 
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    IS_NULLABLE as is_nullable,
                    COLUMN_DEFAULT as column_default,
                    CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                    NUMERIC_PRECISION as numeric_precision,
                    NUMERIC_SCALE as numeric_scale,
                    COLUMN_KEY as column_key,
                    EXTRA as extra
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = %s
            """
            params = [table_name]
            
            if schema_name:
                query += " AND TABLE_SCHEMA = %s"
                params.append(schema_name)
            else:
                query += " AND TABLE_SCHEMA = DATABASE()"
            
            query += " ORDER BY ORDINAL_POSITION"
            
            return self.execute_query(query, params)
            
        except Exception as e:
            error_msg = f"Failed to get table schema for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
    
    def table_exists(self, table_name: str, schema_name: str = None) -> bool:
        """
        Check if a table exists in the MySQL database.
        
        Args:
            table_name: Name of the table to check
            schema_name: Schema name (database name in MySQL, defaults to current database)
            
        Returns:
            bool: True if table exists, False otherwise
            
        Raises:
            MySQLQueryError: If existence check fails
        """
        try:
            query = """
                SELECT COUNT(*) as count 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = %s
            """
            params = [table_name]
            
            if schema_name:
                query += " AND TABLE_SCHEMA = %s"
                params.append(schema_name)
            else:
                query += " AND TABLE_SCHEMA = DATABASE()"
            
            result = self.execute_query(query, params)
            exists = result[0]['count'] > 0 if result else False
            
            self.logger.debug(f"Table existence check for {table_name}: {exists}")
            return exists
            
        except Exception as e:
            error_msg = f"Failed to check table existence for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
    
    def schema_to_ddl(self, table_name: str, schema: List[Dict[str, Any]], 
                      schema_name: str = None, if_not_exists: bool = True,
                      engine: str = 'InnoDB', charset: str = 'utf8mb4') -> str:
        """
        Convert a schema definition to MySQL DDL (Data Definition Language).
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions with keys: name, type, nullable, default, etc.
            schema_name: Schema name to create table in (optional)
            if_not_exists: Whether to include IF NOT EXISTS clause
            engine: MySQL storage engine (default: InnoDB)
            charset: Character set for the table (default: utf8mb4)
            
        Returns:
            str: MySQL DDL CREATE TABLE statement
            
        Raises:
            ValueError: If schema is invalid or empty
        """
        if not schema or not isinstance(schema, list):
            raise ValueError("Schema must be a non-empty list")
        
        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be empty")
        
        # Build table name with schema if provided
        full_table_name = f"`{schema_name}`.`{table_name}`" if schema_name else f"`{table_name}`"
        
        # Start building DDL
        ddl_parts = ["CREATE TABLE"]
        
        if if_not_exists:
            ddl_parts.append("IF NOT EXISTS")
        
        ddl_parts.append(full_table_name)
        ddl_parts.append("(")
        
        # Process columns
        column_definitions = []
        primary_keys = []
        unique_keys = []
        indexes = []
        
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
            col_def = [f"`{col_name}`"]
            
            # Map data type
            mysql_type = self._map_data_type(column['type'])
            col_def.append(mysql_type)
            
            # Handle constraints
            if not column.get('nullable', True):
                col_def.append("NOT NULL")
            
            if 'default' in column and column['default'] is not None:
                default_value = self._format_default_value(column['default'], column['type'])
                col_def.append(f"DEFAULT {default_value}")
            
            if column.get('auto_increment', False):
                col_def.append("AUTO_INCREMENT")
            
            if column.get('unique', False):
                unique_keys.append(col_name)
            
            if column.get('primary_key', False):
                primary_keys.append(col_name)
            
            if column.get('index', False):
                indexes.append(col_name)
            
            # Add column comment if provided
            if column.get('comment'):
                escaped_comment = column['comment'].replace("'", "''")
                col_def.append(f"COMMENT '{escaped_comment}'")
            
            column_definitions.append(" ".join(col_def))
        
        # Add primary key constraint if specified
        if primary_keys:
            pk_cols = ', '.join(f"`{pk}`" for pk in primary_keys)
            column_definitions.append(f"PRIMARY KEY ({pk_cols})")
        
        # Add unique constraints
        for unique_col in unique_keys:
            if unique_col not in primary_keys:  # Don't duplicate primary key columns
                column_definitions.append(f"UNIQUE KEY `uk_{unique_col}` (`{unique_col}`)")
        
        # Add indexes
        for index_col in indexes:
            if index_col not in primary_keys and index_col not in unique_keys:
                column_definitions.append(f"KEY `idx_{index_col}` (`{index_col}`)")
        
        # Combine all parts
        ddl = " ".join(ddl_parts[:3]) + "\n(\n    " + ",\n    ".join(column_definitions) + "\n)"
        
        # Add table options
        table_options = []
        table_options.append(f"ENGINE={engine}")
        table_options.append(f"DEFAULT CHARSET={charset}")
        
        if charset == 'utf8mb4':
            table_options.append("COLLATE=utf8mb4_unicode_ci")
        
        ddl += " " + " ".join(table_options)
        
        self.logger.debug(f"Generated DDL for table {table_name}: {ddl}")
        return ddl
    
    def _map_data_type(self, data_type: str) -> str:
        """
        Map generic data types to MySQL-specific types.
        
        Args:
            data_type: Generic data type string
            
        Returns:
            str: MySQL-specific data type
        """
        # Normalize input
        data_type = str(data_type).lower().strip()
        
        # MySQL type mappings
        type_mappings = {
            # String types
            'string': 'TEXT',
            'str': 'TEXT',
            'text': 'TEXT',
            'varchar': 'VARCHAR(255)',
            'char': 'CHAR(1)',
            
            # Numeric types
            'int': 'INT',
            'integer': 'INT',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'TINYINT',
            'mediumint': 'MEDIUMINT',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'decimal': 'DECIMAL(10,2)',
            'numeric': 'DECIMAL(10,2)',
            
            # Boolean type
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            
            # Date/time types
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'DATETIME',
            'timestamp': 'TIMESTAMP',
            'year': 'YEAR',
            
            # JSON type
            'json': 'JSON',
            
            # Binary types
            'binary': 'BLOB',
            'blob': 'BLOB',
            'longblob': 'LONGBLOB',
            'mediumblob': 'MEDIUMBLOB',
            'tinyblob': 'TINYBLOB',
            
            # Text types
            'longtext': 'LONGTEXT',
            'mediumtext': 'MEDIUMTEXT',
            'tinytext': 'TINYTEXT',
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
        Format default value for MySQL DDL.
        
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
        if data_type in ['string', 'str', 'text', 'varchar', 'char', 'tinytext', 'mediumtext', 'longtext']:
            # Escape single quotes and wrap in quotes
            escaped_value = str(default_value).replace("'", "''")
            return f"'{escaped_value}'"
        
        # Handle boolean types
        if data_type in ['bool', 'boolean']:
            return '1' if default_value else '0'
        
        # Handle numeric types
        if data_type in ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'mediumint', 
                        'float', 'double', 'decimal', 'numeric']:
            return str(default_value)
        
        # Handle date/time types
        if data_type in ['date', 'time', 'datetime', 'timestamp', 'year']:
            if str(default_value).upper() in ['NOW()', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME']:
                return str(default_value).upper()
            else:
                return f"'{default_value}'"
        
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
                     drop_if_exists: bool = False, engine: str = 'InnoDB',
                     charset: str = 'utf8mb4') -> bool:
        """
        Create a table in the MySQL database based on a schema definition.
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions
            schema_name: Schema name to create table in (optional)
            if_not_exists: Whether to use IF NOT EXISTS clause (ignored if drop_if_exists=True)
            drop_if_exists: Whether to drop the table if it already exists
            engine: MySQL storage engine (default: InnoDB)
            charset: Character set for the table (default: utf8mb4)
            
        Returns:
            bool: True if table was created successfully, False otherwise
            
        Raises:
            MySQLQueryError: If table creation fails
            ValueError: If schema is invalid
        """
        try:
            full_table_name = f"`{schema_name}`.`{table_name}`" if schema_name else f"`{table_name}`"
            
            # Check if table exists
            table_exists = self.table_exists(table_name, schema_name)
            
            if table_exists:
                if drop_if_exists:
                    self.logger.info(f"Dropping existing table {full_table_name}")
                    drop_ddl = f"DROP TABLE {full_table_name}"
                    self.execute_query(drop_ddl)
                elif not if_not_exists:
                    raise MySQLQueryError(f"Table {full_table_name} already exists")
                else:
                    self.logger.info(f"Table {full_table_name} already exists, skipping creation")
                    return True
            
            # Generate DDL
            use_if_not_exists = if_not_exists and not drop_if_exists
            ddl = self.schema_to_ddl(table_name, schema, schema_name, use_if_not_exists, engine, charset)
            
            # Execute DDL
            self.logger.info(f"Creating table {full_table_name}")
            self.execute_query(ddl)
            
            # Verify table was created
            if self.table_exists(table_name, schema_name):
                self.logger.info(f"Successfully created table {full_table_name}")
                return True
            else:
                raise MySQLQueryError(f"Table {full_table_name} was not created")
            
        except Exception as e:
            error_msg = f"Failed to create table {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
    
    def drop_table(self, table_name: str, schema_name: str = None, 
                   if_exists: bool = True, cascade: bool = False) -> bool:
        """
        Drop a table from the MySQL database.
        
        Args:
            table_name: Name of the table to drop
            schema_name: Schema name (optional)
            if_exists: Whether to use IF EXISTS clause
            cascade: Whether to use CASCADE option (Note: MySQL doesn't support CASCADE for DROP TABLE)
            
        Returns:
            bool: True if table was dropped successfully, False otherwise
            
        Raises:
            MySQLQueryError: If table drop fails
        """
        try:
            full_table_name = f"`{schema_name}`.`{table_name}`" if schema_name else f"`{table_name}`"
            
            # Build DROP statement
            drop_parts = ["DROP TABLE"]
            
            if if_exists:
                drop_parts.append("IF EXISTS")
            
            drop_parts.append(full_table_name)
            
            # Note: MySQL doesn't support CASCADE for DROP TABLE like PostgreSQL
            if cascade:
                self.logger.warning("CASCADE option is not supported for DROP TABLE in MySQL")
            
            drop_ddl = " ".join(drop_parts)
            
            self.logger.info(f"Dropping table {full_table_name}")
            self.execute_query(drop_ddl)
            
            self.logger.info(f"Successfully dropped table {full_table_name}")
            return True
            
        except Exception as e:
            error_msg = f"Failed to drop table {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)
    
    def get_table_info(self, table_name: str, schema_name: str = None) -> Dict[str, Any]:
        """
        Get detailed information about a MySQL table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (database name, defaults to current database)
            
        Returns:
            Dict containing table information including engine, charset, etc.
            
        Raises:
            MySQLQueryError: If table info retrieval fails
        """
        try:
            query = """
                SELECT 
                    TABLE_NAME as table_name,
                    ENGINE as engine,
                    TABLE_COLLATION as collation,
                    TABLE_ROWS as estimated_rows,
                    DATA_LENGTH as data_length,
                    INDEX_LENGTH as index_length,
                    AUTO_INCREMENT as auto_increment,
                    CREATE_TIME as create_time,
                    UPDATE_TIME as update_time,
                    TABLE_COMMENT as comment
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = %s
            """
            params = [table_name]
            
            if schema_name:
                query += " AND TABLE_SCHEMA = %s"
                params.append(schema_name)
            else:
                query += " AND TABLE_SCHEMA = DATABASE()"
            
            result = self.execute_query(query, params)
            
            if result:
                info = result[0]
                # Convert bytes to human-readable format
                if info['data_length']:
                    info['data_size_mb'] = round(info['data_length'] / (1024 * 1024), 2)
                if info['index_length']:
                    info['index_size_mb'] = round(info['index_length'] / (1024 * 1024), 2)
                
                self.logger.debug(f"Table info for {table_name}: {info}")
                return info
            else:
                raise MySQLQueryError(f"Table {table_name} not found")
            
        except Exception as e:
            error_msg = f"Failed to get table info for {table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise MySQLQueryError(error_msg)