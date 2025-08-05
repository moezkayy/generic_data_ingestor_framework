"""
Database Connector Factory for the Generic Data Ingestor Framework.

This module provides a factory pattern implementation for creating and managing
database connectors with support for connection pooling, retry logic, timeout handling,
and comprehensive configuration validation.
"""

import logging
import threading
import time
import asyncio
from typing import Any, Dict, List, Optional, Type, Union, Callable
from pathlib import Path
import json
from functools import wraps
from enum import Enum

from .database_connector import DatabaseConnector
from .postgresql_connector import PostgreSQLConnector
from .mysql_connector import MySQLConnector
from .sqlite_connector import SQLiteConnector
from .connection_pool_manager import get_pool_manager, ConnectionPoolManager
from .db_config_schema import DatabaseConfigValidator


class ConnectorFactoryError(Exception):
    """Custom exception for connector factory errors."""
    pass


class RetryStrategy(Enum):
    """Retry strategy enumeration."""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    IMMEDIATE = "immediate"


class TimeoutError(Exception):
    """Custom exception for timeout errors."""
    pass


class RetryConfig:
    """Configuration for retry logic."""
    
    def __init__(self, 
                 max_attempts: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                 backoff_multiplier: float = 2.0,
                 jitter: bool = True,
                 retriable_exceptions: Optional[List[Type[Exception]]] = None):
        """
        Initialize retry configuration.
        
        Args:
            max_attempts: Maximum number of retry attempts (default: 3)
            base_delay: Base delay between retries in seconds (default: 1.0)
            max_delay: Maximum delay between retries in seconds (default: 60.0)
            strategy: Retry strategy to use (default: EXPONENTIAL_BACKOFF)
            backoff_multiplier: Multiplier for exponential backoff (default: 2.0)
            jitter: Whether to add random jitter to delays (default: True)
            retriable_exceptions: List of exceptions that should trigger retries
        """
        self.max_attempts = max(1, max_attempts)
        self.base_delay = max(0.0, base_delay)
        self.max_delay = max(base_delay, max_delay)
        self.strategy = strategy
        self.backoff_multiplier = max(1.0, backoff_multiplier)
        self.jitter = jitter
        self.retriable_exceptions = retriable_exceptions or [
            ConnectionError, 
            OSError, 
            TimeoutError,
            Exception  # Catch-all for unknown database connection issues
        ]
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt number.
        
        Args:
            attempt: Current attempt number (0-based)
            
        Returns:
            float: Delay in seconds
        """
        if self.strategy == RetryStrategy.IMMEDIATE:
            return 0.0
        elif self.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.base_delay * (attempt + 1)
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.base_delay * (self.backoff_multiplier ** attempt)
        else:
            delay = self.base_delay
        
        # Cap at max_delay
        delay = min(delay, self.max_delay)
        
        # Add jitter if enabled
        if self.jitter and delay > 0:
            import random
            delay = delay * (0.5 + random.random() * 0.5)  # 50%-100% of calculated delay
        
        return delay
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """
        Determine if an exception should trigger a retry.
        
        Args:
            exception: Exception that occurred
            attempt: Current attempt number (0-based)
            
        Returns:
            bool: True if should retry, False otherwise
        """
        if attempt >= self.max_attempts - 1:
            return False
        
        return any(isinstance(exception, exc_type) for exc_type in self.retriable_exceptions)


class TimeoutConfig:
    """Configuration for timeout handling."""
    
    def __init__(self,
                 connection_timeout: float = 30.0,
                 query_timeout: float = 300.0,
                 transaction_timeout: float = 600.0,
                 total_timeout: Optional[float] = None):
        """
        Initialize timeout configuration.
        
        Args:
            connection_timeout: Timeout for connection establishment (default: 30s)
            query_timeout: Timeout for individual queries (default: 300s)
            transaction_timeout: Timeout for transactions (default: 600s)
            total_timeout: Total timeout for all operations (optional)
        """
        self.connection_timeout = max(0.0, connection_timeout)
        self.query_timeout = max(0.0, query_timeout) if query_timeout else None
        self.transaction_timeout = max(0.0, transaction_timeout) if transaction_timeout else None
        self.total_timeout = max(0.0, total_timeout) if total_timeout else None


def with_retry_and_timeout(retry_config: Optional[RetryConfig] = None,
                          timeout_config: Optional[TimeoutConfig] = None):
    """
    Decorator to add retry logic and timeout handling to methods.
    
    Args:
        retry_config: Retry configuration (uses default if None)
        timeout_config: Timeout configuration (uses default if None)
        
    Returns:
        Decorated function with retry and timeout logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_cfg = retry_config or RetryConfig()
            timeout_cfg = timeout_config or TimeoutConfig()
            
            logger = logging.getLogger('data_ingestion.retry_handler')
            
            start_total_time = time.time()
            last_exception = None
            
            for attempt in range(retry_cfg.max_attempts):
                try:
                    # Check total timeout
                    if timeout_cfg.total_timeout:
                        elapsed = time.time() - start_total_time
                        if elapsed >= timeout_cfg.total_timeout:
                            raise TimeoutError(f"Total timeout of {timeout_cfg.total_timeout}s exceeded")
                    
                    # Execute the function
                    start_time = time.time()
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    
                    # Log successful execution
                    if attempt > 0:
                        logger.info(f"DB_RETRY_SUCCESS: Function {func.__name__} succeeded on attempt {attempt + 1}/{retry_cfg.max_attempts} after {execution_time:.3f}s")
                    
                    return result
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry
                    if not retry_cfg.should_retry(e, attempt):
                        logger.error(f"DB_RETRY_FAILED: Function {func.__name__} failed permanently: {str(e)}")
                        raise e
                    
                    # Calculate delay for next attempt
                    delay = retry_cfg.calculate_delay(attempt)
                    
                    logger.warning(f"DB_RETRY_ATTEMPT: Function {func.__name__} failed on attempt {attempt + 1}/{retry_cfg.max_attempts}: {str(e)}. Retrying in {delay:.2f}s")
                    
                    # Wait before retry
                    if delay > 0:
                        time.sleep(delay)
            
            # All retries exhausted
            logger.error(f"DB_RETRY_EXHAUSTED: Function {func.__name__} failed after {retry_cfg.max_attempts} attempts. Last error: {str(last_exception)}")
            raise last_exception
            
        return wrapper
    return decorator


class DatabaseConnectorFactory:
    """
    Enhanced factory class for creating and managing database connectors.
    
    This factory provides a centralized way to create database connectors with
    proper configuration validation, connection pooling, retry logic, timeout handling,
    and comprehensive resource management.
    
    Attributes:
        supported_databases: Dictionary mapping database types to connector classes
        pool_manager: Connection pool manager instance
        default_retry_config: Default retry configuration for all operations
        default_timeout_config: Default timeout configuration for all operations
        logger: Logger for factory operations
    """
    
    # Supported database types and their corresponding connector classes
    SUPPORTED_DATABASES = {
        'postgresql': PostgreSQLConnector,
        'mysql': MySQLConnector,
        'sqlite': SQLiteConnector
    }
    
    def __init__(self, 
                 pool_manager: Optional[ConnectionPoolManager] = None,
                 default_retry_config: Optional[RetryConfig] = None,
                 default_timeout_config: Optional[TimeoutConfig] = None):
        """
        Initialize the database connector factory.
        
        Args:
            pool_manager: Optional connection pool manager (uses global if None)
            default_retry_config: Default retry configuration for all operations
            default_timeout_config: Default timeout configuration for all operations
        """
        self.pool_manager = pool_manager or get_pool_manager()
        self.default_retry_config = default_retry_config or RetryConfig()
        self.default_timeout_config = default_timeout_config or TimeoutConfig()
        self.logger = logging.getLogger('data_ingestion.connector_factory')
        
        self.logger.info(f"Enhanced database connector factory initialized with retry (max_attempts={self.default_retry_config.max_attempts}) and timeout support")
    
    def create_connector(self, db_type: str, connection_params: Dict[str, Any],
                        pool_name: str = None, use_pooling: bool = True,
                        validate_config: bool = True,
                        retry_config: Optional[RetryConfig] = None,
                        timeout_config: Optional[TimeoutConfig] = None) -> DatabaseConnector:
        """
        Create a database connector instance with retry and timeout support.
        
        Args:
            db_type: Type of database (postgresql, mysql, sqlite)
            connection_params: Database connection parameters
            pool_name: Name for connection pool (auto-generated if None)
            use_pooling: Whether to use connection pooling
            validate_config: Whether to validate configuration
            retry_config: Retry configuration (uses factory default if None)
            timeout_config: Timeout configuration (uses factory default if None)
            
        Returns:
            DatabaseConnector: Configured database connector instance
            
        Raises:
            ConnectorFactoryError: If connector creation fails
            ValueError: If invalid parameters provided
        """
        # Use provided configs or factory defaults
        retry_cfg = retry_config or self.default_retry_config
        timeout_cfg = timeout_config or self.default_timeout_config
        
        @with_retry_and_timeout(retry_cfg, timeout_cfg)
        def _create_connector_with_retry():
            # Normalize database type
            db_type_normalized = db_type.lower().strip()
            
            # Validate database type
            if db_type_normalized not in self.SUPPORTED_DATABASES:
                supported = ', '.join(self.SUPPORTED_DATABASES.keys())
                raise ConnectorFactoryError(f"Unsupported database type: {db_type_normalized}. Supported types: {supported}")
            
            # Validate configuration if requested
            validated_params = connection_params
            if validate_config:
                validated_params = self._validate_configuration(db_type_normalized, connection_params)
            
            # Generate pool name if not provided
            pool_name_final = pool_name
            if not pool_name_final:
                pool_name_final = self._generate_pool_name(db_type_normalized, validated_params)
            
            # Create connector with or without pooling
            if use_pooling:
                connector = self._create_pooled_connector(db_type_normalized, validated_params, pool_name_final)
            else:
                connector = self._create_direct_connector(db_type_normalized, validated_params)
            
            # Wrap connector with enhanced functionality if needed
            if retry_cfg != self.default_retry_config or timeout_cfg != self.default_timeout_config:
                connector = EnhancedConnectorWrapper(connector, retry_cfg, timeout_cfg)
            
            self.logger.info(f"DB_CONNECTOR_CREATED: Successfully created {db_type_normalized} connector with retry/timeout support")
            return connector
        
        try:
            return _create_connector_with_retry()
        except Exception as e:
            error_msg = f"Failed to create {db_type} connector after retries: {str(e)}"
            self.logger.error(f"DB_CONNECTOR_FAILED: {error_msg}")
            raise ConnectorFactoryError(error_msg)
    
    def _validate_configuration(self, db_type: str, connection_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate database configuration parameters.
        
        Args:
            db_type: Type of database
            connection_params: Connection parameters to validate
            
        Returns:
            Dict: Validated and normalized connection parameters
            
        Raises:
            ValueError: If configuration is invalid
        """
        try:
            # Add db_type to params for validation
            params_with_type = connection_params.copy()
            params_with_type['db_type'] = db_type
            
            # Use the database config validator
            validated_params = DatabaseConfigValidator.validate_config(params_with_type)
            
            # Remove db_type as it's not needed in connection params
            validated_params.pop('db_type', None)
            
            self.logger.debug(f"Configuration validated for {db_type}")
            return validated_params
            
        except Exception as e:
            raise ValueError(f"Configuration validation failed for {db_type}: {str(e)}")
    
    def _generate_pool_name(self, db_type: str, connection_params: Dict[str, Any]) -> str:
        """
        Generate a unique pool name based on connection parameters.
        
        Args:
            db_type: Type of database
            connection_params: Connection parameters
            
        Returns:
            str: Generated pool name
        """
        # Create a unique identifier based on connection details
        host = connection_params.get('host', 'localhost')
        port = connection_params.get('port', '')
        database = connection_params.get('database', 'default')
        user = connection_params.get('user', connection_params.get('username', 'default'))
        
        # For SQLite, use the database path
        if db_type == 'sqlite':
            database_path = Path(connection_params.get('database', 'default.db'))
            pool_name = f"sqlite_{database_path.stem}_{abs(hash(str(database_path)))}"
        else:
            pool_name = f"{db_type}_{host}_{port}_{database}_{user}_{abs(hash(str(connection_params)))}"
        
        # Ensure pool name is not too long and contains only safe characters
        pool_name = pool_name.replace('/', '_').replace('\\', '_').replace(':', '_')[:64]
        
        return pool_name
    
    def _create_pooled_connector(self, db_type: str, connection_params: Dict[str, Any], 
                                pool_name: str) -> DatabaseConnector:
        """
        Create a connector with connection pooling.
        
        Args:
            db_type: Type of database
            connection_params: Connection parameters
            pool_name: Name for the connection pool
            
        Returns:
            DatabaseConnector: Pooled connector instance
        """
        # Extract pooling configuration
        max_connections = connection_params.pop('connection_pool_size', 10)
        min_connections = max(1, max_connections // 2)
        connection_timeout = connection_params.get('connection_timeout', 30)
        pool_timeout = connection_params.pop('pool_timeout', 300)
        
        # Create or get existing pool
        if not self.pool_manager.pools.get(pool_name):
            success = self.pool_manager.create_pool(
                pool_name=pool_name,
                db_type=db_type,
                connection_params=connection_params,
                max_connections=max_connections,
                min_connections=min_connections,
                connection_timeout=connection_timeout,
                pool_timeout=pool_timeout
            )
            
            if not success:
                raise ConnectorFactoryError(f"Failed to create connection pool: {pool_name}")
        
        # Return a wrapper that uses the pool
        return PooledConnectorWrapper(self.pool_manager, pool_name)
    
    def _create_direct_connector(self, db_type: str, connection_params: Dict[str, Any]) -> DatabaseConnector:
        """
        Create a direct connector without pooling.
        
        Args:
            db_type: Type of database
            connection_params: Connection parameters
            
        Returns:
            DatabaseConnector: Direct connector instance
        """
        connector_class = self.SUPPORTED_DATABASES[db_type]
        connector = connector_class(connection_params)
        
        self.logger.debug(f"Created direct {db_type} connector")
        return connector
    
    def create_from_config_file(self, config_file_path: str, pool_name: str = None,
                               use_pooling: bool = True) -> DatabaseConnector:
        """
        Create a connector from a configuration file.
        
        Args:
            config_file_path: Path to JSON configuration file
            pool_name: Name for connection pool
            use_pooling: Whether to use connection pooling
            
        Returns:
            DatabaseConnector: Configured connector instance
            
        Raises:
            ConnectorFactoryError: If configuration loading or connector creation fails
        """
        try:
            config = DatabaseConfigValidator.load_config_from_file(config_file_path)
            db_type = config.pop('db_type')
            
            return self.create_connector(
                db_type=db_type,
                connection_params=config,
                pool_name=pool_name,
                use_pooling=use_pooling,
                validate_config=False  # Already validated by load_config_from_file
            )
            
        except Exception as e:
            error_msg = f"Failed to create connector from config file {config_file_path}: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectorFactoryError(error_msg)
    
    def create_from_url(self, database_url: str, pool_name: str = None,
                       use_pooling: bool = True) -> DatabaseConnector:
        """
        Create a connector from a database URL.
        
        Args:
            database_url: Database connection URL
            pool_name: Name for connection pool
            use_pooling: Whether to use connection pooling
            
        Returns:
            DatabaseConnector: Configured connector instance
            
        Raises:
            ConnectorFactoryError: If URL parsing or connector creation fails
        """
        try:
            connection_params = self._parse_database_url(database_url)
            db_type = connection_params.pop('db_type')
            
            return self.create_connector(
                db_type=db_type,
                connection_params=connection_params,
                pool_name=pool_name,
                use_pooling=use_pooling
            )
            
        except Exception as e:
            error_msg = f"Failed to create connector from URL: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectorFactoryError(error_msg)
    
    def _parse_database_url(self, database_url: str) -> Dict[str, Any]:
        """
        Parse a database URL into connection parameters.
        
        Args:
            database_url: Database URL to parse
            
        Returns:
            Dict: Connection parameters
            
        Raises:
            ValueError: If URL format is invalid
        """
        try:
            from urllib.parse import urlparse, parse_qs
            
            parsed = urlparse(database_url)
            
            if not parsed.scheme:
                raise ValueError("Invalid database URL: missing scheme")
            
            # Map URL scheme to database type
            scheme_mapping = {
                'postgresql': 'postgresql',
                'postgres': 'postgresql',
                'mysql': 'mysql',
                'sqlite': 'sqlite'
            }
            
            db_type = scheme_mapping.get(parsed.scheme.lower())
            if not db_type:
                raise ValueError(f"Unsupported database scheme: {parsed.scheme}")
            
            # Build connection parameters
            params = {'db_type': db_type}
            
            if db_type == 'sqlite':
                # For SQLite, the path is the database
                params['database'] = parsed.path.lstrip('/')
            else:
                # For PostgreSQL and MySQL
                if parsed.hostname:
                    params['host'] = parsed.hostname
                if parsed.port:
                    params['port'] = parsed.port
                if parsed.username:
                    params['user'] = parsed.username
                if parsed.password:
                    params['password'] = parsed.password
                if parsed.path and parsed.path != '/':
                    params['database'] = parsed.path.lstrip('/')
                
                # Parse query parameters
                if parsed.query:
                    query_params = parse_qs(parsed.query)
                    for key, values in query_params.items():
                        if values:
                            params[key] = values[0]
            
            return params
            
        except Exception as e:
            raise ValueError(f"Failed to parse database URL: {str(e)}")
    
    def get_pool_manager(self) -> ConnectionPoolManager:
        """
        Get the connection pool manager.
        
        Returns:
            ConnectionPoolManager: Pool manager instance
        """
        return self.pool_manager
    
    def list_supported_databases(self) -> List[str]:
        """
        Get list of supported database types.
        
        Returns:
            List[str]: Supported database types
        """
        return list(self.SUPPORTED_DATABASES.keys())
    
    def health_check(self, pool_name: str = None) -> Dict[str, Any]:
        """
        Perform health check on connection pools.
        
        Args:
            pool_name: Specific pool name (None for all pools)
            
        Returns:
            Dict: Health check results
        """
        return self.pool_manager.health_check(pool_name)
    
    def create_connector_with_config(self, config: Dict[str, Any]) -> DatabaseConnector:
        """
        Create a connector from a comprehensive configuration dictionary.
        
        Args:
            config: Configuration dictionary containing all necessary parameters
                   Expected keys: db_type, connection_params, retry_config, timeout_config, etc.
                   
        Returns:
            DatabaseConnector: Configured connector instance
            
        Raises:
            ConnectorFactoryError: If connector creation fails
        """
        try:
            db_type = config.get('db_type')
            if not db_type:
                raise ValueError("db_type is required in configuration")
            
            connection_params = config.get('connection_params', {})
            
            # Extract retry configuration
            retry_config = None
            retry_settings = config.get('retry_config', {})
            if retry_settings:
                retry_config = RetryConfig(
                    max_attempts=retry_settings.get('max_attempts', 3),
                    base_delay=retry_settings.get('base_delay', 1.0),
                    max_delay=retry_settings.get('max_delay', 60.0),
                    strategy=RetryStrategy(retry_settings.get('strategy', 'exponential_backoff')),
                    backoff_multiplier=retry_settings.get('backoff_multiplier', 2.0),
                    jitter=retry_settings.get('jitter', True)
                )
            
            # Extract timeout configuration
            timeout_config = None
            timeout_settings = config.get('timeout_config', {})
            if timeout_settings:
                timeout_config = TimeoutConfig(
                    connection_timeout=timeout_settings.get('connection_timeout', 30.0),
                    query_timeout=timeout_settings.get('query_timeout', 300.0),
                    transaction_timeout=timeout_settings.get('transaction_timeout', 600.0),
                    total_timeout=timeout_settings.get('total_timeout')
                )
            
            return self.create_connector(
                db_type=db_type,
                connection_params=connection_params,
                pool_name=config.get('pool_name'),
                use_pooling=config.get('use_pooling', True),
                validate_config=config.get('validate_config', True),
                retry_config=retry_config,
                timeout_config=timeout_config
            )
            
        except Exception as e:
            error_msg = f"Failed to create connector from configuration: {str(e)}"
            self.logger.error(f"DB_CONNECTOR_CONFIG_FAILED: {error_msg}")
            raise ConnectorFactoryError(error_msg)


class EnhancedConnectorWrapper:
    """
    Wrapper class that adds retry and timeout functionality to any DatabaseConnector.
    
    This wrapper intercepts method calls to the underlying connector and applies
    retry logic and timeout handling as configured.
    """
    
    def __init__(self, connector: DatabaseConnector, 
                 retry_config: RetryConfig, 
                 timeout_config: TimeoutConfig):
        """
        Initialize the enhanced connector wrapper.
        
        Args:
            connector: The underlying database connector
            retry_config: Retry configuration to apply
            timeout_config: Timeout configuration to apply
        """
        self._connector = connector
        self._retry_config = retry_config
        self._timeout_config = timeout_config
        self.logger = logging.getLogger('data_ingestion.enhanced_connector')
        
        # Methods that should have retry/timeout applied
        self._enhanced_methods = {
            'connect', 'disconnect', 'execute_query', 
            'begin_transaction', 'commit_transaction', 'rollback_transaction',
            'table_exists', 'get_table_schema', 'create_table', 'insert_data'
        }
    
    def __getattr__(self, name):
        """
        Delegate attribute access to the underlying connector with optional enhancement.
        
        Args:
            name: Attribute name
            
        Returns:
            Enhanced method or direct attribute from underlying connector
        """
        attr = getattr(self._connector, name)
        
        # If it's a method that should be enhanced, wrap it
        if callable(attr) and name in self._enhanced_methods:
            @with_retry_and_timeout(self._retry_config, self._timeout_config)
            def enhanced_method(*args, **kwargs):
                return attr(*args, **kwargs)
            
            # Preserve method metadata
            enhanced_method.__name__ = name
            enhanced_method.__doc__ = getattr(attr, '__doc__', None)
            return enhanced_method
        
        return attr
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information including enhancement details."""
        info = self._connector.get_connection_info()
        info.update({
            'enhanced': True,
            'retry_config': {
                'max_attempts': self._retry_config.max_attempts,
                'strategy': self._retry_config.strategy.value,
                'base_delay': self._retry_config.base_delay,
                'max_delay': self._retry_config.max_delay
            },
            'timeout_config': {
                'connection_timeout': self._timeout_config.connection_timeout,
                'query_timeout': self._timeout_config.query_timeout,
                'transaction_timeout': self._timeout_config.transaction_timeout,
                'total_timeout': self._timeout_config.total_timeout
            }
        })
        return info


class PooledConnectorWrapper:
    """
    Wrapper class that provides DatabaseConnector interface using connection pooling.
    
    This wrapper delegates all database operations to the actual connector obtained
    from the connection pool, ensuring proper resource management and connection reuse.
    """
    
    def __init__(self, pool_manager: ConnectionPoolManager, pool_name: str):
        """
        Initialize the pooled connector wrapper.
        
        Args:
            pool_manager: Connection pool manager
            pool_name: Name of the connection pool to use
        """
        self.pool_manager = pool_manager
        self.pool_name = pool_name
        self.logger = logging.getLogger('data_ingestion.pooled_connector')
    
    def __getattr__(self, name):
        """
        Delegate attribute access to the pooled connector.
        
        Args:
            name: Attribute name
            
        Returns:
            Attribute value or method from pooled connector
        """
        # Get a connection from the pool and delegate the method call
        def method_wrapper(*args, **kwargs):
            with self.pool_manager.get_connection(self.pool_name) as connector:
                method = getattr(connector, name)
                return method(*args, **kwargs)
        
        return method_wrapper
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information including pool details."""
        with self.pool_manager.get_connection(self.pool_name) as connector:
            info = connector.get_connection_info()
            
            # Add pool information
            pool_metrics = self.pool_manager.get_pool_metrics(self.pool_name)
            pool_status = self.pool_manager.get_pool_status(self.pool_name)
            
            info.update({
                'pool_name': self.pool_name,
                'pool_status': pool_status.value,
                'pool_metrics': {
                    'total_connections': pool_metrics.total_connections,
                    'active_connections': pool_metrics.active_connections,
                    'total_requests': pool_metrics.total_requests,
                    'successful_requests': pool_metrics.successful_requests,
                    'failed_requests': pool_metrics.failed_requests,
                    'average_response_time': pool_metrics.average_response_time
                }
            })
            
            return info


# Global factory instance
_factory = None
_factory_lock = threading.Lock()


def get_connector_factory() -> DatabaseConnectorFactory:
    """
    Get the global database connector factory instance.
    
    Returns:
        DatabaseConnectorFactory: Global factory instance
    """
    global _factory
    
    if _factory is None:
        with _factory_lock:
            if _factory is None:
                _factory = DatabaseConnectorFactory()
    
    return _factory