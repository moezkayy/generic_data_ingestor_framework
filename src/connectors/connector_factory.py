"""
Database Connector Factory for the Generic Data Ingestor Framework.

This module provides a factory pattern implementation for creating and managing
database connectors with support for connection pooling and configuration validation.
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Type, Union
from pathlib import Path
import json

from .database_connector import DatabaseConnector
from .postgresql_connector import PostgreSQLConnector
from .mysql_connector import MySQLConnector
from .sqlite_connector import SQLiteConnector
from .connection_pool_manager import get_pool_manager, ConnectionPoolManager
from .db_config_schema import DatabaseConfigValidator


class ConnectorFactoryError(Exception):
    """Custom exception for connector factory errors."""
    pass


class DatabaseConnectorFactory:
    """
    Factory class for creating and managing database connectors.
    
    This factory provides a centralized way to create database connectors with
    proper configuration validation, connection pooling, and resource management.
    
    Attributes:
        supported_databases: Dictionary mapping database types to connector classes
        pool_manager: Connection pool manager instance
        logger: Logger for factory operations
    """
    
    # Supported database types and their corresponding connector classes
    SUPPORTED_DATABASES = {
        'postgresql': PostgreSQLConnector,
        'mysql': MySQLConnector,
        'sqlite': SQLiteConnector
    }
    
    def __init__(self, pool_manager: Optional[ConnectionPoolManager] = None):
        """
        Initialize the database connector factory.
        
        Args:
            pool_manager: Optional connection pool manager (uses global if None)
        """
        self.pool_manager = pool_manager or get_pool_manager()
        self.logger = logging.getLogger('data_ingestion.connector_factory')
        
        self.logger.info("Database connector factory initialized")
    
    def create_connector(self, db_type: str, connection_params: Dict[str, Any],
                        pool_name: str = None, use_pooling: bool = True,
                        validate_config: bool = True) -> DatabaseConnector:
        """
        Create a database connector instance.
        
        Args:
            db_type: Type of database (postgresql, mysql, sqlite)
            connection_params: Database connection parameters
            pool_name: Name for connection pool (auto-generated if None)
            use_pooling: Whether to use connection pooling
            validate_config: Whether to validate configuration
            
        Returns:
            DatabaseConnector: Configured database connector instance
            
        Raises:
            ConnectorFactoryError: If connector creation fails
            ValueError: If invalid parameters provided
        """
        try:
            # Normalize database type
            db_type = db_type.lower().strip()
            
            # Validate database type
            if db_type not in self.SUPPORTED_DATABASES:
                supported = ', '.join(self.SUPPORTED_DATABASES.keys())
                raise ConnectorFactoryError(f"Unsupported database type: {db_type}. Supported types: {supported}")
            
            # Validate configuration if requested
            if validate_config:
                connection_params = self._validate_configuration(db_type, connection_params)
            
            # Generate pool name if not provided
            if not pool_name:
                pool_name = self._generate_pool_name(db_type, connection_params)
            
            # Create connector with or without pooling
            if use_pooling:
                return self._create_pooled_connector(db_type, connection_params, pool_name)
            else:
                return self._create_direct_connector(db_type, connection_params)
                
        except Exception as e:
            error_msg = f"Failed to create {db_type} connector: {str(e)}"
            self.logger.error(error_msg)
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