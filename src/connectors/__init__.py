"""
Database connectors module for the Generic Data Ingestor Framework.

This module provides abstract and concrete database connector classes
for connecting to various database systems.
"""

from .database_connector import DatabaseConnector

# Import connectors conditionally to avoid dependency issues
_available_connectors = {}

try:
    from .postgresql_connector import PostgreSQLConnector
    _available_connectors['PostgreSQLConnector'] = PostgreSQLConnector
except ImportError:
    PostgreSQLConnector = None

try:
    from .mysql_connector import MySQLConnector
    _available_connectors['MySQLConnector'] = MySQLConnector
except ImportError:
    MySQLConnector = None

try:
    from .sqlite_connector import SQLiteConnector
    _available_connectors['SQLiteConnector'] = SQLiteConnector
except ImportError:
    SQLiteConnector = None

try:
    from .connection_pool_manager import ConnectionPoolManager, get_pool_manager, shutdown_pool_manager
    _available_connectors.update({
        'ConnectionPoolManager': ConnectionPoolManager,
        'get_pool_manager': get_pool_manager,
        'shutdown_pool_manager': shutdown_pool_manager
    })
except ImportError:
    ConnectionPoolManager = get_pool_manager = shutdown_pool_manager = None

try:
    from .connector_factory import DatabaseConnectorFactory, get_connector_factory
    _available_connectors.update({
        'DatabaseConnectorFactory': DatabaseConnectorFactory,
        'get_connector_factory': get_connector_factory
    })
except ImportError:
    DatabaseConnectorFactory = get_connector_factory = None

# Dynamic __all__ based on available imports
__all__ = ["DatabaseConnector"] + list(_available_connectors.keys())