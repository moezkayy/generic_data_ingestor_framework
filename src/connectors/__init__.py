"""
Database connectors module for the Generic Data Ingestor Framework.

This module provides abstract and concrete database connector classes
for connecting to various database systems.
"""

from .database_connector import DatabaseConnector
from .postgresql_connector import PostgreSQLConnector
from .mysql_connector import MySQLConnector
from .sqlite_connector import SQLiteConnector
from .connection_pool_manager import ConnectionPoolManager, get_pool_manager, shutdown_pool_manager
from .connector_factory import DatabaseConnectorFactory, get_connector_factory

__all__ = [
    "DatabaseConnector", 
    "PostgreSQLConnector", 
    "MySQLConnector", 
    "SQLiteConnector",
    "ConnectionPoolManager",
    "get_pool_manager",
    "shutdown_pool_manager", 
    "DatabaseConnectorFactory",
    "get_connector_factory"
]