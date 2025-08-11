"""
Database connectors module for the Generic Data Ingestor Framework (Simplified FYP Version).

This module provides basic database connector classes for SQLite only.
Designed for educational purposes and final year project demonstration.
"""

from .database_connector import DatabaseConnector
from .sqlite_connector import SQLiteConnector
from .connector_factory import DatabaseConnectorFactory, get_connector_factory

__all__ = [
    "DatabaseConnector",
    "SQLiteConnector", 
    "DatabaseConnectorFactory",
    "get_connector_factory"
]