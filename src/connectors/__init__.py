"""
Database connectors module for the Generic Data Ingestor Framework.

This module provides abstract and concrete database connector classes
for connecting to various database systems.
"""

from .database_connector import DatabaseConnector

__all__ = ["DatabaseConnector"]