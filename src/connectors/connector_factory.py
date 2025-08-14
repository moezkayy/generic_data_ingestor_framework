"""
Database Connector Factory Implementation.
Author: Moez Khan (SRN: 23097401)
FYP Project - University of Hertfordshire

Implements Factory pattern for database connector creation.
Supports extensibility while maintaining simplicity for FYP scope.
"""

import logging
from typing import Dict, Any, List
from .database_connector import DatabaseConnector
from .sqlite_connector import SQLiteConnector


class ConnectorFactoryError(Exception):
    """Custom exception for connector factory errors."""
    pass


class DatabaseConnectorFactory:
    """
    Factory class for creating database connectors.
    
    Design Pattern: Factory Method for extensible object creation
    Current Scope: SQLite only (deliberate FYP limitation)
    Future Extension: Easy addition of new database types
    Referenced in: Implementation section (page 19) - Extensibility design
    """
    
    # Supported database types - SQLite only for FYP scope
    # Architecture supports easy extension: just add new connector classes
    SUPPORTED_DATABASES = {
        'sqlite': SQLiteConnector
        # Future extensions:
        # 'postgresql': PostgreSQLConnector,
        # 'mysql': MySQLConnector
    }
    
    def __init__(self):
        """Initialize the database connector factory."""
        self.logger = logging.getLogger('data_ingestion.connector_factory')
        self.logger.info("Database connector factory initialized (SQLite only)")
    
    def create_connector(self, db_type: str, connection_params: Dict[str, Any]) -> DatabaseConnector:
        """
        Create a database connector instance.
        
        Args:
            db_type: Type of database ('sqlite' for FYP scope)
            connection_params: Database connection parameters
            
        Returns:
            DatabaseConnector: Configured database connector instance
            
        Raises:
            ConnectorFactoryError: If connector creation fails
            ValueError: If database type not supported
        """
        # Normalize database type for consistent handling
        db_type_normalized = db_type.lower().strip()
        
        # Validate database type against supported types
        if db_type_normalized not in self.SUPPORTED_DATABASES:
            supported = ', '.join(self.SUPPORTED_DATABASES.keys())
            raise ConnectorFactoryError(error_msg)
    
    def list_supported_databases(self) -> List[str]:
        """
        Get list of supported database types.
        
        Returns:
            List[str]: Currently supported database types
        """
        return list(self.SUPPORTED_DATABASES.keys())
    
    def create_sqlite_connector(self, database_path: str) -> SQLiteConnector:
        """
        Convenience method to create SQLite connector.
        
        Args:
            database_path: Path to SQLite database file
            
        Returns:
            SQLiteConnector: Configured SQLite connector instance
        """
        connection_params = {'database': database_path}
        return self.create_connector('sqlite', connection_params)


# Global factory instance for application use
_factory = None

def get_connector_factory() -> DatabaseConnectorFactory:
    """
    Get the global database connector factory instance.
    
    Returns:
        DatabaseConnectorFactory: Global factory instance
    """
    global _factory
    
    if _factory is None:
        _factory = DatabaseConnectorFactory()
    
    return _factory
