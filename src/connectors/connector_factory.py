"""
Simple Database Connector Factory for FYP.
Supports SQLite only with basic functionality.
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
    Simple factory class for creating SQLite database connectors.
    Designed for educational purposes and FYP demonstration.
    """
    
    # Only SQLite is supported in the simplified version
    SUPPORTED_DATABASES = {
        'sqlite': SQLiteConnector
    }
    
    def __init__(self):
        """Initialize the database connector factory."""
        self.logger = logging.getLogger('data_ingestion.connector_factory')
        self.logger.info("Simple database connector factory initialized (SQLite only)")
    
    def create_connector(self, db_type: str, connection_params: Dict[str, Any]) -> DatabaseConnector:
        """
        Create a database connector instance.
        
        Args:
            db_type: Type of database (only 'sqlite' supported)
            connection_params: Database connection parameters
            
        Returns:
            DatabaseConnector: Configured database connector instance
            
        Raises:
            ConnectorFactoryError: If connector creation fails
            ValueError: If invalid parameters provided
        """
        # Normalize database type
        db_type_normalized = db_type.lower().strip()
        
        # Validate database type
        if db_type_normalized not in self.SUPPORTED_DATABASES:
            supported = ', '.join(self.SUPPORTED_DATABASES.keys())
            raise ConnectorFactoryError(f"Unsupported database type: {db_type_normalized}. Supported types: {supported}")
        
        try:
            # Create the connector
            connector_class = self.SUPPORTED_DATABASES[db_type_normalized]
            connector = connector_class(connection_params)
            
            self.logger.info(f"Successfully created {db_type_normalized} connector")
            return connector
            
        except Exception as e:
            error_msg = f"Failed to create {db_type} connector: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectorFactoryError(error_msg)
    
    def list_supported_databases(self) -> List[str]:
        """
        Get list of supported database types.
        
        Returns:
            List[str]: Supported database types
        """
        return list(self.SUPPORTED_DATABASES.keys())
    
    def create_sqlite_connector(self, database_path: str) -> SQLiteConnector:
        """
        Convenience method to create SQLite connector.
        
        Args:
            database_path: Path to SQLite database file
            
        Returns:
            SQLiteConnector: SQLite connector instance
        """
        connection_params = {'database': database_path}
        return self.create_connector('sqlite', connection_params)


# Global factory instance
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