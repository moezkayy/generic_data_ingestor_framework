"""
Abstract Database Connector for the Generic Data Ingestor Framework.

This module provides an abstract base class for database connections,
defining a standard interface for all database connector implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Tuple
import logging


class DatabaseConnector(ABC):
    """
    Abstract base class for database connections.
    
    This class defines the standard interface that all database connector
    implementations must follow. It provides methods for connecting to a database,
    executing queries, and managing transactions.
    
    Attributes:
        logger: Logger instance for the connector
        connection_params: Dictionary containing connection parameters
        connection: The active database connection object
        is_connected: Boolean indicating if the connection is active
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the database connector with connection parameters.
        
        Args:
            connection_params: Dictionary containing connection parameters such as
                              host, port, database name, username, password, etc.
        """
        self.logger = logging.getLogger('data_ingestion.database_connector')
        self.connection_params = connection_params
        self.connection = None
        self.is_connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish a connection to the database.
        
        Returns:
            bool: True if connection was successful, False otherwise
        
        Raises:
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Close the database connection.
        
        Returns:
            bool: True if disconnection was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Union[List, Dict]] = None) -> Any:
        """
        Execute a query on the database.
        
        Args:
            query: SQL query string to execute
            params: Parameters to bind to the query
            
        Returns:
            Query results
            
        Raises:
            ConnectionError: If not connected to the database
            QueryError: If query execution fails
        """
        pass
    
    @abstractmethod
    def begin_transaction(self) -> bool:
        """
        Begin a database transaction.
        
        Returns:
            bool: True if transaction was successfully started, False otherwise
        """
        pass
    
    @abstractmethod
    def commit_transaction(self) -> bool:
        """
        Commit the current transaction.
        
        Returns:
            bool: True if transaction was successfully committed, False otherwise
        """
        pass
    
    @abstractmethod
    def rollback_transaction(self) -> bool:
        """
        Rollback the current transaction.
        
        Returns:
            bool: True if transaction was successfully rolled back, False otherwise
        """
        pass
    
    @abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current database connection.
        
        Returns:
            Dict containing connection information such as database type,
            server version, connection status, etc.
        """
        pass
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Test the database connection.
        
        Returns:
            Tuple containing:
                - bool: True if connection test was successful, False otherwise
                - Optional[str]: Error message if connection test failed, None otherwise
        """
        try:
            if not self.is_connected:
                self.connect()
            
            # Get connection info to verify connection is working
            connection_info = self.get_connection_info()
            
            return True, None
        except Exception as e:
            error_message = f"Connection test failed: {str(e)}"
            self.logger.error(error_message)
            return False, error_message
    
    def __enter__(self):
        """
        Context manager entry point - connect to the database.
        
        Returns:
            Self for use in with statement
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point - disconnect from the database.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        self.disconnect()