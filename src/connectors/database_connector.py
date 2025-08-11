"""
Simple Database Connector Base Class for FYP.
Basic abstract base class without production complexity.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class DatabaseConnector(ABC):
    """
    Simple abstract base class for database connectors.
    Designed for educational purposes with minimal required methods.
    """

    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the database connector.
        
        Args:
            connection_params: Dictionary containing connection parameters
        """
        self.connection_params = connection_params

    @abstractmethod
    def connect(self) -> bool:
        """
        Connect to the database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """
        Disconnect from the database.
        
        Returns:
            bool: True if disconnection successful, False otherwise
        """
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            List of dictionaries representing query results
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def create_table(self, table_name: str, schema: List[Dict[str, Any]]) -> bool:
        """
        Create a table with the given schema.
        
        Args:
            table_name: Name of the table to create
            schema: List of column definitions
            
        Returns:
            bool: True if table creation successful, False otherwise
        """
        pass

    @abstractmethod
    def insert_data(self, table_name: str, data: List[Dict[str, Any]], 
                   batch_size: int = 1000) -> int:
        """
        Insert data into a table.
        
        Args:
            table_name: Name of the table
            data: List of records to insert
            batch_size: Number of records to insert per batch
            
        Returns:
            int: Number of records successfully inserted
        """
        pass

    @abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the database connection.
        
        Returns:
            Dictionary containing connection detai  ls
        """
        pass