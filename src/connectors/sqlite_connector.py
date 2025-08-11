"""
Simple SQLite Database Connector for FYP.
Basic implementation without production complexity.
"""

import sqlite3
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path

from .database_connector import DatabaseConnector


class SQLiteConnector(DatabaseConnector):
    """
    Simple SQLite database connector for educational purposes.
    """

    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize SQLite connector.
        
        Args:
            connection_params: Dictionary containing 'database' key with path to SQLite file
        """
        super().__init__(connection_params)
        self.db_path = connection_params.get('database', 'default.db')
        self.connection = None
        self.logger = logging.getLogger('data_ingestion.sqlite_connector')
        
    def connect(self) -> bool:
        """Connect to SQLite database."""
        try:
            # Create directory if it doesn't exist
            db_path = Path(self.db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Enable dict-like row access
            self.logger.info(f"Connected to SQLite database: {self.db_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to SQLite database: {str(e)}")
            return False

    def disconnect(self) -> bool:
        """Disconnect from SQLite database."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.logger.info("Disconnected from SQLite database")
            return True
            
        except Exception as e:
            self.logger.error(f"Error disconnecting from SQLite: {str(e)}")
            return False

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        if not self.connection:
            if not self.connect():
                return []
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # For SELECT queries, fetch results
            if query.strip().upper().startswith('SELECT'):
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            else:
                # For other queries, commit and return row count
                self.connection.commit()
                return [{'rows_affected': cursor.rowcount}]
                
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return []

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = self.execute_query(query, (table_name,))
        return len(result) > 0

    def create_table(self, table_name: str, schema: List[Dict[str, Any]]) -> bool:
        """Create a table with given schema."""
        try:
            # Build column definitions
            columns = []
            for column in schema:
                col_name = column['name']
                col_type = column.get('type', 'TEXT').upper()
                nullable = '' if column.get('nullable', True) else ' NOT NULL'
                columns.append(f'"{col_name}" {col_type}{nullable}')
            
            # Create table query
            columns_sql = ', '.join(columns)
            query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql})'
            
            result = self.execute_query(query)
            success = len(result) > 0
            
            if success:
                self.logger.info(f"Created table '{table_name}' with {len(schema)} columns")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to create table '{table_name}': {str(e)}")
            return False

    def insert_data(self, table_name: str, data: List[Dict[str, Any]], 
                   batch_size: int = 1000) -> int:
        """Insert data into table."""
        if not data:
            return 0
        
        try:
            if not self.connection:
                if not self.connect():
                    return 0
            
            # Get column names from first record
            columns = list(data[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            column_names = ', '.join([f'"{col}"' for col in columns])
            
            query = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
            
            cursor = self.connection.cursor()
            total_inserted = 0
            
            # Insert in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                batch_values = []
                
                for record in batch:
                    row_values = [record.get(col) for col in columns]
                    batch_values.append(row_values)
                
                cursor.executemany(query, batch_values)
                total_inserted += cursor.rowcount
            
            self.connection.commit()
            self.logger.info(f"Inserted {total_inserted} records into '{table_name}'")
            return total_inserted
            
        except Exception as e:
            self.logger.error(f"Failed to insert data into '{table_name}': {str(e)}")
            if self.connection:
                self.connection.rollback()
            return 0

    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information."""
        return {
            'db_type': 'sqlite',
            'database': self.db_path,
            'connected': self.connection is not None
        }

    # Required abstract methods (simplified implementations)
    def begin_transaction(self) -> bool:
        """Begin transaction - auto-handled by SQLite."""
        return True

    def commit_transaction(self) -> bool:
        """Commit transaction."""
        try:
            if self.connection:
                self.connection.commit()
            return True
        except:
            return False

    def rollback_transaction(self) -> bool:
        """Rollback transaction."""
        try:
            if self.connection:
                self.connection.rollback()
            return True
        except:
            return False

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema information."""
        query = f"PRAGMA table_info('{table_name}')"
        result = self.execute_query(query)
        
        schema = []
        for row in result:
            schema.append({
                'column_name': row['name'],
                'data_type': row['type'],
                'nullable': not row['notnull'],
                'default': row['dflt_value']
            })
        
        return schema

    def execute_batch(self, query: str, params_list: List[tuple]) -> int:
        """Execute query with multiple parameter sets."""
        if not self.connection:
            if not self.connect():
                return 0
        
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, params_list)
            self.connection.commit()
            return cursor.rowcount
            
        except Exception as e:
            self.logger.error(f"Batch execution failed: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return 0