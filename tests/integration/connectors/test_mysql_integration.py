"""
Integration tests for MySQL connector.

These tests require a running MySQL database and will be skipped
if the database is not available.
"""

import pytest
import os
from typing import Dict, Any, List

from src.connectors.mysql_connector import MySQLConnector, MySQLConnectionError


# Skip all tests if MySQL is not available
def is_mysql_available():
    """Check if MySQL is available for testing."""
    test_params = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'database': os.getenv('MYSQL_DB', 'test_db'),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', 'mysql')
    }
    
    try:
        connector = MySQLConnector(test_params)
        with connector:
            connector.execute_query("SELECT 1")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not is_mysql_available(),
    reason="MySQL database not available"
)


@pytest.mark.integration
@pytest.mark.mysql
class TestMySQLIntegration:
    """Integration tests for MySQL connector."""
    
    @pytest.fixture
    def db_params(self) -> Dict[str, Any]:
        """MySQL connection parameters."""
        return {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', '3306')),
            'database': os.getenv('MYSQL_DB', 'test_db'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', 'mysql'),
            'connection_pool_size': 3,
            'connection_timeout': 30
        }
    
    @pytest.fixture
    def connector(self, db_params) -> MySQLConnector:
        """Create MySQL connector."""
        connector = MySQLConnector(db_params)
        yield connector
        
        # Cleanup
        if connector.is_connected:
            connector.disconnect()
    
    @pytest.fixture
    def sample_schema(self) -> List[Dict[str, Any]]:
        """Sample table schema for testing."""
        return [
            {
                'name': 'id',
                'type': 'integer',
                'primary_key': True,
                'auto_increment': True,
                'nullable': False
            },
            {
                'name': 'name',
                'type': 'varchar',
                'nullable': False
            },
            {
                'name': 'email',
                'type': 'varchar',
                'nullable': True,
                'unique': True
            },
            {
                'name': 'age',
                'type': 'integer',
                'nullable': True
            },
            {
                'name': 'is_active',
                'type': 'boolean',
                'nullable': False,
                'default': True
            },
            {
                'name': 'created_at',
                'type': 'timestamp',
                'nullable': False,
                'default': 'CURRENT_TIMESTAMP'
            }
        ]
    
    @pytest.fixture
    def sample_data(self) -> List[Dict[str, Any]]:
        """Sample data for testing."""
        return [
            {
                'name': 'John Doe',
                'email': 'john@example.com',
                'age': 30,
                'is_active': True
            },
            {
                'name': 'Jane Smith',
                'email': 'jane@example.com',
                'age': 25,
                'is_active': False
            },
            {
                'name': 'Bob Johnson',
                'email': 'bob@example.com',
                'age': 35,
                'is_active': True
            }
        ]
    
    def cleanup_table(self, connector: MySQLConnector, table_name: str):
        """Helper to cleanup test table."""
        try:
            if connector.is_connected and connector.table_exists(table_name):
                connector.drop_table(table_name, if_exists=True)
        except Exception:
            pass  # Ignore cleanup errors
    
    def test_connection_lifecycle(self, connector):
        """Test connection and disconnection lifecycle."""
        # Initially not connected
        assert not connector.is_connected
        
        # Connect
        result = connector.connect()
        assert result is True
        assert connector.is_connected
        
        # Connection info
        info = connector.get_connection_info()
        assert info['db_type'] == 'mysql'
        assert info['is_connected'] is True
        assert 'server_version' in info
        
        # Disconnect
        result = connector.disconnect()
        assert result is True
        assert not connector.is_connected
    
    def test_context_manager(self, connector):
        """Test context manager functionality."""
        assert not connector.is_connected
        
        with connector:
            assert connector.is_connected
            
            # Execute a simple query
            result = connector.execute_query("SELECT 1 as test")
            assert len(result) == 1
            assert result[0]['test'] == 1
            
        assert not connector.is_connected
    
    def test_table_operations(self, connector, sample_schema):
        """Test table creation, existence check, and schema retrieval."""
        table_name = 'test_users_mysql'
        
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            # Table shouldn't exist initially
            assert not connector.table_exists(table_name)
            
            # Create table
            success = connector.create_table(table_name, sample_schema)
            assert success is True
            
            # Table should now exist
            assert connector.table_exists(table_name)
            
            # Get table schema
            schema = connector.get_table_schema(table_name)
            assert len(schema) > 0
            
            # Verify key columns exist
            column_names = [col['column_name'] for col in schema]
            assert 'id' in column_names
            assert 'name' in column_names
            assert 'email' in column_names
            
            # Drop table
            drop_success = connector.drop_table(table_name)
            assert drop_success is True
            
            # Table shouldn't exist after dropping
            assert not connector.table_exists(table_name)
    
    def test_data_insertion_and_retrieval(self, connector, sample_schema, sample_data):
        """Test data insertion and retrieval operations."""
        table_name = 'test_users_data_mysql'
        
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            try:
                # Insert single row
                insert_sql = f"""
                    INSERT INTO {table_name} (name, email, age, is_active) 
                    VALUES (%s, %s, %s, %s)
                """
                params = ['Single User', 'single@example.com', 28, True]
                result = connector.execute_query(insert_sql, params)
                assert result == 1
                
                # Insert multiple rows using batch
                batch_params = [
                    [data['name'], data['email'], data['age'], data['is_active']]
                    for data in sample_data
                ]
                batch_result = connector.execute_batch(insert_sql, batch_params)
                assert batch_result == len(sample_data)
                
                # Query all data
                select_sql = f"SELECT * FROM {table_name} ORDER BY id"
                results = connector.execute_query(select_sql)
                assert len(results) == len(sample_data) + 1  # +1 for single insert
                
                # Verify data integrity
                first_user = results[0]
                assert first_user['name'] == 'Single User'
                assert first_user['email'] == 'single@example.com'
                assert first_user['age'] == 28
                assert first_user['is_active'] == 1  # MySQL stores boolean as tinyint
                
                # Query with conditions
                active_users_sql = f"SELECT * FROM {table_name} WHERE is_active = %s"
                active_users = connector.execute_query(active_users_sql, [1])
                active_count = len([u for u in results if u['is_active'] == 1])
                assert len(active_users) == active_count
                
            finally:
                # Cleanup
                self.cleanup_table(connector, table_name)
    
    def test_transaction_management(self, connector, sample_schema):
        """Test transaction begin, commit, and rollback."""
        table_name = 'test_transactions_mysql'
        
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            try:
                # Test successful transaction
                connector.begin_transaction()
                
                insert_sql = f"INSERT INTO {table_name} (name, email) VALUES (%s, %s)"
                connector.execute_query(insert_sql, ['User 1', 'user1@example.com'])
                connector.execute_query(insert_sql, ['User 2', 'user2@example.com'])
                
                # Commit transaction
                connector.commit_transaction()
                
                # Verify data was committed
                count_sql = f"SELECT COUNT(*) as count FROM {table_name}"
                result = connector.execute_query(count_sql)
                assert result[0]['count'] == 2
                
                # Test rollback
                connector.begin_transaction()
                
                connector.execute_query(insert_sql, ['User 3', 'user3@example.com'])
                
                # Rollback transaction
                connector.rollback_transaction()
                
                # Verify data was rolled back
                result = connector.execute_query(count_sql)
                assert result[0]['count'] == 2  # Should still be 2, not 3
                
            finally:
                # Cleanup
                self.cleanup_table(connector, table_name)
    
    def test_error_handling(self, connector, sample_schema):
        """Test error handling for various scenarios."""
        table_name = 'test_errors_mysql'
        
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            try:
                # Test duplicate key error (email is unique)
                insert_sql = f"INSERT INTO {table_name} (name, email) VALUES (%s, %s)"
                connector.execute_query(insert_sql, ['User 1', 'test@example.com'])
                
                # This should raise an integrity error
                with pytest.raises(Exception):  # MySQLQueryError
                    connector.execute_query(insert_sql, ['User 2', 'test@example.com'])
                
                # Test syntax error
                with pytest.raises(Exception):  # MySQLQueryError
                    connector.execute_query("INVALID SQL SYNTAX")
                
                # Test query on non-existent table
                with pytest.raises(Exception):
                    connector.execute_query("SELECT * FROM non_existent_table")
                    
            finally:
                # Cleanup
                self.cleanup_table(connector, table_name)
    
    def test_connection_info_details(self, connector):
        """Test detailed connection information."""
        with connector:
            info = connector.get_connection_info()
            
            # Basic info
            assert info['db_type'] == 'mysql'
            assert info['is_connected'] is True
            assert info['host'] == connector.connection_params['host']
            assert info['port'] == connector.connection_params['port']
            assert info['database'] == connector.connection_params['database']
            
            # Extended info (if available)
            extended_fields = [
                'server_version', 'current_database',
                'active_connections', 'character_set'
            ]
            
            for field in extended_fields:
                if field in info:
                    assert info[field] is not None
    
    def test_schema_caching(self, connector, sample_schema):
        """Test schema caching functionality."""
        table_name = 'cache_test_mysql'
        
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            try:
                # Clear cache to start fresh
                connector.clear_schema_cache()
                
                # First call should query database
                schema1 = connector.get_table_schema(table_name)
                
                # Second call should use cache
                schema2 = connector.get_table_schema(table_name)
                
                # Results should be identical
                assert schema1 == schema2
                
                # Check cache stats
                stats = connector.get_schema_cache_stats()
                assert stats['enabled'] is True
                assert stats['total_entries'] > 0
                
                # Test cache invalidation
                connector._invalidate_table_cache(table_name)
                
                # Next call should query database again
                schema3 = connector.get_table_schema(table_name)
                assert schema3 == schema1
                
            finally:
                # Cleanup
                self.cleanup_table(connector, table_name)
    
    def test_connection_pooling(self, db_params):
        """Test connection pooling functionality."""
        # Create connector with small pool
        db_params['connection_pool_size'] = 2
        connector = MySQLConnector(db_params)
        
        try:
            with connector:
                # Execute multiple concurrent-like operations
                results = []
                for i in range(5):
                    result = connector.execute_query(f"SELECT {i} as test_value")
                    results.append(result[0]['test_value'])
                
                # Verify all operations completed successfully
                assert results == [0, 1, 2, 3, 4]
                
        finally:
            if connector.is_connected:
                connector.disconnect()
    
    def test_large_data_handling(self, connector, sample_schema):
        """Test handling of larger datasets."""
        table_name = 'test_large_data_mysql'
        
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            # Create table
            connector.create_table(table_name, sample_schema)
            
            try:
                # Generate larger dataset
                large_data = []
                for i in range(100):
                    large_data.append({
                        'name': f'User {i}',
                        'email': f'user{i}@example.com',
                        'age': 20 + (i % 50),
                        'is_active': i % 2 == 0
                    })
                
                # Insert data in batches
                insert_sql = f"""
                    INSERT INTO {table_name} (name, email, age, is_active) 
                    VALUES (%s, %s, %s, %s)
                """
                
                batch_params = [
                    [data['name'], data['email'], data['age'], data['is_active']]
                    for data in large_data
                ]
                
                result = connector.execute_batch(insert_sql, batch_params)
                assert result == len(large_data)
                
                # Verify data count
                count_sql = f"SELECT COUNT(*) as count FROM {table_name}"
                result = connector.execute_query(count_sql)
                assert result[0]['count'] == len(large_data)
                
                # Test pagination
                page_size = 25
                offset = 0
                
                paginated_sql = f"""
                    SELECT * FROM {table_name} 
                    ORDER BY id 
                    LIMIT %s OFFSET %s
                """
                
                page_result = connector.execute_query(paginated_sql, [page_size, offset])
                assert len(page_result) == page_size
                
            finally:
                # Cleanup
                self.cleanup_table(connector, table_name)
    
    def test_table_info_retrieval(self, connector, sample_schema):
        """Test MySQL-specific table information retrieval."""
        table_name = 'test_table_info_mysql'
        
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            # Create table with specific engine and charset
            success = connector.create_table(
                table_name, 
                sample_schema, 
                engine='InnoDB',
                charset='utf8mb4'
            )
            assert success is True
            
            try:
                # Get table info
                table_info = connector.get_table_info(table_name)
                
                assert table_info['table_name'] == table_name
                assert table_info['engine'] == 'InnoDB'
                assert 'utf8mb4' in table_info['collation']
                assert isinstance(table_info['estimated_rows'], int)
                assert isinstance(table_info['data_length'], int)
                assert isinstance(table_info['index_length'], int)
                
            finally:
                # Cleanup
                self.cleanup_table(connector, table_name)
    
    def test_ddl_generation(self, connector, sample_schema):
        """Test DDL generation for MySQL."""
        table_name = 'test_ddl_mysql'
        
        # Generate DDL
        ddl = connector.schema_to_ddl(
            table_name, 
            sample_schema,
            engine='InnoDB',
            charset='utf8mb4'
        )
        
        # Verify DDL contains expected elements
        assert 'CREATE TABLE' in ddl
        assert table_name in ddl
        assert 'ENGINE=InnoDB' in ddl
        assert 'DEFAULT CHARSET=utf8mb4' in ddl
        assert 'COLLATE=utf8mb4_unicode_ci' in ddl
        
        # Verify columns are included
        assert '`id`' in ddl
        assert 'AUTO_INCREMENT' in ddl
        assert 'PRIMARY KEY' in ddl
        
        # Test execution of generated DDL
        with connector:
            # Clean up first
            self.cleanup_table(connector, table_name)
            
            try:
                # Execute the generated DDL
                connector.execute_query(ddl)
                
                # Verify table was created
                assert connector.table_exists(table_name)
                
                # Verify schema matches
                schema = connector.get_table_schema(table_name)
                assert len(schema) > 0
                
            finally:
                # Cleanup
                self.cleanup_table(connector, table_name)