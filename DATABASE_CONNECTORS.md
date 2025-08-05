# Database Connectors Documentation

## Overview

The Generic Data Ingestor Framework provides a comprehensive set of database connectors with advanced features including retry logic, timeout handling, connection pooling, and robust error management. This document covers all aspects of using and configuring the database connectors.

## Table of Contents

1. [Architecture](#architecture)
2. [Supported Databases](#supported-databases)
3. [Quick Start](#quick-start)
4. [Connector Factory](#connector-factory)
5. [Configuration](#configuration)
6. [Retry Logic](#retry-logic)
7. [Timeout Handling](#timeout-handling)
8. [Connection Pooling](#connection-pooling)
9. [Error Handling](#error-handling)
10. [Performance Tuning](#performance-tuning)
11. [Security Considerations](#security-considerations)
12. [Examples](#examples)
13. [Troubleshooting](#troubleshooting)
14. [API Reference](#api-reference)

---

## Architecture

The database connector system follows a layered architecture with clear separation of concerns:

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│                 Connector Factory                           │
│         ┌──────────────────────────────────────┐           │
│         │  Retry Logic  │  Timeout Handling    │           │
│         │               │  Config Validation   │           │
│         └──────────────────────────────────────┘           │
├─────────────────────────────────────────────────────────────┤
│              Enhanced Connector Wrapper                     │
│   ┌─────────────────────────────────────────────────────┐   │
│   │        Connection Setup    │    Query Execution     │   │
│   │        Teardown Logic      │    Error Handling      │   │
│   └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                 Database Connectors                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ PostgreSQL  │  │   MySQL     │  │      SQLite        │  │
│  │ Connector   │  │ Connector   │  │    Connector       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│              Connection Pool Manager                        │
└─────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Separation of Concerns**: Connection setup, query execution, and teardown are handled separately
2. **Retry Resilience**: Automatic retry with configurable strategies for handling transient failures
3. **Timeout Protection**: Comprehensive timeout handling prevents hung operations
4. **Resource Management**: Connection pooling and proper resource cleanup
5. **Configuration-Driven**: Flexible configuration system supporting multiple formats

---

## Supported Databases

| Database | Connector Class | Python Driver | Status |
|----------|----------------|---------------|---------|
| PostgreSQL | `PostgreSQLConnector` | `psycopg2` | ✅ Full Support |
| MySQL | `MySQLConnector` | `mysql-connector-python` | ✅ Full Support |
| SQLite | `SQLiteConnector` | `sqlite3` (built-in) | ✅ Full Support |

### Database-Specific Features

#### PostgreSQL
- Connection pooling with `psycopg2.pool.ThreadedConnectionPool`
- Advanced SSL support with client certificates
- JSON/JSONB data type support
- Bulk operations with `COPY` commands
- Comprehensive error handling for PostgreSQL-specific exceptions

#### MySQL
- Connection pooling with connection recycling
- SSL/TLS encryption support
- Multiple authentication methods
- Binary data handling
- MySQL-specific SQL dialect support

#### SQLite
- File-based database support
- In-memory database capabilities
- Thread-safe operations
- Optimized for embedded scenarios
- Automatic backup and recovery options

---

## Quick Start

### Basic Usage

```python
from src.connectors.connector_factory import get_connector_factory

# Get the global factory instance
factory = get_connector_factory()

# Create a PostgreSQL connector
connection_params = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mydb',
    'username': 'user',
    'password': 'password'
}

connector = factory.create_connector(
    db_type='postgresql',
    connection_params=connection_params
)

# Use the connector
with connector:
    results = connector.execute_query("SELECT * FROM users LIMIT 10")
    print(f"Found {len(results)} users")
```

### Advanced Usage with Retry and Timeout

```python
from src.connectors.connector_factory import (
    get_connector_factory, 
    RetryConfig, 
    TimeoutConfig, 
    RetryStrategy
)

# Configure retry behavior
retry_config = RetryConfig(
    max_attempts=5,
    base_delay=2.0,
    max_delay=30.0,
    strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    jitter=True
)

# Configure timeouts
timeout_config = TimeoutConfig(
    connection_timeout=15.0,
    query_timeout=120.0,
    transaction_timeout=300.0
)

# Create enhanced connector
factory = get_connector_factory()
connector = factory.create_connector(
    db_type='postgresql',
    connection_params=connection_params,
    retry_config=retry_config,
    timeout_config=timeout_config
)
```

---

## Connector Factory

The `DatabaseConnectorFactory` is the central component for creating and managing database connectors.

### Factory Initialization

```python
from src.connectors.connector_factory import (
    DatabaseConnectorFactory,
    RetryConfig,
    TimeoutConfig
)

# Create factory with custom defaults
factory = DatabaseConnectorFactory(
    default_retry_config=RetryConfig(max_attempts=3),
    default_timeout_config=TimeoutConfig(connection_timeout=30.0)
)
```

### Creation Methods

#### 1. Direct Creation
```python
connector = factory.create_connector(
    db_type='postgresql',
    connection_params=params,
    use_pooling=True,
    validate_config=True
)
```

#### 2. From Configuration File
```python
# config.json
{
    "db_type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "username": "user",
    "password": "password",
    "connection_pool_size": 10
}

connector = factory.create_from_config_file('config.json')
```

#### 3. From Database URL
```python
url = "postgresql://user:password@localhost:5432/mydb?sslmode=require"
connector = factory.create_from_url(url)
```

#### 4. From Comprehensive Configuration
```python
config = {
    'db_type': 'postgresql',
    'connection_params': {
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'username': 'user',
        'password': 'password'
    },
    'retry_config': {
        'max_attempts': 5,
        'strategy': 'exponential_backoff',
        'base_delay': 1.0,
        'max_delay': 60.0
    },
    'timeout_config': {
        'connection_timeout': 30.0,
        'query_timeout': 300.0
    }
}

connector = factory.create_connector_with_config(config)
```

---

## Configuration

### Connection Parameters

#### PostgreSQL
```python
connection_params = {
    'host': 'localhost',               # Database host
    'port': 5432,                      # Database port
    'database': 'mydb',                # Database name
    'username': 'user',                # Username
    'password': 'password',            # Password
    'connection_timeout': 30,          # Connection timeout (seconds)
    'connection_pool_size': 10,        # Max connections in pool
    'ssl_enabled': True,               # Enable SSL
    'ssl_ca_cert': '/path/to/ca.pem',  # CA certificate
    'ssl_client_cert': '/path/to/client.pem',  # Client certificate
    'ssl_client_key': '/path/to/client.key',   # Client private key
    'additional_options': {            # Additional connection options
        'application_name': 'MyApp',
        'statement_timeout': '300s'
    }
}
```

#### MySQL
```python
connection_params = {
    'host': 'localhost',
    'port': 3306,
    'database': 'mydb',
    'user': 'user',                    # Note: 'user' not 'username' for MySQL
    'password': 'password',
    'connection_timeout': 30,
    'connection_pool_size': 10,
    'ssl_disabled': False,             # SSL enabled by default
    'ssl_ca': '/path/to/ca.pem',
    'ssl_cert': '/path/to/cert.pem',
    'ssl_key': '/path/to/key.pem',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}
```

#### SQLite
```python
connection_params = {
    'database': '/path/to/database.db',  # Database file path
    'timeout': 30.0,                     # Lock timeout
    'isolation_level': 'DEFERRED',       # Transaction isolation
    'check_same_thread': False,          # Thread safety
    'cached_statements': 100,            # Statement cache size
    'uri': False,                        # Enable URI syntax
    'backup_count': 3                    # Number of automatic backups
}
```

### Environment Variables

The framework supports configuration via environment variables:

```bash
# Database connection
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=user
DB_PASSWORD=password

# Connection pooling
DB_POOL_SIZE=10
DB_POOL_TIMEOUT=300

# Retry configuration
DB_RETRY_MAX_ATTEMPTS=3
DB_RETRY_BASE_DELAY=1.0
DB_RETRY_MAX_DELAY=60.0
DB_RETRY_STRATEGY=exponential_backoff

# Timeout configuration
DB_CONNECTION_TIMEOUT=30.0
DB_QUERY_TIMEOUT=300.0
DB_TRANSACTION_TIMEOUT=600.0
```

---

## Retry Logic

The framework provides sophisticated retry mechanisms to handle transient failures gracefully.

### Retry Strategies

#### 1. Exponential Backoff (Default)
```python
retry_config = RetryConfig(
    max_attempts=5,
    base_delay=1.0,
    max_delay=60.0,
    strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    backoff_multiplier=2.0,  # Delay doubles each attempt
    jitter=True              # Add randomness to prevent thundering herd
)

# Retry delays: ~1s, ~2s, ~4s, ~8s, ~16s (with jitter)
```

#### 2. Linear Backoff
```python
retry_config = RetryConfig(
    max_attempts=4,
    base_delay=2.0,
    strategy=RetryStrategy.LINEAR_BACKOFF
)

# Retry delays: 2s, 4s, 6s, 8s
```

#### 3. Fixed Delay
```python
retry_config = RetryConfig(
    max_attempts=3,
    base_delay=5.0,
    strategy=RetryStrategy.FIXED_DELAY
)

# Retry delays: 5s, 5s, 5s
```

#### 4. Immediate Retry
```python
retry_config = RetryConfig(
    max_attempts=3,
    strategy=RetryStrategy.IMMEDIATE
)

# No delays between retries
```

### Retriable Exceptions

By default, the following exceptions trigger retries:
- `ConnectionError`
- `OSError`
- `TimeoutError`
- Database-specific connection errors

Custom retriable exceptions:
```python
retry_config = RetryConfig(
    max_attempts=3,
    retriable_exceptions=[
        ConnectionError,
        OSError,
        psycopg2.OperationalError,  # PostgreSQL-specific
        mysql.connector.Error        # MySQL-specific
    ]
)
```

### Retry Decorator

Apply retry logic to custom functions:
```python
from src.connectors.connector_factory import with_retry_and_timeout

@with_retry_and_timeout(retry_config=RetryConfig(max_attempts=3))
def custom_database_operation():
    # Your database operation here
    pass
```

---

## Timeout Handling

Comprehensive timeout protection prevents operations from hanging indefinitely.

### Timeout Types

#### 1. Connection Timeout
Maximum time to establish a database connection:
```python
timeout_config = TimeoutConfig(connection_timeout=30.0)  # 30 seconds
```

#### 2. Query Timeout
Maximum time for individual query execution:
```python
timeout_config = TimeoutConfig(query_timeout=300.0)  # 5 minutes
```

#### 3. Transaction Timeout
Maximum time for transaction completion:
```python
timeout_config = TimeoutConfig(transaction_timeout=600.0)  # 10 minutes
```

#### 4. Total Timeout
Maximum time for entire operation including retries:
```python
timeout_config = TimeoutConfig(total_timeout=900.0)  # 15 minutes total
```

### Timeout Hierarchy

Timeouts are enforced in the following order:
1. **Total Timeout**: Overrides all other timeouts
2. **Operation-Specific Timeout**: Query, connection, or transaction timeout
3. **Database-Specific Timeout**: Driver-level timeouts

### Timeout Configuration Examples

#### Conservative Configuration (Reliable Networks)
```python
timeout_config = TimeoutConfig(
    connection_timeout=10.0,    # Fast connection expected
    query_timeout=60.0,         # Short queries
    transaction_timeout=120.0,  # Brief transactions
    total_timeout=300.0         # 5-minute total limit
)
```

#### Aggressive Configuration (Unreliable Networks)
```python
timeout_config = TimeoutConfig(
    connection_timeout=60.0,     # Allow slow connections
    query_timeout=900.0,         # Long-running queries OK
    transaction_timeout=1800.0,  # Extended transactions
    total_timeout=3600.0         # 1-hour total limit
)
```

---

## Connection Pooling

Connection pooling improves performance by reusing database connections.

### Pool Configuration

```python
connection_params = {
    'host': 'localhost',
    'database': 'mydb',
    'username': 'user',
    'password': 'password',
    'connection_pool_size': 20,      # Maximum connections
    'min_pool_size': 5,              # Minimum connections
    'pool_timeout': 300,             # Pool acquisition timeout
    'pool_recycle': 3600,            # Connection max age (seconds)
    'pool_pre_ping': True            # Validate connections before use
}

# Create pooled connector
connector = factory.create_connector(
    db_type='postgresql',
    connection_params=connection_params,
    use_pooling=True
)
```

### Pool Management

#### Monitor Pool Health
```python
# Check pool status
health = factory.health_check('my_pool')
print(f"Pool status: {health['status']}")
print(f"Active connections: {health['active_connections']}")
print(f"Total connections: {health['total_connections']}")
```

#### Pool Metrics
```python
pool_manager = factory.get_pool_manager()
metrics = pool_manager.get_pool_metrics('my_pool')

print(f"Total requests: {metrics.total_requests}")
print(f"Success rate: {metrics.successful_requests / metrics.total_requests:.2%}")
print(f"Average response time: {metrics.average_response_time:.3f}s")
```

### Pool Best Practices

1. **Size Appropriately**: Pool size should match concurrent workload
2. **Monitor Usage**: Track pool metrics to optimize configuration
3. **Handle Exhaustion**: Implement proper error handling for pool exhaustion
4. **Connection Validation**: Enable pre-ping to detect stale connections
5. **Lifecycle Management**: Properly close pools when shutting down

---

## Error Handling

The framework provides comprehensive error handling with detailed logging.

### Exception Hierarchy

```
ConnectorFactoryError
├── ConfigurationError
├── ConnectionError
├── TimeoutError
└── DatabaseError
    ├── PostgreSQLError
    │   ├── PostgreSQLConnectionError
    │   ├── PostgreSQLQueryError
    │   └── PostgreSQLTransactionError
    ├── MySQLError
    │   ├── MySQLConnectionError
    │   ├── MySQLQueryError
    │   └── MySQLTransactionError
    └── SQLiteError
        ├── SQLiteConnectionError
        ├── SQLiteQueryError
        └── SQLiteTransactionError
```

### Error Handling Patterns

#### 1. Basic Error Handling
```python
try:
    connector = factory.create_connector('postgresql', params)
    results = connector.execute_query("SELECT * FROM users")
except ConnectorFactoryError as e:
    logger.error(f"Factory error: {e}")
except ConnectionError as e:
    logger.error(f"Connection failed: {e}")
except TimeoutError as e:
    logger.error(f"Operation timed out: {e}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
```

#### 2. Database-Specific Error Handling
```python
from src.connectors.postgresql_connector import (
    PostgreSQLConnectionError,
    PostgreSQLQueryError
)

try:
    connector.execute_query("SELECT * FROM users")
except PostgreSQLConnectionError:
    # Handle connection-specific issues
    logger.error("PostgreSQL connection failed")
except PostgreSQLQueryError as e:
    # Handle query-specific issues
    logger.error(f"Query failed: {e}")
```

#### 3. Retry-Aware Error Handling
```python
from src.connectors.connector_factory import RetryConfig

# Configure which exceptions should trigger retries
retry_config = RetryConfig(
    max_attempts=3,
    retriable_exceptions=[
        ConnectionError,
        TimeoutError,
        psycopg2.OperationalError
    ]
)

try:
    connector = factory.create_connector(
        'postgresql', 
        params,
        retry_config=retry_config
    )
    # Operations will automatically retry on retriable exceptions
    results = connector.execute_query("SELECT * FROM users")
except Exception as e:
    # This exception was either:
    # 1. Not retriable, or 
    # 2. Exhausted all retry attempts
    logger.error(f"Operation failed permanently: {e}")
```

### Logging Integration

The framework provides structured logging for all operations:

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Framework logging categories
loggers = [
    'data_ingestion.connector_factory',
    'data_ingestion.retry_handler', 
    'data_ingestion.enhanced_connector',
    'data_ingestion.postgresql_connector',
    'data_ingestion.mysql_connector',
    'data_ingestion.sqlite_connector'
]

for logger_name in loggers:
    logging.getLogger(logger_name).setLevel(logging.DEBUG)
```

---

## Performance Tuning

### Connection Pool Optimization

#### Determine Optimal Pool Size
```python
import concurrent.futures
import time

def benchmark_pool_size(pool_size, concurrent_requests=50):
    """Benchmark different pool sizes."""
    params = {**base_params, 'connection_pool_size': pool_size}
    connector = factory.create_connector(
        'postgresql', 
        params, 
        use_pooling=True
    )
    
    def execute_query():
        return connector.execute_query("SELECT COUNT(*) FROM users")
    
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
        futures = [executor.submit(execute_query) for _ in range(concurrent_requests)]
        results = [f.result() for f in futures]
    
    duration = time.time() - start_time
    return duration, len(results)

# Test different pool sizes
for pool_size in [5, 10, 20, 50]:
    duration, count = benchmark_pool_size(pool_size)
    print(f"Pool size {pool_size}: {duration:.2f}s for {count} queries")
```

### Query Optimization

#### Batch Operations
```python
# Instead of individual inserts
for record in records:
    connector.execute_query(
        "INSERT INTO users (name, email) VALUES (%s, %s)",
        [record['name'], record['email']]
    )

# Use batch operations
connector.insert_data('users', records, batch_size=1000)
```

#### Connection Reuse
```python
# Use context manager for multiple operations
with connector:
    connector.begin_transaction()
    try:
        for query in queries:
            connector.execute_query(query)
        connector.commit_transaction()
    except Exception:
        connector.rollback_transaction()
        raise
```

### Memory Management

#### Large Dataset Handling
```python
# For large result sets, use pagination
def process_large_table(table_name, batch_size=10000):
    offset = 0
    while True:
        query = f"""
            SELECT * FROM {table_name} 
            ORDER BY id 
            LIMIT {batch_size} OFFSET {offset}
        """
        results = connector.execute_query(query)
        
        if not results:
            break
            
        # Process batch
        process_batch(results)
        offset += batch_size
```

---

## Security Considerations

### Connection Security

#### SSL/TLS Configuration
```python
# PostgreSQL with SSL
connection_params = {
    'host': 'db.example.com',
    'database': 'prod_db',
    'username': 'app_user',
    'password': 'secure_password',
    'ssl_enabled': True,
    'ssl_mode': 'require',           # Require SSL
    'ssl_ca_cert': '/path/to/ca.pem',
    'ssl_client_cert': '/path/to/client.pem',
    'ssl_client_key': '/path/to/client.key'
}
```

#### Connection String Security
```python
# Avoid hardcoded credentials
import os

connection_params = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'username': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}
```

### Query Security

#### Parameterized Queries
```python
# SECURE: Use parameterized queries
user_id = request.get('user_id')
query = "SELECT * FROM users WHERE id = %s"
results = connector.execute_query(query, [user_id])

# INSECURE: String interpolation (SQL injection risk)
query = f"SELECT * FROM users WHERE id = {user_id}"  # DON'T DO THIS
```

#### Input Validation
```python
def validate_table_name(table_name):
    """Validate table name to prevent SQL injection."""
    import re
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name

# Use validation
table_name = validate_table_name(user_input)
query = f"SELECT * FROM {table_name}"  # Safe after validation
```

### Access Control

#### Connection-Level Permissions
```sql
-- Create restricted database user
CREATE USER app_user WITH PASSWORD 'secure_password';

-- Grant minimal required permissions
GRANT SELECT, INSERT, UPDATE ON specific_tables TO app_user;
GRANT USAGE ON SCHEMA app_schema TO app_user;

-- Revoke dangerous permissions
REVOKE CREATE ON DATABASE FROM app_user;
REVOKE DROP ON ALL TABLES FROM app_user;
```

#### Application-Level Security
```python
class SecureConnector:
    """Wrapper that enforces security policies."""
    
    def __init__(self, connector, allowed_operations=None):
        self.connector = connector
        self.allowed_operations = allowed_operations or ['SELECT']
    
    def execute_query(self, query, params=None):
        operation = query.strip().upper().split()[0]
        if operation not in self.allowed_operations:
            raise SecurityError(f"Operation {operation} not allowed")
        return self.connector.execute_query(query, params)
```

---

## Examples

### Example 1: Data Migration with Retry Logic

```python
from src.connectors.connector_factory import (
    get_connector_factory,
    RetryConfig,
    TimeoutConfig,
    RetryStrategy
)

def migrate_data():
    """Migrate data between databases with robust error handling."""
    
    # Configure retry for unreliable network
    retry_config = RetryConfig(
        max_attempts=5,
        base_delay=2.0,
        max_delay=60.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        jitter=True
    )
    
    # Configure timeouts for large operations
    timeout_config = TimeoutConfig(
        connection_timeout=30.0,
        query_timeout=1800.0,  # 30 minutes for large queries
        transaction_timeout=3600.0,  # 1 hour for transactions
        total_timeout=7200.0   # 2 hours total
    )
    
    factory = get_connector_factory()
    
    # Source database (MySQL)
    source = factory.create_connector(
        db_type='mysql',
        connection_params={
            'host': 'source-db.example.com',
            'database': 'legacy_db',
            'user': 'migrate_user',
            'password': 'migrate_pass'
        },
        retry_config=retry_config,
        timeout_config=timeout_config
    )
    
    # Target database (PostgreSQL)
    target = factory.create_connector(
        db_type='postgresql',
        connection_params={
            'host': 'target-db.example.com',
            'database': 'new_db',
            'username': 'migrate_user',
            'password': 'migrate_pass'
        },
        retry_config=retry_config,
        timeout_config=timeout_config
    )
    
    tables_to_migrate = ['users', 'orders', 'products']
    
    for table in tables_to_migrate:
        print(f"Migrating table: {table}")
        
        try:
            # Extract data in batches
            with source:
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                total_rows = source.execute_query(count_query)[0]['count']
                print(f"Total rows to migrate: {total_rows}")
                
                batch_size = 10000
                for offset in range(0, total_rows, batch_size):
                    # Extract batch
                    extract_query = f"""
                        SELECT * FROM {table} 
                        ORDER BY id 
                        LIMIT {batch_size} OFFSET {offset}
                    """
                    batch_data = source.execute_query(extract_query)
                    
                    # Load batch
                    with target:
                        target.begin_transaction()
                        try:
                            target.insert_data(table, batch_data, batch_size=1000)
                            target.commit_transaction()
                            print(f"Migrated batch {offset//batch_size + 1}: {len(batch_data)} rows")
                        except Exception as e:
                            target.rollback_transaction()
                            print(f"Batch failed, rolling back: {e}")
                            raise
            
            print(f"Successfully migrated table: {table}")
            
        except Exception as e:
            print(f"Failed to migrate table {table}: {e}")
            raise

if __name__ == "__main__":
    migrate_data()
```

### Example 2: High-Availability Service with Connection Pooling

```python
from src.connectors.connector_factory import (
    DatabaseConnectorFactory,
    RetryConfig,
    RetryStrategy
)
import threading
import time
from typing import List, Dict, Any

class DatabaseService:
    """High-availability database service with connection pooling."""
    
    def __init__(self):
        # Configure aggressive retry for production
        retry_config = RetryConfig(
            max_attempts=10,
            base_delay=0.5,
            max_delay=30.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            jitter=True
        )
        
        self.factory = DatabaseConnectorFactory(
            default_retry_config=retry_config
        )
        
        # Database clusters for high availability
        self.clusters = {
            'primary': self._create_cluster_connectors([
                {'host': 'db1.primary.example.com', 'priority': 1},
                {'host': 'db2.primary.example.com', 'priority': 2}
            ]),
            'replica': self._create_cluster_connectors([
                {'host': 'db1.replica.example.com', 'priority': 1},
                {'host': 'db2.replica.example.com', 'priority': 2},
                {'host': 'db3.replica.example.com', 'priority': 3}
            ])
        }
        
        self._lock = threading.Lock()
        self._health_status = {}
        
        # Start health monitoring
        self._start_health_monitoring()
    
    def _create_cluster_connectors(self, hosts: List[Dict]) -> List[Dict]:
        """Create connectors for database cluster."""
        connectors = []
        
        for host_config in hosts:
            connection_params = {
                'host': host_config['host'],
                'port': 5432,
                'database': 'app_db',
                'username': 'app_user',
                'password': 'app_password',
                'connection_pool_size': 20,
                'pool_timeout': 10
            }
            
            connector = self.factory.create_connector(
                db_type='postgresql',
                connection_params=connection_params,
                pool_name=f"pool_{host_config['host']}",
                use_pooling=True
            )
            
            connectors.append({
                'connector': connector,
                'host': host_config['host'],
                'priority': host_config['priority'],
                'healthy': True
            })
        
        return sorted(connectors, key=lambda x: x['priority'])
    
    def _start_health_monitoring(self):
        """Start background health monitoring."""
        def monitor():
            while True:
                self._check_cluster_health()
                time.sleep(30)  # Check every 30 seconds
        
        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
    
    def _check_cluster_health(self):
        """Check health of all database clusters."""
        for cluster_name, cluster in self.clusters.items():
            for db_config in cluster:
                try:
                    # Simple health check query
                    result = db_config['connector'].execute_query("SELECT 1")
                    db_config['healthy'] = bool(result)
                except Exception as e:
                    print(f"Health check failed for {db_config['host']}: {e}")
                    db_config['healthy'] = False
    
    def get_connector(self, operation_type: str = 'read'):
        """Get appropriate connector based on operation type."""
        cluster_name = 'primary' if operation_type == 'write' else 'replica'
        cluster = self.clusters[cluster_name]
        
        # Find first healthy connector
        for db_config in cluster:
            if db_config['healthy']:
                return db_config['connector']
        
        # Fallback to primary if all replicas are down
        if cluster_name == 'replica':
            for db_config in self.clusters['primary']:
                if db_config['healthy']:
                    print("WARNING: Using primary for read operation (replicas down)")
                    return db_config['connector']
        
        raise Exception(f"No healthy connectors available for {operation_type} operations")
    
    def execute_read_query(self, query: str, params=None):
        """Execute read query on best available replica."""
        connector = self.get_connector('read')
        return connector.execute_query(query, params)
    
    def execute_write_query(self, query: str, params=None):
        """Execute write query on primary database."""
        connector = self.get_connector('write')
        return connector.execute_query(query, params)
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of all database clusters."""
        status = {}
        for cluster_name, cluster in self.clusters.items():
            healthy_count = sum(1 for db in cluster if db['healthy'])
            status[cluster_name] = {
                'total_databases': len(cluster),
                'healthy_databases': healthy_count,
                'status': 'healthy' if healthy_count > 0 else 'degraded',
                'databases': [
                    {
                        'host': db['host'],
                        'priority': db['priority'],
                        'healthy': db['healthy']
                    }
                    for db in cluster
                ]
            }
        return status

# Usage example
if __name__ == "__main__":
    service = DatabaseService()
    
    try:
        # Read operations use replicas
        users = service.execute_read_query(
            "SELECT id, name, email FROM users WHERE active = %s LIMIT 100",
            [True]
        )
        print(f"Found {len(users)} active users")
        
        # Write operations use primary
        result = service.execute_write_query(
            "INSERT INTO audit_log (action, timestamp) VALUES (%s, %s)",
            ['user_query', 'NOW()']
        )
        print(f"Audit log updated: {result} rows affected")
        
        # Check cluster health
        status = service.get_cluster_status()
        print("Cluster Status:", status)
        
    except Exception as e:
        print(f"Database operation failed: {e}")
```

### Example 3: Configuration-Driven Multi-Database Application

```python
import json
import os
from typing import Dict, Any
from src.connectors.connector_factory import get_connector_factory

class MultiDatabaseApp:
    """Application that manages multiple database connections."""
    
    def __init__(self, config_file: str):
        self.factory = get_connector_factory()
        self.connectors = {}
        self.load_configuration(config_file)
    
    def load_configuration(self, config_file: str):
        """Load database configurations from file."""
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        for db_name, db_config in config['databases'].items():
            try:
                connector = self.factory.create_connector_with_config(db_config)
                self.connectors[db_name] = connector
                print(f"Initialized connector for {db_name}")
            except Exception as e:
                print(f"Failed to initialize {db_name}: {e}")
    
    def get_connector(self, database_name: str):
        """Get connector by name."""
        if database_name not in self.connectors:
            raise ValueError(f"Unknown database: {database_name}")
        return self.connectors[database_name]
    
    def execute_cross_database_query(self):
        """Example of cross-database operation."""
        # Get user data from PostgreSQL
        pg_connector = self.get_connector('users_db')
        users = pg_connector.execute_query(
            "SELECT id, name, email FROM users WHERE created_at > %s",
            ['2024-01-01']
        )
        
        # Get order statistics from MySQL
        mysql_connector = self.get_connector('orders_db')
        for user in users:
            orders = mysql_connector.execute_query(
                "SELECT COUNT(*) as order_count, SUM(total) as total_spent "
                "FROM orders WHERE user_id = %s",
                [user['id']]
            )
            user['orders'] = orders[0] if orders else {'order_count': 0, 'total_spent': 0}
        
        # Store analytics in SQLite
        analytics_connector = self.get_connector('analytics_db')
        analytics_data = [
            {
                'user_id': user['id'],
                'order_count': user['orders']['order_count'],
                'total_spent': user['orders']['total_spent'],
                'analysis_date': '2024-01-01'
            }
            for user in users
        ]
        
        analytics_connector.insert_data('user_analytics', analytics_data)
        
        return len(users)

# Configuration file: multi_db_config.json
config_content = {
    "databases": {
        "users_db": {
            "db_type": "postgresql",
            "connection_params": {
                "host": "users-db.example.com",
                "database": "users",
                "username": "app_user",
                "password": "secure_password",
                "connection_pool_size": 15
            },
            "retry_config": {
                "max_attempts": 3,
                "strategy": "exponential_backoff",
                "base_delay": 1.0
            },
            "timeout_config": {
                "connection_timeout": 30.0,
                "query_timeout": 120.0
            }
        },
        "orders_db": {
            "db_type": "mysql",
            "connection_params": {
                "host": "orders-db.example.com",
                "database": "orders",
                "user": "app_user",
                "password": "secure_password",
                "connection_pool_size": 10
            },
            "retry_config": {
                "max_attempts": 5,
                "strategy": "linear_backoff",
                "base_delay": 2.0
            }
        },
        "analytics_db": {
            "db_type": "sqlite",
            "connection_params": {
                "database": "/data/analytics.db"
            },
            "retry_config": {
                "max_attempts": 2,
                "strategy": "fixed_delay",
                "base_delay": 1.0
            }
        }
    }
}

# Save configuration
with open('multi_db_config.json', 'w') as f:
    json.dump(config_content, f, indent=2)

# Use the application
if __name__ == "__main__":
    app = MultiDatabaseApp('multi_db_config.json')
    
    try:
        processed_users = app.execute_cross_database_query()
        print(f"Processed {processed_users} users across databases")
    except Exception as e:
        print(f"Cross-database operation failed: {e}")
```

---

## Troubleshooting

### Common Issues

#### 1. Connection Pool Exhaustion
**Symptoms**: `PoolExhaustedError`, slow response times
**Solutions**:
```python
# Increase pool size
connection_params['connection_pool_size'] = 50

# Reduce pool timeout to fail fast
connection_params['pool_timeout'] = 10

# Monitor pool usage
health = factory.health_check()
print(f"Pool utilization: {health['active_connections']}/{health['total_connections']}")
```

#### 2. Connection Timeouts
**Symptoms**: `TimeoutError`, intermittent failures
**Solutions**:
```python
# Increase connection timeout
timeout_config = TimeoutConfig(
    connection_timeout=60.0,  # Increase from default 30s
    query_timeout=600.0       # 10 minutes for complex queries
)

# Configure retry with longer delays
retry_config = RetryConfig(
    max_attempts=5,
    base_delay=5.0,           # Start with 5s delay
    max_delay=120.0           # Max 2-minute delay
)
```

#### 3. SSL/TLS Issues
**Symptoms**: SSL certificate errors, connection refused
**Solutions**:
```python
# Verify SSL configuration
connection_params = {
    'ssl_enabled': True,
    'ssl_mode': 'require',
    'ssl_ca_cert': '/correct/path/to/ca.pem',
    'ssl_verify_cert': True
}

# Debug SSL issues
import ssl
context = ssl.create_default_context()
context.check_hostname = False  # For debugging only
```

#### 4. Memory Issues with Large Datasets
**Symptoms**: `OutOfMemoryError`, slow performance
**Solutions**:
```python
# Use pagination for large queries
def process_large_table_safely(connector, table_name):
    batch_size = 1000
    offset = 0
    
    while True:
        batch = connector.execute_query(
            f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        )
        
        if not batch:
            break
            
        process_batch(batch)
        offset += batch_size
        
        # Optional: garbage collection
        import gc
        gc.collect()

# Use streaming for very large results
def stream_query_results(connector, query):
    # Implementation depends on database-specific streaming support
    pass
```

### Debugging Tools

#### Enable Debug Logging
```python
import logging

# Enable all database connector logging
logging.getLogger('data_ingestion').setLevel(logging.DEBUG)

# Enable specific connector logging
logging.getLogger('data_ingestion.postgresql_connector').setLevel(logging.DEBUG)
logging.getLogger('data_ingestion.retry_handler').setLevel(logging.DEBUG)
```

#### Connection Diagnostics
```python
def diagnose_connection(connector):
    """Comprehensive connection diagnostics."""
    try:
        # Test basic connectivity
        success, error = connector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'}")
        if error:
            print(f"Error: {error}")
        
        # Get connection information
        info = connector.get_connection_info()
        print(f"Database type: {info.get('db_type')}")
        print(f"Server version: {info.get('server_version')}")
        print(f"Connection status: {info.get('is_connected')}")
        
        # Test simple query
        result = connector.execute_query("SELECT 1 as test")
        print(f"Query test: {'PASS' if result else 'FAIL'}")
        
        # Check pool status if pooled
        if hasattr(connector, 'pool_name'):
            pool_health = factory.health_check(connector.pool_name)
            print(f"Pool health: {pool_health}")
        
    except Exception as e:
        print(f"Diagnostic failed: {e}")

# Usage
diagnose_connection(my_connector)
```

#### Performance Profiling
```python
import time
import statistics
from contextlib import contextmanager

@contextmanager
def performance_timer():
    """Context manager for timing operations."""
    start = time.time()
    yield
    end = time.time()
    print(f"Operation took {end - start:.3f} seconds")

def benchmark_connector(connector, query, iterations=100):
    """Benchmark connector performance."""
    times = []
    
    for i in range(iterations):
        start = time.time()
        try:
            result = connector.execute_query(query)
            times.append(time.time() - start)
        except Exception as e:
            print(f"Query {i} failed: {e}")
    
    if times:
        print(f"Query performance over {len(times)} iterations:")
        print(f"  Average: {statistics.mean(times):.3f}s")
        print(f"  Median:  {statistics.median(times):.3f}s")
        print(f"  Min:     {min(times):.3f}s")
        print(f"  Max:     {max(times):.3f}s")
        print(f"  Std Dev: {statistics.stdev(times):.3f}s")

# Usage
benchmark_connector(my_connector, "SELECT COUNT(*) FROM users")
```

---

## API Reference

### DatabaseConnectorFactory

#### Methods

##### `create_connector(db_type, connection_params, **kwargs)`
Creates a database connector with retry and timeout support.

**Parameters:**
- `db_type` (str): Database type ('postgresql', 'mysql', 'sqlite')
- `connection_params` (Dict): Database connection parameters
- `pool_name` (str, optional): Connection pool name
- `use_pooling` (bool): Enable connection pooling (default: True)
- `validate_config` (bool): Validate configuration (default: True)
- `retry_config` (RetryConfig, optional): Retry configuration
- `timeout_config` (TimeoutConfig, optional): Timeout configuration

**Returns:** DatabaseConnector instance

**Raises:** ConnectorFactoryError

##### `create_from_config_file(config_file_path, **kwargs)`
Creates connector from JSON configuration file.

**Parameters:**
- `config_file_path` (str): Path to configuration file
- `pool_name` (str, optional): Connection pool name
- `use_pooling` (bool): Enable connection pooling (default: True)

**Returns:** DatabaseConnector instance

##### `create_from_url(database_url, **kwargs)`
Creates connector from database URL.

**Parameters:**
- `database_url` (str): Database connection URL
- `pool_name` (str, optional): Connection pool name
- `use_pooling` (bool): Enable connection pooling (default: True)

**Returns:** DatabaseConnector instance

##### `create_connector_with_config(config)`
Creates connector from comprehensive configuration dictionary.

**Parameters:**
- `config` (Dict): Complete configuration dictionary

**Returns:** DatabaseConnector instance

### RetryConfig

#### Constructor Parameters
- `max_attempts` (int): Maximum retry attempts (default: 3)
- `base_delay` (float): Base delay between retries (default: 1.0)
- `max_delay` (float): Maximum delay between retries (default: 60.0)
- `strategy` (RetryStrategy): Retry strategy (default: EXPONENTIAL_BACKOFF)
- `backoff_multiplier` (float): Backoff multiplier (default: 2.0)
- `jitter` (bool): Enable random jitter (default: True)
- `retriable_exceptions` (List[Exception]): Exceptions that trigger retries

#### Methods

##### `calculate_delay(attempt)`
Calculate delay for given attempt number.

##### `should_retry(exception, attempt)`
Determine if exception should trigger retry.

### TimeoutConfig

#### Constructor Parameters
- `connection_timeout` (float): Connection timeout (default: 30.0)
- `query_timeout` (float): Query timeout (default: 300.0)
- `transaction_timeout` (float): Transaction timeout (default: 600.0)
- `total_timeout` (float, optional): Total operation timeout

### DatabaseConnector (Abstract Base Class)

#### Connection Methods
- `connect()`: Establish database connection
- `disconnect()`: Close database connection
- `test_connection()`: Test connection health

#### Query Methods
- `execute_query(query, params=None)`: Execute SQL query
- `begin_transaction()`: Start transaction
- `commit_transaction()`: Commit transaction
- `rollback_transaction()`: Rollback transaction

#### Schema Methods
- `table_exists(table_name, schema_name=None)`: Check if table exists
- `get_table_schema(table_name, schema_name=None)`: Get table schema
- `create_table(table_name, schema, **kwargs)`: Create table
- `drop_table(table_name, **kwargs)`: Drop table

#### Data Methods
- `insert_data(table_name, data, **kwargs)`: Insert data
- `upsert_data(table_name, data, conflict_columns, **kwargs)`: Upsert data
- `replace_data(table_name, data, **kwargs)`: Replace table data

#### Utility Methods
- `get_connection_info()`: Get connection information
- `clear_schema_cache()`: Clear schema cache
- `get_schema_cache_stats()`: Get cache statistics

---

This documentation provides comprehensive coverage of the database connector system. For additional examples and advanced usage patterns, refer to the test files and example applications in the repository.