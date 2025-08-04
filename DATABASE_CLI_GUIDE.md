# Database CLI Configuration Guide

This guide covers the database integration features of the Generic Data Ingestor Framework CLI.

## Overview

The framework supports loading processed data into databases using multiple configuration methods:
- Command-line arguments
- Configuration files (JSON/YAML)
- Database URLs
- Environment variables

## Supported Databases

- **PostgreSQL** - Full-featured with connection pooling and advanced operations
- **MySQL** - Full-featured with connection pooling and batch operations  
- **SQLite** - Lightweight file-based database for development and testing

## CLI Database Options

### Configuration Sources (Choose One)

#### 1. Database URL
```bash
# PostgreSQL
python main.py data/ --database-url postgresql://user:pass@localhost:5432/mydb

# MySQL  
python main.py data/ --database-url mysql://user:pass@localhost:3306/mydb

# SQLite
python main.py data/ --database-url sqlite:///path/to/database.sqlite
```

#### 2. Configuration File
```bash
# JSON configuration
python main.py data/ --db-config-file config/database.json

# YAML configuration (if PyYAML is installed)
python main.py data/ --db-config-file config/database.yaml
```

#### 3. Direct CLI Arguments
```bash
# PostgreSQL example
python main.py data/ --db-type postgresql --db-host localhost --db-port 5432 \
    --db-name mydb --db-user postgres --prompt-password

# MySQL example  
python main.py data/ --db-type mysql --db-host localhost --db-port 3306 \
    --db-name mydb --db-user root --db-password $DB_PASSWORD

# SQLite example
python main.py data/ --db-type sqlite --db-name ./data/mydb.sqlite
```

### Database Operation Options

| Option | Description | Default |
|--------|-------------|---------|
| `--table-name` | Target table name | `ingested_data` |
| `--load-strategy` | Data loading strategy (`append`, `replace`, `upsert`) | `append` |
| `--batch-size` | Batch size for database operations | `1000` |
| `--create-table` | Create table if it doesn't exist | `true` |
| `--no-create-table` | Don't create table automatically | - |

### Connection Options

| Option | Description | Default |
|--------|-------------|---------|
| `--connection-timeout` | Connection timeout in seconds | `30` |
| `--pool-size` | Connection pool size | `10` |
| `--prompt-password` | Securely prompt for password | - |

### Configuration Management

| Option | Description |
|--------|-------------|
| `--create-sample-config FILE` | Create sample config file and exit |
| `--validate-config` | Validate database configuration and exit |

## Configuration Files

### JSON Configuration Example

```json
{
  "db_type": "postgresql",
  "host": "${DB_HOST:-localhost}",
  "port": "${DB_PORT:-5432}",
  "database": "${DB_NAME:-mydb}",
  "user": "${DB_USER:-postgres}",
  "password": "${DB_PASSWORD}",
  "connection_timeout": 30,
  "connection_pool_size": 10,
  "ssl_enabled": false
}
```

### YAML Configuration Example

```yaml
# PostgreSQL database configuration
db_type: postgresql
host: ${DB_HOST:-localhost}
port: ${DB_PORT:-5432}
database: ${DB_NAME:-mydb}
user: ${DB_USER:-postgres}
password: ${DB_PASSWORD}
connection_timeout: 30
connection_pool_size: 10
ssl_enabled: false
```

## Environment Variables

Use environment variables for sensitive information:

### Common Environment Variables

```bash
# Database connection
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=mydb
export DB_USER=postgres
export DB_PASSWORD=your_secure_password

# Connection options
export DB_TIMEOUT=30
export DB_POOL_SIZE=10
```

### Variable Substitution Syntax

In configuration files, use `${VAR_NAME}` or `${VAR_NAME:-default}`:

- `${DB_PASSWORD}` - Required variable (fails if not set)
- `${DB_HOST:-localhost}` - Optional with default value
- `${DB_PORT:-5432}` - Optional with default value

## Data Loading Strategies

### Append Strategy (Default)
```bash
python main.py data/ --database-url postgresql://user:pass@host/db --load-strategy append
```
- Adds new records to existing table
- Fastest for large datasets
- May create duplicates

### Replace Strategy
```bash
python main.py data/ --database-url postgresql://user:pass@host/db --load-strategy replace
```
- Truncates table and loads new data
- Ensures clean dataset
- Use with caution on production data

### Upsert Strategy
```bash
python main.py data/ --database-url postgresql://user:pass@host/db --load-strategy upsert
```
- Updates existing records, inserts new ones
- Prevents duplicates
- Requires primary key or unique constraints

## Complete Examples

### Example 1: PostgreSQL with Environment Variables

```bash
# Set environment variables
export DB_PASSWORD=mypassword
export DB_HOST=production-db.example.com

# Run with config file
python main.py /path/to/data --db-config-file config/prod-postgres.json \
    --table-name sales_data --load-strategy upsert --batch-size 5000
```

### Example 2: SQLite for Development

```bash
# Simple SQLite setup
python main.py test_data/ --db-type sqlite --db-name ./dev.sqlite \
    --table-name test_data --load-strategy replace
```

### Example 3: MySQL with Direct Configuration

```bash
# MySQL with all options
python main.py data/ --db-type mysql --db-host mysql.example.com --db-port 3306 \
    --db-name analytics --db-user analyst --prompt-password \
    --table-name processed_data --load-strategy append --batch-size 2000 \
    --connection-timeout 60 --pool-size 20
```

## Configuration Management Commands

### Create Sample Configuration Files

```bash
# Create PostgreSQL sample
python main.py --create-sample-config config/postgres.json --db-type postgresql

# Create MySQL sample  
python main.py --create-sample-config config/mysql.json --db-type mysql

# Create SQLite sample
python main.py --create-sample-config config/sqlite.json --db-type sqlite
```

### Validate Configuration

```bash
# Validate config file
python main.py --db-config-file config/database.json --validate-config

# Validate direct configuration
python main.py --db-type postgresql --db-host localhost --db-name mydb \
    --db-user postgres --validate-config
```

## Security Best Practices

1. **Never hardcode passwords** in configuration files
2. **Use environment variables** for sensitive credentials
3. **Use `--prompt-password`** for interactive password entry
4. **Set appropriate file permissions** on configuration files
5. **Use SSL/TLS connections** in production environments

### Secure Password Handling

```bash
# Method 1: Environment variable
export DB_PASSWORD=your_password
python main.py data/ --db-config-file config/database.json

# Method 2: Interactive prompt
python main.py data/ --db-type postgresql --db-host host --db-name db \
    --db-user user --prompt-password

# Method 3: Password from file
export DB_PASSWORD=$(cat /secure/path/db_password.txt)
python main.py data/ --database-url postgresql://user:${DB_PASSWORD}@host/db
```

## Troubleshooting

### Common Issues

1. **Missing dependencies**
   ```bash
   pip install psycopg2-binary  # PostgreSQL
   pip install PyMySQL          # MySQL
   pip install PyYAML           # YAML support
   ```

2. **Connection timeout**
   - Increase `--connection-timeout` value
   - Check network connectivity
   - Verify database server is running

3. **Authentication errors**
   - Verify username and password
   - Check database user permissions
   - Ensure database exists

4. **Table creation errors**
   - Use `--no-create-table` if table exists
   - Check user has CREATE permissions
   - Verify schema compatibility

### Debug Commands

```bash
# Test configuration without processing data
python main.py dummy_dir --validate-config --db-config-file config/database.json

# Run with debug logging
python main.py data/ --log-level DEBUG --database-url postgresql://user:pass@host/db

# Dry run to see what would be processed
python main.py data/ --dry-run --database-url postgresql://user:pass@host/db
```

## Advanced Features

### Connection Pooling
- Automatically enabled for better performance
- Configure with `--pool-size` option
- Managed transparently by the framework

### Batch Processing
- Large datasets processed in configurable batches
- Adjust `--batch-size` based on memory and performance
- Default 1000 records per batch

### Schema Inference
- Automatic table creation from data structure
- Supports JSON, numeric, text, and boolean types
- Handles nested objects as JSON columns

### Metadata Enrichment
Processed data includes additional columns:
- `_source_file` - Original file path
- `_file_type` - File type (json, csv, etc.)
- `_processing_timestamp` - When record was processed

## Integration Examples

### CI/CD Pipeline
```yaml
# .github/workflows/data-pipeline.yml
steps:
  - name: Process and load data
    run: |
      python main.py data/ \
        --database-url ${{ secrets.DATABASE_URL }} \
        --table-name daily_data \
        --load-strategy replace \
        --batch-size 5000
```

### Scheduled Processing
```bash
#!/bin/bash
# daily-processing.sh
export DB_PASSWORD=$(vault kv get -field=password secret/database)
python main.py /data/daily/ \
    --db-config-file /config/production.json \
    --table-name daily_metrics \
    --load-strategy upsert \
    --log-level INFO
```

This guide covers all database CLI features. For additional help, use `python main.py --help` or refer to the main project documentation.