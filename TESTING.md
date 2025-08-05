# Testing Guide for Generic Data Ingestor Framework

This document provides comprehensive information about the testing infrastructure for the Generic Data Ingestor Framework.

## Test Structure

```
tests/
├── __init__.py                     # Test package initialization
├── conftest.py                     # Pytest configuration and shared fixtures
├── pytest.ini                     # Pytest configuration file
├── unit/                          # Unit tests using mocks
│   ├── __init__.py
│   └── connectors/
│       ├── __init__.py
│       ├── test_database_connector.py      # Base connector tests
│       ├── test_postgresql_connector.py    # PostgreSQL unit tests
│       ├── test_mysql_connector.py         # MySQL unit tests
│       └── test_sqlite_connector.py        # SQLite unit tests
├── integration/                   # Integration tests with real databases
│   ├── __init__.py
│   └── connectors/
│       ├── __init__.py
│       ├── test_postgresql_integration.py  # PostgreSQL integration tests
│       ├── test_mysql_integration.py       # MySQL integration tests
│       └── test_sqlite_integration.py      # SQLite integration tests
├── utils/                         # Test utilities and helpers
│   ├── __init__.py
│   └── test_helpers.py            # Test helper classes and functions
└── fixtures/                      # Test fixtures and data
    ├── postgres-init.sql          # PostgreSQL initialization script
    └── mysql-init.sql             # MySQL initialization script
```

## Test Categories

### Unit Tests
- **Location**: `tests/unit/`
- **Purpose**: Test individual components in isolation using mocks
- **Dependencies**: None (uses mocks for database connections)
- **Execution**: Fast, always runnable
- **Coverage**: All database connector methods, error conditions, edge cases

### Integration Tests
- **Location**: `tests/integration/`
- **Purpose**: Test components with real database connections
- **Dependencies**: Requires actual database instances
- **Execution**: Slower, requires database setup
- **Coverage**: Real-world scenarios, actual database operations

## Running Tests

### Prerequisites

1. **Install Dependencies**:
   ```bash
   pip install pytest pytest-mock
   ```

2. **For Integration Tests** (optional):
   ```bash
   # PostgreSQL
   pip install psycopg2-binary

   # MySQL
   pip install mysql-connector-python
   ```

### Running All Tests

```bash
# Using the custom test runner
python run_tests.py

# Using pytest directly (may fail if database dependencies are missing)
pytest tests/ -v
```

### Running Specific Test Categories

```bash
# Unit tests only
pytest tests/unit/ -v

# Integration tests only (requires databases)
pytest tests/integration/ -v -m integration

# SQLite tests only
pytest tests/ -v -m sqlite

# PostgreSQL tests only (requires PostgreSQL)
pytest tests/ -v -m postgresql

# MySQL tests only (requires MySQL)
pytest tests/ -v -m mysql
```

### Running Individual Test Files

```bash
# Run specific unit test file
pytest tests/unit/connectors/test_sqlite_connector.py -v

# Run specific integration test
pytest tests/integration/connectors/test_sqlite_integration.py -v
```

## Test Configuration

### Environment Variables

For integration tests, set these environment variables:

```bash
# PostgreSQL
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=test_db
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres

# MySQL
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_DB=test_db
export MYSQL_USER=root
export MYSQL_PASSWORD=mysql
```

### Pytest Markers

The following markers are available:

- `unit`: Unit tests that use mocks
- `integration`: Integration tests requiring real databases
- `postgresql`: Tests requiring PostgreSQL
- `mysql`: Tests requiring MySQL
- `sqlite`: Tests requiring SQLite
- `slow`: Tests that take a long time
- `performance`: Performance benchmark tests

### Test Fixtures

Common fixtures available in `conftest.py`:

- `test_logger`: Configured logger for tests
- `temp_db_file`: Temporary SQLite database file
- `sample_connection_params`: Sample connection parameters
- `sample_table_schema`: Sample table schema for testing
- `sample_data`: Sample data for database operations
- `mock_connection`: Mock database connection
- `mock_connection_pool`: Mock connection pool

## Docker-based Testing

### Using Docker Compose

The project includes a `docker-compose.test.yml` file for running integration tests with containerized databases:

```bash
# Start test databases
docker-compose -f docker-compose.test.yml up -d postgres-test mysql-test

# Run tests in container
docker-compose -f docker-compose.test.yml run test-runner

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

### Test Container

The `Dockerfile.test` provides a containerized test environment with all dependencies:

```bash
# Build test image
docker build -f Dockerfile.test -t data-ingestor-tests .

# Run tests
docker run --rm data-ingestor-tests
```

## Test Coverage

### Generating Coverage Reports

```bash
# Install coverage tools
pip install pytest-cov

# Run tests with coverage
pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

# View HTML report
open htmlcov/index.html
```

### Coverage Goals

- **Unit Tests**: 90%+ line coverage
- **Integration Tests**: Cover real-world usage scenarios
- **Edge Cases**: Error conditions, boundary values, invalid inputs
- **Performance**: Database operation timing and throughput

## Test Data and Fixtures

### Test Data Generation

The `TestDataGenerator` class provides utilities for creating test data:

```python
from tests.utils.test_helpers import TestDataGenerator

generator = TestDataGenerator()

# Create sample schema
schema = generator.create_sample_schema()

# Create sample data
data = generator.create_sample_data(count=100)

# Create large dataset for performance testing
large_data = generator.create_large_dataset(count=10000)
```

### Database Test Helpers

The `TestDatabaseManager` class provides database-specific utilities:

```python
from tests.utils.test_helpers import TestDatabaseManager

manager = TestDatabaseManager()

# Check database availability
sqlite_available = manager.is_database_available('sqlite')
postgres_available = manager.is_database_availability('postgresql')

# Create test connectors
sqlite_connector = manager.create_test_sqlite_connector()
postgres_connector = manager.create_test_postgresql_connector()
```

## Writing New Tests

### Unit Test Example

```python
import pytest
from unittest.mock import Mock, patch
from src.connectors.sqlite_connector import SQLiteConnector

class TestNewFeature:
    @pytest.fixture
    def connector(self, sample_sqlite_params):
        return SQLiteConnector(sample_sqlite_params)
    
    def test_new_method(self, connector):
        # Test implementation
        result = connector.new_method()
        assert result is not None
    
    @patch('src.connectors.sqlite_connector.sqlite3.connect')
    def test_new_method_with_mock(self, mock_connect, connector):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        result = connector.new_method()
        
        mock_connect.assert_called_once()
        assert result == expected_result
```

### Integration Test Example

```python
import pytest
from tests.utils.test_helpers import TestDatabaseManager

@pytest.mark.integration
@pytest.mark.sqlite
class TestNewFeatureIntegration:
    @pytest.fixture
    def connector(self):
        manager = TestDatabaseManager()
        return manager.create_test_sqlite_connector()
    
    def test_real_database_operation(self, connector):
        with connector:
            # Test with real database
            result = connector.execute_query("SELECT 1 as test")
            assert result[0]['test'] == 1
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Missing database drivers
   - Solution: Install optional dependencies or skip tests

2. **Connection Errors**: Database not available
   - Solution: Check database configuration and connectivity

3. **Permission Errors**: Database access denied
   - Solution: Verify database credentials and permissions

4. **Test Isolation**: Tests affecting each other
   - Solution: Use proper cleanup in fixtures and test methods

### Skip Tests Conditionally

```python
import pytest
from tests.utils.test_helpers import TestDatabaseManager

# Skip if database not available
@pytest.mark.skipif(
    not TestDatabaseManager.is_database_available('postgresql'),
    reason="PostgreSQL not available"
)
def test_postgresql_feature():
    # Test implementation
    pass
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:13
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: 3.11
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-cov pytest-mock
    
    - name: Run unit tests
      run: pytest tests/unit/ -v
    
    - name: Run integration tests
      run: pytest tests/integration/ -v
      env:
        POSTGRES_HOST: localhost
        POSTGRES_USER: postgres
        POSTGRES_PASSWORD: postgres
```

## Performance Testing

### Benchmarking Database Operations

```python
from tests.utils.test_helpers import TestTimer, TestDataGenerator

def test_bulk_insert_performance():
    timer = TestTimer()
    generator = TestDataGenerator()
    
    large_data = generator.create_large_dataset(10000)
    
    with timer.measure():
        # Perform bulk insert
        connector.bulk_insert('test_table', large_data)
    
    # Assert performance requirements
    assert timer.elapsed < 5.0  # Must complete within 5 seconds
```

## Best Practices

1. **Test Organization**: Group related tests in classes
2. **Naming**: Use descriptive test names that explain the scenario
3. **Isolation**: Each test should be independent
4. **Cleanup**: Always clean up test data and connections
5. **Mocking**: Use mocks for external dependencies in unit tests
6. **Edge Cases**: Test error conditions and boundary values
7. **Documentation**: Document complex test scenarios
8. **Performance**: Include performance tests for critical operations

## Maintenance

### Regular Tasks

1. **Update Dependencies**: Keep test dependencies current
2. **Review Coverage**: Monitor test coverage metrics
3. **Clean Fixtures**: Remove unused test fixtures and data
4. **Performance Monitoring**: Track test execution times
5. **Database Updates**: Test with new database versions

### Adding New Database Support

When adding a new database connector:

1. Create unit tests in `tests/unit/connectors/`
2. Create integration tests in `tests/integration/connectors/`
3. Add helper methods to `TestDatabaseManager`
4. Update Docker Compose configuration
5. Add database-specific markers and fixtures
6. Update this documentation