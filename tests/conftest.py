"""
Pytest configuration and shared fixtures for all tests.
"""

import pytest
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock
from typing import Dict, Any, Generator
import logging

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.handlers.logging_handler import LoggingHandler


@pytest.fixture(scope="session")
def test_logger() -> logging.Logger:
    """Create a test logger with appropriate configuration."""
    log_handler = LoggingHandler("DEBUG")
    return log_handler.setup_logging()


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Get path to test data directory."""
    return Path(__file__).parent.parent / "test_data"


@pytest.fixture
def sample_customers_file(test_data_dir: Path) -> Path:
    """Path to sample customers JSON file."""
    return test_data_dir / "sample_customers.json"


@pytest.fixture  
def sample_products_file(test_data_dir: Path) -> Path:
    """Path to sample products JSON file."""
    return test_data_dir / "sample_products.json"


@pytest.fixture
def ecommerce_orders_file(test_data_dir: Path) -> Path:
    """Path to ecommerce orders JSON file."""
    return test_data_dir / "ecommerce_orders.json"


@pytest.fixture
def financial_transactions_file(test_data_dir: Path) -> Path:
    """Path to financial transactions JSON file."""
    return test_data_dir / "financial_transactions.json"


@pytest.fixture
def large_dataset_file(test_data_dir: Path) -> Path:
    """Path to large dataset for performance testing."""
    return test_data_dir / "performance" / "large_dataset.json"


@pytest.fixture
def medium_dataset_file(test_data_dir: Path) -> Path:
    """Path to medium dataset for performance testing."""
    return test_data_dir / "performance" / "medium_dataset.json"


@pytest.fixture
def small_dataset_file(test_data_dir: Path) -> Path:
    """Path to small dataset for performance testing."""
    return test_data_dir / "performance" / "small_dataset.json"


@pytest.fixture
def regression_dataset_file(test_data_dir: Path) -> Path:
    """Path to regression dataset for performance testing."""
    return test_data_dir / "performance" / "regression_dataset.json"


@pytest.fixture
def edge_cases_file(test_data_dir: Path) -> Path:
    """Path to edge cases test data."""
    return test_data_dir / "edge_cases.json"


@pytest.fixture
def malformed_json_file(test_data_dir: Path) -> Path:
    """Path to malformed JSON test file."""
    return test_data_dir / "malformed.json"


@pytest.fixture
def empty_data_file(test_data_dir: Path) -> Path:
    """Path to empty data test file."""
    return test_data_dir / "empty_data.json"


@pytest.fixture
def error_handling_dir(test_data_dir: Path) -> Path:
    """Path to error handling test data directory."""
    return test_data_dir / "error_handling"


@pytest.fixture
def integration_test_dir(test_data_dir: Path) -> Path:
    """Path to integration test data directory."""
    return test_data_dir / "integration"


@pytest.fixture
def temp_db_file() -> Generator[str, None, None]:
    """Create a temporary SQLite database file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_file:
        temp_path = temp_file.name
    
    yield temp_path
    
    # Cleanup
    try:
        os.unlink(temp_path)
    except OSError:
        pass


@pytest.fixture
def sample_connection_params() -> Dict[str, Any]:
    """Sample connection parameters for testing."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'username': 'test_user',
        'password': 'test_pass',
        'connection_pool_size': 3,
        'connection_timeout': 30,
        'max_retries': 2,
        'retry_delay': 1
    }


@pytest.fixture
def sample_mysql_params() -> Dict[str, Any]:
    """Sample MySQL connection parameters."""
    return {
        'host': 'localhost',
        'port': 3306,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass',
        'connection_pool_size': 3,
        'connection_timeout': 30
    }


@pytest.fixture
def sample_sqlite_params(temp_db_file: str) -> Dict[str, Any]:
    """Sample SQLite connection parameters."""
    return {
        'database': temp_db_file,
        'timeout': 30,
        'check_same_thread': False,
        'create_if_not_exists': True
    }


@pytest.fixture
def sample_table_schema() -> list:
    """Basic table schema for testing."""
    return [
        {'name': 'id', 'type': 'INTEGER', 'nullable': False},
        {'name': 'name', 'type': 'TEXT', 'nullable': True},
        {'name': 'email', 'type': 'TEXT', 'nullable': True}
    ]


@pytest.fixture
def mock_connection():
    """Create a mock database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Configure cursor mock
    mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'test'}]
    mock_cursor.fetchone.return_value = {'count': 1}
    mock_cursor.rowcount = 1
    
    # Configure connection mock
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    mock_conn.commit.return_value = None
    mock_conn.rollback.return_value = None
    mock_conn.close.return_value = None
    
    return mock_conn


@pytest.fixture
def mock_connection_pool():
    """Create a mock connection pool."""
    mock_pool = MagicMock()
    mock_connection = Mock()
    
    mock_pool.getconn.return_value = mock_connection
    mock_pool.putconn.return_value = None
    mock_pool.closeall.return_value = None
    
    return mock_pool


# Test data constants
TEST_DATABASE_CONFIGS = {
    'postgresql': {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_postgres',
        'username': 'postgres',
        'password': 'postgres'
    },
    'mysql': {
        'host': 'localhost',
        'port': 3306,
        'database': 'test_mysql',
        'user': 'root',
        'password': 'mysql'
    },
    'sqlite': {
        'database': ':memory:'
    }
}


# Skip integration tests if databases are not available
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "postgresql: mark test as requiring PostgreSQL"
    )
    config.addinivalue_line(
        "markers", "mysql: mark test as requiring MySQL"
    )
    config.addinivalue_line(
        "markers", "sqlite: mark test as requiring SQLite"
    )


def pytest_runtest_setup(item):
    """Setup function to skip tests based on markers."""
    # Skip integration tests in CI/CD if databases are not available
    if "integration" in item.keywords:
        # Add logic here to check if databases are available
        pass