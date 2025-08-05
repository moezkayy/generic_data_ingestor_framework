#!/usr/bin/env python3
"""
Test runner for the Generic Data Ingestor Framework.

This script runs tests in the proper order and handles import dependencies.
"""

import sys
import os
import subprocess
from pathlib import Path
from typing import List, Dict, Any


def run_command(cmd: List[str], description: str = "") -> Dict[str, Any]:
    """Run a command and return results."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        return {
            'success': result.returncode == 0,
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except Exception as e:
        print(f"Error running command: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def main():
    """Main test runner."""
    python_exe = sys.executable
    results = []
    
    print("Generic Data Ingestor Framework - Test Runner")
    print("=" * 60)
    
    # Test 1: Basic functionality test
    result = run_command(
        [python_exe, "test_simple.py"],
        "Basic functionality test"
    )
    results.append(("Basic Test", result['success']))
    
    # Test 2: Logging handler tests (isolated)
    logging_test_cmd = [
        python_exe, "-c", """
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))

from src.handlers.logging_handler import LoggingHandler
import logging

# Test logging initialization
handler = LoggingHandler()
logger = handler.setup_logging(level='DEBUG', console_output=True, file_output=False)

# Test logging with database-style messages
test_logger = logging.getLogger('data_ingestion.test')
test_logger.info('DB_CONNECTION_TEST: Testing logging functionality')
test_logger.debug('DB_QUERY_EXECUTE: Mock query executed in 0.001s')
test_logger.info('DB_INSERT: Mock insert completed successfully')

print('Logging tests completed successfully!')
"""
    ]
    
    result = run_command(logging_test_cmd, "Logging Handler Test")
    results.append(("Logging Test", result['success']))
    
    # Test 3: Import tests (test that modules can be imported individually)
    modules_to_test = [
        "src.handlers.logging_handler",
        "src.utils.config_manager",
        "src.processors.schema_processor"
    ]
    
    for module in modules_to_test:
        import_test_cmd = [
            python_exe, "-c", f"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))

try:
    import {module}
    print(f'Successfully imported {module}')
except ImportError as e:
    print(f'Failed to import {module}: {{e}}')
    sys.exit(1)
"""
        ]
        
        result = run_command(import_test_cmd, f"Import test for {module}")
        results.append((f"Import {module}", result['success']))
    
    # Test 4: SQLite connector test (doesn't require external dependencies)
    sqlite_test_cmd = [
        python_exe, "-c", """
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))

from src.connectors.sqlite_connector import SQLiteConnector

# Test SQLite connector with in-memory database
params = {
    'database': ':memory:',
    'timeout': 30,
    'check_same_thread': False
}

try:
    connector = SQLiteConnector(params)
    with connector:
        # Test basic operations
        result = connector.execute_query('SELECT 1 as test')
        assert result[0]['test'] == 1
        
        # Test table creation
        schema = [
            {'name': 'id', 'type': 'integer', 'primary_key': True},
            {'name': 'name', 'type': 'text', 'nullable': False}
        ]
        
        connector.create_table('test_table', schema)
        assert connector.table_exists('test_table')
        
        # Test data insertion
        connector.execute_query('INSERT INTO test_table (name) VALUES (?)', ['Test Name'])
        
        # Test data retrieval
        data = connector.execute_query('SELECT * FROM test_table')
        assert len(data) == 1
        assert data[0]['name'] == 'Test Name'
        
        print('SQLite connector test completed successfully!')

except Exception as e:
    print(f'SQLite connector test failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"""
    ]
    
    result = run_command(sqlite_test_cmd, "SQLite Connector Test")
    results.append(("SQLite Connector", result['success']))
    
    # Test 5: Unit test structure verification
    unit_test_verification_cmd = [
        python_exe, "-c", """
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))

# Check if our test files exist and are structured correctly
test_files = [
    'tests/conftest.py',
    'tests/unit/connectors/test_database_connector.py',
    'tests/unit/connectors/test_sqlite_connector.py',
    'tests/integration/connectors/test_sqlite_integration.py',
    'tests/utils/test_helpers.py'
]

missing_files = []
for test_file in test_files:
    if not Path(test_file).exists():
        missing_files.append(test_file)

if missing_files:
    print(f'Missing test files: {missing_files}')
    sys.exit(1)
else:
    print('All test files exist')
    
# Check test structure
from tests.utils.test_helpers import TestDatabaseManager, TestDataGenerator
manager = TestDatabaseManager()
generator = TestDataGenerator()

# Test helper functions
schema = generator.create_sample_schema()
assert len(schema) > 0
print(f'Generated sample schema with {len(schema)} columns')

data = generator.create_sample_data(5)
assert len(data) == 5
print(f'Generated sample data with {len(data)} records')

# Test database availability check
sqlite_available = manager.is_database_available('sqlite')
print(f'SQLite available: {sqlite_available}')

print('Test structure verification completed successfully!')
"""
    ]
    
    result = run_command(unit_test_verification_cmd, "Test Structure Verification")
    results.append(("Test Structure", result['success']))
    
    # Summary
    print("\n" + "="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)
    
    total_tests = len(results)
    passed_tests = sum(1 for _, success in results if success)
    
    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{test_name:<30} {status}")
    
    print("-" * 60)
    print(f"Total: {total_tests}, Passed: {passed_tests}, Failed: {total_tests - passed_tests}")
    
    if passed_tests == total_tests:
        print("\nAll tests passed! [SUCCESS]")
        return 0
    else:
        print(f"\n{total_tests - passed_tests} test(s) failed! [FAILURE]")
        return 1


if __name__ == "__main__":
    sys.exit(main())