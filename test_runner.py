# test_runner.py - Custom test discovery and execution
import unittest
import sys
import os
from pathlib import Path

class CustomTestRunner:
    def __init__(self):
        self.test_dir = Path(__file__).parent / "tests"
        self.coverage_enabled = True
        
    def discover_and_run(self):
        """Discover and run all tests with coverage reporting"""
        # Add src to path for imports
        src_path = Path(__file__).parent / "src"
        sys.path.insert(0, str(src_path))
        
        # Discover tests
        loader = unittest.TestLoader()
        suite = loader.discover(self.test_dir, pattern='test_*.py')
        
        # Run tests with detailed output
        runner = unittest.TextTestRunner(verbosity=2, buffer=True)
        result = runner.run(suite)
        
        return result

if __name__ == "__main__":
    runner = CustomTestRunner()
    result = runner.discover_and_run()
    sys.exit(0 if result.wasSuccessful() else 1)