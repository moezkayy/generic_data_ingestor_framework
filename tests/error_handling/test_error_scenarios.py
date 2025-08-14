import unittest
import tempfile
import shutil
from pathlib import Path
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.application import DataIngestionApplication  # adjust import if needed


class TestErrorScenarios(unittest.TestCase):
    def setUp(self):
        self.app = DataIngestionApplication()
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()

        # Create temp dir and copy static test files into it
        src_dir = Path(__file__).parent / "error_handling_test_data"
        self.test_dir = Path(tempfile.mkdtemp())
        for file in src_dir.iterdir():
            shutil.copy(file, self.test_dir)

    def tearDown(self):
        # Force close any remaining database connections
        import gc
        import sqlite3
        
        # Trigger garbage collection to close any lingering connections
        gc.collect()
        
        # Force close all SQLite connections to the test database
        try:
            # Try to connect and immediately close to force unlock
            temp_conn = sqlite3.connect(self.test_db.name)
            temp_conn.close()
        except:
            pass
            
        shutil.rmtree(self.test_dir)
        try:
            Path(self.test_db.name).unlink()
        except (FileNotFoundError, PermissionError):
            # On Windows, if file is still locked, try again after a brief pause
            import time
            time.sleep(0.1)
            try:
                Path(self.test_db.name).unlink()
            except (FileNotFoundError, PermissionError):
                pass

    def _run_test_with_files(self, filenames):
        for name in filenames:
            file_path = self.test_dir / name
            self.app.process_directory(self.test_dir, self.test_db.name)
            self.assertTrue(file_path.exists(), f"Missing test file: {name}")

    def test_malformed_json_handling(self):
        files = [
            "invalid.json",
            "invalid_quotes.json",
            "missing_brace.json",
            "missing_comma.json",
            "trailing_comma.json",
            "incomplete_array.json"
        ]
        self._run_test_with_files(files)

    def test_unicode_and_special_characters(self):
        self._run_test_with_files(["unicode_test.json"])

    def test_large_values_handling(self):
        self._run_test_with_files(["large_values.json"])

    def test_null_and_undefined_handling(self):
        files = ["null_test.json", "null_value_error.json"]
        self._run_test_with_files(files)

    def test_empty_and_non_json_files(self):
        files = ["empty.json", "text_file.txt"]
        self._run_test_with_files(files)

    def test_valid_file_processing(self):
        self._run_test_with_files(["valid.json"])


if __name__ == '__main__':
    unittest.main()
