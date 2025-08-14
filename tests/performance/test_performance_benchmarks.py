import unittest
import tempfile
import shutil
from pathlib import Path
import sys
import os
import time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.application import DataIngestionApplication  # adjust if needed


class TestPerformanceScenarios(unittest.TestCase):
    def setUp(self):
        self.app = DataIngestionApplication()
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db.close()

        # Store the source directory for copying specific files as needed
        self.src_dir = Path(__file__).parent / "performance_test_data"

    def tearDown(self):
        import gc
        import sqlite3

        gc.collect()
        try:
            temp_conn = sqlite3.connect(self.test_db.name)
            temp_conn.close()
        except:
            pass

        # Clean up any temporary test directories
        if hasattr(self, 'test_dir') and self.test_dir.exists():
            shutil.rmtree(self.test_dir)
            
        try:
            Path(self.test_db.name).unlink()
        except (FileNotFoundError, PermissionError):
            time.sleep(0.1)
            try:
                Path(self.test_db.name).unlink()
            except (FileNotFoundError, PermissionError):
                pass

    def _run_performance_test(self, filenames, max_seconds, expected_min_throughput=None):
        """
        Run performance test on specific files with timing and throughput validation.
        
        Args:
            filenames: List of filenames to test
            max_seconds: Maximum allowed processing time
            expected_min_throughput: Minimum expected records per second (optional)
        """
        # Create a clean temp directory for this specific test
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Copy only the specified files to the test directory
        available_files = []
        for name in filenames:
            src_file = self.src_dir / name
            if src_file.exists():
                shutil.copy(src_file, self.test_dir)
                available_files.append(name)
            else:
                self.skipTest(f"Test data file not found: {name}")

        if not available_files:
            self.skipTest("No test files available for performance test")

        start_time = time.time()
        result = self.app.process_directory(self.test_dir, self.test_db.name)
        elapsed = time.time() - start_time

        # Validate processing succeeded
        self.assertTrue(result.get('success', False), f"Processing failed: {result}")
        
        # Validate timing performance
        self.assertLessEqual(elapsed, max_seconds, 
                           f"Processing took {elapsed:.2f}s, exceeded limit of {max_seconds}s")
        
        # Validate throughput if specified
        if expected_min_throughput and result.get('total_records', 0) > 0:
            actual_throughput = result['total_records'] / elapsed
            self.assertGreaterEqual(actual_throughput, expected_min_throughput,
                                  f"Throughput {actual_throughput:.2f} records/sec below minimum {expected_min_throughput}")

        print(f"Processed {available_files} in {elapsed:.2f}s "
              f"({result.get('total_records', 0)} records, "
              f"{result.get('total_records', 0)/elapsed:.2f} records/sec)")

    def test_small_dataset_performance(self):
        """Test performance with small dataset - should be very fast."""
        self._run_performance_test(
            filenames=["small_dataset.json"],
            max_seconds=5.0,
            expected_min_throughput=10
        )

    def test_medium_dataset_performance(self):
        """Test performance with medium datasets - moderate performance requirements."""
        self._run_performance_test(
            filenames=["medium_dataset.json"],
            max_seconds=8.0,
            expected_min_throughput=50
        )

    def test_medium_sample_dataset_performance(self):
        """Test performance with medium sample dataset."""
        self._run_performance_test(
            filenames=["medium_dataset_sample.json"],
            max_seconds=5.0,
            expected_min_throughput=10
        )

    def test_large_dataset_performance(self):
        """Test performance with large dataset - stress test for scalability."""
        self._run_performance_test(
            filenames=["large_dataset.json"],
            max_seconds=10.0,
            expected_min_throughput=25
        )

    def test_large_sample_dataset_performance(self):
        """Test performance with large sample dataset."""
        self._run_performance_test(
            filenames=["large_dataset_sample.json"],
            max_seconds=8.0,
            expected_min_throughput=30
        )

    def test_regression_dataset_performance(self):
        """Test performance with regression dataset - ensure no performance degradation."""
        self._run_performance_test(
            filenames=["regression_dataset.json"],
            max_seconds=5.0,
            expected_min_throughput=50
        )

    def test_bulk_file_processing_performance(self):
        """Test bulk processing of all dataset files together."""
        all_dataset_files = [
            "small_dataset.json",
            "medium_dataset.json", 
            "medium_dataset_sample.json",
            "large_dataset.json",
            "large_dataset_sample.json",
            "regression_dataset.json"
        ]
        self._run_performance_test(
            filenames=all_dataset_files,
            max_seconds=20.0,
            expected_min_throughput=20
        )


if __name__ == '__main__':
    unittest.main()
