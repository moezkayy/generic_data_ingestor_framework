# tests/performance/test_performance_benchmarks.py
import unittest
import time
import json
import tempfile
from pathlib import Path
import statistics
import sys
import os

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.application import DataIngestionApplication

class PerformanceTestSuite(unittest.TestCase):
    """Performance benchmarking and regression testing"""
    
    def setUp(self):
        self.app = DataIngestionApplication()
        self.performance_thresholds = {
            'small_dataset': 0.1,    # < 100ms for small datasets
            'medium_dataset': 0.5,   # < 500ms for medium datasets  
            'large_dataset': 2.0,    # < 2s for large datasets
            'memory_limit': 100      # < 100MB memory usage
        }
        
    def create_performance_test_data(self, record_count, complexity='medium'):
        """Generate test data of specified size and complexity"""
        test_data = []
        
        for i in range(record_count):
            if complexity == 'simple':
                record = {
                    "id": i,
                    "name": f"User {i}",
                    "email": f"user{i}@example.com"
                }
            elif complexity == 'medium':
                record = {
                    "user_id": f"usr_{i:06d}",
                    "profile": {
                        "first_name": f"User",
                        "last_name": f"Number{i}",
                        "email": f"user{i}@example.com"
                    },
                    "preferences": {
                        "theme": "dark" if i % 2 == 0 else "light",
                        "notifications": {
                            "email": True,
                            "push": i % 3 == 0,
                            "sms": False
                        }
                    },
                    "metadata": {
                        "created_at": "2024-01-20T10:30:00Z",
                        "last_login": "2024-01-20T15:45:30Z",
                        "login_count": i * 3 + 1
                    }
                }
            else:  # complex
                record = {
                    "transaction_id": f"TXN-{i:08d}",
                    "customer": {
                        "id": f"CUST-{i % 100}",
                        "profile": {
                            "name": f"Customer {i % 100}",
                            "demographics": {
                                "age_group": "25-34",
                                "location": {
                                    "country": "US",
                                    "state": "NY",
                                    "city": "New York",
                                    "coordinates": {
                                        "lat": 40.7128 + (i % 100) * 0.001,
                                        "lng": -74.0060 + (i % 100) * 0.001
                                    }
                                }
                            }
                        }
                    },
                    "transaction_details": {
                        "amount": round(99.99 + (i % 1000) * 0.1, 2),
                        "currency": "USD",
                        "payment_method": {
                            "type": "credit_card",
                            "provider": "visa",
                            "last_four": str(1000 + i % 9999)
                        },
                        "items": [
                            {
                                "product_id": f"PROD-{j:04d}",
                                "quantity": j + 1,
                                "unit_price": 19.99 + j * 5.00
                            } for j in range(min(3, i % 5 + 1))
                        ]
                    }
                }
            
            test_data.append(record)
            
        return test_data
    
    def measure_performance(self, test_data, test_name):
        """Measure processing performance with detailed metrics"""
        # Create temporary test file
        temp_dir = tempfile.mkdtemp()
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        
        test_file = Path(temp_dir) / f"{test_name}.json"
        with open(test_file, 'w') as f:
            json.dump(test_data, f)
        
        # Measure memory before (if psutil available)
        memory_before = 0
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                memory_before = process.memory_info().rss / 1024 / 1024  # MB
            except:
                memory_before = 0
        
        # Measure processing time
        start_time = time.perf_counter()
        result = self.app.process_directory(temp_dir, temp_db.name)
        end_time = time.perf_counter()
        
        processing_time = end_time - start_time
        
        # Measure memory after (if psutil available)
        memory_used = 0
        if PSUTIL_AVAILABLE:
            try:
                memory_after = process.memory_info().rss / 1024 / 1024  # MB
                memory_used = memory_after - memory_before
            except:
                memory_used = 0
        
        # Calculate throughput
        records_processed = result['total_records']
        throughput = records_processed / processing_time if processing_time > 0 else 0
        
        # Clean up
        Path(test_file).unlink()
        # Don't delete database on Windows - it may be locked
        Path(temp_dir).rmdir()
        
        return {
            'processing_time': processing_time,
            'records_processed': records_processed,
            'throughput': throughput,
            'memory_used': memory_used,
            'result': result
        }
    
    def test_small_dataset_performance(self):
        """Test performance with small dataset (10 records)"""
        # Arrange
        test_data = self.create_performance_test_data(10, 'simple')
        
        # Act
        metrics = self.measure_performance(test_data, 'small_dataset')
        
        # Assert
        self.assertLess(metrics['processing_time'], self.performance_thresholds['small_dataset'],
                       f"Small dataset processing took {metrics['processing_time']:.3f}s, expected < {self.performance_thresholds['small_dataset']}s")
        
        self.assertEqual(metrics['records_processed'], 10)
        self.assertGreater(metrics['throughput'], 100)  # > 100 records/sec
        
        print(f"Small Dataset Performance:")
        print(f"  Processing Time: {metrics['processing_time']:.4f}s")
        print(f"  Throughput: {metrics['throughput']:,.0f} records/sec")
        print(f"  Memory Used: {metrics['memory_used']:.1f} MB")
        
    def test_medium_dataset_performance(self):
        """Test performance with medium dataset (500 records)"""
        # Arrange
        test_data = self.create_performance_test_data(500, 'medium')
        
        # Act
        metrics = self.measure_performance(test_data, 'medium_dataset')
        
        # Assert
        self.assertLess(metrics['processing_time'], self.performance_thresholds['medium_dataset'],
                       f"Medium dataset processing took {metrics['processing_time']:.3f}s, expected < {self.performance_thresholds['medium_dataset']}s")
        
        self.assertEqual(metrics['records_processed'], 500)
        self.assertGreater(metrics['throughput'], 1000)  # > 1,000 records/sec
        
        print(f"Medium Dataset Performance:")
        print(f"  Processing Time: {metrics['processing_time']:.4f}s")
        print(f"  Throughput: {metrics['throughput']:,.0f} records/sec")
        print(f"  Memory Used: {metrics['memory_used']:.1f} MB")
        
    def test_large_dataset_performance(self):
        """Test performance with large dataset (2000 records)"""
        # Arrange
        test_data = self.create_performance_test_data(2000, 'complex')
        
        # Act
        metrics = self.measure_performance(test_data, 'large_dataset')
        
        # Assert
        self.assertLess(metrics['processing_time'], self.performance_thresholds['large_dataset'],
                       f"Large dataset processing took {metrics['processing_time']:.3f}s, expected < {self.performance_thresholds['large_dataset']}s")
        
        self.assertEqual(metrics['records_processed'], 2000)
        self.assertGreater(metrics['throughput'], 1000)  # > 1,000 records/sec
        if PSUTIL_AVAILABLE:
            self.assertLess(metrics['memory_used'], self.performance_thresholds['memory_limit'],
                           f"Memory usage {metrics['memory_used']:.1f} MB exceeded limit of {self.performance_thresholds['memory_limit']} MB")
        
        print(f"Large Dataset Performance:")
        print(f"  Processing Time: {metrics['processing_time']:.4f}s")
        print(f"  Throughput: {metrics['throughput']:,.0f} records/sec")
        print(f"  Memory Used: {metrics['memory_used']:.1f} MB")
        
    def test_performance_regression(self):
        """Test for performance regression by running multiple iterations"""
        # Run multiple iterations to check consistency
        iterations = 5
        test_data = self.create_performance_test_data(1000, 'medium')
        
        processing_times = []
        throughputs = []
        
        for i in range(iterations):
            metrics = self.measure_performance(test_data, f'regression_test_{i}')
            processing_times.append(metrics['processing_time'])
            throughputs.append(metrics['throughput'])
        
        # Calculate statistics
        avg_time = statistics.mean(processing_times)
        std_time = statistics.stdev(processing_times) if len(processing_times) > 1 else 0
        avg_throughput = statistics.mean(throughputs)
        std_throughput = statistics.stdev(throughputs) if len(throughputs) > 1 else 0
        
        # Assert consistency (coefficient of variation < 20%)
        cv_time = (std_time / avg_time) * 100 if avg_time > 0 else 0
        self.assertLess(cv_time, 20, f"Processing time too variable: {cv_time:.1f}% CV")
        
        print(f"Performance Regression Test ({iterations} iterations):")
        print(f"  Average Processing Time: {avg_time:.4f}s (±{std_time:.4f}s)")
        print(f"  Average Throughput: {avg_throughput:,.0f} records/sec (±{std_throughput:,.0f})")
        print(f"  Coefficient of Variation: {cv_time:.1f}%")

if __name__ == "__main__":
    unittest.main()