import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import json


class LoggingHandler:

    def __init__(self):
        """Initialize the logging handler"""
        self.log_dir = Path('logs')
        self.logger = None
        self.log_file_path = None
        self.performance_logs = []

    def setup_logging(self, level: str = 'INFO',
                      log_file: Optional[str] = None,
                      console_output: bool = True,
                      file_output: bool = True) -> logging.Logger:

        # Create logs directory
        self.log_dir.mkdir(exist_ok=True)

        # Generate log file name if not provided
        if log_file is None and file_output:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file_path = self.log_dir / f"ingestion_{timestamp}.log"
        elif log_file:
            self.log_file_path = Path(log_file)

        # Remove existing handlers to avoid duplication
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Configure logging format
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )

        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )

        handlers = []

        # Setup file handler
        if file_output and self.log_file_path:
            file_handler = logging.FileHandler(
                self.log_file_path,
                mode='w',
                encoding='utf-8'
            )
            file_handler.setFormatter(detailed_formatter)
            file_handler.setLevel(getattr(logging, level.upper()))
            handlers.append(file_handler)

        # Setup console handler
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(simple_formatter)
            console_handler.setLevel(getattr(logging, level.upper()))
            handlers.append(console_handler)

        # Configure root logger
        root_logger.setLevel(logging.DEBUG)
        for handler in handlers:
            root_logger.addHandler(handler)

        # Get application logger
        self.logger = logging.getLogger('data_ingestion')
        self.logger.info(f"Logging initialized - Level: {level}")
        if self.log_file_path:
            self.logger.info(f"Log file: {self.log_file_path}")

        return self.logger

    def get_logger(self, name: str) -> logging.Logger:

        return logging.getLogger(f'data_ingestion.{name}')

    def log_performance_metric(self, operation: str, duration: float,
                               record_count: Optional[int] = None,
                               file_size: Optional[int] = None,
                               additional_metrics: Optional[Dict] = None):

        performance_data = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'duration_seconds': round(duration, 3),
            'duration_ms': round(duration * 1000, 1)
        }

        if record_count:
            performance_data['record_count'] = record_count
            performance_data['records_per_second'] = round(record_count / duration, 1)

        if file_size:
            performance_data['file_size_bytes'] = file_size
            performance_data['file_size_mb'] = round(file_size / (1024 * 1024), 3)
            performance_data['mb_per_second'] = round((file_size / (1024 * 1024)) / duration, 2)

        if additional_metrics:
            performance_data.update(additional_metrics)

        # Store for later analysis
        self.performance_logs.append(performance_data)

        # Log to main logger
        if self.logger:
            msg = f"PERFORMANCE - {operation}: {duration:.3f}s"
            if record_count:
                msg += f" ({record_count} records, {record_count / duration:.1f} records/s)"
            if file_size:
                size_mb = file_size / (1024 * 1024)
                msg += f" ({size_mb:.2f}MB, {size_mb / duration:.2f}MB/s)"

            self.logger.info(msg)

    def log_file_operation(self, operation: str, file_path: Path,
                           success: bool, details: Optional[str] = None,
                           file_size: Optional[int] = None):

        if self.logger:
            status = "SUCCESS" if success else "FAILED"
            msg = f"FILE {operation} - {status}: {file_path.name}"

            if file_size:
                size_mb = file_size / (1024 * 1024)
                msg += f" ({size_mb:.2f}MB)"

            if details:
                msg += f" - {details}"

            if success:
                self.logger.info(msg)
            else:
                self.logger.error(msg)

    def log_schema_operation(self, operation: str, schema_info: Dict[str, Any],
                             success: bool = True):
        
        if self.logger:
            status = "SUCCESS" if success else "FAILED"

            field_count = len(schema_info.get('properties', {}))
            schema_type = schema_info.get('type', 'unknown')

            msg = f"SCHEMA {operation} - {status}: Type={schema_type}, Fields={field_count}"

            if success:
                self.logger.info(msg)
            else:
                self.logger.error(msg)

    def log_validation_result(self, file_path: Path, validation_result: Dict[str, Any]):
        
        if self.logger:
            is_valid = validation_result.get('valid', False)
            error_count = len(validation_result.get('errors', []))

            if is_valid:
                self.logger.info(f"VALIDATION SUCCESS: {file_path.name}")
            else:
                self.logger.warning(f"VALIDATION FAILED: {file_path.name} ({error_count} errors)")

                # Log first few errors
                for i, error in enumerate(validation_result.get('errors', [])[:3]):
                    self.logger.warning(f"  Validation Error {i + 1}: {error}")

                if error_count > 3:
                    self.logger.warning(f"  ... and {error_count - 3} more errors")

    def log_processing_summary(self, summary_data: Dict[str, Any]):
        
        if self.logger:
            self.logger.info("=" * 60)
            self.logger.info("PROCESSING SUMMARY")
            self.logger.info("=" * 60)

            total_files = summary_data.get('total_files', 0)
            successful_files = summary_data.get('successful_files', 0)
            failed_files = summary_data.get('failed_files', 0)

            self.logger.info(f"Total files processed: {total_files}")
            self.logger.info(f"Successful: {successful_files}")
            self.logger.info(f"Failed: {failed_files}")

            if total_files > 0:
                success_rate = (successful_files / total_files) * 100
                self.logger.info(f"Success rate: {success_rate:.1f}%")

            # Log file type breakdown
            file_types = summary_data.get('file_types', {})
            for file_type, count in file_types.items():
                self.logger.info(f"{file_type.upper()} files: {count}")

            # Log performance summary
            total_duration = summary_data.get('total_duration', 0)
            if total_duration > 0:
                self.logger.info(f"Total processing time: {total_duration:.2f}s")
                if total_files > 0:
                    avg_time = total_duration / total_files
                    self.logger.info(f"Average time per file: {avg_time:.3f}s")

            self.logger.info("=" * 60)

    def export_performance_logs(self, output_file: Optional[Path] = None) -> Path:
        
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.log_dir / f"performance_metrics_{timestamp}.json"

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.performance_logs, f, indent=2, ensure_ascii=False)

            if self.logger:
                self.logger.info(f"Performance logs exported to: {output_file}")

            return output_file

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to export performance logs: {e}")
            raise

    def get_performance_summary(self) -> Dict[str, Any]:
        
        if not self.performance_logs:
            return {'total_operations': 0}

        total_duration = sum(log['duration_seconds'] for log in self.performance_logs)
        total_records = sum(log.get('record_count', 0) for log in self.performance_logs)
        total_size = sum(log.get('file_size_bytes', 0) for log in self.performance_logs)

        operations = [log['operation'] for log in self.performance_logs]
        operation_counts = {}
        for op in operations:
            operation_counts[op] = operation_counts.get(op, 0) + 1

        return {
            'total_operations': len(self.performance_logs),
            'total_duration_seconds': round(total_duration, 3),
            'total_records_processed': total_records,
            'total_data_size_bytes': total_size,
            'total_data_size_mb': round(total_size / (1024 * 1024), 3),
            'operation_breakdown': operation_counts,
            'average_duration_per_operation': round(total_duration / len(self.performance_logs), 3),
            'records_per_second_overall': round(total_records / total_duration, 1) if total_duration > 0 else 0
        }

    def set_log_level(self, level: str):
        
        if self.logger:
            numeric_level = getattr(logging, level.upper())
            self.logger.setLevel(numeric_level)

            # Update all handlers
            for handler in self.logger.handlers:
                handler.setLevel(numeric_level)

            self.logger.info(f"Logging level changed to: {level}")

    def close_logging(self):
        """Close all logging handlers and export final logs"""
        if self.logger:
            self.logger.info("Shutting down logging system")

            # Export performance logs
            try:
                self.export_performance_logs()
            except Exception as e:
                print(f"Warning: Could not export performance logs: {e}")

            # Close all handlers
            for handler in self.logger.handlers[:]:
                handler.close()
                self.logger.removeHandler(handler)
