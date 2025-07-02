from pathlib import Path
from typing import List, Dict, Any
import time

# Use simple absolute imports (no dots)
from handlers.logging_handler import LoggingHandler
from handlers.error_handler import ErrorHandler
from handlers.file_handler import FileHandler
from scanners.file_scanner import FileScanner
from processors.schema_processor import SchemaProcessor
from processors.json_processor import JSONProcessor
from utils.report_generator import ReportGenerator

# ... rest of your application.py code stays the same

class DataIngestionApplication:
    """
    Main application class that orchestrates the data ingestion process
    """

    def __init__(self):
        """Initialize the application with necessary handlers and processors"""
        self.logging_handler = LoggingHandler()
        self.error_handler = ErrorHandler()
        self.file_handler = FileHandler()
        self.report_generator = ReportGenerator()

        self.logger = None
        self.scanner = None
        self.schema_processor = None
        self.json_processor = None

    def run(self, directory: str, output_dir: str, log_level: str = 'INFO',
            dry_run: bool = False, file_types: List[str] = None,
            recursive: bool = True, validate_schemas: bool = True,
            generate_flat_versions: bool = True) -> int:
        """
        Run the complete data ingestion process

        Args:
            directory (str): Source directory to scan
            output_dir (str): Output directory for results
            log_level (str): Logging level
            dry_run (bool): If True, only show what would be processed
            file_types (List[str]): File types to process
            recursive (bool): Whether to scan recursively
            validate_schemas (bool): Whether to validate schemas
            generate_flat_versions (bool): Whether to generate flattened data

        Returns:
            int: Exit code (0 for success, 1 for error)
        """
        start_time = time.time()

        try:
            # Setup logging
            self.logger = self.logging_handler.setup_logging(log_level)
            self.logger.info("🚀 Data Ingestion Framework started")
            self.logger.info(f"📁 Source directory: {directory}")
            self.logger.info(f"📤 Output directory: {output_dir}")

            # Initialize processors
            self._initialize_processors()

            # Validate inputs
            self._validate_inputs(directory, output_dir, file_types)

            # Initialize scanner
            self.scanner = FileScanner(directory)

            # Scan for files
            self.logger.info(f"🔍 Scanning directory: {directory}")
            discovered_files = self.scanner.discover_files(
                file_types=file_types or ['json'],
                recursive=recursive
            )

            if not any(discovered_files.values()):
                self.logger.warning("⚠️  No files found to process")
                return 0

            # Display discovery results
            self._display_discovery_results(discovered_files)

            if dry_run:
                self.logger.info("✅ Dry run completed - no files were processed")
                return 0

            # Create output directory
            output_path = Path(output_dir)
            self.file_handler.create_directory(output_path)

            # Process files
            processing_results = self._process_files(
                discovered_files,
                output_path,
                validate_schemas=validate_schemas,
                generate_flat_versions=generate_flat_versions
            )

            # Generate reports
            self._generate_reports(processing_results, output_path)

            # Calculate and log final statistics
            total_time = time.time() - start_time
            self._log_final_statistics(processing_results, total_time)

            self.logger.info(f"✅ Processing completed successfully in {total_time:.2f}s")
            self.logger.info(f"📊 Results saved to: {output_path}")

            return 0

        except Exception as e:
            self.error_handler.handle_application_error(e, self.logger, "main application")
            return 1
        finally:
            # Cleanup logging
            if self.logging_handler:
                self.logging_handler.close_logging()

    def _initialize_processors(self):
        """Initialize all processors"""
        self.schema_processor = SchemaProcessor()
        self.json_processor = JSONProcessor()
        self.logger.debug("🔧 Processors initialized")

    def _validate_inputs(self, directory: str, output_dir: str, file_types: List[str]):
        """Validate input parameters"""
        # Validate source directory
        if not self.file_handler.directory_exists(directory):
            raise FileNotFoundError(f"Source directory does not exist: {directory}")

        # Validate file types
        if file_types:
            valid_types = ['json', 'csv', 'parquet']
            invalid_types = [ft for ft in file_types if ft not in valid_types]
            if invalid_types:
                raise ValueError(f"Invalid file types: {invalid_types}. Valid types: {valid_types}")

        self.logger.debug("✅ Input validation completed")

    def _display_discovery_results(self, discovered_files: Dict[str, List[Path]]):
        """Display file discovery results with emojis for better UX"""
        total_files = sum(len(files) for files in discovered_files.values())
        self.logger.info(f"📋 Found {total_files} files to process:")

        for file_type, file_list in discovered_files.items():
            if file_list:
                emoji = "📄" if file_type == "json" else "📊" if file_type == "csv" else "🗂️"
                self.logger.info(f"  {emoji} {file_type.upper()}: {len(file_list)} files")

                # Show first few files
                for file_path in file_list[:3]:
                    file_size = self.file_handler.get_file_size(file_path)
                    size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
                    self.logger.info(f"    • {file_path.name} ({size_mb:.2f}MB)")

                if len(file_list) > 3:
                    self.logger.info(f"    • ... and {len(file_list) - 3} more files")

    def _process_files(self, discovered_files: Dict[str, List[Path]],
                       output_path: Path, validate_schemas: bool = True,
                       generate_flat_versions: bool = True) -> Dict[str, Any]:
        """Process all discovered files"""
        results = {}

        # Process JSON files
        if 'json' in discovered_files and discovered_files['json']:
            self.logger.info("🔄 Processing JSON files...")
            results['json'] = self.json_processor.process_files(
                discovered_files['json'],
                output_path,
                validate_schemas=validate_schemas,
                generate_flat_versions=generate_flat_versions
            )

        # Future: Add CSV and Parquet processors
        # if 'csv' in discovered_files and discovered_files['csv']:
        #     self.logger.info("🔄 Processing CSV files...")
        #     results['csv'] = self.csv_processor.process_files(...)
        #
        # if 'parquet' in discovered_files and discovered_files['parquet']:
        #     self.logger.info("🔄 Processing Parquet files...")
        #     results['parquet'] = self.parquet_processor.process_files(...)

        return results

    def _generate_reports(self, processing_results: Dict[str, Any], output_path: Path):
        """Generate comprehensive reports"""
        self.logger.info("📊 Generating reports...")

        try:
            # Generate processing summary
            self.report_generator.generate_processing_summary(processing_results, output_path)

            # Generate schema analysis report
            self.report_generator.generate_schema_analysis_report(processing_results, output_path)

            # Generate error report if there were errors
            error_summary = self.error_handler.get_error_summary()
            if error_summary['total_errors'] > 0:
                error_log_file = output_path / "error_report.json"
                self.error_handler.export_error_log(error_log_file)

            self.logger.info("✅ Reports generated successfully")

        except Exception as e:
            self.logger.error(f"❌ Error generating reports: {e}")

    def _log_final_statistics(self, processing_results: Dict[str, Any], total_time: float):
        """Log final processing statistics"""
        total_files = 0
        successful_files = 0
        total_records = 0

        for file_type, results in processing_results.items():
            if isinstance(results, list):
                total_files += len(results)
                successful_files += sum(1 for r in results if r.get('success', False))
                total_records += sum(r.get('record_count', 0) for r in results if r.get('success', False))

        # Log performance summary
        self.logging_handler.log_processing_summary({
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': total_files - successful_files,
            'file_types': {ft: len(results) if isinstance(results, list) else 0
                           for ft, results in processing_results.items()},
            'total_duration': total_time,
            'total_records': total_records
        })

        # Log performance metrics
        if total_files > 0:
            avg_time_per_file = total_time / total_files
            self.logging_handler.log_performance_metric(
                "complete_processing",
                total_time,
                record_count=total_records,
                additional_metrics={'files_processed': total_files}
            )