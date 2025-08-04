from pathlib import Path
from typing import List, Dict, Any, Optional
import time
import json

from handlers.logging_handler import LoggingHandler
from handlers.error_handler import ErrorHandler
from handlers.file_handler import FileHandler
from scanners.file_scanner import FileScanner
from processors.schema_processor import SchemaProcessor
from processors.json_processor import JSONProcessor
from utils.report_generator import ReportGenerator
from connectors.connector_factory import get_connector_factory, DatabaseConnectorFactory
from connectors.connection_pool_manager import get_pool_manager, shutdown_pool_manager



class DataIngestionApplication:

    def __init__(self):
        self.logging_handler = LoggingHandler()
        self.error_handler = ErrorHandler()
        self.file_handler = FileHandler()
        self.report_generator = ReportGenerator()
        self.connector_factory = get_connector_factory()

        self.logger = None
        self.scanner = None
        self.schema_processor = None
        self.json_processor = None

    def run(self, directory: str, output_dir: str, log_level: str = 'INFO',
            dry_run: bool = False, file_types: List[str] = None,
            recursive: bool = True, validate_schemas: bool = True,
            generate_flat_versions: bool = True) -> int:

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
            
            # Cleanup database connections
            try:
                shutdown_pool_manager()
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error during database cleanup: {str(e)}")

    def _initialize_processors(self):
        self.schema_processor = SchemaProcessor()
        self.json_processor = JSONProcessor()
        self.logger.debug("🔧 Processors initialized")

    def _validate_inputs(self, directory: str, output_dir: str, file_types: List[str]):
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
    
    def load_to_database(self, data: List[Dict[str, Any]], table_name: str,
                        db_config: Optional[Dict[str, Any]] = None,
                        db_config_file: Optional[str] = None,
                        database_url: Optional[str] = None,
                        strategy: str = 'append',
                        create_table: bool = True,
                        batch_size: int = 1000,
                        pool_name: str = None) -> Dict[str, Any]:
        """
        Load processed data to a database using the specified strategy.
        
        Args:
            data: List of dictionaries containing the data to load
            table_name: Name of the target table
            db_config: Database configuration dictionary
            db_config_file: Path to database configuration file
            database_url: Database connection URL
            strategy: Data loading strategy ('append', 'replace', 'upsert')
            create_table: Whether to create table if it doesn't exist
            batch_size: Number of rows to process per batch
            pool_name: Name for connection pool (auto-generated if None)
            
        Returns:
            Dict containing load operation results and statistics
            
        Raises:
            ValueError: If invalid parameters or configuration
            Exception: If database operation fails
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be empty")
        
        if strategy not in ['append', 'replace', 'upsert']:
            raise ValueError(f"Invalid strategy: {strategy}. Must be 'append', 'replace', or 'upsert'")
        
        # Validate configuration sources
        config_sources = [db_config, db_config_file, database_url]
        provided_sources = [src for src in config_sources if src is not None]
        
        if len(provided_sources) != 1:
            raise ValueError("Exactly one of db_config, db_config_file, or database_url must be provided")
        
        start_time = time.time()
        
        try:
            self.logger.info(f"🗄️  Starting database load operation")
            self.logger.info(f"📊 Loading {len(data)} records to table '{table_name}' using '{strategy}' strategy")
            
            # Create database connector
            connector = self._create_database_connector(
                db_config, db_config_file, database_url, pool_name
            )
            
            # Get connection info for logging
            conn_info = connector.get_connection_info()
            self.logger.info(f"🔗 Connected to {conn_info.get('db_type', 'database')}: {conn_info.get('database', 'N/A')}")
            
            # Create table if requested and doesn't exist
            if create_table:
                self._ensure_table_exists(connector, table_name, data)
            
            # Execute loading strategy
            load_result = self._execute_load_strategy(
                connector, table_name, data, strategy, batch_size
            )
            
            execution_time = time.time() - start_time
            
            # Prepare result summary
            result = {
                'success': True,
                'table_name': table_name,
                'strategy': strategy,
                'records_processed': len(data),
                'records_loaded': load_result.get('rows_processed', load_result.get('rows_inserted', 0)),
                'execution_time_seconds': round(execution_time, 2),
                'database_type': conn_info.get('db_type'),
                'database_name': conn_info.get('database'),
                'batch_size': batch_size,
                'operation_details': load_result
            }
            
            self.logger.info(f"✅ Database load completed successfully in {execution_time:.2f}s")
            self.logger.info(f"📈 Loaded {result['records_loaded']} records to {result['database_type']} table '{table_name}'")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Database load operation failed: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            
            result = {
                'success': False,
                'table_name': table_name,
                'strategy': strategy,
                'records_processed': len(data),
                'records_loaded': 0,
                'execution_time_seconds': round(execution_time, 2),
                'error': error_msg,
                'operation_details': {}
            }
            
            # Let the application decide whether to raise or return error result
            raise Exception(error_msg)
    
    def _create_database_connector(self, db_config: Optional[Dict[str, Any]],
                                  db_config_file: Optional[str],
                                  database_url: Optional[str],
                                  pool_name: Optional[str]):
        """Create database connector based on provided configuration."""
        try:
            if db_config:
                # Extract database type and connection parameters
                if 'db_type' not in db_config:
                    raise ValueError("Database configuration must include 'db_type'")
                
                db_type = db_config.pop('db_type')
                return self.connector_factory.create_connector(
                    db_type=db_type,
                    connection_params=db_config,
                    pool_name=pool_name,
                    use_pooling=True
                )
            
            elif db_config_file:
                return self.connector_factory.create_from_config_file(
                    config_file_path=db_config_file,
                    pool_name=pool_name,
                    use_pooling=True
                )
            
            elif database_url:
                return self.connector_factory.create_from_url(
                    database_url=database_url,
                    pool_name=pool_name,
                    use_pooling=True
                )
            
        except Exception as e:
            raise Exception(f"Failed to create database connector: {str(e)}")
    
    def _ensure_table_exists(self, connector, table_name: str, data: List[Dict[str, Any]]):
        """Ensure the target table exists, creating it if necessary."""
        try:
            # Check if table exists
            if connector.table_exists(table_name):
                self.logger.debug(f"Table '{table_name}' already exists")
                return
            
            # Generate schema from data
            schema = self._infer_schema_from_data(data)
            
            # Create table
            self.logger.info(f"📋 Creating table '{table_name}' with {len(schema)} columns")
            success = connector.create_table(table_name, schema)
            
            if success:
                self.logger.info(f"✅ Table '{table_name}' created successfully")
            else:
                raise Exception(f"Failed to create table '{table_name}'")
                
        except Exception as e:
            raise Exception(f"Failed to ensure table exists: {str(e)}")
    
    def _infer_schema_from_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Infer database schema from data sample."""
        if not data:
            raise ValueError("Cannot infer schema from empty data")
        
        # Analyze first few records to infer types
        sample_size = min(100, len(data))
        sample_data = data[:sample_size]
        
        # Collect all unique column names
        all_columns = set()
        for record in sample_data:
            if isinstance(record, dict):
                all_columns.update(record.keys())
        
        schema = []
        
        for column_name in sorted(all_columns):
            # Analyze column values to infer type
            values = []
            for record in sample_data:
                if isinstance(record, dict) and column_name in record:
                    values.append(record[column_name])
            
            # Infer data type
            inferred_type = self._infer_column_type(values)
            
            column_def = {
                'name': column_name,
                'type': inferred_type,
                'nullable': True  # Default to nullable for flexibility
            }
            
            schema.append(column_def)
        
        return schema
    
    def _infer_column_type(self, values: List[Any]) -> str:
        """Infer data type from column values."""
        non_null_values = [v for v in values if v is not None]
        
        if not non_null_values:
            return 'text'  # Default for all-null columns
        
        # Check for integer
        if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null_values):
            return 'integer'
        
        # Check for float
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null_values):
            return 'float'
        
        # Check for boolean
        if all(isinstance(v, bool) for v in non_null_values):
            return 'boolean'
        
        # Check for JSON objects/arrays
        if any(isinstance(v, (dict, list)) for v in non_null_values):
            return 'json'
        
        # Default to text
        return 'text'
    
    def _execute_load_strategy(self, connector, table_name: str, data: List[Dict[str, Any]],
                              strategy: str, batch_size: int) -> Dict[str, Any]:
        """Execute the specified data loading strategy."""
        try:
            if strategy == 'append':
                # Use insert_data method if available, otherwise fall back to basic insert
                if hasattr(connector, 'insert_data'):
                    return {'rows_inserted': connector.insert_data(table_name, data, batch_size=batch_size)}
                else:
                    # Basic batch insert using execute_batch
                    return self._basic_batch_insert(connector, table_name, data, batch_size)
            
            elif strategy == 'replace':
                # Use replace_data method if available
                if hasattr(connector, 'replace_data'):
                    return connector.replace_data(table_name, data, batch_size=batch_size)
                else:
                    # Fallback: truncate and insert
                    return self._truncate_and_insert(connector, table_name, data, batch_size)
            
            elif strategy == 'upsert':
                # Use upsert_data_optimized method if available
                if hasattr(connector, 'upsert_data_optimized'):
                    return connector.upsert_data_optimized(table_name, data, batch_size=batch_size)
                elif hasattr(connector, 'upsert_data'):
                    # Try to detect conflict columns automatically
                    conflict_columns = self._detect_primary_key_columns(connector, table_name)
                    return {'rows_processed': connector.upsert_data(table_name, data, conflict_columns, batch_size=batch_size)}
                else:
                    # Fallback to append strategy
                    self.logger.warning(f"Upsert not supported for this database type, falling back to append")
                    return self._basic_batch_insert(connector, table_name, data, batch_size)
            
        except Exception as e:
            raise Exception(f"Failed to execute {strategy} strategy: {str(e)}")
    
    def _basic_batch_insert(self, connector, table_name: str, data: List[Dict[str, Any]], 
                           batch_size: int) -> Dict[str, Any]:
        """Basic batch insert using execute_batch method."""
        if not data:
            return {'rows_inserted': 0}
        
        # Get column names from first record
        columns = list(data[0].keys())
        column_list = ', '.join(f'"{col}"' for col in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_query = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({placeholders})'
        
        # Prepare batch parameters
        batch_params = []
        for record in data:
            row_params = [record.get(col) for col in columns]
            batch_params.append(row_params)
        
        # Execute in batches
        total_inserted = 0
        for i in range(0, len(batch_params), batch_size):
            batch = batch_params[i:i + batch_size]
            rows_affected = connector.execute_batch(insert_query, batch)
            total_inserted += rows_affected
        
        return {'rows_inserted': total_inserted}
    
    def _truncate_and_insert(self, connector, table_name: str, data: List[Dict[str, Any]], 
                           batch_size: int) -> Dict[str, Any]:
        """Truncate table and insert new data."""
        # Truncate table if method exists
        if hasattr(connector, 'truncate_table'):
            connector.truncate_table(table_name)
        else:
            # Fallback: delete all records
            connector.execute_query(f'DELETE FROM "{table_name}"')
        
        # Insert new data
        result = self._basic_batch_insert(connector, table_name, data, batch_size)
        return {
            'operation': 'replace',
            'rows_inserted': result['rows_inserted'],
            'rows_processed': result['rows_inserted']
        }
    
    def _detect_primary_key_columns(self, connector, table_name: str) -> List[str]:
        """Detect primary key columns for upsert operations."""
        try:
            if hasattr(connector, 'get_primary_key_columns'):
                pk_columns = connector.get_primary_key_columns(table_name)
                if pk_columns:
                    return pk_columns
            
            # Fallback: look for common primary key column names
            schema = connector.get_table_schema(table_name)
            common_pk_names = ['id', 'pk', 'primary_key', 'key']
            
            for column in schema:
                col_name = column.get('column_name', '').lower()
                if col_name in common_pk_names:
                    return [column['column_name']]
            
            # Last resort: use first column
            if schema:
                return [schema[0]['column_name']]
            
            return []
            
        except Exception:
            return []
