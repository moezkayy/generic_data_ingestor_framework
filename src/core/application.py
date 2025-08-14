"""
Core Data Ingestion Application Logic.
Author: Moez Khan (SRN: 23097401)
FYP Project - University of Hertfordshire
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
import time
import json
import logging

from processors.json_processor import JSONProcessor
from scanners.file_scanner import FileScanner
from connectors.connector_factory import get_connector_factory


class DataIngestionApplication:
    """
This class implements the main application logic for the Generic Data Ingestion Framework.
    """

    def __init__(self):
        """Initialize the application with basic components."""
        self.connector_factory = get_connector_factory()
        self.logger = logging.getLogger('data_ingestion')
        
        # Simple console logging setup
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def process_directory(self, directory: str, output_db: str = "output.db", 
                         table_name: str = "processed_data") -> Dict[str, Any]:
        """
        Process all JSON files in a directory and save to SQLite.        
        Args:
            directory: Path to directory containing JSON files
            output_db: Path to SQLite database file
            table_name: Name of table to create/use
            
        Returns:
            Dict containing comprehensive processing results
        """
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting data ingestion from: {directory}")
            
            # Validate input directory
            if not Path(directory).exists():
                raise FileNotFoundError(f"Directory not found: {directory}")
            
            # File discovery using custom scanner
            # Referenced in: Implementation section (page 19)
            scanner = FileScanner(directory)
            discovered_files = scanner.discover_files(file_types=['json'], recursive=True)
            json_files = discovered_files.get('json', [])
            
            if not json_files:
                self.logger.warning("No JSON files found in directory")
                return {'success': False, 'message': 'No JSON files found'}
            
            self.logger.info(f"Found {len(json_files)} JSON files to process")
            
            # Process files with graceful error handling
            # Innovation: Continue-on-error approach vs fail-fast enterprise systems
            processor = JSONProcessor()
            all_data = []
            processed_files = 0
            errors = []
            
            for file_path in json_files:
                try:
                    self.logger.info(f"Processing: {file_path.name}")
                    
                    # Read JSON file with encoding handling
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Ensure data is a list for consistent processing
                    if isinstance(data, dict):
                        data = [data]
                    
                    # Process the data using simplified JSON processor
                    # Referenced in: Implementation section (page 21)
                    processed_data = processor.process_data(data)
                    if processed_data:
                        # Add source file metadata for data lineage
                        for record in processed_data:
                            record['_source_file'] = file_path.name
                        
                        all_data.extend(processed_data)
                        processed_files += 1
                        self.logger.info(f"  ✓ Processed {len(processed_data)} records")
                    else:
                        self.logger.warning(f"  ⚠ No valid data in {file_path.name}")
                        
                except Exception as e:
                    # Graceful error handling - key innovation
                    # Referenced in: Discussion section (page 31)
                    error_msg = f"Error processing {file_path.name}: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(f"  ✗ {error_msg}")
                    # Continue processing other files (graceful degradation)
            
            if not all_data:
                return {
                    'success': False, 
                    'message': 'No data was processed successfully',
                    'errors': errors
                }
            
            # Save to SQLite database with batch optimization
            # Referenced in: Results section (page 48)
            self.logger.info(f"Saving {len(all_data)} records to database: {output_db}")
            db_result = self._save_to_database(all_data, output_db, table_name)
            
            # Calculate comprehensive performance metrics
            processing_time = time.time() - start_time
            
            result = {
                'success': True,
                'total_files': len(json_files),
                'processed_files': processed_files,
                'failed_files': len(json_files) - processed_files,
                'total_records': len(all_data),
                'processing_time_seconds': round(processing_time, 2),
                'database_path': output_db,
                'table_name': table_name,
                'database_records': db_result.get('records_saved', 0),
                'errors': errors,
                'throughput_rps': round(len(all_data) / processing_time, 2) if processing_time > 0 else 0
            }
            
            self.logger.info(f"Processing completed in {processing_time:.2f}s")
            self.logger.info(f"Successfully processed {processed_files}/{len(json_files)} files")
            self.logger.info(f"Saved {result['database_records']} records to {output_db}")
            self.logger.info(f"Throughput: {result['throughput_rps']} records/sec")
            
            return result
            
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'processing_time_seconds': round(time.time() - start_time, 2)
            }

    def _save_to_database(self, data: List[Dict[str, Any]], 
                         db_path: str, table_name: str) -> Dict[str, Any]:
        """
        Save processed data to SQLite database with automatic schema inference.
                Referenced in: Implementation section (page 21) - Schema inference
        """
        connector = None
        try:
            # Create SQLite connector using factory pattern
            connector = self.connector_factory.create_sqlite_connector(db_path)
            
            # Automatic schema inference from data
            # Innovation: Post-aggregation schema unification
            schema = self._infer_simple_schema(data)
            
            # Create table if it doesn't exist
            if not connector.table_exists(table_name):
                self.logger.info(f"Creating table: {table_name}")
                connector.create_table(table_name, schema)
            
            # Insert data with batch optimization
            records_saved = connector.insert_data(table_name, data)
            
            return {
                'success': True,
                'records_saved': records_saved,
                'table_name': table_name
            }
            
        except Exception as e:
            error_msg = f"Database save failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'records_saved': 0
            }
        finally:
            # Ensure database connection is properly closed
            if connector:
                connector.disconnect()

    def _infer_simple_schema(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Infer a simple unified schema from heterogeneous data.
        
        Design Decision: TEXT-based storage for complete data preservation
        Referenced in: Implementation section (page 21) - "All fields as TEXT"
        Innovation: Balances flexibility with structural integrity
        """
        if not data:
            return []
        
        # Sample-based schema inference for performance
        sample_size = min(10, len(data))
        all_columns = set()
        
        for record in data[:sample_size]:
            if isinstance(record, dict):
                all_columns.update(record.keys())
        
        # Create unified schema - everything as TEXT for data preservation
        # This approach ensures zero data loss while maintaining simplicity
        schema = []
        for column_name in sorted(all_columns):
            schema.append({
                'name': column_name,
                'type': 'TEXT',
                'nullable': True
            })
        
        self.logger.debug(f"Inferred schema with {len(schema)} columns: {[col['name'] for col in schema]}")
        return schema

    def get_database_preview(self, db_path: str, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get a preview of data from the database for validation.
        
        Args:
            db_path: Path to SQLite database
            table_name: Name of table to query
            limit: Maximum number of records to return
            
        Returns:
            List of records from the database
        """
        connector = None
        try:
            connector = self.connector_factory.create_sqlite_connector(db_path)
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            return connector.execute_query(query)
        
        except Exception as e:
            self.logger.error(f"Error getting database preview: {str(e)}")
            return []
        finally:
            if connector:
                connector.disconnect()
