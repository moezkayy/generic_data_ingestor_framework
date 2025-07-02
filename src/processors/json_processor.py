import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Union, Optional, Tuple
from datetime import datetime
import time

# Change these imports (remove the .. dots)
from processors.schema_processor import SchemaProcessor
from handlers.file_handler import FileHandler
from handlers.error_handler import ErrorHandler

class JSONProcessor:
    """
    Comprehensive JSON processor with advanced processing capabilities
    """

    def __init__(self):
        """Initialize the JSON processor"""
        self.logger = logging.getLogger('data_ingestion.json_processor')
        self.schema_processor = SchemaProcessor()
        self.file_handler = FileHandler()
        self.error_handler = ErrorHandler()

        # Processing statistics
        self.processing_stats = {
            'files_processed': 0,
            'files_successful': 0,
            'files_failed': 0,
            'total_records': 0,
            'total_processing_time': 0,
            'schemas_inferred': 0,
            'validation_performed': 0
        }

    def process_files(self, file_paths: List[Path], output_dir: Path,
                      validate_schemas: bool = True,
                      generate_flat_versions: bool = True,
                      create_consolidated_schema: bool = True) -> List[Dict[str, Any]]:
        """
        Process multiple JSON files with comprehensive analysis

        Args:
            file_paths (List[Path]): List of JSON file paths to process
            output_dir (Path): Directory to save processed results
            validate_schemas (bool): Whether to perform schema validation
            generate_flat_versions (bool): Whether to generate flattened data
            create_consolidated_schema (bool): Whether to create consolidated schema

        Returns:
            List[Dict[str, Any]]: Processing results for each file
        """
        start_time = time.time()
        self.logger.info(f"Starting JSON processing for {len(file_paths)} files")

        results = []
        all_schemas = []

        # Process each file
        for i, file_path in enumerate(file_paths, 1):
            self.logger.info(f"Processing file {i}/{len(file_paths)}: {file_path.name}")

            try:
                result = self.process_single_file(
                    file_path,
                    output_dir,
                    validate_schemas=validate_schemas,
                    generate_flat_versions=generate_flat_versions
                )
                results.append(result)

                # Collect schema for consolidation
                if result['success'] and 'schema' in result:
                    all_schemas.append(result['schema'])

            except Exception as e:
                error_result = self.error_handler.handle_file_error(e, file_path, "processing")
                results.append(error_result)
                self.processing_stats['files_failed'] += 1

        # Generate consolidated schema and reports
        if create_consolidated_schema and all_schemas:
            self._generate_consolidated_schema(all_schemas, output_dir)

        # Generate processing summary
        self._generate_processing_summary(results, output_dir)

        # Update statistics
        total_time = time.time() - start_time
        self.processing_stats['total_processing_time'] += total_time

        self.logger.info(f"JSON processing completed in {total_time:.2f}s")
        return results

    def process_single_file(self, file_path: Path, output_dir: Path,
                            validate_schemas: bool = True,
                            generate_flat_versions: bool = True) -> Dict[str, Any]:
        """
        Process a single JSON file with comprehensive analysis

        Args:
            file_path (Path): Path to the JSON file
            output_dir (Path): Directory to save processed results
            validate_schemas (bool): Whether to perform schema validation
            generate_flat_versions (bool): Whether to generate flattened data

        Returns:
            Dict[str, Any]: Comprehensive processing result
        """
        start_time = time.time()
        file_stem = file_path.stem

        try:
            # Load JSON data
            data = self.file_handler.read_json_file(file_path)

            # Get file information
            file_info = self.file_handler.get_comprehensive_file_info(file_path)

            # Infer schema
            schema = self.schema_processor.infer_json_schema(data, file_stem)

            # Generate flattened data if requested
            flattened_data = None
            if generate_flat_versions:
                flattened_data = self.flatten_json_data(data)

            # Validate data against schema if requested
            validation_result = None
            if validate_schemas:
                validation_result = self.validate_data_against_schema(data, schema)

            # Generate output files
            output_files = self._save_processing_results(
                file_stem, data, schema, flattened_data, output_dir
            )

            # Calculate processing metrics
            processing_time = time.time() - start_time
            record_count = len(data) if isinstance(data, list) else 1

            # Update statistics
            self.processing_stats['files_processed'] += 1
            self.processing_stats['files_successful'] += 1
            self.processing_stats['total_records'] += record_count
            self.processing_stats['schemas_inferred'] += 1
            if validation_result:
                self.processing_stats['validation_performed'] += 1

            # Log performance
            self.logger.info(f"Successfully processed {file_path.name} in {processing_time:.3f}s")

            return {
                'file': str(file_path),
                'file_name': file_path.name,
                'success': True,
                'processing_time': round(processing_time, 3),
                'file_info': file_info,
                'schema': schema,
                'flat_schema': self.schema_processor.flatten_schema(schema),
                'record_count': record_count,
                'validation_result': validation_result,
                'flattened_data_generated': flattened_data is not None,
                'output_files': output_files,
                'metrics': {
                    'records_per_second': round(record_count / processing_time, 1),
                    'mb_per_second': round(file_info.get('size_mb', 0) / processing_time, 2)
                }
            }

        except Exception as e:
            processing_time = time.time() - start_time
            self.processing_stats['files_processed'] += 1
            self.processing_stats['files_failed'] += 1

            error_details = self.error_handler.handle_file_error(e, file_path, "JSON processing")
            self.logger.error(f"Failed to process {file_path.name}: {str(e)}")

            return {
                'file': str(file_path),
                'file_name': file_path.name,
                'success': False,
                'processing_time': round(processing_time, 3),
                'error': str(e),
                'error_details': error_details
            }

    def flatten_json_data(self, data: Union[Dict, List[Dict]],
                          max_depth: int = 10) -> Union[Dict, List[Dict]]:
        """
        Flatten nested JSON data with depth control

        Args:
            data: JSON data to flatten
            max_depth (int): Maximum flattening depth

        Returns:
            Flattened data structure
        """
        if isinstance(data, list):
            return [self._flatten_object(item, max_depth=max_depth)
                    for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            return self._flatten_object(data, max_depth=max_depth)
        else:
            return data

    def _flatten_object(self, obj: Dict, prefix: str = "",
                        max_depth: int = 10, current_depth: int = 0) -> Dict:
        """
        Flatten a single object with comprehensive handling

        Args:
            obj (Dict): Object to flatten
            prefix (str): Prefix for field names
            max_depth (int): Maximum depth to flatten
            current_depth (int): Current depth level

        Returns:
            Dict: Flattened object
        """
        if current_depth >= max_depth:
            # Stop flattening at max depth
            return {prefix or "max_depth_reached": str(obj)[:100]}

        flattened = {}

        for key, value in obj.items():
            new_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict) and value:  # Non-empty dict
                # Recursively flatten nested objects
                nested_flattened = self._flatten_object(
                    value, new_key, max_depth, current_depth + 1
                )
                flattened.update(nested_flattened)

            elif isinstance(value, list):
                # Handle arrays with different strategies
                flattened.update(self._flatten_array_field(new_key, value, max_depth, current_depth))

            else:
                # Primitive value
                flattened[new_key] = value

        return flattened

    def _flatten_array_field(self, field_name: str, arr: List,
                             max_depth: int, current_depth: int) -> Dict:
        """
        Flatten array fields with intelligent handling

        Args:
            field_name (str): Field name
            arr (List): Array to flatten
            max_depth (int): Maximum depth
            current_depth (int): Current depth

        Returns:
            Dict: Flattened array representation
        """
        if not arr:
            return {field_name: None}

        flattened = {}

        # Check if array contains objects
        if all(isinstance(item, dict) for item in arr):
            # Array of objects - flatten each with index
            for i, item in enumerate(arr[:10]):  # Limit to first 10 items
                if current_depth < max_depth:
                    item_flattened = self._flatten_object(
                        item, f"{field_name}[{i}]", max_depth, current_depth + 1
                    )
                    flattened.update(item_flattened)
                else:
                    flattened[f"{field_name}[{i}]"] = str(item)[:50]

            # Add count if more than 10 items
            if len(arr) > 10:
                flattened[f"{field_name}_count"] = len(arr)
                flattened[f"{field_name}_truncated"] = True

        elif all(isinstance(item, (str, int, float, bool)) for item in arr):
            # Array of primitives - create summary
            if len(arr) <= 5:
                # Small array - list all values
                for i, item in enumerate(arr):
                    flattened[f"{field_name}[{i}]"] = item
            else:
                # Large array - create summary
                flattened[f"{field_name}_count"] = len(arr)
                flattened[f"{field_name}_first"] = arr[0]
                flattened[f"{field_name}_last"] = arr[-1]
                if all(isinstance(item, (int, float)) for item in arr):
                    flattened[f"{field_name}_sum"] = sum(arr)
                    flattened[f"{field_name}_avg"] = sum(arr) / len(arr)
        else:
            # Mixed array - convert to string representation
            flattened[field_name] = str(arr)[:200] if len(str(arr)) > 200 else str(arr)

        return flattened

    def validate_data_against_schema(self, data: Any, schema: Dict) -> Dict[str, Any]:
        """
        Validate data against inferred schema with detailed reporting

        Args:
            data: Data to validate
            schema (Dict): Schema to validate against

        Returns:
            Dict[str, Any]: Detailed validation results
        """
        validation_start = time.time()

        try:
            errors = []
            warnings = []

            # Perform validation based on schema type
            if isinstance(data, list) and schema.get('type') == 'array':
                errors.extend(self._validate_array_data(data, schema))
            elif isinstance(data, dict) and schema.get('type') == 'object':
                errors.extend(self._validate_object_data(data, schema))
            else:
                errors.append(f"Data type mismatch: expected {schema.get('type')}, got {type(data).__name__}")

            validation_time = time.time() - validation_start

            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'validation_time': round(validation_time, 3),
                'error_count': len(errors),
                'warning_count': len(warnings)
            }

        except Exception as e:
            self.logger.error(f"Schema validation error: {e}")
            return {
                'valid': False,
                'errors': [f"Validation failed: {str(e)}"],
                'warnings': [],
                'validation_time': time.time() - validation_start,
                'error_count': 1,
                'warning_count': 0
            }

    def _validate_array_data(self, data: List, schema: Dict) -> List[str]:
        """Validate array data against schema"""
        errors = []

        # Check array constraints
        min_items = schema.get('minItems', 0)
        max_items = schema.get('maxItems', float('inf'))

        if len(data) < min_items:
            errors.append(f"Array too short: {len(data)} < {min_items}")
        if len(data) > max_items:
            errors.append(f"Array too long: {len(data)} > {max_items}")

        # Validate items
        items_schema = schema.get('items', {})
        if items_schema:
            for i, item in enumerate(data[:100]):  # Validate first 100 items
                item_errors = self._validate_single_item(item, items_schema, f"[{i}]")
                errors.extend(item_errors)

        return errors

    def _validate_object_data(self, data: Dict, schema: Dict) -> List[str]:
        """Validate object data against schema"""
        errors = []

        properties = schema.get('properties', {})
        required = schema.get('required', [])

        # Check required fields
        for req_field in required:
            if req_field not in data:
                errors.append(f"Missing required field: {req_field}")

        # Validate properties
        for field, value in data.items():
            if field in properties:
                field_errors = self._validate_single_item(value, properties[field], field)
                errors.extend(field_errors)
            elif not schema.get('additionalProperties', True):
                errors.append(f"Additional property not allowed: {field}")

        return errors

    def _validate_single_item(self, value: Any, schema: Dict, path: str) -> List[str]:
        """Validate a single item against its schema"""
        errors = []
        expected_type = schema.get('type')

        if expected_type and expected_type != 'mixed':
            actual_type = self.schema_processor._get_json_type(value)

            if actual_type != expected_type and not (value is None and schema.get('nullable')):
                errors.append(f"Type mismatch at {path}: expected {expected_type}, got {actual_type}")

        return errors

    def _save_processing_results(self, file_stem: str, data: Any, schema: Dict,
                                 flattened_data: Any, output_dir: Path) -> Dict[str, str]:
        """Save all processing results to files"""
        output_files = {}

        try:
            # Save original schema
            schema_file = output_dir / f"{file_stem}_schema.json"
            if self.file_handler.write_json_file(schema, schema_file):
                output_files['schema'] = str(schema_file)

            # Save flattened schema
            flat_schema = self.schema_processor.flatten_schema(schema)
            flat_schema_file = output_dir / f"{file_stem}_flat_schema.json"
            if self.file_handler.write_json_file(flat_schema, flat_schema_file):
                output_files['flat_schema'] = str(flat_schema_file)

            # Save flattened data if generated
            if flattened_data is not None:
                flattened_file = output_dir / f"{file_stem}_flattened.json"
                if self.file_handler.write_json_file(flattened_data, flattened_file):
                    output_files['flattened_data'] = str(flattened_file)

            # Save schema report
            schema_report = self.schema_processor.generate_schema_report(schema, file_stem)
            report_file = output_dir / f"{file_stem}_schema_report.json"
            if self.file_handler.write_json_file(schema_report, report_file):
                output_files['schema_report'] = str(report_file)

        except Exception as e:
            self.logger.error(f"Error saving processing results for {file_stem}: {e}")

        return output_files

    def _generate_consolidated_schema(self, schemas: List[Dict], output_dir: Path):
        """Generate consolidated schema from all processed schemas"""
        try:
            self.logger.info("Generating consolidated schema...")

            # Merge all schemas
            consolidated_schema = self.schema_processor._merge_multiple_schemas(schemas)

            # Add metadata
            consolidated_schema['$metadata'] = {
                'type': 'consolidated',
                'source_count': len(schemas),
                'generated_at': datetime.now().isoformat(),
                'consolidation_version': '1.0'
            }

            # Save consolidated schema
            consolidated_file = output_dir / "consolidated_schema.json"
            self.file_handler.write_json_file(consolidated_schema, consolidated_file)

            # Generate consistency report
            consistency_result = self.schema_processor.validate_schema_consistency(schemas)
            consistency_file = output_dir / "schema_consistency_report.json"
            self.file_handler.write_json_file(consistency_result, consistency_file)

            self.logger.info(f"Consolidated schema saved to: {consolidated_file}")

        except Exception as e:
            self.logger.error(f"Error generating consolidated schema: {e}")

    def _generate_processing_summary(self, results: List[Dict], output_dir: Path):
        """Generate comprehensive processing summary"""
        try:
            successful_results = [r for r in results if r.get('success')]
            failed_results = [r for r in results if not r.get('success')]

            summary = {
                'processing_summary': {
                    'total_files': len(results),
                    'successful_files': len(successful_results),
                    'failed_files': len(failed_results),
                    'success_rate': round(len(successful_results) / len(results) * 100, 1) if results else 0,
                    'total_processing_time': sum(r.get('processing_time', 0) for r in results),
                    'total_records': sum(r.get('record_count', 0) for r in successful_results),
                    'average_processing_time': round(
                        sum(r.get('processing_time', 0) for r in results) / len(results), 3
                    ) if results else 0
                },
                'file_details': results,
                'statistics': self.processing_stats,
                'generated_at': datetime.now().isoformat()
            }

            # Save summary
            summary_file = output_dir / "json_processing_summary.json"
            self.file_handler.write_json_file(summary, summary_file)

            self.logger.info(f"Processing summary saved to: {summary_file}")

        except Exception as e:
            self.logger.error(f"Error generating processing summary: {e}")

    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        return self.processing_stats.copy()

    def reset_statistics(self):
        """Reset processing statistics"""
        self.processing_stats = {key: 0 for key in self.processing_stats.keys()}