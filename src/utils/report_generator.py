# src/utils/report_generator.py
"""
Report Generator Utility
Generates comprehensive reports and summaries for data ingestion results
Author: [Your Name]
Date: July 2025
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import Counter


class ReportGenerator:
    """
    Utility class for generating comprehensive reports and analysis summaries
    """

    def __init__(self):
        """Initialize the report generator"""
        self.logger = logging.getLogger('data_ingestion.report_generator')

    def generate_processing_summary(self, processing_results: Dict[str, Any],
                                    output_dir: Path) -> Path:
        """
        Generate comprehensive processing summary report

        Args:
            processing_results (Dict[str, Any]): Results from file processing
            output_dir (Path): Output directory for reports

        Returns:
            Path: Path to generated summary report
        """
        self.logger.info("Generating processing summary report...")

        try:
            # Calculate overall statistics
            overall_stats = self._calculate_overall_statistics(processing_results)

            # Generate file type breakdown
            file_type_breakdown = self._generate_file_type_breakdown(processing_results)

            # Generate performance metrics
            performance_metrics = self._calculate_performance_metrics(processing_results)

            # Generate error analysis
            error_analysis = self._analyze_errors(processing_results)

            # Create comprehensive summary
            summary_report = {
                'report_metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'report_type': 'processing_summary',
                    'framework_version': '1.0.0'
                },
                'overall_statistics': overall_stats,
                'file_type_breakdown': file_type_breakdown,
                'performance_metrics': performance_metrics,
                'error_analysis': error_analysis,
                'detailed_results': processing_results
            }

            # Save summary report
            summary_file = output_dir / "processing_summary_report.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary_report, f, indent=2, ensure_ascii=False)

            # Generate human-readable summary
            self._generate_readable_summary(summary_report, output_dir)

            self.logger.info(f"Processing summary saved to: {summary_file}")
            return summary_file

        except Exception as e:
            self.logger.error(f"Error generating processing summary: {e}")
            raise

    def generate_schema_analysis_report(self, processing_results: Dict[str, Any],
                                        output_dir: Path) -> Path:
        """
        Generate detailed schema analysis report

        Args:
            processing_results (Dict[str, Any]): Processing results
            output_dir (Path): Output directory

        Returns:
            Path: Path to schema analysis report
        """
        self.logger.info("Generating schema analysis report...")

        try:
            # Extract schemas from results
            schemas = []
            schema_sources = {}

            for file_type, results in processing_results.items():
                if isinstance(results, list):
                    for result in results:
                        if result.get('success') and 'schema' in result:
                            schema = result['schema']
                            schemas.append(schema)
                            schema_sources[len(schemas) - 1] = {
                                'file_type': file_type,
                                'file_name': result.get('file_name', 'unknown'),
                                'record_count': result.get('record_count', 0)
                            }

            if not schemas:
                self.logger.warning("No schemas found for analysis")
                return None

            # Analyze schema complexity
            complexity_analysis = self._analyze_schema_complexity(schemas, schema_sources)

            # Analyze field distributions
            field_analysis = self._analyze_field_distributions(schemas)

            # Analyze data types
            type_analysis = self._analyze_data_types(schemas)

            # Generate schema consistency analysis
            consistency_analysis = self._analyze_schema_consistency(schemas)

            # Create schema analysis report
            schema_report = {
                'report_metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'report_type': 'schema_analysis',
                    'total_schemas_analyzed': len(schemas)
                },
                'complexity_analysis': complexity_analysis,
                'field_analysis': field_analysis,
                'type_analysis': type_analysis,
                'consistency_analysis': consistency_analysis,
                'schema_sources': schema_sources
            }

            # Save schema analysis report
            schema_file = output_dir / "schema_analysis_report.json"
            with open(schema_file, 'w', encoding='utf-8') as f:
                json.dump(schema_report, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Schema analysis saved to: {schema_file}")
            return schema_file

        except Exception as e:
            self.logger.error(f"Error generating schema analysis: {e}")
            raise

    def _calculate_overall_statistics(self, processing_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate overall processing statistics"""
        total_files = 0
        successful_files = 0
        failed_files = 0
        total_records = 0
        total_processing_time = 0

        for file_type, results in processing_results.items():
            if isinstance(results, list):
                total_files += len(results)

                for result in results:
                    if result.get('success'):
                        successful_files += 1
                        total_records += result.get('record_count', 0)
                    else:
                        failed_files += 1

                    total_processing_time += result.get('processing_time', 0)

        success_rate = (successful_files / total_files * 100) if total_files > 0 else 0
        avg_processing_time = total_processing_time / total_files if total_files > 0 else 0

        return {
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': failed_files,
            'success_rate_percent': round(success_rate, 2),
            'total_records_processed': total_records,
            'total_processing_time_seconds': round(total_processing_time, 3),
            'average_processing_time_per_file': round(avg_processing_time, 3),
            'records_per_second_overall': round(total_records / total_processing_time,
                                                1) if total_processing_time > 0 else 0
        }

    def _generate_file_type_breakdown(self, processing_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate breakdown by file type"""
        breakdown = {}

        for file_type, results in processing_results.items():
            if isinstance(results, list):
                successful = [r for r in results if r.get('success')]
                failed = [r for r in results if not r.get('success')]

                breakdown[file_type] = {
                    'total_files': len(results),
                    'successful_files': len(successful),
                    'failed_files': len(failed),
                    'success_rate_percent': round(len(successful) / len(results) * 100, 2) if results else 0,
                    'total_records': sum(r.get('record_count', 0) for r in successful),
                    'average_file_size_mb': round(
                        sum(r.get('file_info', {}).get('size_mb', 0) for r in successful) / len(successful), 2
                    ) if successful else 0,
                    'processing_time_seconds': sum(r.get('processing_time', 0) for r in results)
                }

        return breakdown

    def _calculate_performance_metrics(self, processing_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate detailed performance metrics"""
        all_processing_times = []
        all_throughput_rates = []
        file_size_performance = []

        for file_type, results in processing_results.items():
            if isinstance(results, list):
                for result in results:
                    if result.get('success'):
                        processing_time = result.get('processing_time', 0)
                        record_count = result.get('record_count', 0)
                        file_size_mb = result.get('file_info', {}).get('size_mb', 0)

                        all_processing_times.append(processing_time)

                        if processing_time > 0:
                            throughput = record_count / processing_time
                            all_throughput_rates.append(throughput)

                            if file_size_mb > 0:
                                mb_per_second = file_size_mb / processing_time
                                file_size_performance.append(mb_per_second)

        return {
            'processing_time_stats': {
                'min_seconds': min(all_processing_times) if all_processing_times else 0,
                'max_seconds': max(all_processing_times) if all_processing_times else 0,
                'average_seconds': sum(all_processing_times) / len(all_processing_times) if all_processing_times else 0,
                'median_seconds': sorted(all_processing_times)[
                    len(all_processing_times) // 2] if all_processing_times else 0
            },
            'throughput_stats': {
                'min_records_per_second': min(all_throughput_rates) if all_throughput_rates else 0,
                'max_records_per_second': max(all_throughput_rates) if all_throughput_rates else 0,
                'average_records_per_second': sum(all_throughput_rates) / len(
                    all_throughput_rates) if all_throughput_rates else 0
            },
            'file_size_performance': {
                'min_mb_per_second': min(file_size_performance) if file_size_performance else 0,
                'max_mb_per_second': max(file_size_performance) if file_size_performance else 0,
                'average_mb_per_second': sum(file_size_performance) / len(
                    file_size_performance) if file_size_performance else 0
            }
        }

    def _analyze_errors(self, processing_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze errors and failure patterns"""
        error_types = Counter()
        error_details = []

        for file_type, results in processing_results.items():
            if isinstance(results, list):
                for result in results:
                    if not result.get('success'):
                        error = result.get('error', 'Unknown error')
                        error_type = result.get('error_details', {}).get('error_type', 'UnknownError')

                        error_types[error_type] += 1
                        error_details.append({
                            'file_type': file_type,
                            'file_name': result.get('file_name', 'unknown'),
                            'error_type': error_type,
                            'error_message': error,
                            'processing_time': result.get('processing_time', 0)
                        })

        return {
            'total_errors': sum(error_types.values()),
            'error_type_distribution': dict(error_types),
            'most_common_errors': error_types.most_common(5),
            'error_details': error_details[:10]  # First 10 errors for detailed analysis
        }

    def _analyze_schema_complexity(self, schemas: List[Dict], schema_sources: Dict) -> Dict[str, Any]:
        """Analyze schema complexity metrics"""
        complexity_metrics = []

        for i, schema in enumerate(schemas):
            # Calculate complexity metrics for each schema
            flat_schema = self._flatten_schema_for_analysis(schema)

            metrics = {
                'source': schema_sources.get(i, {}),
                'total_fields': len(flat_schema),
                'nested_levels': max(field.count('.') for field in flat_schema.keys()) if flat_schema else 0,
                'array_fields': len([f for f in flat_schema.keys() if '[]' in f]),
                'object_fields': len([f for f, t in flat_schema.items() if t == 'object']),
                'primitive_fields': len(
                    [f for f, t in flat_schema.items() if t in ['string', 'integer', 'number', 'boolean']])
            }

            complexity_metrics.append(metrics)

        # Calculate aggregate statistics
        if complexity_metrics:
            avg_fields = sum(m['total_fields'] for m in complexity_metrics) / len(complexity_metrics)
            max_nested = max(m['nested_levels'] for m in complexity_metrics)
            total_array_fields = sum(m['array_fields'] for m in complexity_metrics)

            return {
                'schema_count': len(schemas),
                'average_fields_per_schema': round(avg_fields, 1),
                'maximum_nesting_level': max_nested,
                'total_array_fields_across_schemas': total_array_fields,
                'individual_schema_metrics': complexity_metrics
            }

        return {'schema_count': 0}

    def _analyze_field_distributions(self, schemas: List[Dict]) -> Dict[str, Any]:
        """Analyze field name distributions across schemas"""
        all_fields = []

        for schema in schemas:
            flat_schema = self._flatten_schema_for_analysis(schema)
            all_fields.extend(flat_schema.keys())

        field_frequency = Counter(all_fields)

        return {
            'total_unique_fields': len(field_frequency),
            'most_common_fields': field_frequency.most_common(20),
            'field_frequency_distribution': dict(field_frequency)
        }

    def _analyze_data_types(self, schemas: List[Dict]) -> Dict[str, Any]:
        """Analyze data type distributions"""
        all_types = []

        for schema in schemas:
            flat_schema = self._flatten_schema_for_analysis(schema)
            all_types.extend(flat_schema.values())

        type_frequency = Counter(all_types)

        return {
            'type_distribution': dict(type_frequency),
            'most_common_types': type_frequency.most_common(10),
            'total_type_instances': len(all_types)
        }

    def _analyze_schema_consistency(self, schemas: List[Dict]) -> Dict[str, Any]:
        """Analyze consistency across schemas"""
        if len(schemas) < 2:
            return {'analysis': 'Insufficient schemas for consistency analysis'}

        # This would integrate with SchemaProcessor's consistency validation
        # For now, return basic analysis
        return {
            'schemas_analyzed': len(schemas),
            'consistency_check': 'Would integrate with SchemaProcessor.validate_schema_consistency()'
        }

    def _flatten_schema_for_analysis(self, schema: Dict, prefix: str = "") -> Dict[str, str]:
        """Flatten schema for analysis purposes"""
        flattened = {}

        if schema.get("type") == "object":
            properties = schema.get("properties", {})
            for key, prop_schema in properties.items():
                field_name = f"{prefix}.{key}" if prefix else key
                if prop_schema.get("type") == "object":
                    nested = self._flatten_schema_for_analysis(prop_schema, field_name)
                    flattened.update(nested)
                else:
                    flattened[field_name] = prop_schema.get("type", "unknown")
        elif schema.get("type") == "array":
            items_schema = schema.get("items", {})
            if items_schema.get("type") == "object":
                nested = self._flatten_schema_for_analysis(items_schema, f"{prefix}[]" if prefix else "[]")
                flattened.update(nested)
            else:
                flattened[f"{prefix}[]" if prefix else "[]"] = items_schema.get("type", "unknown")
        else:
            flattened[prefix or "root"] = schema.get("type", "unknown")

        return flattened

    def _generate_readable_summary(self, summary_report: Dict[str, Any], output_dir: Path):
        """Generate human-readable summary in text format"""
        try:
            summary_text = self._format_readable_summary(summary_report)

            summary_text_file = output_dir / "processing_summary.txt"
            with open(summary_text_file, 'w', encoding='utf-8') as f:
                f.write(summary_text)

            self.logger.info(f"Readable summary saved to: {summary_text_file}")

        except Exception as e:
            self.logger.error(f"Error generating readable summary: {e}")

    def _format_readable_summary(self, summary_report: Dict[str, Any]) -> str:
        """Format summary report as readable text"""
        stats = summary_report.get('overall_statistics', {})
        breakdown = summary_report.get('file_type_breakdown', {})
        performance = summary_report.get('performance_metrics', {})
        errors = summary_report.get('error_analysis', {})

        text = f"""
DATA INGESTION FRAMEWORK - PROCESSING SUMMARY
{"=" * 60}

Generated: {summary_report.get('report_metadata', {}).get('generated_at', 'Unknown')}

OVERALL STATISTICS
{"-" * 30}
Total Files Processed: {stats.get('total_files', 0)}
Successful: {stats.get('successful_files', 0)}
Failed: {stats.get('failed_files', 0)}
Success Rate: {stats.get('success_rate_percent', 0)}%
Total Records: {stats.get('total_records_processed', 0):,}
Total Processing Time: {stats.get('total_processing_time_seconds', 0):.2f}s
Average Time per File: {stats.get('average_processing_time_per_file', 0):.3f}s
Overall Throughput: {stats.get('records_per_second_overall', 0):.1f} records/s

FILE TYPE BREAKDOWN
{"-" * 30}
"""

        for file_type, type_stats in breakdown.items():
            text += f"""
{file_type.upper()}:
  Files: {type_stats.get('total_files', 0)} (Success: {type_stats.get('successful_files', 0)}, Failed: {type_stats.get('failed_files', 0)})
  Success Rate: {type_stats.get('success_rate_percent', 0)}%
  Records: {type_stats.get('total_records', 0):,}
  Avg File Size: {type_stats.get('average_file_size_mb', 0):.2f}MB
  Processing Time: {type_stats.get('processing_time_seconds', 0):.2f}s
"""

        if errors.get('total_errors', 0) > 0:
            text += f"""
ERROR ANALYSIS
{"-" * 30}
Total Errors: {errors.get('total_errors', 0)}
Most Common Error Types:
"""
            for error_type, count in errors.get('most_common_errors', []):
                text += f"  • {error_type}: {count}\n"

        text += f"""
{"=" * 60}
Report generated by Data Ingestion Framework v1.0.0
"""

        return text