import logging
import traceback
from typing import Optional, Any, Dict, List
from pathlib import Path
from datetime import datetime
import json


class DataIngestionError(Exception):
    """Base exception for data ingestion framework"""

    def __init__(self, message: str, error_code: str = None, details: Dict = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "GENERIC_ERROR"
        self.details = details or {}
        self.timestamp = datetime.now().isoformat()


class FileProcessingError(DataIngestionError):
    """Exception for file processing errors"""

    def __init__(self, message: str, file_path: Path = None, **kwargs):
        super().__init__(message, error_code="FILE_PROCESSING_ERROR", **kwargs)
        self.file_path = file_path


class SchemaInferenceError(DataIngestionError):
    """Exception for schema inference errors"""

    def __init__(self, message: str, data_sample: Any = None, **kwargs):
        super().__init__(message, error_code="SCHEMA_INFERENCE_ERROR", **kwargs)
        self.data_sample = data_sample


class ValidationError(DataIngestionError):
    """Exception for data validation errors"""

    def __init__(self, message: str, field_path: str = None, **kwargs):
        super().__init__(message, error_code="VALIDATION_ERROR", **kwargs)
        self.field_path = field_path


class ConfigurationError(DataIngestionError):
    """Exception for configuration errors"""

    def __init__(self, message: str, config_section: str = None, **kwargs):
        super().__init__(message, error_code="CONFIGURATION_ERROR", **kwargs)
        self.config_section = config_section


class ErrorHandler:
    """
    Comprehensive error handling with recovery strategies and detailed reporting
    """

    def __init__(self):
        """Initialize the error handler"""
        self.logger = logging.getLogger('data_ingestion.error_handler')

        # Error statistics
        self.error_counts = {
            'file_errors': 0,
            'schema_errors': 0,
            'validation_errors': 0,
            'configuration_errors': 0,
            'system_errors': 0,
            'recovery_attempts': 0,
            'recovery_successes': 0
        }

        # Detailed error log
        self.error_log = []

        # Recovery strategies
        self.recovery_strategies = {
            'file_encoding': ['utf-8', 'latin-1', 'cp1252', 'ascii'],
            'json_repair': True,
            'schema_fallback': True,
            'validation_skip': False
        }

    def handle_file_error(self, error: Exception, file_path: Path,
                          operation: str = "processing",
                          attempt_recovery: bool = True) -> Dict[str, Any]:
        """
        Handle file processing errors with recovery attempts

        Args:
            error (Exception): The error that occurred
            file_path (Path): Path to the problematic file
            operation (str): Operation being performed
            attempt_recovery (bool): Whether to attempt recovery

        Returns:
            Dict[str, Any]: Error details and recovery information
        """
        self.error_counts['file_errors'] += 1

        error_details = {
            'type': 'file_error',
            'operation': operation,
            'file': str(file_path),
            'file_name': file_path.name,
            'file_size': self._get_safe_file_size(file_path),
            'error': str(error),
            'error_type': type(error).__name__,
            'timestamp': datetime.now().isoformat(),
            'recoverable': self._is_recoverable_file_error(error),
            'recovery_attempted': False,
            'recovery_successful': False,
            'recovery_details': None
        }

        # Log the error
        self.logger.error(f"File {operation} error in {file_path.name}: {error}")

        # Attempt recovery if requested and error is recoverable
        if attempt_recovery and error_details['recoverable']:
            recovery_result = self._attempt_file_recovery(error, file_path, operation)
            error_details.update(recovery_result)

        # Add to error log
        self.error_log.append(error_details)

        # Provide specific guidance based on error type
        if isinstance(error, PermissionError):
            self.logger.error("Permission denied - check file permissions and user access rights")
        elif isinstance(error, FileNotFoundError):
            self.logger.error("File not found - verify file path and existence")
        elif isinstance(error, UnicodeDecodeError):
            self.logger.error("Encoding error - file may not be valid UTF-8")

        return error_details

    def handle_schema_error(self, error: Exception, data_sample: Any = None,
                            file_path: Path = None) -> Dict[str, Any]:
        """
        Handle schema inference errors

        Args:
            error (Exception): The error that occurred
            data_sample (Any, optional): Sample of problematic data
            file_path (Path, optional): Source file path

        Returns:
            Dict[str, Any]: Error details for reporting
        """
        self.error_counts['schema_errors'] += 1

        error_details = {
            'type': 'schema_error',
            'file': str(file_path) if file_path else None,
            'error': str(error),
            'error_type': type(error).__name__,
            'timestamp': datetime.now().isoformat(),
            'data_sample': str(data_sample)[:200] if data_sample else None,
            'recovery_suggestion': self._get_schema_recovery_suggestion(error),
            'fallback_available': self.recovery_strategies.get('schema_fallback', False)
        }

        self.logger.error(f"Schema inference error: {error}")
        if data_sample:
            self.logger.debug(f"Problematic data sample: {str(data_sample)[:200]}")

        self.error_log.append(error_details)
        return error_details

    def handle_validation_error(self, error: Exception, field_path: str = None,
                                file_path: Path = None, severity: str = "medium") -> Dict[str, Any]:
        """
        Handle data validation errors

        Args:
            error (Exception): The error that occurred
            field_path (str, optional): Path to the problematic field
            file_path (Path, optional): Source file path
            severity (str): Error severity level

        Returns:
            Dict[str, Any]: Error details for reporting
        """
        self.error_counts['validation_errors'] += 1

        error_details = {
            'type': 'validation_error',
            'file': str(file_path) if file_path else None,
            'field_path': field_path,
            'error': str(error),
            'error_type': type(error).__name__,
            'timestamp': datetime.now().isoformat(),
            'severity': severity,
            'can_skip': self.recovery_strategies.get('validation_skip', False)
        }

        log_level = logging.ERROR if severity == "high" else logging.WARNING
        self.logger.log(log_level, f"Validation error{f' in {field_path}' if field_path else ''}: {error}")

        self.error_log.append(error_details)
        return error_details

    def handle_configuration_error(self, error: Exception, config_section: str = None) -> Dict[str, Any]:
        """
        Handle configuration errors

        Args:
            error (Exception): The error that occurred
            config_section (str, optional): Configuration section with error

        Returns:
            Dict[str, Any]: Error details for reporting
        """
        self.error_counts['configuration_errors'] += 1

        error_details = {
            'type': 'configuration_error',
            'config_section': config_section,
            'error': str(error),
            'error_type': type(error).__name__,
            'timestamp': datetime.now().isoformat(),
            'critical': True
        }

        self.logger.critical(f"Configuration error{f' in {config_section}' if config_section else ''}: {error}")

        self.error_log.append(error_details)
        return error_details

    def handle_application_error(self, error: Exception,
                                 logger: Optional[logging.Logger] = None,
                                 context: str = None):
        """
        Handle critical application errors

        Args:
            error (Exception): The error that occurred
            logger (logging.Logger, optional): Logger instance
            context (str, optional): Error context
        """
        self.error_counts['system_errors'] += 1

        error_details = {
            'type': 'system_error',
            'error': str(error),
            'error_type': type(error).__name__,
            'timestamp': datetime.now().isoformat(),
            'context': context,
            'stack_trace': traceback.format_exc(),
            'critical': True
        }

        error_logger = logger or self.logger
        error_logger.critical(f"Critical application error{f' in {context}' if context else ''}: {error}")
        error_logger.debug(f"Stack trace:\n{traceback.format_exc()}")

        self.error_log.append(error_details)

    def _attempt_file_recovery(self, error: Exception, file_path: Path,
                               operation: str) -> Dict[str, Any]:
        """
        Attempt to recover from file errors

        Args:
            error (Exception): Original error
            file_path (Path): File path
            operation (str): Operation type

        Returns:
            Dict[str, Any]: Recovery attempt results
        """
        self.error_counts['recovery_attempts'] += 1

        recovery_result = {
            'recovery_attempted': True,
            'recovery_successful': False,
            'recovery_details': []
        }

        # Try different encodings for text files
        if isinstance(error, UnicodeDecodeError) and operation == "read":
            for encoding in self.recovery_strategies['file_encoding']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        f.read(100)  # Test read

                    recovery_result['recovery_successful'] = True
                    recovery_result['recovery_details'].append(f"Successfully read with {encoding} encoding")
                    self.error_counts['recovery_successes'] += 1
                    self.logger.info(f"File recovery successful using {encoding} encoding: {file_path.name}")
                    break
                except Exception:
                    recovery_result['recovery_details'].append(f"Failed with {encoding} encoding")

        return recovery_result

    def _is_recoverable_file_error(self, error: Exception) -> bool:
        """Determine if a file error is recoverable"""
        recoverable_errors = (
            UnicodeDecodeError,
            ValueError,  # JSON decode errors
            json.JSONDecodeError
        )
        return isinstance(error, recoverable_errors)

    def _get_schema_recovery_suggestion(self, error: Exception) -> str:
        """Get recovery suggestion for schema errors"""
        error_str = str(error).lower()

        if "recursion" in error_str or "circular" in error_str:
            return "Data contains circular references - try flattening structure"
        elif "type" in error_str or "mixed" in error_str:
            return "Mixed data types detected - consider data cleaning or type coercion"
        elif "deep" in error_str or "nested" in error_str:
            return "Data structure too complex - try processing with simpler schema"
        elif "empty" in error_str:
            return "Empty data detected - verify data source"
        else:
            return "Check data structure and format consistency"

    def _get_safe_file_size(self, file_path: Path) -> int:
        """Safely get file size"""
        try:
            return file_path.stat().st_size
        except Exception:
            return -1

    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive error summary

        Returns:
            Dict[str, Any]: Error summary with statistics
        """
        total_errors = sum(self.error_counts.values()) - self.error_counts['recovery_attempts'] - self.error_counts[
            'recovery_successes']

        recovery_rate = 0
        if self.error_counts['recovery_attempts'] > 0:
            recovery_rate = (self.error_counts['recovery_successes'] / self.error_counts['recovery_attempts']) * 100

        return {
            'total_errors': total_errors,
            'error_breakdown': {
                'file_errors': self.error_counts['file_errors'],
                'schema_errors': self.error_counts['schema_errors'],
                'validation_errors': self.error_counts['validation_errors'],
                'configuration_errors': self.error_counts['configuration_errors'],
                'system_errors': self.error_counts['system_errors']
            },
            'recovery_statistics': {
                'recovery_attempts': self.error_counts['recovery_attempts'],
                'recovery_successes': self.error_counts['recovery_successes'],
                'recovery_rate_percent': round(recovery_rate, 1)
            },
            'error_log_count': len(self.error_log)
        }

    def export_error_log(self, output_file: Path) -> bool:
        """
        Export detailed error log to JSON file

        Args:
            output_file (Path): Output file path

        Returns:
            bool: True if successful
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'error_summary': self.get_error_summary(),
                    'detailed_errors': self.error_log
                }, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Error log exported to: {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to export error log: {e}")
            return False

    def clear_error_log(self):
        """Clear the error log and reset counters"""
        self.error_log.clear()
        self.error_counts = {key: 0 for key in self.error_counts.keys()}
        self.logger.info("Error log cleared")

    def set_recovery_strategy(self, strategy: str, value: Any):
        """
        Set recovery strategy configuration

        Args:
            strategy (str): Strategy name
            value (Any): Strategy value
        """
        if strategy in self.recovery_strategies:
            self.recovery_strategies[strategy] = value
            self.logger.info(f"Recovery strategy updated: {strategy} = {value}")
        else:
            self.logger.warning(f"Unknown recovery strategy: {strategy}")

    def get_critical_errors(self) -> List[Dict[str, Any]]:
        """
        Get list of critical errors that require immediate attention

        Returns:
            List[Dict[str, Any]]: Critical errors
        """
        return [error for error in self.error_log
                if error.get('critical', False) or error.get('type') == 'system_error']