import logging
import traceback
from datetime import datetime


class DataIngestionError(Exception):
    """Base class for data ingestion errors."""
    pass

class RecoverableError(DataIngestionError):
    """For errors that can be recovered from (like encoding issues)."""
    pass


class ErrorHandler:
    def __init__(self, log_file="logs/error_log.txt"):
        self.log_file = log_file
        logging.basicConfig(filename=self.log_file, level=logging.ERROR,
                            format="%(asctime)s - %(levelname)s - %(message)s")

    def log_error(self, error, file_path=None):
        error_type = type(error).__name__
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tb = traceback.format_exc()

        message = f"[{timestamp}] {error_type}: {str(error)}"
        if file_path:
            message += f" | File: {file_path}"

        logging.error(message)
        logging.error(tb)

        print(f"An error occurred: {message}")

    def try_encoding_recovery(self, file_path):
        encodings = ["utf-8", "latin-1"]
        for enc in encodings:
            try:
                with open(file_path, encoding=enc) as f:
                    content = f.read()
                print(f"File recovered using encoding: {enc}")
                return content
            except Exception:
                continue
        raise RecoverableError(f"Could not read file with supported encodings: {file_path}")
        
    def get_error_summary(self):
        """
        Return a summary of all errors encountered during processing.
        
        Returns:
            Dict[str, Any]: A dictionary containing error statistics and details
        """
        # Implementation will parse the error log file to extract statistics
        error_counts = {}
        error_details = []
        
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    if "] ERROR:" in line:
                        # Extract error type
                        error_parts = line.split("] ERROR:", 1)[1].strip()
                        if ":" in error_parts:
                            error_type = error_parts.split(":", 1)[0].strip()
                            error_counts[error_type] = error_counts.get(error_type, 0) + 1
                            
                            # Extract file information if available
                            if "| File:" in line:
                                file_info = line.split("| File:", 1)[1].strip()
                                error_details.append({
                                    "type": error_type,
                                    "message": error_parts,
                                    "file": file_info
                                })
                            else:
                                error_details.append({
                                    "type": error_type,
                                    "message": error_parts
                                })
        except Exception as e:
            # Handle case where log file doesn't exist or can't be read
            return {
                "total_errors": 0,
                "error_types": {},
                "error_details": [],
                "error_reading_logs": str(e)
            }
        
        return {
            "total_errors": sum(error_counts.values()),
            "error_types": error_counts,
            "most_common_errors": sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5],
            "error_details": error_details[:10]  # Limit to first 10 errors for brevity
        }
        
    def export_error_log(self, output_file):
        """
        Export the error log to a JSON file.
        
        Args:
            output_file (Path): Path to the output JSON file
        """
        import json
        from pathlib import Path
        
        # Get the error summary
        error_summary = self.get_error_summary()
        
        # Write to file
        try:
            with open(output_file, 'w') as f:
                json.dump(error_summary, f, indent=2)
            print(f"Error log exported to {output_file}")
        except Exception as e:
            print(f"Failed to export error log: {e}")
