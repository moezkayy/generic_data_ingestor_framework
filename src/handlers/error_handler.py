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
