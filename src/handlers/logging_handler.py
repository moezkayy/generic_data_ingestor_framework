"""
Simple Logging Handler for Generic Data Ingestion Framework.
Author: Moez Khan (SRN: 23097401)
FYP Project - University of Hertfordshire

Provides basic logging configuration for the application.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


class LoggingHandler:
    """
    Simple logging handler for application logging needs.
    """

    def __init__(self, log_level: str = "INFO", log_file: Optional[str] = None):
        """
        Initialize the logging handler.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional log file path
        """
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.log_file = log_file
        self.logger = None

    def setup_logging(self) -> logging.Logger:
        """
        Set up basic logging configuration.
        
        Returns:
            Configured logger instance
        """
        # Create logger
        logger = logging.getLogger('data_ingestion')
        logger.setLevel(self.log_level)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if specified)
        if self.log_file:
            # Create log directory if it doesn't exist
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        self.logger = logger
        return logger

    def get_logger(self) -> logging.Logger:
        """
        Get the configured logger instance.
        
        Returns:
            Logger instance
        """
        if self.logger is None:
            return self.setup_logging()
        return self.logger


# Global logging handler instance
_logging_handler = None

def get_logging_handler(log_level: str = "INFO", log_file: Optional[str] = None) -> LoggingHandler:
    """
    Get the global logging handler instance.
    
    Args:
        log_level: Logging level
        log_file: Optional log file path
        
    Returns:
        LoggingHandler instance
    """
    global _logging_handler
    
    if _logging_handler is None:
        _logging_handler = LoggingHandler(log_level, log_file)
    
    return _logging_handler