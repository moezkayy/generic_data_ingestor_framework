import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Generator, Tuple
import fnmatch
from collections import defaultdict
import os


class FileScanner:
    """
    Advanced file scanner with filtering, classification, and validation capabilities
    """

    # Comprehensive file type mappings
    FILE_TYPE_MAPPINGS = {
        # JSON files
        '.json': 'json',
        '.jsonl': 'json',  # JSON Lines
        '.ndjson': 'json',  # Newline Delimited JSON

        # CSV files
        '.csv': 'csv',
        '.tsv': 'csv',  # Tab-separated values
        '.psv': 'csv',  # Pipe-separated values

        # Parquet files
        '.parquet': 'parquet',
        '.pq': 'parquet',
        '.pqt': 'parquet'
    }

    # File patterns to ignore
    IGNORE_PATTERNS = [
        '.*',  # Hidden files
        '~*',  # Backup files
        '*.tmp',  # Temporary files
        '*.temp',  # Temporary files
        '*.bak',  # Backup files
        '__pycache__',  # Python cache
        '*.pyc',  # Python compiled
        '.git*',  # Git files
        'node_modules',  # Node.js modules
        '.DS_Store'  # macOS metadata
    ]

    def __init__(self, root_directory: str):
        """
        Initialize file scanner

        Args:
            root_directory (str): Root directory to scan
        """
        self.root_directory = Path(root_directory).resolve()
        self.logger = logging.getLogger('data_ingestion.file_scanner')

        # Validate root directory
        self._validate_root_directory()

        # Statistics
        self.scan_stats = {
            'directories_scanned': 0,
            'files_found': 0,
            'files_classified': 0,
            'files_ignored': 0,
            'errors_encountered': 0
        }

    def _validate_root_directory(self):
        """Validate the root directory"""
        if not self.root_directory.exists():
            raise FileNotFoundError(f"Directory does not exist: {self.root_directory}")

        if not self.root_directory.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {self.root_directory}")

        # Check if directory is readable
        if not os.access(self.root_directory, os.R_OK):
            raise PermissionError(f"Cannot read directory: {self.root_directory}")

    def discover_files(self, file_types: List[str] = None,
                       recursive: bool = True,
                       include_patterns: List[str] = None,
                       exclude_patterns: List[str] = None) -> Dict[str, List[Path]]:
        """
        Discover and classify files in the directory

        Args:
            file_types (List[str], optional): File types to include
            recursive (bool): Whether to scan subdirectories
            include_patterns (List[str], optional): Patterns to include
            exclude_patterns (List[str], optional): Additional patterns to exclude

        Returns:
            Dict[str, List[Path]]: Dictionary mapping file types to file paths
        """
        self.logger.info(f"Starting file discovery in: {self.root_directory}")

        # Reset statistics
        self._reset_stats()

        # Set default file types
        if file_types is None:
            file_types = list(set(self.FILE_TYPE_MAPPINGS.values()))

        # Initialize results
        discovered_files = {file_type: [] for file_type in file_types}

        # Combine exclude patterns
        all_exclude_patterns = self.IGNORE_PATTERNS.copy()
        if exclude_patterns:
            all_exclude_patterns.extend(exclude_patterns)

        try:
            # Scan files
            for file_path in self._scan_directory(recursive):
                self.scan_stats['files_found'] += 1

                # Check if file should be ignored
                if self._should_ignore_file(file_path, all_exclude_patterns):
                    self.scan_stats['files_ignored'] += 1
                    continue

                # Check include patterns if specified
                if include_patterns and not self._matches_patterns(file_path, include_patterns):
                    continue

                # Classify file
                file_type = self._classify_file(file_path)

                if file_type and file_type in file_types:
                    discovered_files[file_type].append(file_path)
                    self.scan_stats['files_classified'] += 1
                    self.logger.debug(f"Classified {file_type}: {file_path.name}")

            # Log results
            self._log_discovery_results(discovered_files)

            return discovered_files

        except Exception as e:
            self.scan_stats['errors_encountered'] += 1
            self.logger.error(f"Error during file discovery: {e}")
            raise

    def _scan_directory(self, recursive: bool) -> Generator[Path, None, None]:
        """
        Scan directory and yield file paths

        Args:
            recursive (bool): Whether to scan recursively

        Yields:
            Path: File paths found
        """
        if recursive:
            pattern = "**/*"
        else:
            pattern = "*"

        try:
            for path in self.root_directory.glob(pattern):
                if path.is_file():
                    yield path
                elif path.is_dir() and recursive:
                    self.scan_stats['directories_scanned'] += 1
        except PermissionError as e:
            self.logger.warning(f"Permission denied accessing: {e}")
        except Exception as e:
            self.logger.error(f"Error scanning directory: {e}")
            raise

    def _classify_file(self, file_path: Path) -> Optional[str]:
        """
        Classify file based on extension

        Args:
            file_path (Path): Path to file

        Returns:
            Optional[str]: File type or None if not supported
        """
        extension = file_path.suffix.lower()
        return self.FILE_TYPE_MAPPINGS.get(extension)

    def _should_ignore_file(self, file_path: Path, exclude_patterns: List[str]) -> bool:
        """
        Check if file should be ignored based on patterns

        Args:
            file_path (Path): File path to check
            exclude_patterns (List[str]): Patterns to exclude

        Returns:
            bool: True if file should be ignored
        """
        filename = file_path.name

        for pattern in exclude_patterns:
            if fnmatch.fnmatch(filename, pattern):
                return True

        return False

    def _matches_patterns(self, file_path: Path, patterns: List[str]) -> bool:
        """
        Check if file matches any of the given patterns

        Args:
            file_path (Path): File path to check
            patterns (List[str]): Patterns to match

        Returns:
            bool: True if file matches any pattern
        """
        filename = file_path.name

        for pattern in patterns:
            if fnmatch.fnmatch(filename, pattern):
                return True

        return False

    def get_file_details(self, file_path: Path) -> Dict:
        """
        Get detailed information about a file

        Args:
            file_path (Path): Path to file

        Returns:
            Dict: Detailed file information
        """
        try:
            stat_info = file_path.stat()

            return {
                'path': str(file_path.resolve()),
                'name': file_path.name,
                'stem': file_path.stem,
                'extension': file_path.suffix.lower(),
                'size_bytes': stat_info.st_size,
                'size_mb': round(stat_info.st_size / (1024 * 1024), 3),
                'modified_timestamp': stat_info.st_mtime,
                'file_type': self._classify_file(file_path),
                'is_readable': os.access(file_path, os.R_OK),
                'parent_directory': str(file_path.parent)
            }
        except Exception as e:
            self.logger.error(f"Error getting file details for {file_path}: {e}")
            return {
                'path': str(file_path),
                'error': str(e)
            }

    def validate_discovered_files(self, discovered_files: Dict[str, List[Path]]) -> Dict[str, List[Path]]:
        """
        Validate that discovered files are accessible and readable

        Args:
            discovered_files (Dict[str, List[Path]]): Files to validate

        Returns:
            Dict[str, List[Path]]: Validated files (removes inaccessible ones)
        """
        validated_files = {}

        for file_type, file_list in discovered_files.items():
            validated_list = []

            for file_path in file_list:
                try:
                    # Check if file still exists and is readable
                    if file_path.exists() and file_path.is_file() and os.access(file_path, os.R_OK):
                        # Test basic read access
                        with open(file_path, 'rb') as f:
                            f.read(1)  # Try to read first byte
                        validated_list.append(file_path)
                    else:
                        self.logger.warning(f"File validation failed: {file_path}")
                except Exception as e:
                    self.logger.warning(f"File validation error for {file_path}: {e}")

            validated_files[file_type] = validated_list

            # Log validation results
            removed_count = len(file_list) - len(validated_list)
            if removed_count > 0:
                self.logger.warning(f"Removed {removed_count} invalid {file_type} files")

        return validated_files

    def get_supported_file_types(self) -> List[str]:
        """
        Get list of supported file types

        Returns:
            List[str]: Supported file types
        """
        return list(set(self.FILE_TYPE_MAPPINGS.values()))

    def get_supported_extensions(self) -> Dict[str, str]:
        """
        Get mapping of extensions to file types

        Returns:
            Dict[str, str]: Extension to file type mapping
        """
        return self.FILE_TYPE_MAPPINGS.copy()

    def _reset_stats(self):
        """Reset scan statistics"""
        self.scan_stats = {
            'directories_scanned': 0,
            'files_found': 0,
            'files_classified': 0,
            'files_ignored': 0,
            'errors_encountered': 0
        }

    def _log_discovery_results(self, discovered_files: Dict[str, List[Path]]):
        """Log discovery results"""
        total_discovered = sum(len(files) for files in discovered_files.values())

        self.logger.info(f"File discovery completed:")
        self.logger.info(f"  Total files found: {self.scan_stats['files_found']}")
        self.logger.info(f"  Files classified: {self.scan_stats['files_classified']}")
        self.logger.info(f"  Files ignored: {self.scan_stats['files_ignored']}")
        self.logger.info(f"  Directories scanned: {self.scan_stats['directories_scanned']}")

        for file_type, files in discovered_files.items():
            if files:
                self.logger.info(f"  {file_type.upper()}: {len(files)} files")

    def get_scan_statistics(self) -> Dict:
        """
        Get scan statistics

        Returns:
            Dict: Scan statistics
        """
        return self.scan_stats.copy()