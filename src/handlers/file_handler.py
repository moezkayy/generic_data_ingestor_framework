import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
import os
import shutil
from datetime import datetime
import hashlib


class FileHandler:

    def __init__(self):
        """Initialize the file handler"""
        self.logger = logging.getLogger('data_ingestion.file_handler')
        self.operation_history = []

    def file_exists(self, file_path: Union[str, Path]) -> bool:

        try:
            path = Path(file_path)
            return path.exists() and path.is_file()
        except Exception as e:
            self.logger.debug(f"Error checking file existence {file_path}: {e}")
            return False

    def directory_exists(self, dir_path: Union[str, Path]) -> bool:

        try:
            path = Path(dir_path)
            return path.exists() and path.is_dir()
        except Exception as e:
            self.logger.debug(f"Error checking directory existence {dir_path}: {e}")
            return False

    def create_directory(self, dir_path: Union[str, Path],
                         parents: bool = True, exist_ok: bool = True) -> bool:

        try:
            path = Path(dir_path)
            path.mkdir(parents=parents, exist_ok=exist_ok)

            self._log_operation("CREATE_DIRECTORY", str(path), True)
            self.logger.debug(f"Created directory: {path}")
            return True

        except Exception as e:
            self._log_operation("CREATE_DIRECTORY", str(dir_path), False, str(e))
            self.logger.error(f"Error creating directory {dir_path}: {e}")
            return False

    def get_comprehensive_file_info(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        path = Path(file_path)

        file_info = {
            'path': str(path.resolve()),
            'name': path.name,
            'stem': path.stem,
            'suffix': path.suffix.lower(),
            'parent_directory': str(path.parent),
            'exists': False,
            'is_file': False,
            'is_readable': False,
            'is_writable': False,
            'size_bytes': -1,
            'size_mb': -1,
            'modified_timestamp': None,
            'created_timestamp': None,
            'file_hash': None,
            'encoding_detected': None,
            'error': None
        }

        try:
            if path.exists():
                file_info['exists'] = True
                file_info['is_file'] = path.is_file()

                if file_info['is_file']:
                    # Get file stats
                    stat = path.stat()
                    file_info['size_bytes'] = stat.st_size
                    file_info['size_mb'] = round(stat.st_size / (1024 * 1024), 3)
                    file_info['modified_timestamp'] = stat.st_mtime
                    file_info['created_timestamp'] = stat.st_ctime

                    # Check permissions
                    file_info['is_readable'] = os.access(path, os.R_OK)
                    file_info['is_writable'] = os.access(path, os.W_OK)

                    # Calculate file hash (for small files)
                    if stat.st_size < 10 * 1024 * 1024:  # Less than 10MB
                        file_info['file_hash'] = self._calculate_file_hash(path)

                    # Detect encoding for text files
                    if path.suffix.lower() in ['.json', '.csv', '.txt']:
                        file_info['encoding_detected'] = self._detect_encoding(path)

        except Exception as e:
            file_info['error'] = str(e)
            self.logger.debug(f"Error getting file info for {file_path}: {e}")

        return file_info

    def validate_file_access(self, file_path: Union[str, Path],
                             operation: str = 'read') -> Tuple[bool, str]:

        path = Path(file_path)

        try:
            if not path.exists():
                return False, f"File does not exist: {path}"

            if not path.is_file():
                return False, f"Path is not a file: {path}"

            # Check specific permissions
            if operation == 'read':
                if not os.access(path, os.R_OK):
                    return False, f"No read permission: {path}"
            elif operation == 'write':
                if not os.access(path, os.W_OK):
                    return False, f"No write permission: {path}"
            elif operation == 'execute':
                if not os.access(path, os.X_OK):
                    return False, f"No execute permission: {path}"

            return True, "File access validated successfully"

        except Exception as e:
            return False, f"Error validating file access: {e}"

    def read_json_file(self, file_path: Union[str, Path],
                       encoding: str = 'utf-8-sig',
                       fallback_encodings: List[str] = None) -> Any:

        path = Path(file_path)

        # Validate file access
        is_valid, error_msg = self.validate_file_access(path, 'read')
        if not is_valid:
            if "does not exist" in error_msg:
                raise FileNotFoundError(error_msg)
            else:
                raise PermissionError(error_msg)

        # Set default fallback encodings
        if fallback_encodings is None:
            fallback_encodings = ['utf-8', 'latin-1', 'cp1252', 'ascii']

        encodings_to_try = [encoding] + fallback_encodings

        for enc in encodings_to_try:
            try:
                with open(path, 'r', encoding=enc) as f:
                    data = json.load(f)

                self._log_operation("READ_JSON", str(path), True, f"encoding: {enc}")
                self.logger.debug(f"Successfully read JSON file with {enc}: {path}")
                return data

            except UnicodeDecodeError as e:
                if enc == encodings_to_try[-1]:  # Last encoding
                    self._log_operation("READ_JSON", str(path), False, f"All encodings failed")
                    self.logger.error(f"All encoding attempts failed for {path}")
                    raise
                else:
                    self.logger.debug(f"Encoding {enc} failed for {path}, trying next...")
                    continue

            except json.JSONDecodeError as e:
                self._log_operation("READ_JSON", str(path), False, f"Invalid JSON: {str(e)[:100]}")
                self.logger.error(f"Invalid JSON in file {path}: {e}")
                raise

            except Exception as e:
                self._log_operation("READ_JSON", str(path), False, str(e))
                self.logger.error(f"Unexpected error reading JSON file {path}: {e}")
                raise

    def write_json_file(self, data: Any, file_path: Union[str, Path],
                        indent: int = 2, ensure_ascii: bool = False,
                        backup_existing: bool = True) -> bool:

        path = Path(file_path)

        try:
            # Create parent directories if needed
            path.parent.mkdir(parents=True, exist_ok=True)

            # Create backup if file exists
            backup_path = None
            if backup_existing and path.exists():
                backup_path = self._create_backup(path)

            # Write to temporary file first
            temp_path = path.with_suffix(path.suffix + '.tmp')

            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)

            # Validate written file
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    json.load(f)  # Test if it's valid JSON
            except json.JSONDecodeError as e:
                temp_path.unlink()  # Remove invalid file
                raise json.JSONDecodeError(f"Written JSON is invalid: {e}", "", 0)

            # Move temp file to final location
            shutil.move(str(temp_path), str(path))

            file_size = path.stat().st_size
            self._log_operation("WRITE_JSON", str(path), True, f"size: {file_size} bytes")
            self.logger.debug(f"Successfully wrote JSON file: {path} ({file_size} bytes)")

            return True

        except Exception as e:
            # Clean up temp file if it exists
            if 'temp_path' in locals() and temp_path.exists():
                temp_path.unlink()

            # Restore backup if write failed and backup exists
            if backup_existing and 'backup_path' in locals() and backup_path and backup_path.exists():
                shutil.move(str(backup_path), str(path))
                self.logger.info(f"Restored backup file: {path}")

            self._log_operation("WRITE_JSON", str(path), False, str(e))
            self.logger.error(f"Error writing JSON file {path}: {e}")
            return False

    def write_text_file(self, content: str, file_path: Union[str, Path],
                        encoding: str = 'utf-8',
                        backup_existing: bool = True) -> bool:

        path = Path(file_path)

        try:
            # Create parent directories if needed
            path.parent.mkdir(parents=True, exist_ok=True)

            # Create backup if file exists
            backup_path = None
            if backup_existing and path.exists():
                backup_path = self._create_backup(path)

            # Write content
            with open(path, 'w', encoding=encoding) as f:
                f.write(content)

            file_size = path.stat().st_size
            self._log_operation("WRITE_TEXT", str(path), True, f"size: {file_size} bytes")
            self.logger.debug(f"Successfully wrote text file: {path} ({file_size} bytes)")

            return True

        except Exception as e:
            # Restore backup if write failed and backup exists
            if backup_existing and 'backup_path' in locals() and backup_path and backup_path.exists():
                shutil.move(str(backup_path), str(path))
                self.logger.info(f"Restored backup file: {path}")

            self._log_operation("WRITE_TEXT", str(path), False, str(e))
            self.logger.error(f"Error writing text file {path}: {e}")
            return False

    def copy_file(self, source: Union[str, Path],
                  destination: Union[str, Path],
                  preserve_metadata: bool = True) -> bool:

        try:
            src_path = Path(source)
            dst_path = Path(destination)

            # Validate source
            is_valid, error_msg = self.validate_file_access(src_path, 'read')
            if not is_valid:
                raise FileNotFoundError(error_msg)

            # Create destination directory
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy file
            if preserve_metadata:
                shutil.copy2(src_path, dst_path)
            else:
                shutil.copy(src_path, dst_path)

            self._log_operation("COPY_FILE", f"{src_path} -> {dst_path}", True)
            self.logger.debug(f"Copied file from {src_path} to {dst_path}")
            return True

        except Exception as e:
            self._log_operation("COPY_FILE", f"{source} -> {destination}", False, str(e))
            self.logger.error(f"Error copying file from {source} to {destination}: {e}")
            return False

    def move_file(self, source: Union[str, Path],
                  destination: Union[str, Path]) -> bool:

        try:
            src_path = Path(source)
            dst_path = Path(destination)

            # Validate source
            is_valid, error_msg = self.validate_file_access(src_path, 'read')
            if not is_valid:
                raise FileNotFoundError(error_msg)

            # Create destination directory
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            # Move file
            shutil.move(str(src_path), str(dst_path))

            self._log_operation("MOVE_FILE", f"{src_path} -> {dst_path}", True)
            self.logger.debug(f"Moved file from {src_path} to {dst_path}")
            return True

        except Exception as e:
            self._log_operation("MOVE_FILE", f"{source} -> {destination}", False, str(e))
            self.logger.error(f"Error moving file from {source} to {destination}: {e}")
            return False

    def delete_file(self, file_path: Union[str, Path],
                    create_backup: bool = False) -> bool:

        try:
            path = Path(file_path)

            if not path.exists():
                self.logger.warning(f"File to delete does not exist: {path}")
                return True

            # Create backup if requested
            if create_backup:
                backup_path = self._create_backup(path)
                self.logger.info(f"Created backup before deletion: {backup_path}")

            # Delete file
            path.unlink()

            self._log_operation("DELETE_FILE", str(path), True)
            self.logger.debug(f"Deleted file: {path}")
            return True

        except Exception as e:
            self._log_operation("DELETE_FILE", str(file_path), False, str(e))
            self.logger.error(f"Error deleting file {file_path}: {e}")
            return False

    def get_file_size(self, file_path: Union[str, Path]) -> int:

        try:
            return Path(file_path).stat().st_size
        except Exception as e:
            self.logger.debug(f"Error getting file size for {file_path}: {e}")
            return -1

    def _calculate_file_hash(self, file_path: Path, algorithm: str = 'md5') -> str:
        """Calculate file hash"""
        try:
            hash_obj = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception as e:
            self.logger.debug(f"Error calculating hash for {file_path}: {e}")
            return None

    def _detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding by trying common encodings"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'ascii']

        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(1024)  # Read first 1KB
                return encoding
            except UnicodeDecodeError:
                continue

        return 'unknown'

    def _create_backup(self, file_path: Path) -> Path:
        """Create backup of file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = file_path.with_suffix(f'.backup_{timestamp}{file_path.suffix}')
        shutil.copy2(file_path, backup_path)
        return backup_path

    def _log_operation(self, operation: str, target: str, success: bool, details: str = None):
        """Log file operation"""
        self.operation_history.append({
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'target': target,
            'success': success,
            'details': details
        })

    def get_operation_history(self) -> List[Dict[str, Any]]:
        """Get file operation history"""
        return self.operation_history.copy()

    def clear_operation_history(self):
        """Clear operation history"""
        self.operation_history.clear()
