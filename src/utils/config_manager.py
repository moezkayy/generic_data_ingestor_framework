"""
Configuration Manager for Database and Application Settings.

This module provides secure configuration loading from multiple sources including
JSON/YAML files, environment variables, and command-line arguments with proper
credential handling and validation.
"""

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import re
import getpass

# Optional YAML support
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass


class DatabaseConfigManager:
    """
    Secure database configuration manager.
    
    This class handles loading database configuration from multiple sources
    with support for environment variable substitution, credential masking,
    and secure credential handling.
    """
    
    # Environment variable patterns
    ENV_VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')
    CREDENTIAL_KEYS = {'password', 'passwd', 'pwd', 'secret', 'key', 'token', 'api_key'}
    
    def __init__(self):
        """Initialize the configuration manager."""
        self.logger = logging.getLogger('data_ingestion.config_manager')
        self._loaded_configs = {}
        self._env_vars_used = set()
    
    def load_database_config(self, config_source: Optional[str] = None,
                           config_dict: Optional[Dict[str, Any]] = None,
                           database_url: Optional[str] = None,
                           prompt_for_missing: bool = False) -> Dict[str, Any]:
        """
        Load database configuration from various sources.
        
        Args:
            config_source: Path to JSON/YAML configuration file
            config_dict: Direct configuration dictionary
            database_url: Database connection URL
            prompt_for_missing: Whether to prompt for missing credentials
            
        Returns:
            Dict: Validated database configuration
            
        Raises:
            ConfigurationError: If configuration loading fails
        """
        try:
            config = {}
            
            # Load from file if provided
            if config_source:
                config = self._load_config_file(config_source)
                self.logger.info(f"Loaded database configuration from: {config_source}")
            
            # Override with direct config if provided
            if config_dict:
                config.update(config_dict)
                self.logger.debug("Applied direct configuration overrides")
            
            # Parse database URL if provided
            if database_url:
                url_config = self._parse_database_url(database_url)
                config.update(url_config)
                self.logger.info("Applied database URL configuration")
            
            # Substitute environment variables
            config = self._substitute_environment_variables(config)
            
            # Prompt for missing credentials if requested
            if prompt_for_missing:
                config = self._prompt_for_missing_credentials(config)
            
            # Validate configuration
            self._validate_database_config(config)
            
            # Mask sensitive information in logs
            masked_config = self._mask_sensitive_data(config)
            self.logger.debug(f"Final database configuration: {masked_config}")
            
            return config
            
        except Exception as e:
            error_msg = f"Failed to load database configuration: {str(e)}"
            self.logger.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def _load_config_file(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from JSON or YAML file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Dict: Configuration data
            
        Raises:
            ConfigurationError: If file loading fails
        """
        path = Path(config_path)
        
        if not path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        if not path.is_file():
            raise ConfigurationError(f"Configuration path is not a file: {config_path}")
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Determine file format
            suffix = path.suffix.lower()
            
            if suffix == '.json':
                return json.loads(content)
            elif suffix in ['.yaml', '.yml']:
                if not HAS_YAML:
                    raise ConfigurationError("YAML support not available. Install PyYAML: pip install PyYAML")
                return yaml.safe_load(content)
            else:
                # Try to detect format from content
                content = content.strip()
                if content.startswith('{') and content.endswith('}'):
                    return json.loads(content)
                elif HAS_YAML and (':' in content or '-' in content[:10]):
                    return yaml.safe_load(content)
                else:
                    raise ConfigurationError(f"Unknown configuration file format: {suffix}")
                    
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in configuration file: {str(e)}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in configuration file: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Error reading configuration file: {str(e)}")
    
    def _substitute_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Substitute environment variables in configuration values.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dict: Configuration with environment variables substituted
        """
        def substitute_value(value):
            if isinstance(value, str):
                # Find all environment variable references
                matches = self.ENV_VAR_PATTERN.findall(value)
                for match in matches:
                    # Parse variable name and optional default
                    if ':-' in match:
                        var_name, default_value = match.split(':-', 1)
                    else:
                        var_name, default_value = match, None
                    
                    # Get environment variable value
                    env_value = os.environ.get(var_name.strip(), default_value)
                    
                    if env_value is None:
                        self.logger.warning(f"Environment variable not found: {var_name}")
                        env_value = f"${{{match}}}"  # Keep original if not found
                    else:
                        self._env_vars_used.add(var_name.strip())
                    
                    # Replace in value
                    value = value.replace(f"${{{match}}}", str(env_value))
                
                return value
            elif isinstance(value, dict):
                return {k: substitute_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(item) for item in value]
            else:
                return value
        
        substituted_config = substitute_value(config)
        
        if self._env_vars_used:
            self.logger.info(f"Substituted environment variables: {', '.join(sorted(self._env_vars_used))}")
        
        return substituted_config
    
    def _parse_database_url(self, database_url: str) -> Dict[str, Any]:
        """
        Parse database URL into configuration parameters.
        
        Args:
            database_url: Database connection URL
            
        Returns:
            Dict: Database configuration parameters
        """
        try:
            from urllib.parse import urlparse, parse_qs
            
            parsed = urlparse(database_url)
            
            if not parsed.scheme:
                raise ConfigurationError("Invalid database URL: missing scheme")
            
            # Map URL scheme to database type
            scheme_mapping = {
                'postgresql': 'postgresql',
                'postgres': 'postgresql',
                'mysql': 'mysql',
                'sqlite': 'sqlite'
            }
            
            db_type = scheme_mapping.get(parsed.scheme.lower())
            if not db_type:
                supported = ', '.join(scheme_mapping.keys())
                raise ConfigurationError(f"Unsupported database scheme: {parsed.scheme}. Supported: {supported}")
            
            config = {'db_type': db_type}
            
            if db_type == 'sqlite':
                # For SQLite, the path is the database
                config['database'] = parsed.path.lstrip('/')
            else:
                # For PostgreSQL and MySQL
                if parsed.hostname:
                    config['host'] = parsed.hostname
                if parsed.port:
                    config['port'] = parsed.port
                if parsed.username:
                    config['user'] = parsed.username
                if parsed.password:
                    config['password'] = parsed.password
                if parsed.path and parsed.path != '/':
                    config['database'] = parsed.path.lstrip('/')
                
                # Parse query parameters
                if parsed.query:
                    query_params = parse_qs(parsed.query)
                    for key, values in query_params.items():
                        if values:
                            # Convert common query parameters
                            if key == 'sslmode':
                                config['ssl_enabled'] = values[0].lower() not in ['disable', 'false']
                            else:
                                config[key] = values[0]
            
            return config
            
        except Exception as e:
            raise ConfigurationError(f"Failed to parse database URL: {str(e)}")
    
    def _prompt_for_missing_credentials(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prompt user for missing credentials.
        
        Args:
            config: Current configuration
            
        Returns:
            Dict: Configuration with prompted credentials
        """
        db_type = config.get('db_type', 'database')
        
        # Check for missing required credentials
        if db_type in ['postgresql', 'mysql']:
            if not config.get('password') and config.get('user'):
                try:
                    password = getpass.getpass(f"Enter password for {config['user']}@{config.get('host', 'localhost')}: ")
                    if password:
                        config['password'] = password
                        self.logger.info("Password provided via prompt")
                except (KeyboardInterrupt, EOFError):
                    self.logger.warning("Password prompt cancelled")
        
        return config
    
    def _validate_database_config(self, config: Dict[str, Any]) -> None:
        """
        Validate database configuration.
        
        Args:
            config: Configuration to validate
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not config:
            raise ConfigurationError("Database configuration cannot be empty")
        
        db_type = config.get('db_type')
        if not db_type:
            raise ConfigurationError("Database type (db_type) is required")
        
        valid_types = ['postgresql', 'mysql', 'sqlite']
        if db_type not in valid_types:
            raise ConfigurationError(f"Invalid database type: {db_type}. Valid types: {', '.join(valid_types)}")
        
        # Type-specific validation
        if db_type == 'sqlite':
            if not config.get('database'):
                raise ConfigurationError("SQLite database path is required")
        else:
            # PostgreSQL and MySQL
            required_fields = ['host', 'database', 'user']
            missing_fields = [field for field in required_fields if not config.get(field)]
            if missing_fields:
                raise ConfigurationError(f"Missing required fields for {db_type}: {', '.join(missing_fields)}")
            
            # Validate port if provided
            port = config.get('port')
            if port is not None:
                try:
                    port_int = int(port)
                    if not (1 <= port_int <= 65535):
                        raise ValueError()
                    config['port'] = port_int
                except ValueError:
                    raise ConfigurationError(f"Invalid port number: {port}")
    
    def _mask_sensitive_data(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a copy of config with sensitive data masked for logging.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dict: Configuration with sensitive data masked
        """
        masked_config = {}
        
        for key, value in config.items():
            if isinstance(key, str) and any(cred_key in key.lower() for cred_key in self.CREDENTIAL_KEYS):
                # Mask credential values
                if value:
                    masked_config[key] = '*' * min(len(str(value)), 8)
                else:
                    masked_config[key] = value
            elif isinstance(value, dict):
                masked_config[key] = self._mask_sensitive_data(value)
            else:
                masked_config[key] = value
        
        return masked_config
    
    def get_environment_variables_used(self) -> List[str]:
        """
        Get list of environment variables that were used in configuration.
        
        Returns:
            List[str]: Environment variable names
        """
        return sorted(list(self._env_vars_used))
    
    def create_sample_config_file(self, file_path: str, db_type: str = 'postgresql',
                                 include_comments: bool = True) -> None:
        """
        Create a sample configuration file.
        
        Args:
            file_path: Path where to create the sample file
            db_type: Database type for the sample
            include_comments: Whether to include explanatory comments
        """
        try:
            path = Path(file_path)
            
            # Create sample configuration
            if db_type == 'postgresql':
                sample_config = {
                    "db_type": "postgresql",
                    "host": "${DB_HOST:-localhost}",
                    "port": "${DB_PORT:-5432}",
                    "database": "${DB_NAME:-mydb}",
                    "user": "${DB_USER:-postgres}",
                    "password": "${DB_PASSWORD}",
                    "connection_timeout": 30,
                    "connection_pool_size": 10,
                    "ssl_enabled": False
                }
            elif db_type == 'mysql':
                sample_config = {
                    "db_type": "mysql",
                    "host": "${DB_HOST:-localhost}",
                    "port": "${DB_PORT:-3306}",
                    "database": "${DB_NAME:-mydb}",
                    "user": "${DB_USER:-root}",
                    "password": "${DB_PASSWORD}",
                    "connection_timeout": 30,
                    "connection_pool_size": 10,
                    "ssl_disabled": True
                }
            elif db_type == 'sqlite':
                sample_config = {
                    "db_type": "sqlite",
                    "database": "${DB_PATH:-./data/mydb.sqlite}",
                    "timeout": 30
                }
            else:
                raise ConfigurationError(f"Unknown database type for sample: {db_type}")
            
            # Add comments if requested and format is JSON
            if include_comments and path.suffix.lower() == '.json':
                # JSON doesn't support comments, so add them as a special key
                sample_config["_comments"] = {
                    "db_type": "Database type: postgresql, mysql, or sqlite",
                    "host": "Database server hostname or IP address",
                    "port": "Database server port number",
                    "database": "Database name or file path for SQLite",
                    "user": "Database username",
                    "password": "Database password (use environment variable)",
                    "environment_variables": "Use ${VAR_NAME} or ${VAR_NAME:-default} syntax",
                    "example_env": "Set DB_PASSWORD environment variable before running"
                }
            
            # Write configuration file
            with open(path, 'w', encoding='utf-8') as f:
                if path.suffix.lower() == '.json':
                    json.dump(sample_config, f, indent=2)
                elif path.suffix.lower() in ['.yaml', '.yml']:
                    if not HAS_YAML:
                        raise ConfigurationError("YAML support not available. Install PyYAML: pip install PyYAML")
                    
                    # Add comments for YAML
                    if include_comments:
                        f.write(f"# Sample {db_type.upper()} database configuration\n")
                        f.write("# Environment variables: Use ${VAR_NAME} or ${VAR_NAME:-default}\n")
                        f.write(f"# Example: export DB_PASSWORD=your_password\n\n")
                    
                    # Remove JSON comments for YAML
                    yaml_config = {k: v for k, v in sample_config.items() if k != '_comments'}
                    yaml.dump(yaml_config, f, default_flow_style=False, indent=2)
            
            self.logger.info(f"Created sample configuration file: {file_path}")
            
        except Exception as e:
            raise ConfigurationError(f"Failed to create sample config file: {str(e)}")


# Global configuration manager instance
_config_manager = None


def get_config_manager() -> DatabaseConfigManager:
    """
    Get the global database configuration manager.
    
    Returns:
        DatabaseConfigManager: Global configuration manager instance
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = DatabaseConfigManager()
    return _config_manager