"""
Database Configuration Schema for the Generic Data Ingestor Framework.

This module defines the schema and validation rules for database connection
configurations used by database connectors.
"""

from typing import Dict, Any, List, Optional, Union
import re
import json
from pathlib import Path


# Common database configuration schema
DB_CONFIG_SCHEMA = {
    "type": "object",
    "required": ["db_type", "host"],
    "properties": {
        "db_type": {
            "type": "string",
            "enum": ["mysql", "postgresql", "sqlite", "oracle", "sqlserver", "mongodb"],
            "description": "Type of database system"
        },
        "host": {
            "type": "string",
            "description": "Database server hostname or IP address"
        },
        "port": {
            "type": "integer",
            "description": "Database server port"
        },
        "database": {
            "type": "string",
            "description": "Database name"
        },
        "username": {
            "type": "string",
            "description": "Database username"
        },
        "password": {
            "type": "string",
            "description": "Database password"
        },
        "connection_timeout": {
            "type": "integer",
            "minimum": 1,
            "default": 30,
            "description": "Connection timeout in seconds"
        },
        "connection_pool_size": {
            "type": "integer",
            "minimum": 1,
            "default": 5,
            "description": "Maximum number of connections in the pool"
        },
        "ssl_enabled": {
            "type": "boolean",
            "default": False,
            "description": "Whether to use SSL/TLS for the connection"
        },
        "ssl_ca_cert": {
            "type": "string",
            "description": "Path to CA certificate file for SSL connections"
        },
        "ssl_client_cert": {
            "type": "string",
            "description": "Path to client certificate file for SSL connections"
        },
        "ssl_client_key": {
            "type": "string",
            "description": "Path to client key file for SSL connections"
        },
        "additional_options": {
            "type": "object",
            "description": "Additional database-specific connection options"
        }
    },
    "additionalProperties": False
}


# Database-specific configuration schemas
DB_TYPE_SPECIFIC_SCHEMAS = {
    "mysql": {
        "required": ["host", "database", "username"],
        "properties": {
            "port": {"default": 3306}
        }
    },
    "postgresql": {
        "required": ["host", "database", "username"],
        "properties": {
            "port": {"default": 5432},
            "schema": {"type": "string"}
        }
    },
    "sqlite": {
        "required": ["database"],
        "properties": {
            "host": {"default": "localhost"},
            "database": {"description": "Path to SQLite database file"}
        }
    },
    "mongodb": {
        "required": ["host", "database"],
        "properties": {
            "port": {"default": 27017},
            "connection_string": {"type": "string"}
        }
    }
}


class DatabaseConfigValidator:
    """
    Validator for database configuration parameters.
    
    This class provides methods to validate database configuration parameters
    against the defined schema and specific database type requirements.
    """
    
    @staticmethod
    def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate database configuration against the schema.
        
        Args:
            config: Database configuration dictionary
            
        Returns:
            Validated and normalized configuration dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        if not isinstance(config, dict):
            raise ValueError("Database configuration must be a dictionary")
        
        # Check required fields from base schema
        for field in DB_CONFIG_SCHEMA["required"]:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate db_type
        db_type = config.get("db_type")
        if db_type not in DB_CONFIG_SCHEMA["properties"]["db_type"]["enum"]:
            valid_types = ", ".join(DB_CONFIG_SCHEMA["properties"]["db_type"]["enum"])
            raise ValueError(f"Invalid database type: {db_type}. Valid types are: {valid_types}")
        
        # Apply database-specific validation
        if db_type in DB_TYPE_SPECIFIC_SCHEMAS:
            specific_schema = DB_TYPE_SPECIFIC_SCHEMAS[db_type]
            
            # Check database-specific required fields
            for field in specific_schema.get("required", []):
                if field not in config:
                    raise ValueError(f"Missing required field for {db_type}: {field}")
            
            # Apply default values from specific schema
            for field, field_schema in specific_schema.get("properties", {}).items():
                if "default" in field_schema and field not in config:
                    config[field] = field_schema["default"]
        
        # Validate port is a positive integer
        if "port" in config:
            port = config["port"]
            if not isinstance(port, int) or port <= 0:
                raise ValueError("Port must be a positive integer")
        
        # Validate connection timeout
        if "connection_timeout" in config:
            timeout = config["connection_timeout"]
            if not isinstance(timeout, int) or timeout <= 0:
                raise ValueError("Connection timeout must be a positive integer")
        
        # Validate SSL configuration
        if config.get("ssl_enabled", False):
            if "ssl_ca_cert" in config and not Path(config["ssl_ca_cert"]).exists():
                raise ValueError(f"SSL CA certificate file not found: {config['ssl_ca_cert']}")
            
            if "ssl_client_cert" in config and not Path(config["ssl_client_cert"]).exists():
                raise ValueError(f"SSL client certificate file not found: {config['ssl_client_cert']}")
            
            if "ssl_client_key" in config and not Path(config["ssl_client_key"]).exists():
                raise ValueError(f"SSL client key file not found: {config['ssl_client_key']}")
        
        return config
    
    @staticmethod
    def load_config_from_file(file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load database configuration from a JSON file.
        
        Args:
            file_path: Path to the configuration file
            
        Returns:
            Validated database configuration dictionary
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file contains invalid JSON or configuration
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")
        
        try:
            with open(path, 'r') as f:
                config = json.load(f)
            
            return DatabaseConfigValidator.validate_config(config)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    @staticmethod
    def get_default_config(db_type: str) -> Dict[str, Any]:
        """
        Get default configuration for a specific database type.
        
        Args:
            db_type: Database type
            
        Returns:
            Default configuration dictionary
            
        Raises:
            ValueError: If the database type is invalid
        """
        if db_type not in DB_CONFIG_SCHEMA["properties"]["db_type"]["enum"]:
            valid_types = ", ".join(DB_CONFIG_SCHEMA["properties"]["db_type"]["enum"])
            raise ValueError(f"Invalid database type: {db_type}. Valid types are: {valid_types}")
        
        # Start with base defaults
        config = {
            "db_type": db_type,
            "host": "localhost",
            "connection_timeout": 30,
            "connection_pool_size": 5,
            "ssl_enabled": False
        }
        
        # Add database-specific defaults
        if db_type in DB_TYPE_SPECIFIC_SCHEMAS:
            specific_schema = DB_TYPE_SPECIFIC_SCHEMAS[db_type]
            for field, field_schema in specific_schema.get("properties", {}).items():
                if "default" in field_schema:
                    config[field] = field_schema["default"]
        
        return config