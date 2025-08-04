# main.py
import sys
import argparse
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.application import DataIngestionApplication
from utils.config_manager import get_config_manager, ConfigurationError



def main():
    parser = argparse.ArgumentParser(
        description='Generic Data Ingestion Framework for Semi-Structured Data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Database Configuration Examples:
  Using database URL:
    --database-url postgresql://user:pass@localhost:5432/mydb
    --database-url mysql://user:pass@localhost:3306/mydb
    --database-url sqlite:///path/to/database.sqlite

  Using configuration file:
    --db-config-file config/database.json
    --db-config-file config/database.yaml

  Direct configuration:
    --db-type postgresql --db-host localhost --db-port 5432 --db-name mydb --db-user postgres

  Environment Variables:
    Export DB_PASSWORD, DB_HOST, DB_USER, etc. and reference in config files as ${DB_PASSWORD}
        """
    )

    # Core arguments
    parser.add_argument('directory', help='Project directory to scan for data files')
    parser.add_argument('--output', '-o', default='output', help='Output directory')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed')
    parser.add_argument('--file-types', nargs='+', default=['json'], choices=['json', 'csv', 'parquet'])
    parser.add_argument('--recursive', action='store_true', default=True)
    
    # Database configuration arguments
    db_group = parser.add_argument_group('Database Configuration', 'Options for configuring database connections')
    
    # Configuration sources (mutually exclusive)
    config_group = db_group.add_mutually_exclusive_group()
    config_group.add_argument('--database-url', '--db-url', 
                             help='Database connection URL (e.g., postgresql://user:pass@host:port/db)')
    config_group.add_argument('--db-config-file', '--config-file',
                             help='Path to JSON/YAML database configuration file')
    
    # Direct database configuration
    db_group.add_argument('--db-type', choices=['postgresql', 'mysql', 'sqlite'],
                         help='Database type (required if not using URL or config file)')
    db_group.add_argument('--db-host', '--host', default='localhost',
                         help='Database host (default: localhost)')
    db_group.add_argument('--db-port', '--port', type=int,
                         help='Database port (defaults: PostgreSQL=5432, MySQL=3306)')
    db_group.add_argument('--db-name', '--database', '--db',
                         help='Database name or SQLite file path')
    db_group.add_argument('--db-user', '--user', '--username',
                         help='Database username')
    db_group.add_argument('--db-password', '--password',
                         help='Database password (use environment variables for security)')
    
    # Database operation options
    db_group.add_argument('--table-name', default='ingested_data',
                         help='Target table name for database loading (default: ingested_data)')
    db_group.add_argument('--load-strategy', choices=['append', 'replace', 'upsert'], default='append',
                         help='Data loading strategy (default: append)')
    db_group.add_argument('--batch-size', type=int, default=1000,
                         help='Batch size for database operations (default: 1000)')
    db_group.add_argument('--create-table', action='store_true', default=True,
                         help='Create table if it does not exist (default: enabled)')
    db_group.add_argument('--no-create-table', dest='create_table', action='store_false',
                         help='Do not create table automatically')
    
    # Security and connection options
    db_group.add_argument('--prompt-password', action='store_true',
                         help='Prompt for database password securely')
    db_group.add_argument('--connection-timeout', type=int, default=30,
                         help='Database connection timeout in seconds (default: 30)')
    db_group.add_argument('--pool-size', type=int, default=10,
                         help='Connection pool size (default: 10)')
    
    # Configuration management
    db_group.add_argument('--create-sample-config', metavar='FILE',
                         help='Create a sample database configuration file and exit')
    db_group.add_argument('--validate-config', action='store_true',
                         help='Validate database configuration and exit')

    args = parser.parse_args()

    # Handle special configuration operations
    if args.create_sample_config:
        return create_sample_config(args.create_sample_config, args.db_type or 'postgresql')
    
    if args.validate_config:
        return validate_database_config(args)
    
    # Build database configuration if any database options are provided
    db_config = None
    if any([args.database_url, args.db_config_file, args.db_type]):
        try:
            db_config = build_database_config(args)
        except Exception as e:
            print(f"❌ Database configuration error: {e}")
            return 1
    
    app = DataIngestionApplication()

    try:
        return app.run(
            directory=args.directory,
            output_dir=args.output,
            log_level=args.log_level,
            dry_run=args.dry_run,
            file_types=args.file_types,
            recursive=args.recursive,
            db_config=db_config,
            table_name=args.table_name if db_config else None,
            load_strategy=args.load_strategy if db_config else None,
            batch_size=args.batch_size if db_config else None,
            create_table=args.create_table if db_config else None
        )
    except Exception as e:
        print(f"❌ Application error: {e}")
        return 1


def build_database_config(args) -> dict:
    """
    Build database configuration from command line arguments.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        dict: Database configuration dictionary
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    config_manager = get_config_manager()
    
    try:
        # Build configuration based on source
        if args.database_url:
            # Load from database URL
            db_config = config_manager.load_database_config(
                database_url=args.database_url,
                prompt_for_missing=args.prompt_password
            )
        
        elif args.db_config_file:
            # Load from configuration file
            db_config = config_manager.load_database_config(
                config_source=args.db_config_file,
                prompt_for_missing=args.prompt_password
            )
        
        else:
            # Build from individual arguments
            if not args.db_type:
                raise ConfigurationError("Database type (--db-type) is required when not using URL or config file")
            
            direct_config = {'db_type': args.db_type}
            
            # Add connection parameters
            if args.db_type != 'sqlite':
                direct_config['host'] = args.db_host
                
                # Set default ports if not specified
                if args.db_port:
                    direct_config['port'] = args.db_port
                elif args.db_type == 'postgresql':
                    direct_config['port'] = 5432
                elif args.db_type == 'mysql':
                    direct_config['port'] = 3306
                
                if args.db_user:
                    direct_config['user'] = args.db_user
                if args.db_password:
                    direct_config['password'] = args.db_password
            
            if args.db_name:
                direct_config['database'] = args.db_name
            elif args.db_type != 'sqlite':
                raise ConfigurationError(f"Database name (--db-name) is required for {args.db_type}")
            
            # Add connection options
            if args.connection_timeout:
                direct_config['connection_timeout'] = args.connection_timeout
            if args.pool_size:
                direct_config['connection_pool_size'] = args.pool_size
            
            db_config = config_manager.load_database_config(
                config_dict=direct_config,
                prompt_for_missing=args.prompt_password
            )
        
        print(f"✅ Database configuration loaded successfully")
        print(f"📊 Database: {db_config.get('db_type', 'unknown')} - {db_config.get('database', 'N/A')}")
        
        # Show environment variables used
        env_vars = config_manager.get_environment_variables_used()
        if env_vars:
            print(f"🔧 Environment variables used: {', '.join(env_vars)}")
        
        return db_config
        
    except Exception as e:
        raise ConfigurationError(f"Failed to build database configuration: {str(e)}")


def create_sample_config(file_path: str, db_type: str) -> int:
    """
    Create a sample configuration file.
    
    Args:
        file_path: Path where to create the sample file
        db_type: Database type for the sample
        
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    try:
        config_manager = get_config_manager()
        config_manager.create_sample_config_file(file_path, db_type, include_comments=True)
        
        print(f"✅ Sample configuration file created: {file_path}")
        print(f"📝 Database type: {db_type}")
        print(f"💡 Edit the file and set environment variables before use")
        
        return 0
        
    except Exception as e:
        print(f"❌ Failed to create sample configuration: {e}")
        return 1


def validate_database_config(args) -> int:
    """
    Validate database configuration.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    try:
        print("🔍 Validating database configuration...")
        
        # Build configuration (this will validate it)
        db_config = build_database_config(args)
        
        print("✅ Database configuration is valid")
        print(f"📊 Configuration summary:")
        print(f"   Database Type: {db_config.get('db_type')}")
        print(f"   Database: {db_config.get('database')}")
        
        if db_config.get('db_type') != 'sqlite':
            print(f"   Host: {db_config.get('host')}:{db_config.get('port')}")
            print(f"   User: {db_config.get('user')}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())