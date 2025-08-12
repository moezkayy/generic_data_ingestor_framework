"""
CLI for Generic Data Ingestion Framework.
"""

import sys
import argparse
from pathlib import Path
import os

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.application import DataIngestionApplication


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Generic Data Ingestion Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data/                        # Process JSON files in data/ directory
  %(prog)s data/ --output mydata.db     # Save to custom database file
  %(prog)s data/ --table customers      # Use custom table name
        """
    )
    
    parser.add_argument(
        'directory',
        help='Directory containing JSON files to process'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='output.db',
        help='Output SQLite database file (default: output.db)'
    )
    
    parser.add_argument(
        '--table', '-t',
        default='processed_data',
        help='Table name for storing data (default: processed_data)'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress informational messages'
    )
    
    args = parser.parse_args()
    
    # Validate directory
    if not Path(args.directory).exists():
        print(f"Error: Directory '{args.directory}' does not exist")
        return 1
    
    if not Path(args.directory).is_dir():
        print(f"Error: '{args.directory}' is not a directory")
        return 1
    
    try:
        # Initialize application
        app = DataIngestionApplication()
        
        if not args.quiet:
            print("=== Generic Data Ingestion Framework ===")
            print(f"Processing directory: {args.directory}")
            print(f"Output database: {args.output}")
            print(f"Table name: {args.table}")
            print()
        
        # Process the directory
        result = app.process_directory(
            directory=args.directory,
            output_db=args.output,
            table_name=args.table
        )
        
        if result['success']:
            if not args.quiet:
                print("\n=== Processing completed successfully! ===")
                print("Summary:")
                print(f"  Files processed: {result['processed_files']}/{result['total_files']}")
                print(f"  Records saved: {result['database_records']}")
                print(f"  Processing time: {result['processing_time_seconds']}s")
                print(f"  Database: {result['database_path']}")
                print(f"  Table: {result['table_name']}")
                
                if result.get('errors'):
                    print(f"  Errors: {len(result['errors'])}")
                    for error in result['errors']:
                        print(f"    - {error}")
            
            return 0
        else:
            print(f"\nProcessing failed: {result.get('message', 'Unknown error')}")
            if result.get('errors'):
                print("Errors:")
                for error in result['errors']:
                    print(f"  - {error}")
            return 1
            
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        return 1
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
