# main.py
import sys
import argparse
from pathlib import Path

# main.py
import sys
import argparse
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.application import DataIngestionApplication



def main():
    parser = argparse.ArgumentParser(
        description='Generic Data Ingestion Framework for Semi-Structured Data'
    )

    parser.add_argument('directory', help='Project directory to scan for data files')
    parser.add_argument('--output', '-o', default='output', help='Output directory')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed')
    parser.add_argument('--file-types', nargs='+', default=['json'], choices=['json', 'csv', 'parquet'])
    parser.add_argument('--recursive', action='store_true', default=True)

    args = parser.parse_args()

    app = DataIngestionApplication()

    try:
        return app.run(
            directory=args.directory,
            output_dir=args.output,
            log_level=args.log_level,
            dry_run=args.dry_run,
            file_types=args.file_types,
            recursive=args.recursive
        )
    except Exception as e:
        print(f"❌ Application error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())