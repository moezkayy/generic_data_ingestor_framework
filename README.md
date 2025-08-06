# 📦 Generic Data Ingestor Framework

A robust, modular Python framework for ingesting, processing, and analyzing semi-structured data sources with automatic schema inference and data validation.

## 🚀 Overview

The Generic Data Ingestor Framework is a comprehensive solution for handling semi-structured data sources, particularly JSON files. It provides a complete pipeline for data ingestion, including file scanning, schema inference, data validation, and reporting. The framework is designed to be extensible, allowing for easy addition of new data formats and processing capabilities.

## Key Features

- 🔍 Automatic file discovery and processing
- 📊 Schema inference for JSON data (including nested structures)
- 🔄 JSON flattening to handle complex nested structures
- ✅ Data validation against inferred schemas
- 📝 Comprehensive logging and error handling
- 📈 Performance metrics and processing statistics
- 📋 Report generation for processed data

## 🛠️ Project Journey

### Initial Development Phase

The project began with a simple goal: create a tool to ingest and process JSON data files. The initial implementation focused on basic file handling and simple JSON parsing. However, I quickly encountered challenges with more complex data structures.

### Challenge: Nested JSON Structures

One of the first major challenges was handling nested JSON structures. Simple flat JSON files were easy to process, but real-world data often contains nested objects and arrays. This required developing a more sophisticated approach to:

1. Parse nested structures recursively
2. Infer schemas for complex hierarchical data
3. Flatten nested JSON for easier processing

The solution was to implement a recursive JSON flattening algorithm in the JSONProcessor class that could handle arbitrary levels of nesting while preserving the relationship between fields.

### Modular Architecture Evolution

As the project grew in complexity, I recognized the need for a more modular architecture. This led to the separation of concerns into distinct components:

- Core: Application orchestration and workflow management
- Handlers: File operations, error handling, and logging
- Processors: Data processing and transformation logic
- Scanners: File discovery and metadata extraction
- Utils: Reporting and utility functions

This modular approach improved code maintainability and made it easier to extend the framework with new capabilities.

### Error Handling Improvements

Robust error handling became a priority as the framework was tested with diverse data sources. The ErrorHandler class was developed to:

- Categorize errors (recoverable vs. non-recoverable)
- Provide detailed error logging
- Attempt recovery strategies (e.g., trying different file encodings)
- Generate error summaries for reporting

### Schema Processing Advancements

The schema processing capabilities evolved significantly throughout the project:

- Initial support for flat JSON schema inference
- Addition of nested schema detection and representation
- Schema merging to handle variations across multiple files
- Schema flattening for simplified data models
- Schema validation and consistency checking

## 🏗️ Architecture

The framework follows a modular architecture with clear separation of concerns:

### Core Components

- DataIngestionApplication: Main application orchestrator that coordinates the ingestion workflow
- JSONProcessor: Handles JSON file processing, flattening, and validation
- SchemaProcessor: Manages schema inference, merging, and validation
- FileHandler: Provides file system operations and metadata extraction
- ErrorHandler: Manages error logging, recovery, and reporting
- FileScanner: Discovers and categorizes data files
- ReportGenerator: Creates processing reports and summaries

### Workflow

1. Discovery: Scan directories for data files
2. Processing: Parse files and infer schemas
3. Validation: Validate data against schemas
4. Transformation: Flatten nested structures if needed
5. Output: Generate processed data and reports
6. Reporting: Summarize processing statistics and errors

## 💡 Technical Challenges & Solutions

### Challenge: Schema Inference for Diverse Data

**Problem**: JSON data can vary widely in structure, making schema inference challenging.

**Solution**: Implemented a sophisticated schema inference system that:
- Analyzes sample data to determine field types
- Handles arrays, objects, and primitive types
- Merges schemas from multiple instances to create a comprehensive schema
- Detects patterns in string fields (dates, emails, etc.)

### Challenge: Handling Large Files

**Problem**: Large JSON files could cause memory issues when loaded entirely.

**Solution**: Implemented streaming processing for large files and chunked processing where appropriate.

### Challenge: Nested JSON Flattening

**Problem**: Nested JSON structures are difficult to work with directly.

**Solution**: Created a recursive flattening algorithm that:
- Preserves the relationship between nested fields using dot notation
- Handles arrays with special indexing
- Allows configurable maximum depth
- Maintains type information during flattening

### Challenge: Error Recovery

**Problem**: Various file issues (encoding, format) could halt processing.

**Solution**: Implemented recovery strategies:
- Automatic encoding detection and fallback options
- Graceful error handling with detailed logging
- Continuation of processing despite individual file failures

## 📊 Performance Considerations

The framework includes performance monitoring and optimization:

- Processing time tracking for each file
- Records-per-second metrics
- MB-per-second throughput calculation
- Memory usage optimization for large files
- Parallel processing capabilities for multi-file workloads

## 🔜 Future Enhancements

While the framework currently focuses on JSON processing, several enhancements are planned:

- Support for CSV and Parquet file formats
- Database integration for direct loading
- Schema evolution tracking
- Incremental processing and change detection
- Web interface for monitoring and configuration
- Expanded validation rules and custom validators

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Required packages (see requirements.txt)

### Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`

### Usage Options

The framework offers two ways to interact with the data ingestion system:

#### 1. **Streamlit Web Interface (Recommended)** 🌐

Launch the interactive web application for a user-friendly experience:

```bash
# Start the Streamlit web interface
streamlit run app.py
```

The web interface provides:
- **📁 Upload Tab**: Drag-and-drop file uploads or directory selection
- **📋 Schema Tab**: Interactive schema preview and column type overrides
- **🚀 Ingest Tab**: One-click data processing with real-time progress tracking
- **📊 Results Tab**: Visual analytics and processing summaries
- **📝 Logs Tab**: Detailed processing logs and system information

**Features of the Web Interface:**
- 🗄️ **Database Configuration**: Sidebar-based setup for PostgreSQL, MySQL, and SQLite
- 🔐 **Secure Credentials**: Integration with Streamlit Secrets and environment variables
- 📊 **Schema Management**: Toggle validation, preview schemas, override column types
- 🎯 **Loading Strategies**: Choose between append, replace, or upsert operations
- 💾 **Preset Management**: Save and load configuration presets
- 📈 **Real-time Feedback**: Progress bars, status messages, and expandable log panels
- 🔄 **Batch Processing**: Upload multiple files with per-file status tracking
- 📱 **Responsive Design**: Polished interface that works on desktop and mobile

#### 2. **Command Line Interface** ⌨️

For programmatic usage and automation:

```bash
# Process a directory of JSON files
python main.py /path/to/data/directory --output /path/to/output --file-types json

# With database loading
python main.py /path/to/data --db-type postgresql --db-host localhost --db-name mydb --db-user postgres --table-name data_table
```

### Command Line Options

#### Core Arguments
- `directory`: Path to scan for data files
- `--output, -o`: Output directory (default: "output")
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--dry-run`: Show what would be processed without actual processing
- `--file-types`: File types to process (json, csv, parquet)
- `--recursive`: Scan directories recursively (default: True)

#### Database Configuration
- `--database-url`: Complete database URL (e.g., `postgresql://user:pass@host:port/db`)
- `--db-config-file`: Path to JSON/YAML configuration file
- `--db-type`: Database type (postgresql, mysql, sqlite)
- `--db-host`: Database host (default: localhost)
- `--db-port`: Database port
- `--db-name`: Database name
- `--db-user`: Database username
- `--db-password`: Database password
- `--table-name`: Target table name (default: ingested_data)
- `--load-strategy`: Data loading strategy (append, replace, upsert)
- `--batch-size`: Batch size for database operations (default: 1000)

#### Database Examples
```bash
# Using database URL
python main.py /data --database-url postgresql://user:pass@localhost:5432/mydb

# Using configuration file
python main.py /data --db-config-file config/database.yaml

# Direct configuration with environment variables
export DB_PASSWORD=mypassword
python main.py /data --db-type postgresql --db-host localhost --db-name mydb --db-user postgres
```

## 📝 Conclusion

The Generic Data Ingestor Framework represents a comprehensive solution for handling semi-structured data, particularly JSON. Through iterative development and addressing real-world challenges, it has evolved into a robust tool that can handle complex data structures, provide valuable insights through schema inference, and ensure data quality through validation.

The modular architecture ensures that the framework can continue to grow with new capabilities while maintaining a clean, maintainable codebase. Whether processing simple flat files or complex nested structures, the framework provides the tools needed for effective data ingestion and analysis.