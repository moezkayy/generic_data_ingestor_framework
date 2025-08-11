# 📊 Generic Data Ingestion Framework - FYP Version

A simplified Python framework for ingesting and processing JSON data files with automatic schema inference and SQLite database storage. Designed for academic demonstration and final year project requirements.

## 🎯 Project Overview

This is an educational implementation of a data ingestion framework that demonstrates core concepts of data processing, schema inference, and database integration. It focuses on simplicity and clear demonstration of fundamental principles.

### Key Features (Simplified)

- 📁 JSON file processing and validation
- 🔍 Automatic schema inference from data
- 💾 SQLite database storage
- 🌐 Simple web interface using Streamlit
- ⌨️ Command-line interface for automation
- 📊 Basic error handling and logging

### Academic Focus Areas

This project demonstrates understanding of:
- File I/O operations and data processing
- JSON parsing and validation
- Database design and integration
- Schema inference algorithms  
- Web interface development
- Error handling strategies
- Modular software architecture

## 🏗️ Simplified Architecture

```
src/
├── core/
│   └── application.py          # Main application logic
├── connectors/
│   ├── database_connector.py   # Base database interface
│   ├── sqlite_connector.py     # SQLite implementation
│   └── connector_factory.py    # Simple factory pattern
├── processors/
│   ├── json_processor.py       # JSON data processing
│   └── schema_processor.py     # Schema inference
├── scanners/
│   └── file_scanner.py         # File discovery
└── handlers/
    ├── file_handler.py         # File operations
    ├── error_handler.py        # Error management
    └── logging_handler.py      # Logging utilities
```

### Design Patterns Demonstrated

- **Factory Pattern**: Database connector creation
- **Strategy Pattern**: Data processing approaches
- **Modular Architecture**: Clear separation of concerns
- **Error Handling**: Graceful failure management

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Basic understanding of JSON data format
- SQLite (included with Python)

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd generic_data_ingestor_framework
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Usage Options

#### Option 1: Web Interface (Recommended)

Launch the simplified Streamlit interface:

```bash
streamlit run app.py
```

**Web Interface Features:**
- 📤 File upload with drag-and-drop
- 🔍 Automatic data analysis 
- 💾 SQLite database storage
- 📊 Results preview and download
- ✅ Processing status indicators

#### Option 2: Command Line

Use the simple CLI for batch processing:

```bash
# Basic usage
python main.py data/

# Specify output database and table
python main.py data/ --output mydata.db --table customers

# Quiet mode (minimal output)
python main.py data/ --quiet
```

**CLI Options:**
- `directory`: Path to folder containing JSON files
- `--output, -o`: SQLite database file name (default: output.db)
- `--table, -t`: Table name (default: processed_data)
- `--quiet, -q`: Suppress informational messages

## 📋 Example Workflow

### 1. Prepare Sample Data

Create some JSON files in a `data/` directory:

**data/customers.json**
```json
[
  {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
  {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25}
]
```

**data/products.json**
```json
[
  {"product_id": "P001", "name": "Laptop", "price": 999.99, "in_stock": true},
  {"product_id": "P002", "name": "Mouse", "price": 29.99, "in_stock": false}
]
```

### 2. Process with Web Interface

1. Run `streamlit run app.py`
2. Upload your JSON files
3. Click "Process Files"
4. View results and download processed data

### 3. Process with CLI

```bash
python main.py data/ --output sales_data.db --table all_data
```

### 4. Examine Results

The processed data will be stored in SQLite with:
- Inferred column types
- Source file metadata
- Processing timestamps

## 🎓 Academic Learning Outcomes

This project demonstrates proficiency in:

### Technical Skills
- **Python Programming**: Object-oriented design, error handling, file I/O
- **Data Processing**: JSON parsing, validation, transformation
- **Database Integration**: Schema design, SQL operations, data loading
- **Web Development**: Streamlit interface creation
- **Software Architecture**: Modular design, design patterns

### Analytical Skills
- **Schema Inference**: Automatic detection of data types and structures
- **Error Handling**: Graceful handling of malformed data
- **Performance Considerations**: Efficient processing of multiple files

### Project Management
- **Documentation**: Clear code documentation and user guides
- **Testing**: Basic validation and error scenarios
- **Version Control**: Structured codebase organization

## 📊 Sample Output

After processing, you'll see results like:

```
🚀 Generic Data Ingestion Framework - FYP Version
📁 Processing directory: data/
🗄️  Output database: output.db
📊 Table name: processed_data

Found 2 JSON files to process
Processing: customers.json
  ✓ Processed 2 records
Processing: products.json  
  ✓ Processed 2 records
Saving 4 records to database: output.db
Creating table: processed_data

✅ Processing completed successfully!
📈 Summary:
  • Files processed: 2/2
  • Records saved: 4
  • Processing time: 0.15s
  • Database: output.db
  • Table: processed_data
```

## 🔮 Future Enhancements

The current simplified version provides a solid foundation for potential improvements:

### Phase 1: Enhanced Features
- Support for CSV and XML file formats
- Advanced data validation rules
- Improved error reporting and recovery
- Configuration file support

### Phase 2: Production Features  
- Multiple database support (PostgreSQL, MySQL)
- Connection pooling and optimization
- Real-time processing capabilities
- Advanced web interface with authentication

### Phase 3: Enterprise Scale
- Distributed processing support
- Cloud deployment options
- API development for external integration
- Machine learning-based data quality assessment

*See [FUTURE_ADVANCEMENTS.md](FUTURE_ADVANCEMENTS.md) for detailed roadmap.*

## 🧪 Testing

Basic testing can be performed with:

```bash
# Test with sample data
python main.py test_data/

# Verify database contents
sqlite3 output.db "SELECT * FROM processed_data LIMIT 5;"
```

## 📚 Project Structure

```
generic_data_ingestor_framework/
├── README.md                        # This file
├── FUTURE_ADVANCEMENTS.md           # Detailed enhancement roadmap
├── requirements.txt                 # Python dependencies
├── app.py                           # Streamlit web interface  
├── main.py                          # Command-line interface
├── src/                            # Source code
│   ├── core/application.py         # Main application (186 lines)
│   ├── connectors/                 # Database layer (4 files)
│   ├── processors/                 # Data processing (2 files)
│   ├── scanners/                   # File discovery (1 file)
│   └── handlers/                   # Utilities (3 files)
└── test_data/                      # Sample data for testing
```

## ✅ Requirements Fulfilled

This implementation satisfies the FYP requirements:

- ✅ **FR1**: JSON file ingestion and processing
- ✅ **FR2**: Data validation and format checking  
- ✅ **FR3**: Recursive directory traversal
- ✅ **FR4**: Filtering and error handling
- ✅ **FR5**: Metadata extraction and logging
- ✅ **FR6**: Configurable processing rules
- ✅ **FR7**: Consistent output generation
- ✅ **FR8**: Extensible architecture for new formats

- ✅ **NFR1**: Modular design with clear separation
- ✅ **NFR2**: Extensible system architecture  
- ✅ **NFR3**: Efficient processing performance
- ✅ **NFR4**: Comprehensive error logging
- ✅ **NFR5**: Robust error handling
- ✅ **NFR6**: Clear code documentation

## 🎉 Conclusion

This simplified Generic Data Ingestion Framework demonstrates core data processing concepts while maintaining clean, understandable code suitable for academic evaluation. The architecture provides a solid foundation for understanding enterprise data processing systems while remaining accessible for educational purposes.

The project successfully balances academic requirements with real-world applicability, providing both immediate functionality and a clear path for future enhancement into production-ready systems.

---

*This is the simplified FYP version. For the full-featured production roadmap, see [FUTURE_ADVANCEMENTS.md](FUTURE_ADVANCEMENTS.md).*