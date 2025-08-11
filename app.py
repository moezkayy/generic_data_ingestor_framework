#!/usr/bin/env python3

import streamlit as st
import sys
import os
import json
import tempfile
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from processors.json_processor import JSONProcessor

# Configure Streamlit page
st.set_page_config(
    page_title="Generic Data Ingestor Framework - Moez Khan",
    page_icon="📊",
    layout="wide"
)

# Simple CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #4CAF50, #45a049);
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        color: white;
        margin-bottom: 2rem;
    }
    .success-box {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    if 'files' not in st.session_state:
        st.session_state.files = []
    if 'results' not in st.session_state:
        st.session_state.results = {}
    if 'processed' not in st.session_state:
        st.session_state.processed = False

def main():
    """Main application"""
    initialize_session_state()
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>📊 Generic Data Ingestion Framework</h1>
        <p> Transform Semi-Structured files into structured database data</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Simple workflow in sections
    render_file_upload()
    render_processing()
    render_results()

def render_file_upload():
    """File upload section"""
    st.header("1️⃣ Upload JSON Files")
    
    # File uploader
    uploaded_files = st.file_uploader(
        "Choose JSON files to process",
        type=['json'],
        accept_multiple_files=True,
        help="Select one or more JSON files containing your data"
    )
    
    if uploaded_files:
        # Save uploaded files temporarily
        temp_dir = Path(tempfile.mkdtemp())
        st.session_state.files = []
        
        for uploaded_file in uploaded_files:
            file_path = temp_dir / uploaded_file.name
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            st.session_state.files.append(file_path)
        
        st.success(f"✅ Uploaded {len(uploaded_files)} files successfully!")
        
        # Show file preview
        with st.expander("📋 File Details", expanded=True):
            for file_path in st.session_state.files:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**{file_path.name}**")
                with col2:
                    size_mb = file_path.stat().st_size / 1024 / 1024
                    st.write(f"Size: {size_mb:.2f} MB")
                with col3:
                    st.write("Status: Ready")

def render_processing():
    """Processing section"""
    st.header("2️⃣ Process Data")
    
    if not st.session_state.files:
        st.info("👆 Please upload JSON files first")
        return
    
    # Simple settings
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Processing Settings")
        validate_data = st.checkbox("Enable data validation", value=True)
        table_name = st.text_input("SQLite table name", value="processed_data")
        
    with col2:
        st.subheader("🗄️ Database Output")
        db_path = st.text_input("SQLite database file", value="output.db")
        st.info("💡 Database will be created automatically if it doesn't exist")
    
    # Process button
    if st.button("🚀 Process Files", type="primary", use_container_width=True):
        if table_name and db_path:
            process_files(validate_data, table_name, db_path)
        else:
            st.error("❌ Please fill in all required fields")

def process_files(validate_data: bool, table_name: str, db_path: str):
    """Process the uploaded files"""
    st.subheader("⚡ Processing...")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    processor = JSONProcessor()
    total_records = 0
    processed_files = []
    errors = []
    
    try:
        # Initialize SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for i, file_path in enumerate(st.session_state.files):
            status_text.text(f"Processing {file_path.name}...")
            
            try:
                # Read and process JSON
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convert to list if single object
                if isinstance(data, dict):
                    data = [data]
                
                # Process with JSON processor
                processed_data = processor.process_data(data)
                
                if processed_data:
                    # Convert to DataFrame and save to SQLite
                    df = pd.DataFrame(processed_data)
                    df.to_sql(table_name, conn, if_exists='append', index=False)
                    
                    total_records += len(processed_data)
                    processed_files.append({
                        'file': file_path.name,
                        'records': len(processed_data),
                        'status': 'Success'
                    })
                else:
                    processed_files.append({
                        'file': file_path.name,
                        'records': 0,
                        'status': 'No data found'
                    })
                
            except Exception as e:
                error_msg = f"Error processing {file_path.name}: {str(e)}"
                errors.append(error_msg)
                processed_files.append({
                    'file': file_path.name,
                    'records': 0,
                    'status': f'Error: {str(e)}'
                })
            
            # Update progress
            progress_bar.progress((i + 1) / len(st.session_state.files))
        
        conn.close()
        
        # Store results
        st.session_state.results = {
            'total_files': len(st.session_state.files),
            'total_records': total_records,
            'processed_files': processed_files,
            'errors': errors,
            'db_path': db_path,
            'table_name': table_name,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        st.session_state.processed = True
        
        # Show immediate results
        if errors:
            st.warning(f"⚠️ Processed with {len(errors)} errors")
        else:
            st.success(f"✅ Successfully processed all files!")
        
        status_text.text("Processing complete!")
        
    except Exception as e:
        st.error(f"❌ Processing failed: {str(e)}")

def render_results():
    """Results section"""
    st.header("3️⃣ Results")
    
    if not st.session_state.processed:
        st.info("🔄 Process your files to see results here")
        return
    
    results = st.session_state.results
    
    # Summary metrics
    st.subheader("📊 Processing Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Files Processed", results['total_files'])
    with col2:
        st.metric("Total Records", results['total_records'])
    with col3:
        st.metric("Errors", len(results['errors']))
    with col4:
        success_rate = ((results['total_files'] - len(results['errors'])) / results['total_files'] * 100)
        st.metric("Success Rate", f"{success_rate:.1f}%")
    
    # File details
    st.subheader("📋 File Processing Details")
    files_df = pd.DataFrame(results['processed_files'])
    st.dataframe(files_df, use_container_width=True)
    
    # Database info
    st.subheader("🗄️ Database Information")
    col1, col2 = st.columns(2)
    with col1:
        st.write(f"**Database:** `{results['db_path']}`")
        st.write(f"**Table:** `{results['table_name']}`")
    with col2:
        st.write(f"**Records Stored:** {results['total_records']}")
        st.write(f"**Processed At:** {results['timestamp']}")
    
    # View data button
    if st.button("👁️ Preview Database Data"):
        show_database_preview(results['db_path'], results['table_name'])
    
    # Errors section
    if results['errors']:
        st.subheader("❌ Processing Errors")
        for error in results['errors']:
            st.error(error)
    
    # Reset button
    if st.button("🔄 Process New Files", type="secondary"):
        st.session_state.files = []
        st.session_state.results = {}
        st.session_state.processed = False
        st.rerun()

def show_database_preview(db_path: str, table_name: str):
    """Show preview of database data"""
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name} LIMIT 100"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        st.subheader("🔍 Database Preview (First 100 records)")
        st.dataframe(df, use_container_width=True)
        
        # Download option
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv_data,
            file_name=f"{table_name}_export.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"❌ Error reading database: {str(e)}")

if __name__ == "__main__":
    main()