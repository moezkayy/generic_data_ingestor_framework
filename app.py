#!/usr/bin/env python3
"""
Streamlit Web Interface for Generic Data Ingestion Framework.
Author: Moez Khan (SRN: 23097401)
FYP Project - University of Hertfordshire
"""

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
from connectors.connector_factory import get_connector_factory

# Configure Streamlit page
st.set_page_config(
    page_title="Generic Data Ingestor Framework - Moez Khan",
    page_icon="📊",
    layout="wide"
)

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
    <div style="background: linear-gradient(90deg, #4CAF50, #45a049); padding: 1rem; 
                border-radius: 10px; text-align: center; color: white; margin-bottom: 2rem;">
        <h1>📊 Generic Data Ingestion Framework</h1>
        <p>Transform Semi-Structured JSON files into structured database data</p>
        <p><em>FYP Project by Moez Khan (SRN: 23097401)</em></p>
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
    """Process uploaded JSON files"""
    with st.spinner("🔄 Processing files..."):
        try:
            processor = JSONProcessor()
            factory = get_connector_factory()
            
            # Create database connector
            connector = factory.create_sqlite_connector(db_path)
            if not connector.connect():
                st.error("❌ Failed to connect to database")
                return
            
            results = []
            all_processed_data = []
            
            for file_path in st.session_state.files:
                st.write(f"Processing {file_path.name}...")
                
                # Read and process the JSON file
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # Ensure data is in list format for processing
                if isinstance(json_data, dict):
                    json_data = [json_data]
                elif not isinstance(json_data, list):
                    json_data = [json_data]
                
                # Process the JSON data
                result = processor.process_data(json_data)
                all_processed_data.extend(result)
                
                results.append({
                    'file': file_path.name,
                    'status': 'success',
                    'records': len(result),
                    'data': result
                })
            
            # Create table schema from processed data
            if all_processed_data:
                st.write("Creating database table...")
                sample_record = all_processed_data[0]
                schema = []
                for key in sample_record.keys():
                    schema.append({
                        'name': key,
                        'type': 'TEXT',
                        'nullable': True
                    })
                
                # Create table
                if connector.create_table(table_name, schema):
                    st.write("Inserting data into database...")
                    # Insert all processed data
                    inserted_count = connector.insert_data(table_name, all_processed_data)
                    st.success(f"✅ Inserted {inserted_count} records into database!")
                else:
                    st.error("❌ Failed to create database table")
            
            connector.disconnect()
            
            # Save results to session state
            st.session_state.results = {
                'files_processed': len(results),
                'total_records': sum(r['records'] for r in results),
                'table_name': table_name,
                'db_path': db_path,
                'data': results
            }
            st.session_state.processed = True
            
            st.success(f"✅ Successfully processed {len(results)} files and saved to database!")
            
        except Exception as e:
            st.error(f"❌ Error processing files: {str(e)}")

def render_results():
    """Results display section"""
    st.header("3️⃣ Processing Results")
    
    if not st.session_state.processed:
        st.info("👆 Process your files first to see results")
        return
    
    results = st.session_state.results
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Files Processed", results['files_processed'])
    with col2:
        st.metric("Total Records", results['total_records'])
    with col3:
        st.metric("Table Name", results['table_name'])
    with col4:
        st.metric("Database", results['db_path'])
    
    # Detailed results
    st.subheader("📋 File Processing Details")
    
    for file_result in results['data']:
        with st.expander(f"📄 {file_result['file']}", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Status:** {file_result['status']}")
                st.write(f"**Records:** {file_result['records']}")
            
            with col2:
                if st.button(f"View Data", key=f"view_{file_result['file']}"):
                    st.json(file_result['data'])
    
    # Download options
    st.subheader("💾 Export Options")
    
    if st.button("📊 View Database Schema", use_container_width=True):
        try:
            conn = sqlite3.connect(results['db_path'])
            schema_query = f"PRAGMA table_info({results['table_name']})"
            schema_df = pd.read_sql_query(schema_query, conn)
            st.dataframe(schema_df, use_container_width=True)
            conn.close()
        except Exception as e:
            st.error(f"Error viewing schema: {str(e)}")
    
    if st.button("🔍 Preview Database Data", use_container_width=True):
        try:
            conn = sqlite3.connect(results['db_path'])
            preview_query = f"SELECT * FROM {results['table_name']} LIMIT 100"
            preview_df = pd.read_sql_query(preview_query, conn)
            st.dataframe(preview_df, use_container_width=True)
            conn.close()
        except Exception as e:
            st.error(f"Error previewing data: {str(e)}")

if __name__ == "__main__":
    main()
