#!/usr/bin/env python3
# setup_ui.py - Installs the Streamlit UI for the Data Scraper project

import os
import shutil
import subprocess
import sys

def create_directory(path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")
    return path

def main():
    # Get the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create UI directory
    ui_dir = create_directory(os.path.join(script_dir, "ui"))
    
    # Create app.py in the UI directory
    app_path = os.path.join(ui_dir, "app.py")
    with open(app_path, "w") as f:
        f.write("""# app.py
import streamlit as st
import pandas as pd
import os
import sys
import io
import base64
from datetime import datetime

# Add the project root to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from the existing project structure
from src.connectors.web.html_scraper import scrape_website
from src.connectors.document.pdf_reader import extract_text
from src.connectors.document.excel_reader import process_csv
from src.processors.extractors.email_extractor import extract_emails

# Integration with existing modules
def scrape_html(url, selectors=None):
    \"\"\"Wrapper for your existing HTML scraper functionality\"\"\"
    try:
        # Call your actual scrape_website function
        return scrape_website(url, selectors)
    except Exception as e:
        st.error(f"Error in HTML scraper: {str(e)}")
        # Return empty DataFrame as fallback
        return pd.DataFrame()

def extract_pdf(uploaded_file):
    \"\"\"Wrapper for your existing PDF extractor\"\"\"
    try:
        # Convert Streamlit's UploadedFile to a format your function can use
        pdf_bytes = uploaded_file.read()
        # Call your actual extract_text function
        text = extract_text(io.BytesIO(pdf_bytes))
        # Use your email extractor on the PDF text
        emails = extract_emails(text)
        return text, emails
    except Exception as e:
        st.error(f"Error in PDF extraction: {str(e)}")
        return "", []

def clean_csv(uploaded_file):
    \"\"\"Wrapper for your existing CSV processor\"\"\"
    try:
        # Your process_csv function likely expects a DataFrame or file path
        df = pd.read_csv(uploaded_file)
        return process_csv(df)
    except Exception as e:
        st.error(f"Error in CSV processing: {str(e)}")
        # Return the original DataFrame as fallback
        return pd.read_csv(uploaded_file)

def get_download_link(df, filename, text):
    \"\"\"Generate a download link for a dataframe\"\"\"
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download {text}</a>'
    return href

def main():
    st.set_page_config(
        page_title="Universal Data Scraper",
        page_icon="📊",
        layout="wide",
    )
    
    st.title("Universal Data Scraper")
    st.sidebar.title("Options")
    
    # Create a tabbed interface for different types of scraping
    tab1, tab2, tab3, tab4 = st.tabs(["Web Scraping", "PDF Processing", "CSV Cleaning", "Text Analysis"])
    
    # Tab 1: Web Scraping
    with tab1:
        st.header("Web Scraping")
        url = st.text_input("Enter the URL to scrape:")
        
        col1, col2 = st.columns(2)
        with col1:
            scrape_button = st.button("Scrape Website")
        
        advanced_options = st.expander("Advanced Options")
        with advanced_options:
            css_selector = st.text_input("CSS Selector (optional):", "")
            st.text("Leave blank to use default selectors")
        
        if scrape_button and url:
            with st.spinner("Scraping website..."):
                try:
                    df = scrape_html(url, css_selector if css_selector else None)
                    st.success("Scraping completed!")
                    st.dataframe(df)
                    
                    # Create download link
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"scraped_data_{timestamp}.csv"
                    st.markdown(get_download_link(df, filename, "CSV"), unsafe_allow_html=True)
                    
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")
    
    # Tab 2: PDF Processing
    with tab2:
        st.header("PDF Document Processing")
        uploaded_pdf = st.file_uploader("Upload PDF document", type=["pdf"])
        
        if uploaded_pdf is not None:
            with st.spinner("Processing PDF..."):
                try:
                    text, emails = extract_pdf(uploaded_pdf)
                    
                    st.subheader("Extracted Text")
                    st.text_area("Content", text, height=250)
                    
                    st.subheader("Extracted Emails")
                    if emails:
                        for email in emails:
                            st.write(f"- {email}")
                    else:
                        st.write("No emails found in the document.")
                        
                    # Export options
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Export Text"):
                            text_bytes = text.encode()
                            b64 = base64.b64encode(text_bytes).decode()
                            href = f'<a href="data:file/txt;base64,{b64}" download="extracted_text.txt">Download Text</a>'
                            st.markdown(href, unsafe_allow_html=True)
                    
                    with col2:
                        if emails and st.button("Export Emails"):
                            emails_df = pd.DataFrame(emails, columns=["Email"])
                            st.markdown(get_download_link(emails_df, "extracted_emails.csv", "Emails CSV"), unsafe_allow_html=True)
                
                except Exception as e:
                    st.error(f"An error occurred while processing the PDF: {str(e)}")
    
    # Tab 3: CSV Cleaning
    with tab3:
        st.header("CSV Cleaning & Processing")
        uploaded_csv = st.file_uploader("Upload CSV file", type=["csv"])
        
        if uploaded_csv is not None:
            with st.spinner("Processing CSV..."):
                try:
                    df = clean_csv(uploaded_csv)
                    st.success("CSV processed successfully!")
                    
                    # Show basic stats
                    st.subheader("Data Preview")
                    st.dataframe(df.head())
                    
                    st.subheader("Summary Statistics")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Rows", df.shape[0])
                    col2.metric("Columns", df.shape[1])
                    col3.metric("Missing Values", df.isna().sum().sum())
                    
                    # Download options
                    st.subheader("Export Options")
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    st.markdown(get_download_link(df, f"cleaned_data_{timestamp}.csv", "Cleaned CSV"), unsafe_allow_html=True)
                    
                except Exception as e:
                    st.error(f"An error occurred while processing the CSV: {str(e)}")
    
    # Tab 4: Text Analysis
    with tab4:
        st.header("Text Analysis")
        text_input = st.text_area("Enter text to analyze:", height=200)
        
        if st.button("Extract Information") and text_input:
            with st.spinner("Analyzing text..."):
                try:
                    emails = extract_emails(text_input)
                    
                    st.subheader("Extracted Emails")
                    if emails:
                        for email in emails:
                            st.write(f"- {email}")
                    else:
                        st.write("No emails found in the text.")
                        
                    # Add additional extractors here
                    
                except Exception as e:
                    st.error(f"An error occurred during analysis: {str(e)}")
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.subheader("About")
    st.sidebar.info(
        \"\"\"
        This Universal Data Scraper application allows you to extract, process, 
        and analyze data from various sources including websites, PDFs, and CSV files.
        \"\"\"
    )
    st.sidebar.markdown("---")
    st.sidebar.write("© 2025 Universal Data Scraper")

if __name__ == "__main__":
    main()
""")
    print(f"Created app.py in {ui_dir}")
    
    # Create requirements.txt in the project root
    requirements_path = os.path.join(script_dir, "requirements.txt")
    if not os.path.exists(requirements_path):
        with open(requirements_path, "w") as f:
            f.write("""# Core UI
streamlit>=1.29.0

# Data processing
pandas>=2.0.0
numpy>=1.24.0

# Web scraping
requests>=2.28.0
beautifulsoup4>=4.11.0
lxml>=4.9.0

# PDF processing
PyPDF2>=3.0.0
pdfplumber>=0.9.0

# Excel support
openpyxl>=3.1.0
xlrd>=2.0.0

# Database
sqlalchemy>=2.0.0
pymysql>=1.0.0
psycopg2-binary>=2.9.0

# Advanced scraping (for JS-heavy sites)
selenium>=4.10.0
webdriver-manager>=3.8.0
""")
        print(f"Created requirements.txt in {script_dir}")
    
    # Create README in the UI directory
    readme_path = os.path.join(ui_dir, "README.md")
    with open(readme_path, "w") as f:
        f.write("""# Data Scraper UI

This directory contains the Streamlit UI for the Data Scraper project.

## Running the UI

From the project root directory:

```bash
cd ui
streamlit run app.py
```

The app should open automatically in your default web browser at `http://localhost:8501`.

## Modifying the UI

The UI integrates with your existing Data Scraper modules. If you make changes to those modules,
the UI will automatically use the updated functionality.

To add new features to the UI, edit the `app.py` file and add new tabs or components as needed.
""")
    print(f"Created README.md in {ui_dir}")
    
    # Try to install requirements
    print("\nInstalling requirements...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", requirements_path], check=True)
        print("Successfully installed requirements")
    except subprocess.CalledProcessError:
        print("Failed to install requirements. Please install them manually with:")
        print(f"pip install -r {requirements_path}")
    
    print("\nSetup complete! To run the UI:")
    print(f"cd {os.path.relpath(ui_dir)}")
    print("streamlit run app.py")

if __name__ == "__main__":
    main()
