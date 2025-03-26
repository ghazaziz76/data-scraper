# app.py
# app.py
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
from src.connectors.web.html_scraper import HTMLScraper
# Update these imports based on your actual implementations
# from src.connectors.document.pdf_reader import extract_text
# from src.connectors.document.excel_reader import process_csv
# from src.processors.extractors.email_extractor import extract_emails

# Integration with existing modules
def scrape_html(url, selectors=None):
    """Wrapper for your existing HTML scraper functionality"""
    try:
        # Create an instance of your HTMLScraper class
        scraper = HTMLScraper(url)
        
        # Fetch the page
        if scraper.fetch_page():
            # If selectors are provided, use them to extract table data
            if selectors:
                data = scraper.extract_table_data(selectors)
            else:
                # Try a common table selector if none provided
                data = scraper.extract_table_data('table')
                
                # If no tables found, try to extract list data
                if not data:
                    data = scraper.extract_list_data('ul') or scraper.extract_list_data('ol')
            
            # If data is a list of dictionaries, convert directly to DataFrame
            if data and isinstance(data[0], dict):
                return pd.DataFrame(data)
            # If data is a list of strings, create a simple DataFrame
            elif data:
                return pd.DataFrame({"Content": data})
            else:
                st.warning("No structured data found. Try specifying a selector.")
                return pd.DataFrame()
        else:
            st.error("Failed to fetch the webpage.")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error in HTML scraper: {str(e)}")
        # Return empty DataFrame as fallback
        return pd.DataFrame()

# Placeholder functions until you connect your actual implementations
def extract_pdf(uploaded_file):
    """Placeholder for PDF extraction functionality"""
    try:
        # This is a placeholder - replace with your actual PDF extraction code
        text = f"Placeholder text extracted from {uploaded_file.name}"
        emails = ["example@example.com"]  # Placeholder
        return text, emails
    except Exception as e:
        st.error(f"Error in PDF extraction: {str(e)}")
        return "", []

def clean_csv(uploaded_file):
    """Placeholder for CSV processing functionality"""
    try:
        # This is a placeholder - replace with your actual CSV processing code
        df = pd.read_csv(uploaded_file)
        return df
    except Exception as e:
        st.error(f"Error in CSV processing: {str(e)}")
        return pd.DataFrame()

def extract_emails(text):
    """Placeholder for email extraction functionality"""
    try:
        # This is a placeholder - replace with your actual email extraction code
        import re
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        return re.findall(email_pattern, text)
    except Exception as e:
        st.error(f"Error in email extraction: {str(e)}")
        return []

def get_download_link(df, filename, text):
    """Generate a download link for a dataframe"""
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
                    
                    if not df.empty:
                        st.success("Scraping completed!")
                        st.dataframe(df)
                        
                        # Create download link
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"scraped_data_{timestamp}.csv"
                        st.markdown(get_download_link(df, filename, "CSV"), unsafe_allow_html=True)
                    else:
                        st.warning("No data was scraped from the URL. Try using a different selector.")
                    
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
        """
        This Universal Data Scraper application allows you to extract, process, 
        and analyze data from various sources including websites, PDFs, and CSV files.
        """
    )
    st.sidebar.markdown("---")
    st.sidebar.write("© 2025 Universal Data Scraper")

if __name__ == "__main__":
    main()
