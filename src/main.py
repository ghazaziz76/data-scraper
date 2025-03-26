"""
Main entry point for the Universal Data Scraper application.
"""
import argparse
import os
import sys
from typing import Optional, List

# Import connectors
from connectors.web.html_scraper import HTMLScraper
from connectors.document.pdf_reader import PDFReader
from connectors.document.csv_processor import CSVProcessor
from connectors.database.sql_connector import SQLConnector

# Import processors
from processors.extractors.email_extractor import EmailExtractor

# Import utilities
from utils.file_manager import FileManager

# Import configuration
from config.settings import APP_NAME, APP_VERSION


def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(description=f'{APP_NAME} v{APP_VERSION}')
    
    # Source arguments
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--urls', nargs='+', help='One or more URLs to scrape')
    source_group.add_argument('--pdfs', nargs='+', help='One or more PDF files to process')
    source_group.add_argument('--csvs', nargs='+', help='One or more CSV files to process and clean')
    source_group.add_argument('--input-file', help='File containing URLs or file paths (one per line)')
    source_group.add_argument('--db', help='Database connection string (e.g., sqlite:///mydatabase.db)')
    
    # Extraction options
    parser.add_argument('--table', help='CSS selector for table extraction or database table')
    parser.add_argument('--list', help='CSS selector for list extraction')
    parser.add_argument('--query', help='SQL query to execute')
    parser.add_argument('--emails', action='store_true', help='Extract email addresses')
    
    # Output options
    parser.add_argument('--output-dir', help='Output directory (defaults to timestamped directory)')
    parser.add_argument('--format', choices=['csv', 'json', 'excel'], default='csv', 
                       help='Output format (default: csv)')
    
    args = parser.parse_args()
    
    print("Arguments parsed:", args)
    print("Starting processing...")
    
    # Create project directory
    project_dir = args.output_dir or FileManager.create_project_directory()
    
    # Create raw and output directories if they don't exist
    if not os.path.exists(os.path.join(project_dir, 'raw')):
        os.makedirs(os.path.join(project_dir, 'raw'))
    if not os.path.exists(os.path.join(project_dir, 'output')):
        os.makedirs(os.path.join(project_dir, 'output'))
    
    # Process sources
    if args.urls:
        process_urls(args.urls, args, project_dir)
    elif args.pdfs:
        process_pdfs(args.pdfs, args, project_dir)
    elif args.csvs:
        process_csvs(args.csvs, args, project_dir)
    elif args.input_file:
        process_from_file(args.input_file, args, project_dir)
    elif args.db:
        process_database(args.db, args, project_dir)
    else:
        parser.print_help()
        return
        
    print(f"Processing complete. Results saved to {project_dir}")

def process_from_file(input_file: str, args, project_dir: str):
    """Process sources from a file containing URLs or file paths."""
    if not os.path.exists(input_file):
        print(f"Error: Input file not found: {input_file}")
        return
        
    try:
        # Read URLs or file paths from input file
        with open(input_file, 'r', encoding='utf-8') as f:
            sources = [line.strip() for line in f if line.strip()]
            
        # Determine source type based on first item
        if sources and (sources[0].startswith('http://') or sources[0].startswith('https://')):
            process_urls(sources, args, project_dir)
        else:
            process_pdfs(sources, args, project_dir)
            
    except Exception as e:
        print(f"Error processing input file: {e}")

def process_urls(urls: List[str], args, project_dir: str):
    """Process multiple web URLs."""
    print(f"Processing {len(urls)} URLs...")
    
    all_emails = []
    
    for i, url in enumerate(urls):
        print(f"[{i+1}/{len(urls)}] Scraping URL: {url}")
        
        # Initialize scraper
        scraper = HTMLScraper(url)
        
        # Fetch page
        if not scraper.fetch_page():
            print(f"  Failed to fetch the webpage: {url}")
            continue
        
        # Save raw HTML
        html_content = scraper.soup.prettify()
        url_filename = url.replace('://', '_').replace('/', '_').replace(':', '_')
        if len(url_filename) > 100:  # Truncate if too long
            url_filename = url_filename[:100]
        FileManager.save_raw_html(html_content, project_dir, f"{url_filename}.html")
        
        # Extract data based on selectors
        if args.table:
            scraper.extract_table_data(args.table)
            
            # Save to file
            if hasattr(scraper, 'data') and scraper.data:
                output_path = os.path.join(project_dir, 'output', f"{url_filename}.csv")
                scraper.save_to_csv(output_path)
                print(f"  Table data saved to {output_path}")
        elif args.list:
            scraper.extract_list_data(args.list)
            
            # Save to file
            if hasattr(scraper, 'data') and scraper.data:
                output_path = os.path.join(project_dir, 'output', f"{url_filename}.csv")
                scraper.save_to_csv(output_path)
                print(f"  List data saved to {output_path}")
        
        # Extract emails if requested
        if args.emails:
            email_extractor = EmailExtractor()
            emails = email_extractor.extract(html_content)
            if emails:
                print(f"  Found {len(emails)} email addresses in {url}")
                for email in emails:
                    print(f"    - {email}")
                all_emails.extend(emails)
    
    # Save all emails to a single file
    if args.emails and all_emails:
        email_output_path = os.path.join(project_dir, 'output', 'all_emails.txt')
        with open(email_output_path, 'w', encoding='utf-8') as f:
            for email in sorted(set(all_emails)):
                f.write(f"{email}\n")
        print(f"All unique emails saved to {email_output_path}")

def process_pdfs(pdf_paths: List[str], args, project_dir: str):
    """Process multiple PDF files."""
    print(f"Processing {len(pdf_paths)} PDF files...")
    
    all_emails = []
    
    for i, pdf_path in enumerate(pdf_paths):
        if not os.path.exists(pdf_path):
            print(f"[{i+1}/{len(pdf_paths)}] Error: PDF file not found: {pdf_path}")
            continue
            
        print(f"[{i+1}/{len(pdf_paths)}] Processing PDF: {pdf_path}")
        
        try:
            # Initialize PDF reader
            pdf_reader = PDFReader()
            
            # Extract content from PDF
            pdf_data = pdf_reader.read_file(pdf_path)
            
            # Generate output filename based on PDF filename
            pdf_filename = os.path.basename(pdf_path)
            base_filename = os.path.splitext(pdf_filename)[0]
            
            print(f"  PDF processed: {pdf_data['page_count']} pages")
            
            # Extract emails if requested
            if args.emails:
                email_extractor = EmailExtractor()
                emails = email_extractor.extract(pdf_data['text'])
                if emails:
                    print(f"  Found {len(emails)} email addresses in {pdf_filename}")
                    for email in emails:
                        print(f"    - {email}")
                    all_emails.extend(emails)
            
            # Save extracted text
            text_path = os.path.join(project_dir, 'raw', f"{base_filename}_text.txt")
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(pdf_data['text'])
            
            print(f"  Extracted text saved to {text_path}")
            
        except Exception as e:
            print(f"  Error processing PDF: {e}")
    
    # Save all emails to a single file
    if args.emails and all_emails:
        email_output_path = os.path.join(project_dir, 'output', 'all_emails.txt')
        with open(email_output_path, 'w', encoding='utf-8') as f:
            for email in sorted(set(all_emails)):
                f.write(f"{email}\n")
        print(f"All unique emails saved to {email_output_path}")

def process_csvs(csv_paths: List[str], args, project_dir: str):
    """Process and clean multiple CSV files."""
    print(f"Processing {len(csv_paths)} CSV files...")
    
    for i, csv_path in enumerate(csv_paths):
        if not os.path.exists(csv_path):
            print(f"[{i+1}/{len(csv_paths)}] Error: CSV file not found: {csv_path}")
            continue
            
        print(f"[{i+1}/{len(csv_paths)}] Processing CSV: {csv_path}")
        
        try:
            # Initialize CSV processor
            csv_processor = CSVProcessor(csv_path)
            
            # Print info about the data
            info = csv_processor.get_info()
            print(f"  CSV info: {info['rows']} rows, {info['columns']} columns")
            print(f"  Columns: {', '.join(info['column_names'])}")
            print(f"  Duplicates: {info['duplicates']}")
            
            # Clean the data
            print("  Cleaning data...")
            csv_processor.clean_column_names()
            csv_processor.remove_duplicates()
            csv_processor.remove_empty_rows(threshold=0.7)  # Remove rows with more than 30% missing values
            
            # Fill missing values in numeric columns with mean
            numeric_columns = [col for col in csv_processor.data.columns 
                              if pd.api.types.is_numeric_dtype(csv_processor.data[col])]
            if numeric_columns:
                csv_processor.fill_missing_values(strategy='mean', columns=numeric_columns)
            
            # Fill missing values in string columns with empty string
            string_columns = [col for col in csv_processor.data.columns 
                             if pd.api.types.is_string_dtype(csv_processor.data[col])]
            if string_columns:
                csv_processor.fill_missing_values(strategy='empty', columns=string_columns)
            
            # Generate output filename based on CSV filename
            csv_filename = os.path.basename(csv_path)
            base_filename = os.path.splitext(csv_filename)[0]
            
            # Save cleaned CSV
            output_path = os.path.join(project_dir, 'output', f"{base_filename}_cleaned.csv")
            csv_processor.save_csv(output_path)
            
        except Exception as e:
            print(f"  Error processing CSV: {e}")

def process_database(connection_string: str, args, project_dir: str):
    """Process a database connection."""
    print(f"Connecting to database: {connection_string}")
    
    try:
        # Initialize SQL connector
        sql_connector = SQLConnector()
        
        # Connect to the database
        if not sql_connector.connect(connection_string):
            return
        
        # List tables if no specific table or query is provided
        if not args.table and not args.query:
            tables = sql_connector.list_tables()
            if not tables:
                print("No tables found in database")
                return
                
            # Save table list
            tables_path = os.path.join(project_dir, 'output', 'database_tables.txt')
            with open(tables_path, 'w', encoding='utf-8') as f:
                for table in tables:
                    f.write(f"{table}\n")
            print(f"Table list saved to {tables_path}")
            
            return
        
        # Execute query if provided
        if args.query:
            print(f"Executing query: {args.query}")
            if not sql_connector.execute_query(args.query):
                return
        
        # Extract table data if specified
        elif args.table:
            print(f"Extracting data from table: {args.table}")
            
            # Get table structure
            columns = sql_connector.get_table_structure(args.table)
            
            # Save table structure
            structure_path = os.path.join(project_dir, 'output', f"{args.table}_structure.txt")
            with open(structure_path, 'w', encoding='utf-8') as f:
                for col in columns:
                    f.write(f"{col['name']}: {col['type']}\n")
            
            # Extract table data
            if not sql_connector.get_table_data(args.table):
                return
        
        # Save extracted data
        if sql_connector.data is not None:
            output_filename = args.table or 'query_results'
            output_path = os.path.join(project_dir, 'output', f"{output_filename}.csv")
            sql_connector.save_to_csv(output_path)
        
        # Close the connection
        sql_connector.close()
        
    except Exception as e:
        print(f"Error processing database: {e}")

if __name__ == '__main__':
    main()
