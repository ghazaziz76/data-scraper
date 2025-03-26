Universal Data Scraper
Overview
A flexible Python-based web scraping tool that can extract data from various web sources.
Features

Scrape static HTML pages
Extract data from tables and lists
Save output to CSV
Configurable extraction rules

Installation

Clone the repository
Create a virtual environment
Install dependencies:
pip install -r requirements.txt

Usage
Basic usage:
python src/main.py https://example.com --table 'table.data-table'
python src/main.py https://example.com --list 'ul.items'

Options

url: Target website URL (required)
--table: CSS selector for table extraction
--list: CSS selector for list extraction
--output: Custom output filename (default: scraped_data.csv)

Development Stages

✅ Core CLI Scraper (HTML only)
Planned: JSON API & Dynamic Page Support
Planned: Configurable Extraction Rules
Planned: Multiple Export Formats
Planned: Multi-URL and Automation
Planned: UI Dashboard

Contributing
Contributions are welcome! Please read the contributing guidelines.

# data-scraper
# Create the extractor
extractor = EnhancedWebExtractor(
    use_selenium=True,  # Use Selenium for JavaScript-rendered pages
    headless=True,      # Run in headless mode
    rate_limit=2.0      # Wait 2 seconds between requests
)

try:
    # Extract multiple items from a page with pagination
    products = extractor.extract_multiple(
        url='https://example.com/products',
        container_selector='.product',
        field_selectors={
            'id': '.product-id',
            'name': '.product-name',
            'price': '.product-price'
        },
        pagination_selector='.next-page:not(.disabled)',
        max_pages=3  # Extract up to 3 pages
    )
    
    print(f"Extracted {len(products)} products")
    
finally:
    # Always close the extractor to release resources
    extractor.close()
