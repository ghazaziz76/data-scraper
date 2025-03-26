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

