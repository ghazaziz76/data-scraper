import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Optional

class HTMLScraper:
    def __init__(self, url: str):
        """
        Initialize the HTML scraper with a target URL

        :param url: Target website URL to scrape
        """
        self.url = url
        self.soup = None
        self.data = []

    def fetch_page(self, headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Fetch the webpage content

        :param headers: Optional custom headers to simulate browser request
        :return: Boolean indicating successful page fetch
        """
        try:
            # Default headers to mimic browser request
            default_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            # Merge default headers with any custom headers
            request_headers = {**default_headers, **(headers or {})}

            # Send GET request
            response = requests.get(self.url, headers=request_headers)
            response.raise_for_status()  # Raise exception for bad status codes

            # Parse HTML content
            self.soup = BeautifulSoup(response.text, 'html.parser')
            return True
        except requests.RequestException as e:
            print(f"Error fetching page: {e}")
            return False

    def extract_table_data(self, table_selector: str) -> List[Dict[str, str]]:
        """
        Extract data from an HTML table

        :param table_selector: CSS selector for the table
        :return: List of dictionaries containing table data
        """
        if not self.soup:
            raise ValueError("Page not fetched. Call fetch_page() first.")

        # Find the table
        table = self.soup.select_one(table_selector)
        if not table:
            print(f"No table found with selector: {table_selector}")
            return []

        # Extract headers
        headers = [header.get_text(strip=True) for header in table.select('th')]

        # Extract rows
        rows = []
        for row in table.select('tr')[1:]:  # Skip header row
            cells = row.select('td')
            if len(cells) == len(headers):
                row_data = {headers[i]: cell.get_text(strip=True) for i, cell in enumerate(cells)}
                rows.append(row_data)

        self.data = rows
        return rows

    def extract_list_data(self, list_selector: str) -> List[str]:
        """
        Extract data from a list (ul or ol)

        :param list_selector: CSS selector for the list
        :return: List of extracted text items
        """
        if not self.soup:
            raise ValueError("Page not fetched. Call fetch_page() first.")

        # Find the list
        list_items = self.soup.select(f"{list_selector} li")

        # Extract text from list items
        items = [item.get_text(strip=True) for item in list_items]

        self.data = items
        return items

    def save_to_csv(self, filename: str = 'scraped_data.csv'):
        """
        Save extracted data to CSV

        :param filename: Output filename
        """
        if not self.data:
            print("No data to save. Run an extraction method first.")
            return

        # Convert to DataFrame and save
        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

def main():
    # Example usage
    url = 'https://example.com'  # Replace with your target URL
    scraper = HTMLScraper(url)

    if scraper.fetch_page():
        # Example of table extraction
        table_data = scraper.extract_table_data('table.data-table')

        # Example of list extraction
        # list_data = scraper.extract_list_data('ul.items')

        # Save to CSV
        scraper.save_to_csv()

if __name__ == '__main__':
    main()

