import unittest
import os
import tempfile
import json
import csv
from unittest.mock import patch, MagicMock

# Import using your actual package structure
from datascraper.pipeline import ETLPipeline
from datascraper.extractors import WebExtractor, APIExtractor
from datascraper.transformers import DataCleaner, DataNormalizer
from datascraper.loaders import CSVLoader, JSONLoader
from datascraper.validators import SchemaValidator

class TestEndToEndWorkflow(unittest.TestCase):
    """End-to-end tests for the complete data scraper workflow"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary directory for test output
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = self.temp_dir.name
        
        # Test schema for product data
        self.product_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "price": {"type": "number"},
                "in_stock": {"type": "boolean"},
                "category": {"type": "string"}
            },
            "required": ["id", "name", "price", "in_stock"]
        }
        
        # Mock HTML content for WebExtractor
        self.mock_html = """
        <html>
          <body>
            <div class="product">
              <span class="id">1</span>
              <h2 class="name">Product A</h2>
              <span class="price">$19.99</span>
              <span class="stock">In Stock</span>
              <span class="category">Electronics</span>
            </div>
            <div class="product">
              <span class="id">2</span>
              <h2 class="name">Product B</h2>
              <span class="price">$24.99</span>
              <span class="stock">Out of Stock</span>
              <span class="category">Home Goods</span>
            </div>
          </body>
        </html>
        """
        
        # Mock API response for APIExtractor
        self.mock_api_response = {
            "products": [
                {
                    "product_id": 101,
                    "product_name": "API Product 1",
                    "product_price": "29.99",
                    "product_in_stock": "true",
                    "product_category": "Software"
                },
                {
                    "product_id": 102,
                    "product_name": "API Product 2",
                    "product_price": "invalid price",
                    "product_in_stock": "false",
                    "product_category": "Services"
                }
            ]
        }

    def tearDown(self):
        """Clean up after test"""
        self.temp_dir.cleanup()

    @patch('requests.get')
    def test_web_scraping_to_csv_workflow(self, mock_get):
        """Test complete workflow from web scraping to CSV output"""
        # Configure the mock response
        mock_response = MagicMock()
        mock_response.text = self.mock_html
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # Configure output path
        output_path = os.path.join(self.output_dir, "products.csv")
        
        # Create extractor with custom selectors
        extractor = WebExtractor(
            url="https://example.com/products",
            selectors={
                'product': '.product',
                'id': '.id',
                'name': '.name',
                'price': '.price',
                'in_stock': '.stock',
                'category': '.category'
            }
        )
        
        # Create transformers
        cleaner = DataCleaner(
            clean_rules={
                'price': lambda x: float(x.replace('$', '')),
                'in_stock': lambda x: x == 'In Stock'
            }
        )
        
        normalizer = DataNormalizer(
            field_mapping={
                'id': 'id',
                'name': 'name',
                'price': 'price',
                'in_stock': 'in_stock',
                'category': 'category'
            }
        )
        
        # Create validator
        validator = SchemaValidator(schema=self.product_schema)
        
        # Create loader
        loader = CSVLoader(output_path=output_path)
        
        # Create and run pipeline
        pipeline = ETLPipeline(
            extractor=extractor,
            transformers=[cleaner, normalizer],
            validators=[validator],
            loader=loader,
            name="WebScraperTest"
        )
        
        # Execute pipeline
        result = pipeline.run()
        
        # Verify pipeline success
        self.assertTrue(result.success)
        self.assertEqual(result.records_processed, 2)
        self.assertEqual(result.records_loaded, 2)
        
        # Verify output file exists
        self.assertTrue(os.path.exists(output_path))
        
        # Verify file contents
        with open(output_path, 'r') as f:
            reader = csv.DictReader(f)
            data = list(reader)
            
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0]['name'], 'Product A')
            self.assertEqual(data[0]['price'], '19.99')
            self.assertEqual(data[0]['in_stock'], 'True')
            self.assertEqual(data[1]['name'], 'Product B')

    @patch('requests.get')
    def test_api_to_json_workflow(self, mock_get):
        """Test complete workflow from API extraction to JSON output"""
        # Configure the mock response
        mock_response = MagicMock()
        mock_response.json.return_value = self.mock_api_response
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # Configure output path
        output_path = os.path.join(self.output_dir, "api_products.json")
        
        # Create extractor
        extractor = APIExtractor(
            endpoint="https://api.example.com/products",
            api_key="test_key",
            response_key="products"
        )
        
        # Create transformers for field mapping
        normalizer = DataNormalizer(
            field_mapping={
                'product_id': 'id',
                'product_name': 'name',
                'product_price': 'price',
                'product_in_stock': 'in_stock',
                'product_category': 'category'
            }
        )
        
        # Create data cleaner
        cleaner = DataCleaner(
            clean_rules={
                'price': lambda x: float(x) if x.replace('.', '').isdigit() else None,
                'in_stock': lambda x: x.lower() == 'true'
            }
        )
        
        # Create validator
        validator = SchemaValidator(schema=self.product_schema)
        
        # Create loader
        loader = JSONLoader(output_path=output_path)
        
        # Create and run pipeline
        pipeline = ETLPipeline(
            extractor=extractor,
            transformers=[normalizer, cleaner],
            validators=[validator],
            loader=loader,
            name="APIExtractorTest"
        )
        
        # Execute pipeline
        result = pipeline.run()
        
        # Verify pipeline success
        self.assertTrue(result.success)
        self.assertEqual(len(result.valid_records), 1)  # Only one valid record
        self.assertEqual(len(result.invalid_records), 1)  # One invalid record due to price
        
        # Verify output file exists
        self.assertTrue(os.path.exists(output_path))
        
        # Verify file contents
        with open(output_path, 'r') as f:
            data = json.load(f)
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0]['name'], 'API Product 1')
            self.assertEqual(data[0]['price'], 29.99)


if __name__ == '__main__':
    unittest.main()
