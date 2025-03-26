import pytest
from bs4 import BeautifulSoup
import requests_mock
from datascraper.extractors import WebExtractor, APIExtractor

class TestWebExtractor:
    def test_extract_from_html(self, sample_html):
        extractor = WebExtractor()
        data = extractor.extract_from_html(sample_html, {"title": "h2", "price": ".price", "description": ".description"})
        
        assert data["title"] == "Product Title"
        assert data["price"] == "$99.99"
        assert data["description"] == "This is a sample product description."
    
    def test_extract_from_url(self, sample_html):
        with requests_mock.Mocker() as m:
            m.get("http://example.com", text=sample_html)
            
            extractor = WebExtractor()
            data = extractor.extract_from_url(
                "http://example.com", 
                {"title": "h2", "price": ".price", "description": ".description"}
            )
            
            assert data["title"] == "Product Title"
            assert data["price"] == "$99.99"
            assert data["description"] == "This is a sample product description."
    
    def test_extract_multiple_items(self):
        html = """
        <html><body>
            <div class="product">
                <h2>Product 1</h2>
                <span class="price">$99.99</span>
            </div>
            <div class="product">
                <h2>Product 2</h2>
                <span class="price">$149.99</span>
            </div>
        </body></html>
        """
        
        extractor = WebExtractor()
        data = extractor.extract_multiple(
            html, 
            container=".product", 
            fields={"title": "h2", "price": ".price"}
        )
        
        assert len(data) == 2
        assert data[0]["title"] == "Product 1"
        assert data[0]["price"] == "$99.99"
        assert data[1]["title"] == "Product 2"
        assert data[1]["price"] == "$149.99"

class TestAPIExtractor:
    def test_extract_from_json(self, sample_json):
        extractor = APIExtractor()
        items = extractor.extract(sample_json, "items")
        
        assert len(items) == 3
        assert items[0]["name"] == "Item 1"
        assert items[2]["price"] == 30.99
    
    def test_extract_with_json_path(self, sample_json):
        extractor = APIExtractor()
        names = extractor.extract_with_path(sample_json, "items[*].name")
        
        assert len(names) == 3
        assert names == ["Item 1", "Item 2", "Item 3"]
    
    def test_extract_from_api(self):
        with requests_mock.Mocker() as m:
            mock_response = {"data": {"products": [{"id": 1, "name": "Test Product"}]}}
            m.get("https://api.example.com/products", json=mock_response)
            
            extractor = APIExtractor()
            products = extractor.extract_from_api(
                "https://api.example.com/products",
                json_path="data.products"
            )
            
            assert len(products) == 1
            assert products[0]["name"] == "Test Product"
