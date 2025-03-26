import pytest
from datascraper.transformers import DataCleaner, DataTransformer

class TestDataCleaner:
    def test_remove_whitespace(self):
        cleaner = DataCleaner()
        result = cleaner.clean("  Hello  World  ")
        assert result == "Hello World"
    
    def test_extract_numeric(self):
        cleaner = DataCleaner()
        result = cleaner.extract_numeric("$1,299.99")
        assert result == 1299.99
    
    def test_normalize_text(self):
        cleaner = DataCleaner()
        result = cleaner.normalize("HELLO world!")
        assert result == "hello world"

class TestDataTransformer:
    def test_rename_fields(self):
        transformer = DataTransformer()
        data = {"old_name": "value", "another_field": 123}
        result = transformer.rename_fields(data, {"old_name": "new_name"})
        
        assert "new_name" in result
        assert "old_name" not in result
        assert result["new_name"] == "value"
        assert result["another_field"] == 123
    
    def test_apply_transformations(self):
        transformer = DataTransformer()
        data = {"price": "$99.99", "title": "  Product Title  ", "in_stock": "Yes"}
        
        transformations = {
            "price": lambda x: float(x.replace("$", "")),
            "title": str.strip,
            "in_stock": lambda x: x.lower() == "yes"
        }
        
        result = transformer.apply_transformations(data, transformations)
        
        assert result["price"] == 99.99
        assert result["title"] == "Product Title"
        assert result["in_stock"] is True
