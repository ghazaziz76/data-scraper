import pytest
import json
import csv
import os
from datascraper.loaders import JSONLoader, CSVLoader, DatabaseLoader

class TestJSONLoader:
    def test_save_to_json(self, temp_output_dir):
        data = [{"id": 1, "name": "Test"}, {"id": 2, "name": "Test 2"}]
        
        loader = JSONLoader()
        file_path = temp_output_dir / "output.json"
        
        loader.save(data, str(file_path))
        
        assert os.path.exists(file_path)
        
        with open(file_path, "r") as f:
            loaded_data = json.load(f)
            assert loaded_data == data

class TestCSVLoader:
    def test_save_to_csv(self, temp_output_dir):
        data = [
            {"id": 1, "name": "Test 1", "price": 10.99},
            {"id": 2, "name": "Test 2", "price": 20.99}
        ]
        
        loader = CSVLoader()
        file_path = temp_output_dir / "output.csv"
        
        loader.save(data, str(file_path))
        
        assert os.path.exists(file_path)
        
        with open(file_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            loaded_data = list(reader)
            
            assert len(loaded_data) == 2
            assert loaded_data[0]["name"] == "Test 1"
            assert loaded_data[1]["price"] == "20.99"  # CSV stores as strings

class TestDatabaseLoader:
    def test_save_to_database(self, mock_db_connection):
        data = [
            {"id": 1, "name": "Test 1", "price": 10.99},
            {"id": 2, "name": "Test 2", "price": 20.99}
        ]
        
        loader = DatabaseLoader(mock_db_connection)
        result = loader.save(data, "products")
        
        assert result is True
        assert len(mock_db_connection.data) == 2
        assert mock_db_connection.data[0]["name"] == "Test 1"
