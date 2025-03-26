import pytest
import os
import tempfile
import json
from pathlib import Path

@pytest.fixture
def sample_html():
    return """
    <html>
        <body>
            <div class="product">
                <h2>Product Title</h2>
                <span class="price">$99.99</span>
                <div class="description">This is a sample product description.</div>
            </div>
        </body>
    </html>
    """

@pytest.fixture
def sample_json():
    return {
        "items": [
            {"id": 1, "name": "Item 1", "price": 10.99},
            {"id": 2, "name": "Item 2", "price": 20.99},
            {"id": 3, "name": "Item 3", "price": 30.99}
        ]
    }

@pytest.fixture
def temp_output_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)
        
@pytest.fixture
def mock_db_connection():
    # Mock database connection for testing
    class MockDB:
        def __init__(self):
            self.data = []
            
        def insert(self, data):
            self.data.append(data)
            return True
            
        def query(self, query_string):
            return self.data
            
        def close(self):
            pass
    
    return MockDB()
