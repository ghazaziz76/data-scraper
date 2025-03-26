import pytest
from datascraper.validators import SchemaValidator, DataQualityValidator

class TestSchemaValidator:
    def test_validate_required_fields(self):
        validator = SchemaValidator()
        schema = {
            "required": ["id", "name", "price"],
            "types": {"id": int, "price": float}
        }
        
        valid_data = {"id": 1, "name": "Test", "price": 10.99}
        invalid_data = {"id": 1, "name": "Test"}
        
        assert validator.validate(valid_data, schema) is True
        assert validator.validate(invalid_data, schema) is False
    
    def test_validate_field_types(self):
        validator = SchemaValidator()
        schema = {
            "required": ["id", "name"],
            "types": {"id": int, "price": float}
        }
        
        valid_data = {"id": 1, "name": "Test", "price": 10.99}
        invalid_data = {"id": "1", "name": "Test", "price": "10.99"}
        
        assert validator.validate(valid_data, schema) is True
        assert validator.validate(invalid_data, schema) is False

class TestDataQualityValidator:
    def test_check_data_quality(self):
        validator = DataQualityValidator()
        rules = [
            {"field": "price", "rule": lambda x: x > 0, "message": "Price must be positive"},
            {"field": "name", "rule": lambda x: len(x) > 3, "message": "Name too short"}
        ]
        
        valid_item = {"name": "Test Product", "price": 10.99}
        invalid_item = {"name": "Tst", "price": -5}
        
        assert validator.validate(valid_item, rules) == []
        
        errors = validator.validate(invalid_item, rules)
        assert len(errors) == 2
        assert "Price must be positive" in [e["message"] for e in errors]
        assert "Name too short" in [e["message"] for e in errors]
