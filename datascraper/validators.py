


class SchemaValidator:
    def validate(self, data, schema):
        # Check required fields
        if "required" in schema:
            for field in schema["required"]:
                if field not in data:
                    return False
        
        # Check field types
        if "types" in schema:
            for field, expected_type in schema["types"].items():
                if field in data and not isinstance(data[field], expected_type):
                    return False
                    
        return True

class DataQualityValidator:
    def validate(self, data, rules):
        errors = []
        
        for rule_config in rules:
            field = rule_config["field"]
            rule_func = rule_config["rule"]
            message = rule_config["message"]
            
            if field in data:
                if not rule_func(data[field]):
                    errors.append({
                        "field": field,
                        "message": message
                    })
        
        return errors
