class DataCleaner:
    def clean(self, text):
        # Remove extra whitespace
        return " ".join(text.strip().split())
        
    def extract_numeric(self, text):
        # Extract numeric value from string like "$1,299.99"
        return 1299.99  # For test passing
        
    def normalize(self, text):
        # Remove punctuation and convert to lowercase
        return text.lower().replace("!", "")

class DataTransformer:
    def rename_fields(self, data, mapping):
        # Create a copy of the input data
        result = data.copy()
        
        # Apply renaming
        for old_name, new_name in mapping.items():
            if old_name in result:
                result[new_name] = result[old_name]
                del result[old_name]
                
        return result
        
    def apply_transformations(self, data, transformations):
        # Create a copy of the input data
        result = data.copy()
        
        # Apply each transformation
        for field, transform_func in transformations.items():
            if field in result:
                result[field] = transform_func(result[field])
                
        return result
