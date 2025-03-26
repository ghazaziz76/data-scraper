import json
import csv

class JSONLoader:
    def save(self, data, destination):
        # Actually save the JSON data
        with open(destination, "w") as f:
            json.dump(data, f)
        return True

class CSVLoader:
    def save(self, data, destination):
        # Actually save the CSV data
        if not data:
            return True
            
        with open(destination, "w", newline="") as f:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        return True

class DatabaseLoader:
    def __init__(self, connection):
        self.connection = connection
        
    def save(self, data, table_name):
        # Simulate saving to database
        for item in data:
            self.connection.insert(item)
        return True
