import unittest
import tempfile
import os
import json
from unittest.mock import patch, MagicMock

# Import your existing classes
from datascraper.extractors import WebExtractor, APIExtractor
from datascraper.transformers import DataCleaner, DataTransformer
from datascraper.loaders import JSONLoader, CSVLoader
from datascraper.validators import SchemaValidator, DataQualityValidator
from datascraper.pipeline import ETLPipeline

class TestETLPipelineIntegration(unittest.TestCase):
    """Basic integration tests for the ETLPipeline class"""

    def setUp(self):
        """Set up test fixtures before each test"""
        # Create temporary directory for test outputs
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_path = self.temp_dir.name
        
        # Sample data for testing
        self.sample_data = [
            {"id": 1, "name": "Product A", "price": "$19.99", "in_stock": "Yes"},
            {"id": 2, "name": "Product B", "price": "$24.50", "in_stock": "No"},
            {"id": 3, "name": "Product C", "price": "invalid", "in_stock": "Yes"}
        ]

    def tearDown(self):
        """Clean up test fixtures after each test"""
        self.temp_dir.cleanup()

    def test_simple_extraction_pipeline(self):
        """Test a simple extraction pipeline with no transformations or validations"""
        # Create a mock extractor that returns sample data
        mock_extractor = MagicMock(spec=APIExtractor)
        mock_extractor.extract.return_value = self.sample_data
        
        # Create a loader
        output_file = os.path.join(self.output_path, "test_output.json")
        loader = JSONLoader()
        loader.destination = output_file
        
        # Create and run the pipeline
        pipeline = ETLPipeline(
            extractor=mock_extractor,
            loader=loader,
            name="SimpleExtractionTest"
        )
        
        # Run the pipeline
        result = pipeline.run()
        
        # Verify the pipeline executed successfully
        self.assertTrue(result.success)
        self.assertEqual(result.records_processed, 3)
        self.assertEqual(result.records_loaded, 3)
        
        # Verify the output file exists
        self.assertTrue(os.path.exists(output_file))
        
        # Read the output file and verify its contents
        with open(output_file, 'r') as f:
            loaded_data = json.load(f)
            self.assertEqual(len(loaded_data), 3)
            self.assertEqual(loaded_data[0]["name"], "Product A")

    def test_data_transformation_pipeline(self):
        """Test a pipeline with data transformation"""
        # Create a mock extractor
        mock_extractor = MagicMock(spec=APIExtractor)
        mock_extractor.extract.return_value = self.sample_data
        
        # Create a transformer to rename fields
        transformer = DataTransformer()
        # Set the mapping as an attribute so the pipeline can access it
        transformer.transformations = {}  # Not used in this test
        
        # We need to modify the pipeline to call rename_fields directly
        # since that's how your actual class works
        output_file = os.path.join(self.output_path, "transformed_output.json")
        
        # Create a custom pipeline for this test
        class CustomPipeline(ETLPipeline):
            def run(self):
                result = super().run()
                # Re-read the data and verify it has the expected structure
                return result
        
        # Create a loader
        loader = JSONLoader()
        loader.destination = output_file
        
        # Create a wrapper method that will apply the renaming to each record
        def apply_renaming(data):
            field_mapping = {
                "name": "product_name",
                "price": "product_price"
            }
            return [transformer.rename_fields(record, field_mapping) for record in data]
        
        # Create a mock transformer that will call our wrapper
        mock_transformer = MagicMock()
        mock_transformer.__class__.__name__ = "CustomTransformer"
        mock_transformer.transform = apply_renaming
        
        # Create and run the pipeline
        pipeline = ETLPipeline(
            extractor=mock_extractor,
            transformers=[mock_transformer],
            loader=loader,
            name="TransformationTest"
        )
        
        # Run the pipeline
        result = pipeline.run()
        
        # Verify the pipeline executed successfully
        self.assertTrue(result.success)
        
        # Verify the output file exists
        self.assertTrue(os.path.exists(output_file))
        
        # Read the output file and verify the field renaming
        with open(output_file, 'r') as f:
            loaded_data = json.load(f)
            self.assertEqual(len(loaded_data), 3)
            self.assertTrue("product_name" in loaded_data[0])
            self.assertFalse("name" in loaded_data[0])
            self.assertTrue("product_price" in loaded_data[0])
            self.assertFalse("price" in loaded_data[0])

    def test_data_validation_pipeline(self):
        """Test a pipeline with data validation"""
        # Create a mock extractor
        mock_extractor = MagicMock(spec=APIExtractor)
        mock_extractor.extract.return_value = self.sample_data
        
        # Create a validator
        validator = SchemaValidator()
        validator.schema = {
            "required": ["id", "name", "price"],
            "types": {
                "id": int,
                "name": str
            }
        }
        
        # Create a loader
        output_file = os.path.join(self.output_path, "validated_output.json")
        loader = JSONLoader()
        loader.destination = output_file
        
        # Create and run the pipeline
        pipeline = ETLPipeline(
            extractor=mock_extractor,
            validators=[validator],
            loader=loader,
            name="ValidationTest"
        )
        
        # Run the pipeline
        result = pipeline.run()
        
        # Verify the pipeline executed successfully
        self.assertTrue(result.success)
        
        # Verify the output file exists
        self.assertTrue(os.path.exists(output_file))
        
        # Read the output file
        with open(output_file, 'r') as f:
            loaded_data = json.load(f)
            # All our sample data has the required fields, so all should be valid
            self.assertEqual(len(loaded_data), 3)

    def test_csv_loading_pipeline(self):
        """Test a pipeline that loads data to CSV"""
        # Create a mock extractor
        mock_extractor = MagicMock(spec=APIExtractor)
        mock_extractor.extract.return_value = self.sample_data
        
        # Create a loader
        output_file = os.path.join(self.output_path, "output.csv")
        loader = CSVLoader()
        loader.destination = output_file
        
        # Create and run the pipeline
        pipeline = ETLPipeline(
            extractor=mock_extractor,
            loader=loader,
            name="CSVLoadingTest"
        )
        
        # Run the pipeline
        result = pipeline.run()
        
        # Verify the pipeline executed successfully
        self.assertTrue(result.success)
        
        # Verify the output file exists
        self.assertTrue(os.path.exists(output_file))
        
        # We could read and verify the CSV file contents here,
        # but for now just checking it exists is sufficient

    def test_pipeline_error_handling(self):
        """Test that the pipeline properly handles errors"""
        # Create a mock extractor that raises an exception
        mock_extractor = MagicMock(spec=WebExtractor)
        mock_extractor.extract_from_url.side_effect = Exception("Extraction failed")
        mock_extractor.url = "http://example.com"
        mock_extractor.selectors = {}
        
        # Create a loader
        output_file = os.path.join(self.output_path, "error_output.json")
        loader = JSONLoader()
        loader.destination = output_file
        
        # Create and run the pipeline
        pipeline = ETLPipeline(
            extractor=mock_extractor,
            loader=loader,
            name="ErrorHandlingTest"
        )
        
        # Run the pipeline
        result = pipeline.run()
        
        # Verify the pipeline reports failure
        self.assertFalse(result.success)
        self.assertIn("Extraction failed", result.error_message)
        
        # Verify the output file does not exist (since extraction failed)
        self.assertFalse(os.path.exists(output_file))

    def test_dry_run_pipeline(self):
        """Test the dry_run method of the pipeline"""
        # Create a mock extractor
        mock_extractor = MagicMock(spec=APIExtractor)
        mock_extractor.extract.return_value = self.sample_data
        
        # Create a mock loader
        mock_loader = MagicMock(spec=JSONLoader)
        
        # Create and run the pipeline
        pipeline = ETLPipeline(
            extractor=mock_extractor,
            loader=mock_loader,
            name="DryRunTest"
        )
        
        # Run the pipeline in dry run mode
        result = pipeline.dry_run()
        
        # Verify the pipeline executed successfully
        self.assertTrue(result.success)
        
        # Verify the loader was not called
        mock_loader.save.assert_not_called()


if __name__ == '__main__':
    unittest.main()
