from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PipelineResult:
    """Class to hold the results of an ETL pipeline run"""
    success: bool
    start_time: datetime
    end_time: datetime
    records_processed: int = 0
    records_loaded: int = 0
    valid_records: List[Dict[str, Any]] = None
    invalid_records: List[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        """Initialize lists if None"""
        if self.valid_records is None:
            self.valid_records = []
        if self.invalid_records is None:
            self.invalid_records = []
    
    @property
    def duration(self) -> float:
        """Calculate duration of pipeline run in seconds"""
        return (self.end_time - self.start_time).total_seconds()


class ETLPipeline:
    """
    ETL pipeline class that coordinates data extraction, transformation,
    validation, and loading.
    """
    
    def __init__(
        self,
        extractor=None,
        transformers=None,
        validators=None,
        loader=None,
        name="ETLPipeline"
    ):
        """
        Initialize the ETL pipeline with components
        
        Args:
            extractor: Component responsible for extracting data
            transformers: List of components that transform data
            validators: List of components that validate data
            loader: Component responsible for loading data
            name: Name of the pipeline for logging
        """
        self.extractor = extractor
        self.transformers = transformers or []
        self.validators = validators or []
        self.loader = loader
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    def run(self) -> PipelineResult:
        """
        Execute the ETL pipeline
        
        Returns:
            PipelineResult with execution statistics
        """
        start_time = datetime.now()
        result = PipelineResult(
            success=False,
            start_time=start_time,
            end_time=start_time,
            valid_records=[],
            invalid_records=[]
        )
        
        try:
            self.logger.info(f"Starting ETL pipeline: {self.name}")
            
            # Extract data - handle different extractor classes
            self.logger.info(f"Extracting data using {self.extractor.__class__.__name__}")
            if hasattr(self.extractor, 'extract'):
                # For extractors with an extract method
                data = self.extractor.extract()
            elif hasattr(self.extractor, 'extract_from_url'):
                # For WebExtractor
                data = self.extractor.extract_from_url(self.extractor.url, self.extractor.selectors)
            elif hasattr(self.extractor, 'extract_from_api'):
                # For APIExtractor
                data = self.extractor.extract_from_api(self.extractor.url, self.extractor.path)
            else:
                raise ValueError(f"Extractor {self.extractor.__class__.__name__} has no supported extraction method")
            
            # Ensure data is a list
            if not isinstance(data, list):
                data = [data]
                
            result.records_processed = len(data)
            self.logger.info(f"Extracted {result.records_processed} records")
            
            # Transform data
            transformed_data = data
            for transformer in self.transformers:
                self.logger.info(f"Applying transformer: {transformer.__class__.__name__}")
                if hasattr(transformer, 'transform'):
                    transformed_data = transformer.transform(transformed_data)
                elif hasattr(transformer, 'apply_transformations'):
                    # For DataTransformer
                    # Assume transformations are stored in transformer.transformations
                    transformed_data = [
                        transformer.apply_transformations(record, getattr(transformer, 'transformations', {}))
                        for record in transformed_data
                    ]
                elif hasattr(transformer, 'rename_fields'):
                    # For DataTransformer's rename_fields
                    # Assume mapping is stored in transformer.mapping
                    transformed_data = [
                        transformer.rename_fields(record, getattr(transformer, 'mapping', {}))
                        for record in transformed_data
                    ]
                else:
                    self.logger.warning(f"Transformer {transformer.__class__.__name__} has no supported transform method")
            
            # Validate data
            valid_records = transformed_data
            invalid_records = []
            
            for validator in self.validators:
                self.logger.info(f"Applying validator: {validator.__class__.__name__}")
                if hasattr(validator, 'validate') and len(valid_records) > 0:
                    # Get the validate method signature
                    if validator.__class__.__name__ == 'SchemaValidator':
                        # Handle schema validator
                        schema = getattr(validator, 'schema', {})
                        # Filter records using the validate method
                        new_valid_records = []
                        for record in valid_records:
                            if validator.validate(record, schema):
                                new_valid_records.append(record)
                            else:
                                invalid_records.append(record)
                        valid_records = new_valid_records
                    elif validator.__class__.__name__ == 'DataQualityValidator':
                        # Handle data quality validator
                        rules = getattr(validator, 'rules', [])
                        # Check each record against rules
                        new_valid_records = []
                        for record in valid_records:
                            errors = validator.validate(record, rules)
                            if not errors:
                                new_valid_records.append(record)
                            else:
                                record['validation_errors'] = errors
                                invalid_records.append(record)
                        valid_records = new_valid_records
                    else:
                        # Generic validator with custom validate method
                        # Assume it returns (valid, invalid) tuple
                        try:
                            valid_records, new_invalid_records = validator.validate(valid_records)
                            if new_invalid_records:
                                invalid_records.extend(new_invalid_records)
                        except Exception as e:
                            self.logger.warning(f"Validator {validator.__class__.__name__} failed: {str(e)}")
                
                self.logger.info(f"Validation result: {len(valid_records)} valid, {len(invalid_records)} invalid records")
            
            result.valid_records = valid_records
            result.invalid_records = invalid_records
            
            # Load data
            if self.loader and valid_records:
                self.logger.info(f"Loading {len(valid_records)} records using {self.loader.__class__.__name__}")
                if hasattr(self.loader, 'load'):
                    # For loaders with load method
                    self.loader.load(valid_records)
                elif hasattr(self.loader, 'save'):
                    # For your custom loaders with save method
                    if hasattr(self.loader, 'destination'):
                        self.loader.save(valid_records, self.loader.destination)
                    elif hasattr(self.loader, 'output_path'):
                        self.loader.save(valid_records, self.loader.output_path)
                    elif hasattr(self.loader, 'table_name'):
                        self.loader.save(valid_records, self.loader.table_name)
                    else:
                        raise ValueError(f"Loader {self.loader.__class__.__name__} has no destination attribute")
                else:
                    raise ValueError(f"Loader {self.loader.__class__.__name__} has no supported load method")
                
                result.records_loaded = len(valid_records)
            
            result.success = True
            self.logger.info(f"Pipeline completed successfully: {len(result.valid_records)} records loaded, "
                             f"{len(result.invalid_records)} invalid records skipped")
            
        except Exception as e:
            self.logger.exception(f"Pipeline failed: {str(e)}")
            result.error_message = str(e)
        
        finally:
            result.end_time = datetime.now()
            self.logger.info(f"Pipeline execution time: {result.duration:.2f} seconds")
            return result
    
    def dry_run(self) -> PipelineResult:
        """
        Execute the pipeline without loading data
        
        Returns:
            PipelineResult with validation results but no data loaded
        """
        # Save the original loader
        original_loader = self.loader
        self.loader = None
        
        # Run the pipeline without loading
        result = self.run()
        
        # Restore the original loader
        self.loader = original_loader
        
        return result
