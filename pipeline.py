from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

from data_scraper.extractors import BaseExtractor
from data_scraper.transformers import BaseTransformer
from data_scraper.loaders import BaseLoader
from data_scraper.validators import BaseValidator

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
    The main ETL pipeline class that coordinates the extraction, transformation,
    validation, and loading of data.
    """
    
    def __init__(
        self,
        extractor: BaseExtractor,
        transformers: List[BaseTransformer] = None,
        validators: List[BaseValidator] = None,
        loader: BaseLoader = None,
        name: str = "ETLPipeline"
    ):
        """
        Initialize the ETL pipeline with its components
        
        Args:
            extractor: Component responsible for extracting data from source
            transformers: List of components that transform the data
            validators: List of components that validate the data
            loader: Component responsible for loading data to destination
            name: Name of the pipeline for logging purposes
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
            PipelineResult object containing statistics and results of the run
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
            
            # Extract data
            self.logger.info(f"Extracting data using {self.extractor.__class__.__name__}")
            data = self.extractor.extract()
            result.records_processed = len(data)
            self.logger.info(f"Extracted {result.records_processed} records")
            
            # Transform data
            transformed_data = data
            for transformer in self.transformers:
                self.logger.info(f"Applying transformer: {transformer.__class__.__name__}")
                transformed_data = transformer.transform(transformed_data)
            
            # Validate data
            valid_records = transformed_data
            for validator in self.validators:
                self.logger.info(f"Applying validator: {validator.__class__.__name__}")
                valid_records, invalid_records = validator.validate(valid_records)
                result.invalid_records.extend(invalid_records)
                self.logger.info(f"Validation result: {len(valid_records)} valid, {len(invalid_records)} invalid records")
            
            result.valid_records = valid_records
            
            # Load data
            if self.loader and valid_records:
                self.logger.info(f"Loading {len(valid_records)} records using {self.loader.__class__.__name__}")
                self.loader.load(valid_records)
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
