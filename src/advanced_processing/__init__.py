# src/advanced_processing/__init__.py
"""
Advanced Processing Module
--------------------------

This module provides advanced data processing capabilities, including:
- AI-powered information extraction
- Advanced pattern recognition
- Automated data classification
- Batch processing

Components:
- AIExtractor: Extract entities, relationships, and key information using NLP
- PatternRecognizer: Detect complex patterns with fuzzy matching capabilities
- DataClassifier: Categorize data using machine learning
- BatchProcessor: Process large volumes of data efficiently
- AdvancedProcessingManager: Coordinate all advanced processing capabilities

Example usage:
    from advanced_processing import AdvancedProcessingManager
    
    manager = AdvancedProcessingManager()
    results = manager.process_text_content(
        text="Sample text to analyze",
        extract_entities=True,
        extract_patterns=True
    )
"""

# Import main components
from .ai_extractor import AIExtractor
from .pattern_recognizer import PatternRecognizer
from .data_classifier import DataClassifier
from .batch_processor import BatchProcessor
from .manager import AdvancedProcessingManager

# Define module exports
__all__ = [
    'AIExtractor',
    'PatternRecognizer',
    'DataClassifier',
    'BatchProcessor',
    'AdvancedProcessingManager'
]
