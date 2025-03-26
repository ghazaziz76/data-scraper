# data-scraper/examples/advanced_processing_demo.py
"""
Advanced Processing Demonstration
---------------------------------

This script demonstrates usage of the advanced processing features.
It showcases AI extraction, pattern recognition, classification, and batch processing.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime
from pprint import pprint

# Add project root to path to ensure modules can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from the data-scraper project
from src.advanced_processing import AdvancedProcessingManager
from src.advanced_processing import AIExtractor, PatternRecognizer, DataClassifier, BatchProcessor

def main():
    print("Advanced Processing Demonstration")
    print("=================================")
    
    # 1. Initialize the AdvancedProcessingManager
    print("\n1. Initializing AdvancedProcessingManager...")
    config_path = os.path.join('src', 'config', 'advanced_processing_config.json')
    
    if os.path.exists(config_path):
        manager = AdvancedProcessingManager(config_path=config_path)
        print(f"  ✓ Initialized with configuration from {config_path}")
    else:
        manager = AdvancedProcessingManager()
        print("  ✓ Initialized with default configuration")
    
    # 2. Process a sample text with AI extraction
    print("\n2. Processing text with AI-powered extraction...")
    sample_text = """
    Apple Inc. announced today that its new iPhone 13 will be released on September 24, 2021.
    The announcement was made by CEO Tim Cook during a virtual event streamed from Apple's headquarters in Cupertino.
    The new model features improved battery life, a faster A15 processor, and an enhanced camera system.
    For more information, contact Apple Support at support@apple.com or call 1-800-MY-APPLE.
    """
    
    extraction_results = manager.process_text_content(
        text=sample_text,
        extract_entities=True,
        extract_patterns=True
    )
    
    print("\n  AI Extraction Results:")
    if 'ai_extraction' in extraction_results:
        entities = extraction_results['ai_extraction'].get('entities', {})
        print(f"  * Identified {sum(len(v) for v in entities.values())} entities across {len(entities)} types")
        for entity_type, values in entities.items():
            print(f"    - {entity_type}: {', '.join(values[:3])}{' ...' if len(values) > 3 else ''}")
        
        key_phrases = extraction_results['ai_extraction'].get('key_phrases', [])
        print(f"  * Extracted {len(key_phrases)} key phrases")
    
    print("\n  Pattern Recognition Results:")
    if 'pattern_recognition' in extraction_results:
        patterns = extraction_results['pattern_recognition']
        print(f"  * Identified patterns from {len(patterns)} categories")
        for pattern_type, matches in patterns.items():
            if matches:
                print(f"    - {pattern_type}: {len(matches)} matches")
                for match in matches[:2]:
                    print(f"      * {match.get('match')} (confidence: {match.get('confidence', 0):.2f})")
                if len(matches) > 2:
                    print(f"      * ... and {len(matches)-2} more")
    
    # 3. Create a simple classifier
    print("\n3. Creating a default text classifier...")
    classifier_result = manager.create_default_classifier('text')
    
    print(f"  ✓ Classifier created: {classifier_result.get('classifier_name')}")
    print(f"  ✓ Trained with {classifier_result.get('sample_count', 0)} samples")
    print(f"  ✓ Categories: {', '.join(classifier_result.get('category_distribution', {}).keys())}")
    
    # 4. Batch processing demonstration
    print("\n4. Demonstrating batch processing...")
    
    # Create sample documents
    sample_documents = [
        {
            "id": "doc1",
            "title": "Apple Product Announcement",
            "text": sample_text,
            "source": "Example"
        },
        {
            "id": "doc2",
            "title": "Contact Information",
            "text": "Please reach out to John Smith at john.smith@example.com or call 555-123-4567. Our office is located at 123 Main St, New York, NY 10001.",
            "source": "Example"
        },
        {
            "id": "doc3",
            "title": "Project Update",
            "text": "The project is on track for completion by December 15, 2024. Current progress is at 85%, with all major milestones achieved. Budget spent to date: $125,000.",
            "source": "Example"
        }
    ]
    
    # Process the batch of documents
    job_id = manager.batch_process_documents(
        documents=sample_documents,
        processing_config={
            'extract_entities': True,
            'extract_patterns': True,
            'classifier_name': 'text_classifier',
            'classify': True
        }
    )
    
    print(f"  ✓ Started batch processing job: {job_id}")
    
    # Get initial status
    status = manager.get_job_status(job_id)
    print(f"  ✓ Job status: {status.get('status')}")
    
    # Wait until job is completed (this is fast with our small example)
    import time
    timeout = 10
    start_time = time.time()
    
    while status.get('status') not in ('completed', 'failed') and time.time() - start_time < timeout:
        time.sleep(0.5)
        status = manager.get_job_status(job_id)
        progress = status.get('progress', 0)
        print(f"  ✓ Processing: {progress:.1f}% complete", end='\r')
    
    print("\n  ✓ Job completed!")
    
    # 5. Display batch processing results
    print("\n5. Retrieving and displaying batch results...")
    results = manager.get_job_results(job_id)
    
    if isinstance(results, list):
        print(f"  ✓ Processed {len(results)} documents")
        
        # Display simplified results for each document
        for i, doc_result in enumerate(results):
            doc_id = doc_result.get('document_id', f'unknown-{i}')
            print(f"\n  Document: {doc_id}")
            
            if 'ai_extraction' in doc_result:
                entities = doc_result['ai_extraction'].get('entities', {})
                entity_count = sum(len(v) for v in entities.values())
                print(f"  * Entities: {entity_count}")
            
            if 'pattern_recognition' in doc_result:
                patterns = doc_result['pattern_recognition']
                pattern_count = sum(len(matches) for matches in patterns.values())
                print(f"  * Patterns: {pattern_count}")
                
                # Show specific pattern types if found
                for key in ['email', 'phone', 'date', 'person_name']:
                    if key in patterns and patterns[key]:
                        matches = [m['match'] for m in patterns[key]]
                        print(f"    - {key}: {', '.join(matches)}")
            
            if 'classification' in doc_result:
                classification = doc_result['classification']
                category = classification.get('category', 'unknown')
                confidence = classification.get('confidence', 0) * 100
                print(f"  * Classification: {category} (confidence: {confidence:.1f}%)")
    
    # 6. Save results to file
    print("\n6. Saving results to file...")
    output_dir = 'examples/output'
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, f'batch_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    save_result = manager.save_job_results(job_id, output_path, format='json')
    
    if 'error' not in save_result:
        print(f"  ✓ Results saved to {output_path}")
    else:
        print(f"  ✗ Error saving results: {save_result.get('error')}")
    
    print("\nAdvanced Processing Demonstration completed!")

if __name__ == "__main__":
    main()
