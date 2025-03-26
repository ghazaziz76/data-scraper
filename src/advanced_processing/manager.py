# src/advanced_processing/manager.py
import os
import logging
import pandas as pd
import json
from typing import List, Dict, Any, Optional, Union, Tuple, Callable
from datetime import datetime

from .ai_extractor import AIExtractor
from .pattern_recognizer import PatternRecognizer
from .data_classifier import DataClassifier
from .batch_processor import BatchProcessor

class AdvancedProcessingManager:
    """
    Integration manager for advanced processing capabilities.
    Coordinates AI extraction, pattern recognition, data classification,
    and batch processing features.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the advanced processing manager.
        
        Args:
            config_path (str, optional): Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        
        # Load configuration if provided
        self.config = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        
        # Initialize components with configuration
        ai_config = self.config.get('ai_extractor', {})
        pattern_config = self.config.get('pattern_recognizer', {})
        classifier_config = self.config.get('data_classifier', {})
        batch_config = self.config.get('batch_processor', {})
        
        # Initialize components
        self.ai_extractor = AIExtractor(
            language=ai_config.get('language', 'en'),
            use_gpu=ai_config.get('use_gpu', False)
        )
        
        self.pattern_recognizer = PatternRecognizer(
            confidence_threshold=pattern_config.get('confidence_threshold', 0.75),
            fuzzy_threshold=pattern_config.get('fuzzy_threshold', 85)
        )
        
        self.data_classifier = DataClassifier(
            model_dir=classifier_config.get('model_dir', './models')
        )
        
        self.batch_processor = BatchProcessor(
            max_workers=batch_config.get('max_workers', None),
            use_processes=batch_config.get('use_processes', False),
            chunk_size=batch_config.get('chunk_size', 100),
            log_dir=batch_config.get('log_dir', './logs')
        )
        
        self.logger.info("Advanced Processing Manager initialized")
    
    def process_text_content(self, text: str, extract_entities: bool = True, 
                           extract_patterns: bool = True, custom_patterns: Optional[Dict] = None,
                           classify: bool = False, classifier_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Process text content with multiple extraction methods.
        
        Args:
            text (str): Text content to process
            extract_entities (bool): Whether to extract named entities
            extract_patterns (bool): Whether to recognize patterns
            custom_patterns (Dict, optional): Custom patterns for recognition
            classify (bool): Whether to classify the text
            classifier_name (str, optional): Name of classifier to use
            
        Returns:
            Dict[str, Any]: Processing results
        """
        results = {}
        
        # Extract entities and information using AI
        if extract_entities:
            ai_results = self.ai_extractor.process_document(
                text=text,
                extract_topics=True,
                extract_entities=True,
                extract_key_phrases=True,
                extract_relationships=True,
                classify=True
            )
            results['ai_extraction'] = ai_results
        
        # Recognize patterns
        if extract_patterns:
            pattern_results = self.pattern_recognizer.recognize_pattern(
                text=text,
                include_sensitive=False
            )
            
            # If custom patterns are provided, extract them too
            if custom_patterns:
                custom_results = self.ai_extractor.extract_custom_entities(text, custom_patterns)
                pattern_results['custom'] = custom_results
                
            results['pattern_recognition'] = pattern_results
        
        # Classify text if requested
        if classify and classifier_name:
            # Prepare data for classification
            data = {
                'text': text,
                'length': len(text),
                'word_count': len(text.split())
            }
            
            # Add recognized entities as features
            if extract_entities and 'ai_extraction' in results:
                entities = results['ai_extraction'].get('entities', {})
                for entity_type, values in entities.items():
                    if values:
                        data[f'has_{entity_type.lower()}'] = True
                        data[f'{entity_type.lower()}_count'] = len(values)
            
            # Add recognized patterns as features
            if extract_patterns and 'pattern_recognition' in results:
                for pattern_type, matches in results['pattern_recognition'].items():
                    if matches:
                        data[f'has_{pattern_type.lower()}'] = True
                        data[f'{pattern_type.lower()}_count'] = len(matches)
            
            # Perform classification
            classification = self.data_classifier.classify(data, classifier_name)
            results['classification'] = classification
        
        return results
    
    def batch_process_documents(self, documents: List[Dict[str, str]], 
                               processing_config: Dict[str, Any],
                               job_name: Optional[str] = None) -> str:
        """
        Process a batch of documents with advanced extraction and recognition.
        
        Args:
            documents (List[Dict[str, str]]): List of documents with text content
            processing_config (Dict[str, Any]): Configuration for processing
            job_name (str, optional): Name for this batch job
            
        Returns:
            str: Job ID for tracking the processing
        """
        if not job_name:
            job_name = f"doc_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create processor function
        def document_processor(doc_batch, **kwargs):
            results = []
            
            for doc in doc_batch:
                # Get text content from document
                text = doc.get('text', '')
                if not text and 'content' in doc:
                    text = doc['content']
                
                # Skip empty documents
                if not text:
                    results.append({
                        'document_id': doc.get('id', 'unknown'),
                        'error': 'No text content found'
                    })
                    continue
                
                # Process with configured options
                try:
                    doc_result = self.process_text_content(
                        text=text,
                        extract_entities=kwargs.get('extract_entities', True),
                        extract_patterns=kwargs.get('extract_patterns', True),
                        custom_patterns=kwargs.get('custom_patterns'),
                        classify=kwargs.get('classify', False),
                        classifier_name=kwargs.get('classifier_name')
                    )
                    
                    # Add document identifiers to results
                    doc_result['document_id'] = doc.get('id', 'unknown')
                    doc_result['title'] = doc.get('title', '')
                    doc_result['source'] = doc.get('source', '')
                    
                    results.append(doc_result)
                except Exception as e:
                    results.append({
                        'document_id': doc.get('id', 'unknown'),
                        'error': str(e)
                    })
            
            return results
        
        # Convert documents to DataFrame for batch processing
        df = pd.DataFrame(documents)
        
        # Start batch processing
        job_id = self.batch_processor.process_dataframe(
            df=df,
            processor_func=document_processor,
            job_name=job_name,
            use_tqdm=True,
            **processing_config
        )
        
        return job_id
    
    def process_dataframe(self, df: pd.DataFrame, column_config: Dict[str, Any],
                          job_name: Optional[str] = None) -> str:
        """
        Process DataFrame columns with advanced pattern recognition.
        
        Args:
            df (pd.DataFrame): DataFrame to process
            column_config (Dict[str, Any]): Configuration for column processing
            job_name (str, optional): Name for this batch job
            
        Returns:
            str: Job ID for tracking the processing
        """
        if not job_name:
            job_name = f"df_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create processor function
        def dataframe_processor(df_chunk, **kwargs):
            result_df = df_chunk.copy()
            
            # Process text columns
            text_columns = kwargs.get('text_columns', [])
            for col in text_columns:
                if col not in df_chunk.columns:
                    continue
                
                # Create new columns for extracted entities
                if kwargs.get('extract_entities', False):
                    # Process each row
                    entities_list = []
                    for text in df_chunk[col].fillna(''):
                        if not text:
                            entities_list.append({})
                            continue
                            
                        # Extract entities
                        try:
                            entities = self.ai_extractor.extract_entities(text)
                            entities_list.append(entities)
                        except Exception as e:
                            entities_list.append({'error': str(e)})
                    
                    # Add as new column
                    result_df[f"{col}_entities"] = entities_list
                
                # Create new columns for pattern recognition
                if kwargs.get('extract_patterns', False):
                    # Process each row
                    patterns_list = []
                    for text in df_chunk[col].fillna(''):
                        if not text:
                            patterns_list.append({})
                            continue
                            
                        # Recognize patterns
                        try:
                            patterns = self.pattern_recognizer.recognize_pattern(text)
                            patterns_list.append(patterns)
                        except Exception as e:
                            patterns_list.append({'error': str(e)})
                    
                    # Add as new column
                    result_df[f"{col}_patterns"] = patterns_list
            
            # Apply data classification if configured
            if kwargs.get('classify', False) and kwargs.get('classifier_name'):
                classifier_name = kwargs.get('classifier_name')
                
                # Prepare each row for classification
                classifications = []
                
                for idx, row in df_chunk.iterrows():
                    # Create a feature dictionary from the row
                    data = {}
                    feature_columns = kwargs.get('feature_columns', [])
                    
                    for fcol in feature_columns:
                        if fcol in row:
                            data[fcol] = row[fcol]
                    
                    # Perform classification
                    try:
                        classification = self.data_classifier.classify(data, classifier_name)
                        classifications.append(classification)
                    except Exception as e:
                        classifications.append({'error': str(e)})
                
                # Add classification results
                result_df['classification'] = classifications
            
            return result_df
        
        # Start batch processing
        job_id = self.batch_processor.process_dataframe(
            df=df,
            processor_func=dataframe_processor,
            job_name=job_name,
            use_tqdm=True,
            **column_config
        )
        
        return job_id
    
    def analyze_document_structure(self, text: str) -> Dict[str, Any]:
        """
        Analyze document structure to identify sections, headings, and logical flow.
        
        Args:
            text (str): Document text
            
        Returns:
            Dict[str, Any]: Structure analysis results
        """
        # Split text into lines
        lines = text.split('\n')
        
        # Identify potential headings
        headings = []
        sections = []
        current_section = {'title': '', 'content': '', 'level': 0}
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            # Check if line is a heading
            is_heading = False
            heading_level = 0
            
            if line.isupper() and len(line) < 100:
                # All uppercase, likely a main heading
                is_heading = True
                heading_level = 1
            elif re.match(r'^#{1,6}\s+', line):
                # Markdown heading
                heading_level = len(re.match(r'^(#+)', line).group(1))
                is_heading = True
                line = re.sub(r'^#{1,6}\s+', '', line)
            elif re.match(r'^[0-9]+\.[0-9]*\s+[A-Z]', line):
                # Numbered heading (e.g., "1.2 Title")
                is_heading = True
                heading_level = 2
            elif len(line) < 80 and i < len(lines) - 1 and not lines[i+1].strip():
                # Short line followed by an empty line
                if line[0].isupper() and line[-1] not in '.,:;?!':
                    is_heading = True
                    heading_level = 3
            
            if is_heading:
                # Save previous section if it has content
                if current_section['content'].strip():
                    sections.append(current_section)
                
                # Create new section
                current_section = {
                    'title': line,
                    'content': '',
                    'level': heading_level,
                    'start_line': i,
                    'end_line': i
                }
                
                headings.append({
                    'text': line,
                    'level': heading_level,
                    'line': i
                })
            else:
                # Add line to current section content
                if current_section['content']:
                    current_section['content'] += '\n'
                current_section['content'] += line
                current_section['end_line'] = i
        
        # Add the last section
        if current_section['content'].strip():
            sections.append(current_section)
        
        # Analyze structure
        structure = {
            'headings': headings,
            'sections': sections,
            'total_sections': len(sections),
            'max_heading_level': max([h['level'] for h in headings]) if headings else 0,
            'has_hierarchical_structure': len(set([h['level'] for h in headings])) > 1 if headings else False
        }
        
        return structure
    
    def identify_data_tables(self, text: str) -> List[Dict[str, Any]]:
        """
        Identify potential data tables in text content.
        
        Args:
            text (str): Document text
            
        Returns:
            List[Dict[str, Any]]: Identified tables
        """
        tables = []
        lines = text.split('\n')
        
        # Look for common table markers
        in_table = False
        current_table = {'lines': [], 'start_line': 0, 'end_line': 0}
        separator_pattern = re.compile(r'^[\s\-+|]+$')
        
        for i, line in enumerate(lines):
            # Check for separator lines or pipe-delimited content
            is_separator = separator_pattern.match(line)
            has_multiple_pipes = line.count('|') > 1
            has_multiple_tabs = line.count('\t') > 1
            has_aligned_spaces = re.search(r'\s{2,}[^\s]', line) and re.search(r'[^\s]\s{2,}', line)
            
            # Table markers
            is_table_row = has_multiple_pipes or has_multiple_tabs or has_aligned_spaces
            
            if not in_table and (is_separator or is_table_row):
                # Start of a new table
                in_table = True
                current_table = {
                    'lines': [line],
                    'start_line': i,
                    'end_line': i,
                    'format': 'pipe' if has_multiple_pipes else ('tab' if has_multiple_tabs else 'aligned')
                }
            elif in_table:
                if is_table_row or is_separator or line.strip() == '':
                    # Continue the table
                    current_table['lines'].append(line)
                    current_table['end_line'] = i
                else:
                    # End of table if we have 2+ non-empty rows
                    if len([l for l in current_table['lines'] if l.strip()]) >= 2:
                        tables.append(current_table)
                    in_table = False
        
        # Add the last table if it exists
        if in_table and len([l for l in current_table['lines'] if l.strip()]) >= 2:
            tables.append(current_table)
        
        # Process identified tables
        parsed_tables = []
        for table_data in tables:
            table = self._parse_table(table_data)
            if table and len(table.get('rows', [])) > 0:
                parsed_tables.append(table)
        
        return parsed_tables
    
    def _parse_table(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse a detected table into structured data"""
        table_format = table_data.get('format', 'unknown')
        lines = table_data['lines']
        
        # Remove empty lines
        lines = [line for line in lines if line.strip()]
        if not lines:
            return None
        
        if table_format == 'pipe':
            return self._parse_pipe_table(lines)
        elif table_format == 'tab':
            return self._parse_tab_table(lines)
        elif table_format == 'aligned':
            return self._parse_aligned_table(lines)
        else:
            return {
                'format': 'unknown',
                'raw_lines': lines,
                'start_line': table_data['start_line'],
                'end_line': table_data['end_line']
            }
    
    def _parse_pipe_table(self, lines: List[str]) -> Dict[str, Any]:
        """Parse a pipe-delimited table"""
        # Check if it's a markdown table
        is_markdown = False
        if len(lines) > 1 and re.match(r'^\s*\|[\s\-+:]+\|\s*$', lines[1]):
            is_markdown = True
        
        # Extract header and rows
        headers = []
        rows = []
        
        # Parse header row
        if lines:
            header_line = lines[0].strip()
            header_cells = [cell.strip() for cell in header_line.split('|')]
            # Remove empty cells at start/end if present
            if not header_cells[0]:
                header_cells = header_cells[1:]
            if not header_cells[-1]:
                header_cells = header_cells[:-1]
            headers = header_cells
        
        # Skip separator line if markdown
        start_row = 2 if is_markdown else 1
        
        # Parse data rows
        for line in lines[start_row:]:
            line = line.strip()
            if not line or (is_markdown and re.match(r'^\s*\|[\s\-+:]+\|\s*$', line)):
                continue
                
            cells = [cell.strip() for cell in line.split('|')]
            # Remove empty cells at start/end if present
            if not cells[0]:
                cells = cells[1:]
            if not cells[-1]:
                cells = cells[:-1]
            
            if cells:
                rows.append(cells)
        
        return {
            'format': 'pipe',
            'is_markdown': is_markdown,
            'headers': headers,
            'rows': rows,
            'num_rows': len(rows),
            'num_columns': len(headers)
        }
    
    def _parse_tab_table(self, lines: List[str]) -> Dict[str, Any]:
        """Parse a tab-delimited table"""
        headers = []
        rows = []
        
        # Parse header row
        if lines:
            header_cells = [cell.strip() for cell in lines[0].split('\t')]
            headers = header_cells
        
        # Parse data rows
        for line in lines[1:]:
            cells = [cell.strip() for cell in line.split('\t')]
            if cells:
                rows.append(cells)
        
        return {
            'format': 'tab',
            'headers': headers,
            'rows': rows,
            'num_rows': len(rows),
            'num_columns': len(headers)
        }
    
    def _parse_aligned_table(self, lines: List[str]) -> Dict[str, Any]:
        """Parse a space-aligned table"""
        # This is more complex as we need to detect column boundaries
        if not lines:
            return None
            
        # Try to detect column boundaries based on multiple spaces
        col_boundaries = []
        
        # Analyze first few lines to find consistent column breaks
        num_lines_to_analyze = min(5, len(lines))
        
        for i in range(num_lines_to_analyze):
            line = lines[i]
            # Find positions of multiple spaces
            spaces = [m.start() for m in re.finditer(r'\s{2,}', line)]
            
            if not col_boundaries:
                col_boundaries = spaces
            else:
                # Keep only boundaries that appear consistently
                new_boundaries = []
                for boundary in col_boundaries:
                    # Check if there's a boundary close to this position
                    if any(abs(boundary - s) <= 1 for s in spaces):
                        new_boundaries.append(boundary)
                col_boundaries = new_boundaries
        
        # If no clear boundaries found, fallback to simple splitting
        if not col_boundaries:
            return {
                'format': 'aligned',
                'raw_lines': lines,
                'note': 'Could not determine column boundaries'
            }
        
        # Sort boundaries
        col_boundaries.sort()
        
        # Extract data using boundaries
        headers = []
        rows = []
        
        # Parse header
        if lines:
            header_line = lines[0]
            header_cells = []
            
            start_pos = 0
            for boundary in col_boundaries:
                cell = header_line[start_pos:boundary].strip()
                header_cells.append(cell)
                start_pos = boundary
            
            # Last cell
            header_cells.append(header_line[start_pos:].strip())
            headers = header_cells
        
        # Parse rows
        for line in lines[1:]:
            if not line.strip():
                continue
                
            cells = []
            start_pos = 0
            
            for boundary in col_boundaries:
                if boundary < len(line):
                    cell = line[start_pos:boundary].strip()
                    cells.append(cell)
                    start_pos = boundary
                else:
                    cells.append('')
            
            # Last cell
            if start_pos < len(line):
                cells.append(line[start_pos:].strip())
            
            if cells:
                rows.append(cells)
        
        return {
            'format': 'aligned',
            'headers': headers,
            'rows': rows,
            'num_rows': len(rows),
            'num_columns': len(headers)
        }
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get status of a processing job.
        
        Args:
            job_id (str): ID of the job
            
        Returns:
            Dict[str, Any]: Job status information
        """
        return self.batch_processor.get_job_status(job_id)
    
    def get_job_results(self, job_id: str, as_dataframe: bool = False) -> Any:
        """
        Get results of a processing job.
        
        Args:
            job_id (str): ID of the job
            as_dataframe (bool): Whether to return results as DataFrame
            
        Returns:
            Any: Job results
        """
        return self.batch_processor.get_job_results(job_id, as_dataframe)
    
    def save_job_results(self, job_id: str, output_path: str, format: str = 'json') -> Dict[str, Any]:
        """
        Save job results to a file.
        
        Args:
            job_id (str): ID of the job
            output_path (str): Path to save results
            format (str): Output format (json, csv, xlsx)
            
        Returns:
            Dict[str, Any]: Save operation result
        """
        return self.batch_processor.save_job_results(job_id, output_path, format)
    
    def create_default_classifier(self, data_type: str, sample_data: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a default classifier for a data type.
        
        Args:
            data_type (str): Type of data (text, person, document, etc.)
            sample_data (List[Dict[str, Any]], optional): Sample data
            
        Returns:
            Dict[str, Any]: Classifier creation result
        """
        return self.data_classifier.create_default_classifier(data_type, sample_data)
    
    def add_custom_pattern(self, pattern_name: str, pattern_config: Dict[str, Any]) -> None:
        """
        Add a custom pattern for recognition.
        
        Args:
            pattern_name (str): Name of the pattern
            pattern_config (Dict[str, Any]): Pattern configuration
        """
        return self.pattern_recognizer.add_pattern(pattern_name, pattern_config)
