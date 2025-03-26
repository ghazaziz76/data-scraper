# src/advanced_processing/pattern_recognizer.py
import re
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple, Set, Union
import logging
from collections import defaultdict
import jellyfish  # For fuzzy matching
from fuzzywuzzy import fuzz  # For string similarity
import json
import os
from datetime import datetime

class PatternRecognizer:
    """
    Advanced pattern recognition class for detecting complex patterns
    across different data types and formats with fuzzy matching capabilities.
    """
    
    def __init__(self, confidence_threshold: float = 0.75, fuzzy_threshold: int = 85):
        """
        Initialize the pattern recognizer.
        
        Args:
            confidence_threshold (float): Minimum confidence level for pattern matches (0-1)
            fuzzy_threshold (int): Minimum similarity score for fuzzy matching (0-100)
        """
        self.confidence_threshold = confidence_threshold
        self.fuzzy_threshold = fuzzy_threshold
        self.logger = logging.getLogger(__name__)
        
        # Load default patterns
        self.patterns = self._load_default_patterns()
        
        # History of recognized patterns for learning
        self.recognition_history = defaultdict(list)
    
    def _load_default_patterns(self) -> Dict[str, Dict[str, Any]]:
        """
        Load default regex patterns for common data types.
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of pattern types and their settings
        """
        # Extended and improved patterns for common data types
        return {
            "email": {
                "patterns": [
                    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
                ],
                "validation": lambda x: '@' in x and '.' in x.split('@')[1],
                "confidence": 0.95
            },
            "phone": {
                "patterns": [
                    # International format
                    r'\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
                    # US format
                    r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
                    # Simple numeric sequence
                    r'\b\d{8,15}\b'
                ],
                "validation": lambda x: sum(c.isdigit() for c in x) >= 7,
                "confidence": 0.85,
                "formatter": lambda x: re.sub(r'[^0-9+]', '', x)
            },
            "url": {
                "patterns": [
                    r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/\w\.-]*\??[/\w\.-]*',
                    r'www\.(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/\w\.-]*\??[/\w\.-]*'
                ],
                "validation": lambda x: '.' in x and any(tld in x.lower() for tld in ['.com', '.org', '.net', '.edu', '.gov', '.io']),
                "confidence": 0.9
            },
            "date": {
                "patterns": [
                    # ISO format
                    r'\d{4}-\d{2}-\d{2}',
                    # US format
                    r'\d{1,2}/\d{1,2}/\d{2,4}',
                    # European format
                    r'\d{1,2}[-./]\d{1,2}[-./]\d{2,4}',
                    # Written format
                    r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}'
                ],
                "validation": self._validate_date,
                "confidence": 0.8,
                "formatter": self._format_date
            },
            "ssn": {  # US Social Security Number
                "patterns": [
                    r'\d{3}[-]\d{2}[-]\d{4}',
                    r'\b\d{9}\b'
                ],
                "validation": lambda x: len(re.sub(r'[^0-9]', '', x)) == 9,
                "confidence": 0.9,
                "sensitive": True
            },
            "credit_card": {
                "patterns": [
                    r'\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}',
                    r'\d{13,16}'
                ],
                "validation": self._validate_credit_card,
                "confidence": 0.9,
                "sensitive": True,
                "formatter": lambda x: re.sub(r'[^0-9]', '', x)
            },
            "ip_address": {
                "patterns": [
                    # IPv4
                    r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
                    # IPv6
                    r'\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b'
                ],
                "validation": self._validate_ip,
                "confidence": 0.95
            },
            "address": {
                "patterns": [
                    r'\d+\s+[A-Za-z0-9\s,.]+(?:Avenue|Ave|Street|St|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Plaza|Plz|Square|Sq)\W+(?:[A-Za-z]+\W+)?(?:[A-Z]{2})?\W+\d{5}(?:-\d{4})?',
                    r'\d+\s+[A-Za-z0-9\s,.]+'
                ],
                "confidence": 0.7,
                "requires_context": True
            },
            "person_name": {
                "patterns": [
                    r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',
                    r'\b[A-Z][a-z]+\s+[A-Z]\.\s+[A-Z][a-z]+\b',
                    r'\bDr\.\s+[A-Z][a-z]+\b',
                    r'\bMr\.\s+[A-Z][a-z]+\b',
                    r'\bMs\.\s+[A-Z][a-z]+\b',
                    r'\bMrs\.\s+[A-Z][a-z]+\b'
                ],
                "confidence": 0.7,
                "requires_context": True
            },
            "company_name": {
                "patterns": [
                    r'\b[A-Z][A-Za-z0-9\s]+(?:Inc|LLC|Ltd|Corp|Corporation|Company)\b',
                    r'\b[A-Z][A-Za-z0-9\s]+(?:Inc\.|LLC\.|Ltd\.|Corp\.|Corporation)\b'
                ],
                "confidence": 0.75,
                "requires_context": True
            }
        }
    
    def _validate_date(self, date_str: str) -> bool:
        """Validate if a string is a plausible date"""
        # Remove common separators and check if it has appropriate length
        clean_date = re.sub(r'[/\-\s,.]', '', date_str)
        if not clean_date.isdigit():
            return False
        
        # For numeric-only dates
        if len(clean_date) < 6 or len(clean_date) > 8:
            return False
        
        return True
    
    def _format_date(self, date_str: str) -> str:
        """Format date to ISO format when possible"""
        try:
            # Try to parse various date formats
            for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y', '%m.%d.%Y', '%d.%m.%Y'):
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # Handle written dates like "January 1, 2020"
            match = re.match(r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})', date_str)
            if match:
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month_abbr = match.group(1).lower()[:3]
                if month_abbr in month_map:
                    month = month_map[month_abbr]
                    day = int(match.group(2))
                    year = int(match.group(3))
                    return f"{year:04d}-{month:02d}-{day:02d}"
        except Exception:
            pass
        
        # Return original if parsing fails
        return date_str
    
    def _validate_credit_card(self, card_num: str) -> bool:
        """Validate a credit card number using Luhn algorithm"""
        # Remove non-digit characters
        card_num = re.sub(r'[^0-9]', '', card_num)
        
        # Check if length is valid for major credit cards
        if len(card_num) < 13 or len(card_num) > 19:
            return False
        
        # Luhn algorithm check
        total = 0
        reverse_digits = card_num[::-1]
        for i, digit in enumerate(reverse_digits):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        
        return total % 10 == 0
    
    def _validate_ip(self, ip: str) -> bool:
        """Validate an IP address"""
        # Check IPv4
        if re.match(r'^(?:\d{1,3}\.){3}\d{1,3}$', ip):
            octets = ip.split('.')
            return all(0 <= int(octet) <= 255 for octet in octets)
        
        # Check IPv6 (simplified)
        if ':' in ip:
            return len(ip.split(':')) <= 8
        
        return False
    
    def add_pattern(self, pattern_name: str, pattern_config: Dict[str, Any]) -> None:
        """
        Add a new pattern for recognition.
        
        Args:
            pattern_name (str): Name of the pattern
            pattern_config (Dict[str, Any]): Configuration including patterns, validation, confidence
        """
        self.patterns[pattern_name] = pattern_config
        self.logger.info(f"Added new pattern: {pattern_name}")
    
    def remove_pattern(self, pattern_name: str) -> bool:
        """
        Remove a pattern.
        
        Args:
            pattern_name (str): Name of the pattern to remove
            
        Returns:
            bool: True if removal was successful, False otherwise
        """
        if pattern_name in self.patterns:
            del self.patterns[pattern_name]
            self.logger.info(f"Removed pattern: {pattern_name}")
            return True
        return False
    
    def recognize_pattern(self, text: str, pattern_types: Optional[List[str]] = None, 
                          include_sensitive: bool = False) -> Dict[str, List[Dict[str, Any]]]:
        """
        Recognize patterns in text.
        
        Args:
            text (str): The text to analyze
            pattern_types (List[str], optional): Specific pattern types to recognize
            include_sensitive (bool): Whether to include sensitive pattern types
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary of pattern types and their matches
        """
        results = defaultdict(list)
        
        # Determine which patterns to use
        patterns_to_use = {}
        for pattern_name, pattern_config in self.patterns.items():
            if pattern_types and pattern_name not in pattern_types:
                continue
            if not include_sensitive and pattern_config.get('sensitive', False):
                continue
            patterns_to_use[pattern_name] = pattern_config
        
        # Apply each pattern
        for pattern_name, pattern_config in patterns_to_use.items():
            pattern_list = pattern_config['patterns']
            confidence = pattern_config.get('confidence', self.confidence_threshold)
            validation_func = pattern_config.get('validation', lambda x: True)
            formatter_func = pattern_config.get('formatter', lambda x: x)
            
            for pattern in pattern_list:
                matches = re.finditer(pattern, text)
                
                for match in matches:
                    match_text = match.group(0)
                    
                    # Validate the match
                    if validation_func(match_text):
                        # Format the match if needed
                        formatted_match = formatter_func(match_text)
                        
                        # Check if this match is already in results
                        is_duplicate = False
                        for existing_match in results[pattern_name]:
                            if existing_match['match'] == formatted_match:
                                is_duplicate = True
                                break
                        
                        if not is_duplicate:
                            match_info = {
                                'match': formatted_match,
                                'position': match.span(),
                                'confidence': confidence,
                                'context': text[max(0, match.start() - 20):min(len(text), match.end() + 20)]
                            }
                            results[pattern_name].append(match_info)
                            
                            # Record in recognition history for learning
                            self.recognition_history[pattern_name].append(formatted_match)
        
        return dict(results)
    
    def find_data_patterns(self, df: pd.DataFrame, sample_size: int = 100) -> Dict[str, Dict[str, Any]]:
        """
        Identify patterns and data types in DataFrame columns.
        
        Args:
            df (pd.DataFrame): The DataFrame to analyze
            sample_size (int): Number of rows to sample for analysis
            
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of column names and detected patterns
        """
        results = {}
        
        # Sample rows for analysis
        sample = df.sample(min(sample_size, len(df))) if len(df) > sample_size else df
        
        for column in df.columns:
            column_data = sample[column].astype(str).fillna('').tolist()
            pattern_counts = defaultdict(int)
            
            # Find patterns in each column's data
            for value in column_data:
                if not value or value.lower() in ('nan', 'null', 'none', ''):
                    continue
                    
                patterns_found = self.recognize_pattern(value)
                for pattern_type, matches in patterns_found.items():
                    if matches:
                        pattern_counts[pattern_type] += 1
            
            # Determine primary pattern based on frequency
            primary_pattern = None
            pattern_confidence = 0.0
            
            if pattern_counts:
                most_common_pattern = max(pattern_counts.items(), key=lambda x: x[1])
                pattern_frequency = most_common_pattern[1] / len([x for x in column_data if x])
                
                if pattern_frequency >= self.confidence_threshold:
                    primary_pattern = most_common_pattern[0]
                    pattern_confidence = pattern_frequency
            
            # Determine data type based on values
            data_type = self._infer_data_type(column_data)
            
            results[column] = {
                'primary_pattern': primary_pattern,
                'pattern_confidence': pattern_confidence,
                'detected_patterns': dict(pattern_counts),
                'inferred_data_type': data_type,
                'sample_values': column_data[:5]
            }
        
        return results
    
    def _infer_data_type(self, values: List[str]) -> str:
        """
        Infer the data type of a list of values.
        
        Args:
            values (List[str]): List of values as strings
            
        Returns:
            str: Inferred data type
        """
        # Filter out empty values
        non_empty_values = [v for v in values if v and v.lower() not in ('nan', 'null', 'none', '')]
        if not non_empty_values:
            return 'unknown'
        
        # Check if all values are numeric
        numeric_count = sum(1 for v in non_empty_values if re.match(r'^-?\d+(\.\d+)?$', v))
        if numeric_count / len(non_empty_values) > 0.9:
            # Check if integers or floats
            if all('.' not in v for v in non_empty_values if re.match(r'^-?\d+(\.\d+)?$', v)):
                return 'integer'
            return 'float'
        
        # Check if dates
        date_count = sum(1 for v in non_empty_values if self._validate_date(v))
        if date_count / len(non_empty_values) > 0.8:
            return 'date'
        
        # Check if boolean
        bool_values = {'true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n'}
        if all(v.lower() in bool_values for v in non_empty_values):
            return 'boolean'
        
        # Default to string
        return 'string'
    
    def find_similar_values(self, values: List[str], threshold: Optional[int] = None) -> Dict[str, List[str]]:
        """
        Find similar values using fuzzy matching.
        
        Args:
            values (List[str]): List of values to compare
            threshold (int, optional): Similarity threshold (0-100)
            
        Returns:
            Dict[str, List[str]]: Dictionary of canonical values and their similar values
        """
        if threshold is None:
            threshold = self.fuzzy_threshold
        
        # Remove duplicates and empty values
        unique_values = list(set(v for v in values if v and v.lower() not in ('nan', 'null', 'none')))
        if len(unique_values) <= 1:
            return {}
        
        # Group similar values
        similar_groups = {}
        processed_values = set()
        
        for i, value1 in enumerate(unique_values):
            if value1 in processed_values:
                continue
                
            group = [value1]
            for j, value2 in enumerate(unique_values):
                if i != j and value2 not in processed_values:
                    similarity = fuzz.ratio(value1.lower(), value2.lower())
                    if similarity >= threshold:
                        group.append(value2)
                        processed_values.add(value2)
            
            if len(group) > 1:
                # Use the most frequent value as canonical
                canonical = value1
                similar_groups[canonical] = [v for v in group if v != canonical]
                processed_values.add(value1)
        
        return similar_groups
    
    def suggest_pattern_improvements(self) -> Dict[str, Any]:
        """
        Suggest improvements to patterns based on recognition history.
        
        Returns:
            Dict[str, Any]: Suggestions for pattern improvements
        """
        suggestions = {}
        
        for pattern_type, history in self.recognition_history.items():
            if len(history) < 10:
                continue
                
            unique_matches = list(set(history))
            if len(unique_matches) < 5:
                continue
            
            # Analyze matches for common characteristics
            if pattern_type == 'phone':
                self._suggest_phone_improvements(unique_matches, suggestions)
            elif pattern_type == 'email':
                self._suggest_email_improvements(unique_matches, suggestions)
            elif pattern_type == 'date':
                self._suggest_date_improvements(unique_matches, suggestions)
        
        return suggestions
    
    def _suggest_phone_improvements(self, matches: List[str], suggestions: Dict[str, Any]) -> None:
        """Suggest improvements for phone number patterns"""
        formats = defaultdict(int)
        for match in matches:
            # Strip non-digit characters to analyze format
            digits = re.sub(r'\D', '', match)
            digit_count = len(digits)
            formats[digit_count] += 1
        
        # Find common digit counts not in patterns
        common_formats = [count for count, freq in formats.items() 
                         if freq >= len(matches) * 0.1 and count >= 7]
        
        # Suggest new patterns if needed
        if common_formats:
            if 'phone' not in suggestions:
                suggestions['phone'] = {'new_patterns': []}
            
            for digit_count in common_formats:
                if not any(f'\\b\\d{{{digit_count}}}\\b' in p for p in self.patterns['phone']['patterns']):
                    suggestions['phone']['new_patterns'].append(f'\\b\\d{{{digit_count}}}\\b')
    
    def _suggest_email_improvements(self, matches: List[str], suggestions: Dict[str, Any]) -> None:
        """Suggest improvements for email patterns"""
        domains = defaultdict(int)
        for match in matches:
            try:
                domain = match.split('@')[1].lower()
                domains[domain] += 1
            except IndexError:
                continue
        
        # Find common domains
        common_domains = [domain for domain, freq in domains.items() 
                         if freq >= len(matches) * 0.1]
        
        if common_domains:
            if 'email' not in suggestions:
                suggestions['email'] = {'domain_whitelist': []}
            
            suggestions['email']['domain_whitelist'] = common_domains
    
    def _suggest_date_improvements(self, matches: List[str], suggestions: Dict[str, Any]) -> None:
        """Suggest improvements for date patterns"""
        formats = defaultdict(int)
        for match in matches:
            # Look for common separators
            if re.match(r'\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}', match):
                separator = re.sub(r'[0-9]', '', match)[0]
                if re.match(r'\d{1,2}' + re.escape(separator) + r'\d{1,2}' + re.escape(separator) + r'\d{2,4}', match):
                    formats[f'MM{separator}DD{separator}YYYY'] += 1
            elif re.match(r'\d{4}[/.-]\d{1,2}[/.-]\d{1,2}', match):
                separator = re.sub(r'[0-9]', '', match)[0]
                formats[f'YYYY{separator}MM{separator}DD'] += 1
        
        # Find common formats not in patterns
        if formats:
            most_common = max(formats.items(), key=lambda x: x[1])
            if most_common[1] >= len(matches) * 0.2:
                format_str = most_common[0]
                if 'date' not in suggestions:
                    suggestions['date'] = {'new_formats': []}
                
                # Check if this format is already in patterns
                separator = re.sub(r'[A-Za-z]', '', format_str)[0]
                new_pattern = None
                
                if format_str.startswith('MM'):
                    new_pattern = r'\d{1,2}' + re.escape(separator) + r'\d{1,2}' + re.escape(separator) + r'\d{2,4}'
                elif format_str.startswith('YYYY'):
                    new_pattern = r'\d{4}' + re.escape(separator) + r'\d{1,2}' + re.escape(separator) + r'\d{1,2}'
                
                if new_pattern and not any(new_pattern == p for p in self.patterns['date']['patterns']):
                    suggestions['date']['new_formats'].append(new_pattern)
    
    def learn_from_corrections(self, pattern_type: str, false_positives: List[str], 
                              false_negatives: List[str]) -> Dict[str, Any]:
        """
        Learn from user corrections to improve pattern recognition.
        
        Args:
            pattern_type (str): The pattern type to improve
            false_positives (List[str]): Incorrectly matched values
            false_negatives (List[str]): Values that should have been matched
            
        Returns:
            Dict[str, Any]: Changes made to the pattern
        """
        if pattern_type not in self.patterns:
            return {'error': f"Pattern type '{pattern_type}' not found"}
        
        changes = {'updated': False}
        
        # Handle false positives by improving validation
        if false_positives:
            # Analyze false positives to identify common characteristics
            common_chars = self._find_common_characteristics(false_positives)
            
            if common_chars:
                # Create exclusion pattern
                old_validation = self.patterns[pattern_type].get('validation')
                
                # Create new validation function that combines old validation and new exclusions
                def new_validation(x):
                    if old_validation and not old_validation(x):
                        return False
                    
                    # Check if value has any of the common false positive characteristics
                    for char, ratio in common_chars.items():
                        if char in x and ratio > 0.5:  # If characteristic appears in more than half of false positives
                            return False
                    
                    return True
                
                self.patterns[pattern_type]['validation'] = new_validation
                changes['updated'] = True
                changes['false_positives_handled'] = len(false_positives)
        
        # Handle false negatives by adding patterns
        if false_negatives and len(false_negatives) >= 3:
            # Try to identify a common pattern in false negatives
            common_pattern = self._extract_common_pattern(false_negatives)
            
            if common_pattern and common_pattern not in self.patterns[pattern_type]['patterns']:
                self.patterns[pattern_type]['patterns'].append(common_pattern)
                changes['updated'] = True
                changes['new_pattern_added'] = common_pattern
        
        return changes
    
    def _find_common_characteristics(self, values: List[str]) -> Dict[str, float]:
        """Find common characteristics in a list of values"""
        if not values:
            return {}
            
        # Count character frequency
        char_count = defaultdict(int)
        for value in values:
            unique_chars = set(value)
            for char in unique_chars:
                char_count[char] += 1
        
        # Calculate ratio of values containing each character
        return {char: count / len(values) for char, count in char_count.items() if count > 1}
    
    def _extract_common_pattern(self, values: List[str]) -> Optional[str]:
        """Try to extract a common regex pattern from values"""
        if not values:
            return None
        
        # Simple approach: identify character classes
        pattern_parts = []
        min_length = min(len(v) for v in values)
        
        for i in range(min_length):
            chars = [v[i] for v in values]
            
            if all(c.isdigit() for c in chars):
                pattern_parts.append("\\d")
            elif all(c.isalpha() for c in chars):
                if all(c.isupper() for c in chars):
                    pattern_parts.append("[A-Z]")
                elif all(c.islower() for c in chars):
                    pattern_parts.append("[a-z]")
                else:
                    pattern_parts.append("[A-Za-z]")
            elif all(c in '@._-+' for c in chars):
                pattern_parts.append(re.escape(chars[0]))
            else:
                pattern_parts.append(".")
        
        # Add flexible ending
        pattern = ''.join(pattern_parts)
        if all(len(v) > min_length for v in values):
            pattern += ".+"
        
        return pattern if pattern else None
