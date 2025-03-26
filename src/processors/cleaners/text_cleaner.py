# src/processors/cleaners/text_cleaner.py

import re
import unicodedata
import html
from typing import List, Dict, Optional, Union, Callable


class TextCleaner:
    """
    Text cleaning and normalization utility.
    """
    
    def __init__(self):
        """Initialize the text cleaner."""
        pass
    
    def clean_text(self, text: str, 
                  remove_html: bool = True,
                  fix_unicode: bool = True,
                  normalize_whitespace: bool = True,
                  remove_punctuation: bool = False,
                  lowercase: bool = False) -> str:
        """
        Clean and normalize text with multiple options.
        
        Args:
            text: Text to clean
            remove_html: Remove HTML tags and decode HTML entities
            fix_unicode: Normalize Unicode characters
            normalize_whitespace: Replace multiple whitespace with single space
            remove_punctuation: Remove punctuation characters
            lowercase: Convert to lowercase
            
        Returns:
            str: Cleaned text
        """
        if text is None:
            return ""
            
        # Convert to string if not already
        if not isinstance(text, str):
            text = str(text)
            
        # Fix Unicode
        if fix_unicode:
            text = self.fix_unicode(text)
            
        # Remove HTML
        if remove_html:
            text = self.remove_html(text)
            
        # Remove punctuation
        if remove_punctuation:
            text = self.remove_punctuation(text)
            
        # Normalize whitespace
        if normalize_whitespace:
            text = self.normalize_whitespace(text)
            
        # Convert to lowercase
        if lowercase:
            text = text.lower()
            
        return text
    
    def fix_unicode(self, text: str) -> str:
        """
        Normalize Unicode characters.
        
        Args:
            text: Text to normalize
            
        Returns:
            str: Normalized text
        """
        # Normalize unicode characters
        text = unicodedata.normalize('NFKC', text)
        
        # Replace common problematic characters
        replacements = {
            '\u2018': "'",  # Left single quotation mark
            '\u2019': "'",  # Right single quotation mark
            '\u201c': '"',  # Left double quotation mark
            '\u201d': '"',  # Right double quotation mark
            '\u2013': '-',  # En dash
            '\u2014': '--',  # Em dash
            '\u00a0': ' ',  # Non-breaking space
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
            
        return text
    
    def remove_html(self, text: str) -> str:
        """
        Remove HTML tags and decode HTML entities.
        
        Args:
            text: Text with potential HTML
            
        Returns:
            str: Text with HTML removed
        """
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        
        return text
    
    def normalize_whitespace(self, text: str) -> str:
        """
        Normalize whitespace characters.
        
        Args:
            text: Text to normalize
            
        Returns:
            str: Text with normalized whitespace
        """
        # Replace all whitespace characters with a single space
        text = re.sub(r'\s+', ' ', text)
        
        # Trim leading and trailing whitespace
        text = text.strip()
        
        return text
    
    def remove_punctuation(self, text: str) -> str:
        """
        Remove punctuation characters.
        
        Args:
            text: Text to process
            
        Returns:
            str: Text without punctuation
        """
        # Remove all punctuation except apostrophes in words
        text = re.sub(r'[^\w\s\']', ' ', text)
        text = re.sub(r'\s\'|\'\s', ' ', text)  # Remove standalone apostrophes
        
        return self.normalize_whitespace(text)
    
    def remove_numbers(self, text: str) -> str:
        """
        Remove numbers from text.
        
        Args:
            text: Text to process
            
        Returns:
            str: Text without numbers
        """
        text = re.sub(r'\d+', '', text)
        return self.normalize_whitespace(text)
    
    def remove_special_characters(self, text: str, keep_chars: str = '') -> str:
        """
        Remove special characters from text.
        
        Args:
            text: Text to process
            keep_chars: Characters to keep (in addition to alphanumerics)
            
        Returns:
            str: Text without special characters
        """
        pattern = r'[^a-zA-Z0-9\s' + re.escape(keep_chars) + r']'
        text = re.sub(pattern, '', text)
        return self.normalize_whitespace(text)
    
    def remove_stopwords(self, text: str, stopwords: List[str]) -> str:
        """
        Remove stopwords from text.
        
        Args:
            text: Text to process
            stopwords: List of stopwords to remove
            
        Returns:
            str: Text without stopwords
        """
        words = text.split()
        filtered_words = [word for word in words if word.lower() not in stopwords]
        return ' '.join(filtered_words)
    
    def replace_text(self, text: str, replacements: Dict[str, str], 
                     case_sensitive: bool = False) -> str:
        """
        Replace specific text patterns.
        
        Args:
            text: Text to process
            replacements: Dictionary of {find: replace} pairs
            case_sensitive: Whether to perform case-sensitive replacements
            
        Returns:
            str: Text with replacements applied
        """
        for old, new in replacements.items():
            if case_sensitive:
                text = text.replace(old, new)
            else:
                text = re.sub(re.escape(old), new, text, flags=re.IGNORECASE)
                
        return text
    
    def clean_dataframe_text(self, df, columns: List[str], **kwargs) -> None:
        """
        Clean text in specified dataframe columns.
        
        Args:
            df: pandas DataFrame
            columns: List of column names to clean
            **kwargs: Arguments to pass to clean_text method
            
        Returns:
            None (modifies df in place)
        """
        for col in columns:
            if col in df.columns:
                df[col] = df[col].astype(str).apply(lambda x: self.clean_text(x, **kwargs))


# src/processors/cleaners/deduplicator.py

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union, Callable
from rapidfuzz import fuzz


class Deduplicator:
    """
    Data deduplication utility for lists and DataFrames.
    """
    
    def __init__(self, threshold: float = 0.9):
        """
        Initialize the deduplicator.
        
        Args:
            threshold: Similarity threshold for fuzzy matching (0.0 to 1.0)
        """
        self.threshold = threshold
    
    def deduplicate_list(self, items: List[str], fuzzy: bool = False) -> List[str]:
        """
        Remove duplicate items from a list.
        
        Args:
            items: List of items to deduplicate
            fuzzy: Whether to use fuzzy matching
            
        Returns:
            List[str]: Deduplicated list
        """
        if not fuzzy:
            # Simple deduplication preserving order
            deduplicated = []
            seen = set()
            for item in items:
                if item not in seen:
                    deduplicated.append(item)
                    seen.add(item)
            return deduplicated
        else:
            # Fuzzy deduplication
            return self._fuzzy_deduplicate_list(items)
    
    def _fuzzy_deduplicate_list(self, items: List[str]) -> List[str]:
        """
        Remove duplicate items using fuzzy matching.
        
        Args:
            items: List of items to deduplicate
            
        Returns:
            List[str]: Deduplicated list
        """
        if not items:
            return []
            
        # Start with the first item
        result = [items[0]]
        
        # Compare each item with items already in the result
        for item in items[1:]:
            # Check if the item is similar to any item in the result
            is_duplicate = False
            for existing in result:
                similarity = fuzz.ratio(item.lower(), existing.lower()) / 100.0
                if similarity >= self.threshold:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                result.append(item)
                
        return result
    
    def deduplicate_dataframe(self, df: pd.DataFrame, 
                             columns: List[str], 
                             fuzzy: bool = False,
                             keep: str = 'first') -> pd.DataFrame:
        """
        Remove duplicate rows from a DataFrame.
        
        Args:
            df: DataFrame to deduplicate
            columns: List of column names to consider for duplication
            fuzzy: Whether to use fuzzy matching
            keep: Which duplicate to keep ('first', 'last', or False to drop all duplicates)
            
        Returns:
            pd.DataFrame: Deduplicated DataFrame
        """
        if not fuzzy:
            # Use pandas built-in deduplication
            return df.drop_duplicates(subset=columns, keep=keep)
        else:
            # Fuzzy deduplication
            return self._fuzzy_deduplicate_dataframe(df, columns, keep)
    
    def _fuzzy_deduplicate_dataframe(self, df: pd.DataFrame,
                                   columns: List[str],
                                   keep: str = 'first') -> pd.DataFrame:
        """
        Remove duplicate rows using fuzzy matching.
        
        Args:
            df: DataFrame to deduplicate
            columns: List of column names to consider for duplication
            keep: Which duplicate to keep ('first', 'last')
            
        Returns:
            pd.DataFrame: Deduplicated DataFrame
        """
        if df.empty:
            return df
            
        # Create a copy of the dataframe
        result = df.copy()
        
        # Add a column to track duplicates
        result['__is_duplicate'] = False
        
        # Create a concatenated string of columns for comparison
        result['__concat'] = result[columns].astype(str).apply(lambda x: ' '.join(x), axis=1)
        
        # Process rows
        for i in range(len(result)):
            # Skip if already marked as duplicate
            if result.loc[i, '__is_duplicate']:
                continue
                
            # Get the concatenated string for the current row
            current = result.loc[i, '__concat']
            
            # Compare with all subsequent rows
            for j in range(i + 1, len(result)):
                # Skip if already marked as duplicate
                if result.loc[j, '__is_duplicate']:
                    continue
                    
                # Get the concatenated string for the comparison row
                comparison = result.loc[j, '__concat']
                
                # Calculate similarity
                similarity = fuzz.ratio(current.lower(), comparison.lower()) / 100.0
                
                # Mark as duplicate if similarity is above threshold
                if similarity >= self.threshold:
                    if keep == 'first':
                        result.loc[j, '__is_duplicate'] = True
                    elif keep == 'last':
                        result.loc[i, '__is_duplicate'] = True
                        break
                    else:  # keep=False
                        result.loc[i, '__is_duplicate'] = True
                        result.loc[j, '__is_duplicate'] = True
        
        # Remove the helper columns and filter out duplicates
        result = result[~result['__is_duplicate']].drop(columns=['__is_duplicate', '__concat'])
        
        return result
    
    def find_similar_items(self, items: List[str], threshold: Optional[float] = None) -> Dict[str, List[str]]:
        """
        Find similar items in a list.
        
        Args:
            items: List of items to compare
            threshold: Similarity threshold (overrides the default)
            
        Returns:
            Dict[str, List[str]]: Dictionary of {item: [similar_items]}
        """
        if threshold is None:
            threshold = self.threshold
            
        result = {}
        
        for i, item1 in enumerate(items):
            similar = []
            for j, item2 in enumerate(items):
                if i != j:
                    similarity = fuzz.ratio(item1.lower(), item2.lower()) / 100.0
                    if similarity >= threshold:
                        similar.append(item2)
            
            if similar:
                result[item1] = similar
                
        return result
