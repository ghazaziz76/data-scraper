# src/advanced_processing/data_classifier.py
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
import logging
from collections import defaultdict
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import joblib
import os
from datetime import datetime

class DataClassifier:
    """
    Automated data classification system that uses machine learning
    to categorize data based on content and structure.
    """
    
    def __init__(self, model_dir: str = './models'):
        """
        Initialize the data classifier.
        
        Args:
            model_dir (str): Directory to store trained models
        """
        self.model_dir = model_dir
        self.logger = logging.getLogger(__name__)
        
        # Create model directory if it doesn't exist
        os.makedirs(model_dir, exist_ok=True)
        
        # Pre-defined classifiers
        self.classifiers = {}
        
        # Feature extractors
        self.feature_extractors = {
            'text': self._extract_text_features,
            'numeric': self._extract_numeric_features,
            'mixed': self._extract_mixed_features
        }
        
        # Default categories for common data types
        self.default_categories = {
            'text': ['article', 'description', 'title', 'name', 'comment', 'address'],
            'person': ['customer', 'employee', 'contact', 'user', 'author'],
            'document': ['invoice', 'receipt', 'contract', 'report', 'email', 'letter'],
            'contact': ['personal', 'business', 'support', 'sales', 'marketing'],
            'product': ['physical', 'digital', 'service', 'subscription']
        }
    
    def _extract_text_features(self, texts: List[str], max_features: int = 500) -> np.ndarray:
        """
        Extract features from text data using TF-IDF.
        
        Args:
            texts (List[str]): List of text documents
            max_features (int): Maximum number of features to extract
            
        Returns:
            np.ndarray: Feature matrix
        """
        vectorizer = TfidfVectorizer(
            max_features=max_features,
            strip_accents='unicode',
            analyzer='word',
            token_pattern=r'\w{1,}',
            min_df=2,
            max_df=0.9,
            use_idf=True,
            sublinear_tf=True
        )
        
        try:
            return vectorizer.fit_transform(texts).toarray()
        except ValueError as e:
            self.logger.warning(f"Error in text feature extraction: {e}")
            # Fall back to simpler approach
            return self._fallback_text_features(texts)
    
    def _fallback_text_features(self, texts: List[str]) -> np.ndarray:
        """Fallback method for text feature extraction if TF-IDF fails"""
        features = np.zeros((len(texts), 5))
        
        for i, text in enumerate(texts):
            # Length features
            features[i, 0] = len(text)
            features[i, 1] = len(text.split())
            
            # Character distribution
            features[i, 2] = sum(c.isdigit() for c in text) / max(1, len(text))
            features[i, 3] = sum(c.isalpha() for c in text) / max(1, len(text))
            features[i, 4] = sum(c.isupper() for c in text) / max(1, sum(c.isalpha() for c in text))
        
        return features
    
    def _extract_numeric_features(self, values: List[Union[float, int, str]]) -> np.ndarray:
        """
        Extract features from numeric data.
        
        Args:
            values (List[Union[float, int, str]]): List of numeric values
            
        Returns:
            np.ndarray: Feature matrix
        """
        # Convert strings to numeric if needed
        numeric_values = []
        for value in values:
            if isinstance(value, (int, float)):
                numeric_values.append(float(value))
            elif isinstance(value, str):
                try:
                    numeric_values.append(float(re.sub(r'[^\d.-]', '', value)))
                except ValueError:
                    numeric_values.append(np.nan)
            else:
                numeric_values.append(np.nan)
        
        # Replace NaN with median
        numeric_array = np.array(numeric_values)
        median = np.nanmedian(numeric_array)
        numeric_array = np.nan_to_num(numeric_array, nan=median)
        
        # Extract features: raw values, log values, is_integer
        features = np.zeros((len(numeric_array), 3))
        features[:, 0] = numeric_array
        features[:, 1] = np.log1p(np.abs(numeric_array))  # Log transform
        features[:, 2] = np.array([float(v).is_integer() for v in numeric_array])
        
        # Normalize features
        scaler = StandardScaler()
        return scaler.fit_transform(features)
    
    def _extract_mixed_features(self, values: List[Any]) -> np.ndarray:
        """
        Extract features from mixed data types.
        
        Args:
            values (List[Any]): List of mixed values
            
        Returns:
            np.ndarray: Feature matrix
        """
        features = np.zeros((len(values), 10))
        
        for i, value in enumerate(values):
            value_str = str(value)
            
            # Length
            features[i, 0] = len(value_str)
            
            # Type indicators
            features[i, 1] = float(isinstance(value, int) or isinstance(value, float))
            features[i, 2] = float(isinstance(value, str))
            
            # Content indicators
            features[i, 3] = sum(c.isdigit() for c in value_str) / max(1, len(value_str))
            features[i, 4] = sum(c.isalpha() for c in value_str) / max(1, len(value_str))
            features[i, 5] = sum(c.isupper() for c in value_str) / max(1, sum(c.isalpha() for c in value_str))
            features[i, 6] = sum(c in '.,;:' for c in value_str) / max(1, len(value_str))
            features[i, 7] = float('.' in value_str and all(c.isdigit() or c == '.' for c in value_str))
            features[i, 8] = float('@' in value_str)
            features[i, 9] = float('/' in value_str or '-' in value_str)
        
        return features
    
    def train_classifier(self, data: List[Dict[str, Any]], category_field: str, 
                        feature_fields: List[str], classifier_name: str,
                        algorithm: str = 'random_forest') -> Dict[str, Any]:
        """
        Train a new classifier on labeled data.
        
        Args:
            data (List[Dict[str, Any]]): List of data records
            category_field (str): Field containing category labels
            feature_fields (List[str]): Fields to use as features
            classifier_name (str): Name to identify this classifier
            algorithm (str): Machine learning algorithm to use
            
        Returns:
            Dict[str, Any]: Training results and statistics
        """
        if not data or not feature_fields:
            return {'error': 'No data or feature fields provided'}
        
        # Extract categories and features
        categories = []
        feature_sets = defaultdict(list)
        
        for record in data:
            if category_field not in record:
                continue
                
            categories.append(record[category_field])
            
            for field in feature_fields:
                if field in record:
                    feature_sets[field].append(record[field])
                else:
                    feature_sets[field].append('')
        
        if not categories:
            return {'error': 'No categories found in data'}
        
        # Extract features for each field
        all_features = []
        
        for field, values in feature_sets.items():
            # Determine feature type
            if all(isinstance(v, (int, float)) for v in values if v):
                feature_type = 'numeric'
            elif all(isinstance(v, str) for v in values if v):
                feature_type = 'text'
            else:
                feature_type = 'mixed'
            
            # Extract features
            extractor = self.feature_extractors[feature_type]
            field_features = extractor(values)
            
            all_features.append(field_features)
        
        # Combine features from all fields
        if all_features:
            if len(all_features) == 1:
                X = all_features[0]
            else:
                # Make sure all feature arrays have the same number of samples
                assert all(f.shape[0] == len(categories) for f in all_features)
                
                # Concatenate along feature axis
                X = np.hstack(all_features)
        else:
            return {'error': 'Failed to extract features'}
        
        # Convert categories to numpy array
        y = np.array(categories)
        
        # Train classifier based on selected algorithm
        if algorithm == 'random_forest':
            clf = RandomForestClassifier(n_estimators=100, random_state=42)
        elif algorithm == 'kmeans':
            clf = KMeans(n_clusters=len(set(categories)), random_state=42)
        else:
            return {'error': f'Unsupported algorithm: {algorithm}'}
        
        clf.fit(X, y)
        
        # Save classifier and metadata
        model_path = os.path.join(self.model_dir, f"{classifier_name}.joblib")
        
        metadata = {
            'algorithm': algorithm,
            'feature_fields': feature_fields,
            'category_field': category_field,
            'categories': list(set(categories)),
            'feature_types': {field: self._detect_feature_type(feature_sets[field]) for field in feature_fields},
            'created_at': datetime.now().isoformat(),
            'sample_count': len(categories)
        }
        
        # Save both classifier and metadata
        joblib.dump({'classifier': clf, 'metadata': metadata}, model_path)
        
        # Add to classifiers dictionary
        self.classifiers[classifier_name] = {
            'classifier': clf,
            'metadata': metadata
        }
        
        # Calculate basic statistics
        category_counts = {}
        for category in categories:
            category_counts[category] = category_counts.get(category, 0) + 1
        
        return {
            'status': 'success',
            'classifier_name': classifier_name,
            'algorithm': algorithm,
            'sample_count': len(categories),
            'category_distribution': category_counts,
            'model_path': model_path
        }
    
    def _detect_feature_type(self, values: List[Any]) -> str:
        """Detect the type of features in a list of values"""
        if all(isinstance(v, (int, float)) for v in values if v):
            return 'numeric'
        elif all(isinstance(v, str) for v in values if v):
            return 'text'
        else:
            return 'mixed'
    
    def load_classifier(self, classifier_name: str) -> bool:
        """
        Load a previously saved classifier.
        
        Args:
            classifier_name (str): Name of the classifier to load
            
        Returns:
            bool: True if loading was successful, False otherwise
        """
        model_path = os.path.join(self.model_dir, f"{classifier_name}.joblib")
        
        if not os.path.exists(model_path):
            self.logger.warning(f"Classifier not found: {classifier_name}")
            return False
        
        try:
            loaded_data = joblib.load(model_path)
            
            if isinstance(loaded_data, dict) and 'classifier' in loaded_data and 'metadata' in loaded_data:
                self.classifiers[classifier_name] = loaded_data
                self.logger.info(f"Loaded classifier: {classifier_name}")
                return True
            else:
                self.logger.warning(f"Invalid classifier format: {classifier_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error loading classifier {classifier_name}: {e}")
            return False
    
    def list_classifiers(self) -> List[Dict[str, Any]]:
        """
        List all available classifiers.
        
        Returns:
            List[Dict[str, Any]]: List of classifier metadata
        """
        result = []
        
        # First check loaded classifiers
        for name, data in self.classifiers.items():
            metadata = data.get('metadata', {})
            result.append({
                'name': name,
                'algorithm': metadata.get('algorithm', 'unknown'),
                'categories': metadata.get('categories', []),
                'created_at': metadata.get('created_at', 'unknown'),
                'sample_count': metadata.get('sample_count', 0),
                'feature_fields': metadata.get('feature_fields', [])
            })
        
        # Also check for saved classifiers that aren't loaded
        for filename in os.listdir(self.model_dir):
            if filename.endswith('.joblib'):
                name = filename[:-7]  # Remove .joblib extension
                
                if name not in self.classifiers:
                    try:
                        # Just load the metadata
                        model_path = os.path.join(self.model_dir, filename)
                        loaded_data = joblib.load(model_path)
                        
                        if isinstance(loaded_data, dict) and 'metadata' in loaded_data:
                            metadata = loaded_data['metadata']
                            result.append({
                                'name': name,
                                'algorithm': metadata.get('algorithm', 'unknown'),
                                'categories': metadata.get('categories', []),
                                'created_at': metadata.get('created_at', 'unknown'),
                                'sample_count': metadata.get('sample_count', 0),
                                'feature_fields': metadata.get('feature_fields', []),
                                'loaded': False
                            })
                    except Exception as e:
                        self.logger.warning(f"Error reading classifier metadata {name}: {e}")
        
        return result
    
    def classify(self, data: Dict[str, Any], classifier_name: str) -> Dict[str, Any]:
        """
        Classify a single data record.
        
        Args:
            data (Dict[str, Any]): The data record to classify
            classifier_name (str): Name of the classifier to use
            
        Returns:
            Dict[str, Any]: Classification results with probabilities
        """
        if classifier_name not in self.classifiers:
            if not self.load_classifier(classifier_name):
                return {'error': f'Classifier not found: {classifier_name}'}
        
        classifier_data = self.classifiers[classifier_name]
        clf = classifier_data['classifier']
        metadata = classifier_data['metadata']
        
        # Extract features based on classifier's feature fields
        feature_fields = metadata['feature_fields']
        all_features = []
        
        for field in feature_fields:
            if field not in data:
                # Handle missing field
                feature_type = metadata['feature_types'].get(field, 'mixed')
                if feature_type == 'numeric':
                    values = [0]
                else:
                    values = ['']
            else:
                values = [data[field]]
            
            # Determine feature type and extract features
            feature_type = metadata['feature_types'].get(field, self._detect_feature_type(values))
            extractor = self.feature_extractors[feature_type]
            field_features = extractor(values)
            
            all_features.append(field_features)
        
        # Combine features
        if all_features:
            if len(all_features) == 1:
                X = all_features[0]
            else:
                # Ensure all feature arrays have the same number of samples (should be 1)
                assert all(f.shape[0] == 1 for f in all_features)
                X = np.hstack(all_features)
        else:
            return {'error': 'Failed to extract features'}
        
        # Perform classification
        algorithm = metadata.get('algorithm', 'random_forest')
        
        if algorithm == 'random_forest':
            # For RandomForest, we can get probability estimates
            category = clf.predict(X)[0]
            probabilities = clf.predict_proba(X)[0]
            
            # Map probabilities to category names
            probability_map = {}
            for i, prob in enumerate(probabilities):
                category_name = clf.classes_[i]
                probability_map[category_name] = float(prob)
            
            return {
                'category': category,
                'probabilities': probability_map,
                'confidence': float(max(probabilities))
            }
            
        elif algorithm == 'kmeans':
            # For KMeans, we can get distance to cluster centers
            cluster_idx = clf.predict(X)[0]
            distances = clf.transform(X)[0]
            
            # Convert cluster index to category name
            categories = metadata.get('categories', [])
            if cluster_idx < len(categories):
                category = categories[cluster_idx]
            else:
                category = f"cluster_{cluster_idx}"
            
            # Calculate confidence based on distance
            confidence = 1.0 / (1.0 + distances[cluster_idx])
            
            return {
                'category': category,
                'cluster_distances': {f"cluster_{i}": float(d) for i, d in enumerate(distances)},
                'confidence': float(confidence)
            }
        
        else:
            # Generic classification without probabilities
            category = clf.predict(X)[0]
            return {
                'category': category,
                'confidence': 1.0
            }
    
    def batch_classify(self, data_list: List[Dict[str, Any]], classifier_name: str) -> List[Dict[str, Any]]:
        """
        Classify a batch of data records.
        
        Args:
            data_list (List[Dict[str, Any]]): List of data records to classify
            classifier_name (str): Name of the classifier to use
            
        Returns:
            List[Dict[str, Any]]: Classification results for each record
        """
        results = []
        
        for data in data_list:
            result = self.classify(data, classifier_name)
            results.append(result)
        
        return results
    
    def create_default_classifier(self, data_type: str, sample_data: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a default classifier for common data types.
        
        Args:
            data_type (str): Type of data to classify (text, person, document, etc.)
            sample_data (List[Dict[str, Any]], optional): Sample data to improve the classifier
            
        Returns:
            Dict[str, Any]: Creation results
        """
        if data_type not in self.default_categories:
            return {'error': f'Unsupported data type: {data_type}'}
        
        categories = self.default_categories[data_type]
        
        # Generate synthetic training data if no sample data is provided
        if not sample_data:
            sample_data = self._generate_synthetic_data(data_type, categories)
        
        # Merge synthetic and real sample data if available
        if sample_data:
            # Ensure all sample data has required fields
            for record in sample_data:
                if 'category' not in record:
                    # Try to infer category from data
                    inferred = self._infer_category(record, data_type)
                    if inferred:
                        record['category'] = inferred
        
        # Determine feature fields based on data type
        feature_fields = self._get_default_feature_fields(data_type)
        
        # Train the classifier
        return self.train_classifier(
            data=sample_data,
            category_field='category',
            feature_fields=feature_fields,
            classifier_name=f"{data_type}_classifier",
            algorithm='random_forest'
        )
    
    def _generate_synthetic_data(self, data_type: str, categories: List[str]) -> List[Dict[str, Any]]:
        """Generate synthetic training data for default classifiers"""
        synthetic_data = []
        
        # Generate synthetic data based on data type
        if data_type == 'text':
            for category in categories:
                # Generate 10 samples per category
                for i in range(10):
                    synthetic_data.append({
                        'category': category,
                        'text': self._generate_text_sample(category),
                        'length': len(self._generate_text_sample(category))
                    })
        
        elif data_type == 'person':
            for category in categories:
                # Generate 10 samples per category
                for i in range(10):
                    synthetic_data.append({
                        'category': category,
                        'name': f"Person {i+1}",
                        'role': category,
                        'active': i % 2 == 0
                    })
        
        elif data_type == 'document':
            for category in categories:
                # Generate 10 samples per category
                for i in range(10):
                    synthetic_data.append({
                        'category': category,
                        'title': f"{category.title()} {i+1}",
                        'content': f"Sample content for {category} {i+1}",
                        'date': f"2024-{(i%12)+1:02d}-{(i%28)+1:02d}"
                    })
        
        return synthetic_data
    
    def _generate_text_sample(self, category: str) -> str:
        """Generate synthetic text sample for a category"""
        # Simple templates for different text categories
        templates = {
            'article': "This is a detailed article about {topic}. It contains multiple paragraphs discussing various aspects of {topic}.",
            'description': "A {adjective} {item} with {feature1} and {feature2}.",
            'title': "{Topic}: {Subtitle}",
            'name': "{Title} {FirstName} {LastName}",
            'comment': "I {feeling} this {item}. It {opinion}.",
            'address': "{number} {street}, {city}, {state} {zip}"
        }
        
        # Fill templates with random content
        if category in templates:
            template = templates[category]
            
            # Replace placeholders with random content
            for placeholder in re.findall(r'\{(\w+)\}', template):
                replacement = self._get_random_content(placeholder.lower())
                template = template.replace(f"{{{placeholder}}}", replacement)
            
            return template
        else:
            return f"Sample text for {category} category"
    
    def _get_random_content(self, content_type: str) -> str:
        """Get random content for synthetic data generation"""
        content_options = {
            'topic': ['technology', 'science', 'history', 'art', 'business', 'health'],
            'adjective': ['red', 'large', 'modern', 'efficient', 'innovative', 'complex'],
            'item': ['product', 'device', 'tool', 'application', 'solution', 'system'],
            'feature1': ['high performance', 'low cost', 'easy setup', 'advanced features'],
            'feature2': ['long battery life', 'compact design', 'fast processing', 'elegant interface'],
            'topic': ['Introduction', 'Analysis', 'Overview', 'Guide', 'Review'],
            'subtitle': ['Part 1', 'A New Approach', 'Key Insights', 'Future Directions'],
            'title': ['Mr.', 'Ms.', 'Dr.', 'Prof.'],
            'firstname': ['John', 'Jane', 'David', 'Sarah', 'Michael', 'Emily'],
            'lastname': ['Smith', 'Johnson', 'Brown', 'Davis', 'Wilson', 'Lee'],
            'feeling': ['like', 'love', 'appreciate', 'dislike', 'hate'],
            'opinion': ['works well', 'saved me time', 'exceeded expectations', 'was disappointing'],
            'number': ['123', '456', '789', '1011'],
            'street': ['Main St', 'Park Ave', 'Oak Rd', 'Cedar Ln'],
            'city': ['Springfield', 'Rivertown', 'Lakeside', 'Hillcrest'],
            'state': ['CA', 'NY', 'TX', 'FL', 'IL'],
            'zip': ['12345', '67890', '54321', '98765']
        }
        
        if content_type in content_options:
            import random
            options = content_options[content_type]
            return random.choice(options)
        else:
            return content_type.title()
    
    def _infer_category(self, record: Dict[str, Any], data_type: str) -> Optional[str]:
        """Infer category from record data"""
        if data_type not in self.default_categories:
            return None
        
        categories = self.default_categories[data_type]
        
        # Simple heuristics for category inference
        if data_type == 'text':
            if 'text' in record:
                text = record['text'].lower()
                
                # Check if text contains category keywords
                for category in categories:
                    if category in text:
                        return category
                
                # Length-based heuristics
                if len(text) < 50:
                    return 'title' if text.istitle() else 'name'
                elif len(text) > 500:
                    return 'article'
                else:
                    return 'description'
        
        elif data_type == 'person':
            if 'role' in record:
                role = record['role'].lower()
                for category in categories:
                    if category in role:
                        return category
        
        # Default to first category if no match
        return categories[0] if categories else None
    
    def _get_default_feature_fields(self, data_type: str) -> List[str]:
        """Get default feature fields for a data type"""
        default_fields = {
            'text': ['text', 'length'],
            'person': ['name', 'role', 'active'],
            'document': ['title', 'content', 'date'],
            'contact': ['name', 'email', 'phone', 'department'],
            'product': ['name', 'description', 'price', 'category']
        }
        
        return default_fields.get(data_type, ['name', 'description'])
    
    def analyze_classification_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze classification results to identify patterns and potential improvements.
        
        Args:
            results (List[Dict[str, Any]]): List of classification results
            
        Returns:
            Dict[str, Any]: Analysis of results
        """
        if not results:
            return {'error': 'No classification results to analyze'}
        
        # Count categories
        category_counts = defaultdict(int)
        confidence_by_category = defaultdict(list)
        low_confidence_entries = []
        
        for result in results:
            if 'error' in result:
                continue
                
            category = result.get('category')
            confidence = result.get('confidence', 0.0)
            
            if category:
                category_counts[category] += 1
                confidence_by_category[category].append(confidence)
                
                # Track low confidence entries
                if confidence < 0.7:
                    low_confidence_entries.append(result)
        
        # Calculate statistics
        total_classified = sum(category_counts.values())
        category_percentages = {cat: (count / total_classified * 100) for cat, count in category_counts.items()}
        
        avg_confidence_by_category = {}
        for category, confidences in confidence_by_category.items():
            avg_confidence_by_category[category] = sum(confidences) / len(confidences)
        
        overall_avg_confidence = sum(conf for confs in confidence_by_category.values() for conf in confs) / total_classified
        
        return {
            'total_classified': total_classified,
            'category_distribution': dict(category_counts),
            'category_percentages': category_percentages,
            'average_confidence': overall_avg_confidence,
            'confidence_by_category': avg_confidence_by_category,
            'low_confidence_count': len(low_confidence_entries),
            'top_categories': sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        }
