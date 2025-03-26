"""
PDF document extraction module.
"""
import os
import io
from typing import Dict, List, Any, Optional

try:
    import PyPDF2
    from PyPDF2 import PdfReader
except ImportError:
    print("PyPDF2 not installed. Run: pip install PyPDF2")
    PyPDF2 = None

class PDFReader:
    """Extract text and metadata from PDF documents."""
    
    def __init__(self):
        """Initialize the PDF reader."""
        if PyPDF2 is None:
            raise ImportError("PyPDF2 is required. Install with: pip install PyPDF2")
    
    def read_file(self, file_path: str) -> Dict[str, Any]:
        """
        Extract text and metadata from a PDF file.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Dictionary with text content and metadata
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"PDF file not found: {file_path}")
            
        try:
            with open(file_path, 'rb') as file:
                return self._process_pdf(file)
        except Exception as e:
            raise ValueError(f"Error reading PDF file: {e}")
    
    def read_bytes(self, pdf_bytes: bytes) -> Dict[str, Any]:
        """
        Extract text and metadata from PDF bytes.

        Args:
            pdf_bytes: PDF file as bytes

        Returns:
            Dictionary with text content and metadata
        """
        try:
            return self._process_pdf(io.BytesIO(pdf_bytes))
        except Exception as e:
            raise ValueError(f"Error reading PDF bytes: {e}")

    def _process_pdf(self, file_obj: Any) -> Dict[str, Any]:
        """Process a PDF file object and extract content."""
        reader = PdfReader(file_obj)

        # Extract metadata
        metadata = {}
        if reader.metadata:
            for key, value in reader.metadata.items():
                if key.startswith('/'):
                    key = key[1:]  # Remove leading slash
                metadata[key] = value

        # Extract text from each page
        pages = []
        for i, page in enumerate(reader.pages):
            content = page.extract_text() or ""
            pages.append({
                'page_number': i + 1,
                'content': content,
                'char_count': len(content)
            })

        return {
            'page_count': len(reader.pages),
            'metadata': metadata,
            'pages': pages,
            'text': "\n\n".join(page['content'] for page in pages)
        }
