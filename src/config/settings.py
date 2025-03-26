"""
Global settings for the Data Scraper application.
"""

# Application settings
APP_NAME = "Universal Data Scraper"
APP_VERSION = "0.1.0"

# Default directories
DEFAULT_PROJECTS_DIR = "projects"
DEFAULT_LOGS_DIR = "logs"

# Timeouts (in seconds)
DEFAULT_REQUEST_TIMEOUT = 30

# User agent for web scraping
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Export settings
DEFAULT_EXPORT_FORMAT = "csv"
AVAILABLE_EXPORT_FORMATS = ["csv", "excel", "json", "pdf"]

# Pattern definitions
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
PHONE_PATTERN = r'(\+\d{1,3}[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}'
URL_PATTERN = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
