"""
Enhanced Web Extractor with support for JavaScript-rendered pages, pagination, rate limiting,
and automatic retries.

## Dependencies:
- selenium
- beautifulsoup4
- requests

## ChromeDriver Compatibility Note:
For Chrome version 115 or newer, you must use compatible ChromeDriver versions.
Use the Chrome for Testing availability dashboard to find the correct version:
https://googlechromelabs.github.io/chrome-for-testing/

Available JSON endpoints for ChromeDriver downloads:
- known-good-versions.json: All CfT assets available for download
- known-good-versions-with-downloads.json: Same with full download URLs
- last-known-good-versions.json: Latest versions per channel (Stable/Beta/Dev/Canary)
- last-known-good-versions-with-downloads.json: Same with full download URLs
- latest-patch-versions-per-build.json: Latest versions for each MAJOR.MINOR.BUILD
- latest-patch-versions-per-build-with-downloads.json: Same with full download URLs
- latest-versions-per-milestone.json: Latest versions for each Chrome milestone
- latest-versions-per-milestone-with-downloads.json: Same with full download URLs

## Alternative: Using webdriver-manager
You can use webdriver-manager to automatically manage ChromeDriver:

```python
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
```

## Usage Example:
```python
# Create the extractor
extractor = EnhancedWebExtractor(
    use_selenium=True,  # Use Selenium for JavaScript-rendered pages
    headless=True,      # Run in headless mode
    rate_limit=2.0,     # Wait 2 seconds between requests
    retry_count=3       # Retry failed requests up to 3 times
)

# Extract data from multiple pages
products = extractor.extract_multiple(
    url='https://example.com/products',
    container_selector='.product',
    field_selectors={
        'id': '.product-id',
        'name': '.product-name',
        'price': '.product-price'
    },
    pagination_selector='.next-page',
    max_pages=3
)

# Always close the extractor when done
extractor.close()
```
"""
import time
import logging
from typing import Dict, List, Any, Optional, Union
import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedWebExtractor:
    """
    Enhanced Web Extractor that supports both static and JavaScript-rendered pages,
    with pagination support and built-in rate limiting.
    """
    
    def __init__(
        self,
        use_selenium: bool = False,
        headless: bool = True,
        wait_time: int = 10,
        rate_limit: float = 1.0,
        retry_count: int = 3,
        webdriver_path: Optional[str] = None,
        user_agent: Optional[str] = None,
        use_webdriver_manager: bool = True
    ):
        """
        Initialize the enhanced web extractor.
        
        Args:
            use_selenium: Whether to use Selenium for JavaScript-rendered pages
            headless: Whether to run browser in headless mode (if using Selenium)
            wait_time: Maximum time to wait for elements to load (seconds)
            rate_limit: Minimum time between requests (seconds)
            retry_count: Number of times to retry failed requests
            webdriver_path: Path to the webdriver executable (if using Selenium)
            user_agent: Custom user agent string to use for requests
            use_webdriver_manager: Whether to use webdriver-manager for automatic ChromeDriver management
        """
        self.use_selenium = use_selenium
        self.headless = headless
        self.wait_time = wait_time
        self.rate_limit = rate_limit
        self.retry_count = retry_count
        self.webdriver_path = webdriver_path
        self.use_webdriver_manager = use_webdriver_manager
        self.user_agent = user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        self.driver = None
        self.last_request_time = 0
        
        # Initialize Selenium if needed
        if self.use_selenium:
            self._initialize_selenium()
    
    def _initialize_selenium(self):
        """Initialize the Selenium WebDriver"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            
            options = Options()
            if self.headless:
                options.add_argument('--headless')
            
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument(f'user-agent={self.user_agent}')
            
            if self.use_webdriver_manager:
                try:
                    # Try to use webdriver_manager for easier ChromeDriver management
                    from webdriver_manager.chrome import ChromeDriverManager
                    from selenium.webdriver.chrome.service import Service
                    
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(service=service, options=options)
                    logger.info("Initialized Chrome using webdriver-manager")
                except ImportError:
                    logger.warning("webdriver-manager not installed, falling back to manual ChromeDriver")
                    self._initialize_selenium_manual(webdriver, options)
            else:
                self._initialize_selenium_manual(webdriver, options)
                
            self.driver.set_page_load_timeout(self.wait_time)
            logger.info("Selenium WebDriver initialized")
        except ImportError:
            logger.error("Selenium is not installed. Run 'pip install selenium' to use JavaScript-rendered page support.")
            raise
    
    def _initialize_selenium_manual(self, webdriver, options):
        """Initialize Selenium WebDriver manually"""
        # Set up the service with the webdriver path if provided
        if self.webdriver_path:
            from selenium.webdriver.chrome.service import Service
            service = Service(executable_path=self.webdriver_path)
            self.driver = webdriver.Chrome(service=service, options=options)
            logger.info(f"Initialized Chrome with custom webdriver path: {self.webdriver_path}")
        else:
            # Let Selenium find the webdriver automatically
            self.driver = webdriver.Chrome(options=options)
            logger.info("Initialized Chrome with automatically detected webdriver")
    
    def _respect_rate_limit(self):
        """Enforce rate limiting between requests"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.rate_limit:
            sleep_time = self.rate_limit - elapsed
            logger.debug(f"Rate limiting: Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def extract_from_url(
        self, 
        url: str, 
        selectors: Dict[str, str],
        wait_for_selector: Optional[str] = None,
        pagination_selector: Optional[str] = None,
        max_pages: int = 1
    ) -> Dict[str, Any]:
        """
        Extract data from a URL using the provided CSS selectors.
        
        Args:
            url: The URL to extract data from
            selectors: Dictionary mapping field names to CSS selectors
            wait_for_selector: CSS selector to wait for before extracting data
            pagination_selector: CSS selector for the "next page" link
            max_pages: Maximum number of pages to extract
        
        Returns:
            Dictionary containing the extracted data
        """
        if self.use_selenium:
            return self._extract_with_selenium(url, selectors, wait_for_selector, pagination_selector, max_pages)
        else:
            return self._extract_with_requests(url, selectors, pagination_selector, max_pages)
    
    def _extract_with_requests(
        self, 
        url: str, 
        selectors: Dict[str, str],
        pagination_selector: Optional[str] = None,
        max_pages: int = 1
    ) -> Dict[str, Any]:
        """Extract data using the requests library (for static pages)"""
        results = {}
        current_url = url
        page_count = 0
        
        while current_url and page_count < max_pages:
            # Respect rate limiting
            self._respect_rate_limit()
            
            # Try the request with retries
            for attempt in range(self.retry_count):
                try:
                    headers = {'User-Agent': self.user_agent}
                    response = requests.get(current_url, headers=headers, timeout=self.wait_time)
                    response.raise_for_status()
                    break
                except (requests.RequestException, requests.HTTPError) as e:
                    logger.warning(f"Request failed (attempt {attempt+1}/{self.retry_count}): {str(e)}")
                    if attempt == self.retry_count - 1:
                        logger.error(f"Failed to fetch {current_url} after {self.retry_count} attempts")
                        return results
                    time.sleep(1)  # Wait before retrying
            
            # Parse the HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data for each selector
            for field, selector in selectors.items():
                elements = soup.select(selector)
                if field not in results:
                    results[field] = []
                
                for element in elements:
                    results[field].append(element.text.strip())
            
            # Handle pagination if needed
            page_count += 1
            if pagination_selector and page_count < max_pages:
                next_link = soup.select_one(pagination_selector)
                if next_link and 'href' in next_link.attrs:
                    # Handle relative URLs
                    if next_link['href'].startswith('/'):
                        base_url = '/'.join(current_url.split('/')[:3])  # http(s)://domain.com
                        current_url = base_url + next_link['href']
                    else:
                        current_url = next_link['href']
                    logger.info(f"Moving to next page: {current_url}")
                else:
                    current_url = None
            else:
                current_url = None
        
        return results
    
    def _extract_with_selenium(
        self, 
        url: str, 
        selectors: Dict[str, str],
        wait_for_selector: Optional[str] = None,
        pagination_selector: Optional[str] = None,
        max_pages: int = 1
    ) -> Dict[str, Any]:
        """Extract data using Selenium (for JavaScript-rendered pages)"""
        # Import Selenium components here to avoid import errors when Selenium is not installed
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException
        
        if not self.driver:
            self._initialize_selenium()
        
        results = {}
        current_url = url
        page_count = 0
        
        while current_url and page_count < max_pages:
            # Respect rate limiting
            self._respect_rate_limit()
            
            # Try to load the page with retries
            for attempt in range(self.retry_count):
                try:
                    self.driver.get(current_url)
                    
                    # Wait for the specific element if requested
                    if wait_for_selector:
                        WebDriverWait(self.driver, self.wait_time).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector))
                        )
                    break
                except (TimeoutException, Exception) as e:
                    logger.warning(f"Selenium load failed (attempt {attempt+1}/{self.retry_count}): {str(e)}")
                    if attempt == self.retry_count - 1:
                        logger.error(f"Failed to load {current_url} with Selenium after {self.retry_count} attempts")
                        return results
                    time.sleep(1)  # Wait before retrying
            
            # Extract data for each selector
            for field, selector in selectors.items():
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if field not in results:
                        results[field] = []
                    
                    for element in elements:
                        results[field].append(element.text.strip())
                except Exception as e:
                    logger.warning(f"Error extracting {field} with selector {selector}: {str(e)}")
            
            # Handle pagination if needed
            page_count += 1
            if pagination_selector and page_count < max_pages:
                try:
                    next_link = self.driver.find_element(By.CSS_SELECTOR, pagination_selector)
                    if next_link:
                        current_url = next_link.get_attribute('href')
                        logger.info(f"Moving to next page: {current_url}")
                    else:
                        current_url = None
                except Exception:
                    current_url = None
            else:
                current_url = None
        
        return results
    
    def extract_multiple(
        self, 
        url: str, 
        container_selector: str, 
        field_selectors: Dict[str, str],
        wait_for_selector: Optional[str] = None,
        pagination_selector: Optional[str] = None,
        max_pages: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Extract multiple items from a page.
        
        Args:
            url: The URL to extract data from
            container_selector: CSS selector for the container of each item
            field_selectors: Dict mapping field names to CSS selectors (relative to container)
            wait_for_selector: CSS selector to wait for before extracting data
            pagination_selector: CSS selector for the "next page" link
            max_pages: Maximum number of pages to extract
        
        Returns:
            List of dictionaries, each containing the extracted fields for an item
        """
        items = []
        current_url = url
        page_count = 0
        
        while current_url and page_count < max_pages:
            # Respect rate limiting
            self._respect_rate_limit()
            
            if self.use_selenium:
                # Import Selenium components here to avoid import errors when Selenium is not installed
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from selenium.common.exceptions import TimeoutException
                
                if not self.driver:
                    self._initialize_selenium()
                
                # Load the page with Selenium
                for attempt in range(self.retry_count):
                    try:
                        self.driver.get(current_url)
                        
                        # Wait for the specific element if requested
                        if wait_for_selector:
                            WebDriverWait(self.driver, self.wait_time).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector))
                            )
                        break
                    except (TimeoutException, Exception) as e:
                        logger.warning(f"Selenium load failed (attempt {attempt+1}/{self.retry_count}): {str(e)}")
                        if attempt == self.retry_count - 1:
                            logger.error(f"Failed to load {current_url} with Selenium after {self.retry_count} attempts")
                            return items
                        time.sleep(1)  # Wait before retrying
                
                # Find all item containers
                containers = self.driver.find_elements(By.CSS_SELECTOR, container_selector)
                
                # Extract data from each container
                for container in containers:
                    item = {}
                    for field, selector in field_selectors.items():
                        try:
                            elements = container.find_elements(By.CSS_SELECTOR, selector)
                            if elements:
                                item[field] = elements[0].text.strip()
                            else:
                                item[field] = None
                        except Exception as e:
                            logger.warning(f"Error extracting {field} with selector {selector}: {str(e)}")
                            item[field] = None
                    
                    items.append(item)
                
                # Handle pagination if needed
                page_count += 1
                if pagination_selector and page_count < max_pages:
                    try:
                        next_link = self.driver.find_element(By.CSS_SELECTOR, pagination_selector)
                        if next_link:
                            current_url = next_link.get_attribute('href')
                            logger.info(f"Moving to next page: {current_url}")
                        else:
                            current_url = None
                    except Exception:
                        current_url = None
                else:
                    current_url = None
                
            else:
                # Use requests + BeautifulSoup for static pages
                for attempt in range(self.retry_count):
                    try:
                        headers = {'User-Agent': self.user_agent}
                        response = requests.get(current_url, headers=headers, timeout=self.wait_time)
                        response.raise_for_status()
                        break
                    except (requests.RequestException, requests.HTTPError) as e:
                        logger.warning(f"Request failed (attempt {attempt+1}/{self.retry_count}): {str(e)}")
                        if attempt == self.retry_count - 1:
                            logger.error(f"Failed to fetch {current_url} after {self.retry_count} attempts")
                            return items
                        time.sleep(1)  # Wait before retrying
                
                # Parse the HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all item containers
                containers = soup.select(container_selector)
                
                # Extract data from each container
                for container in containers:
                    item = {}
                    for field, selector in field_selectors.items():
                        elements = container.select(selector)
                        if elements:
                            item[field] = elements[0].text.strip()
                        else:
                            item[field] = None
                    
                    items.append(item)
                
                # Handle pagination if needed
                page_count += 1
                if pagination_selector and page_count < max_pages:
                    next_link = soup.select_one(pagination_selector)
                    if next_link and 'href' in next_link.attrs:
                        # Handle relative URLs
                        if next_link['href'].startswith('/'):
                            base_url = '/'.join(current_url.split('/')[:3])  # http(s)://domain.com
                            current_url = base_url + next_link['href']
                        else:
                            current_url = next_link['href']
                        logger.info(f"Moving to next page: {current_url}")
                    else:
                        current_url = None
                else:
                    current_url = None
        
        return items
    
    def close(self):
        """Close the Selenium WebDriver if it's open"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Selenium WebDriver closed")
            except Exception as e:
                logger.warning(f"Error closing WebDriver: {str(e)}")
            finally:
                self.driver = None
    
    def __del__(self):
        """Ensure WebDriver is closed when object is deleted"""
        self.close()
