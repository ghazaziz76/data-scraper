# src/connectors/api/session_manager.py

import os
import json
import time
import logging
import datetime
import pickle
import hashlib
import threading
from typing import Dict, List, Optional, Any, Union, Callable, Set, Tuple
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class SessionManager:
    """
    Session persistence manager to maintain connection state between application sessions
    and provide enhanced request capabilities with retry, pooling, and caching.
    """
    
    def __init__(self, storage_dir: Optional[str] = None,
               enable_cache: bool = True,
               default_timeout: int = 30,
               pool_connections: int = 10,
               pool_maxsize: int = 10):
        """
        Initialize the session manager.
        
        Args:
            storage_dir: Directory to store session data
            enable_cache: Whether to enable request caching
            default_timeout: Default request timeout in seconds
            pool_connections: Number of connection pools to cache
            pool_maxsize: Maximum number of connections to save in the pool
        """
        self.storage_dir = storage_dir
        self.enable_cache = enable_cache
        self.default_timeout = default_timeout
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        
        self.logger = logging.getLogger(__name__)
        
        # Dictionary to store sessions for different services
        self.sessions = {}
        
        # Cache for request responses
        self.cache = {}
        self.cache_ttl = {}
        
        # Set of active requests to prevent duplicate simultaneous requests
        self.active_requests = set()
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        # Create storage directory if it doesn't exist
        if storage_dir:
            os.makedirs(storage_dir, exist_ok=True)
    
    def create_session(self, service_name: str, headers: Optional[Dict[str, str]] = None,
                     retry_strategy: Optional[Retry] = None) -> requests.Session:
        """
        Create a new session for a service with connection pooling.
        
        Args:
            service_name: Name of the service
            headers: Default headers for the session
            retry_strategy: Custom retry strategy
            
        Returns:
            requests.Session: Configured session
        """
        with self.lock:
            # Create a new session
            session = requests.Session()
            
            # Set default headers
            if headers:
                session.headers.update(headers)
            
            # Set default timeout
            session.request = functools.partial(session.request, timeout=self.default_timeout)
            
            # Configure connection pooling
            adapter = HTTPAdapter(
                pool_connections=self.pool_connections,
                pool_maxsize=self.pool_maxsize
            )
            
            # Add retry strategy if provided
            if retry_strategy:
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=self.pool_connections,
                    pool_maxsize=self.pool_maxsize
                )
            
            # Mount the adapter for both HTTP and HTTPS
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            # Store the session
            self.sessions[service_name] = session
            
            # Save session cookies if storage directory is specified
            if self.storage_dir:
                self._save_cookies(service_name)
                
            return session
    
    def get_session(self, service_name: str) -> requests.Session:
        """
        Get an existing session or create a new one.
        
        Args:
            service_name: Name of the service
            
        Returns:
            requests.Session: Session for the service
        """
        with self.lock:
            # If session exists, return it
            if service_name in self.sessions:
                return self.sessions[service_name]
                
            # Try to load persisted session
            if self.storage_dir:
                loaded = self._load_cookies(service_name)
                if loaded:
                    return self.sessions[service_name]
                    
            # Create a new session if loading failed
            return self.create_session(service_name)
    
    def update_session_headers(self, service_name: str, 
                             headers: Dict[str, str]) -> bool:
        """
        Update headers for a session.
        
        Args:
            service_name: Name of the service
            headers: Headers to update
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name not in self.sessions:
                return False
                
            session = self.sessions[service_name]
            session.headers.update(headers)
            
            return True
    
    def clear_session(self, service_name: str) -> bool:
        """
        Clear a session (cookies, cache, etc.).
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name not in self.sessions:
                return False
                
            # Get the session
            session = self.sessions[service_name]
            
            # Clear cookies
            session.cookies.clear()
            
            # Clear cache for this service
            if service_name in self.cache:
                del self.cache[service_name]
            if service_name in self.cache_ttl:
                del self.cache_ttl[service_name]
                
            # Clear session file if it exists
            if self.storage_dir:
                cookies_path = os.path.join(self.storage_dir, f"{service_name}_cookies.pickle")
                if os.path.exists(cookies_path):
                    try:
                        os.remove(cookies_path)
                    except Exception as e:
                        self.logger.error(f"Error removing cookies file for {service_name}: {str(e)}")
                        
            return True
    
    def close_session(self, service_name: str) -> bool:
        """
        Close a session and remove it from the manager.
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name not in self.sessions:
                return False
                
            # Get the session
            session = self.sessions[service_name]
            
            # Close the session
            session.close()
            
            # Remove from sessions dictionary
            del self.sessions[service_name]
            
            # Clear cache for this service
            if service_name in self.cache:
                del self.cache[service_name]
            if service_name in self.cache_ttl:
                del self.cache_ttl[service_name]
                
            return True
    
    def close_all_sessions(self) -> None:
        """Close all sessions and clean up."""
        with self.lock:
            for service_name in list(self.sessions.keys()):
                self.close_session(service_name)
                
            # Clear cache
            self.cache = {}
            self.cache_ttl = {}
    
    def _save_cookies(self, service_name: str) -> bool:
        """Save session cookies to a file."""
        if not self.storage_dir or service_name not in self.sessions:
            return False
            
        try:
            cookies_path = os.path.join(self.storage_dir, f"{service_name}_cookies.pickle")
            
            # Get the session
            session = self.sessions[service_name]
            
            # Save cookies using pickle
            with open(cookies_path, 'wb') as f:
                pickle.dump(session.cookies, f)
                
            return True
        except Exception as e:
            self.logger.error(f"Error saving cookies for {service_name}: {str(e)}")
            return False
    
    def _load_cookies(self, service_name: str) -> bool:
        """Load session cookies from a file."""
        if not self.storage_dir:
            return False
            
        cookies_path = os.path.join(self.storage_dir, f"{service_name}_cookies.pickle")
        
        if not os.path.exists(cookies_path):
            return False
            
        try:
            # Create a new session
            session = requests.Session()
            
            # Load cookies
            with open(cookies_path, 'rb') as f:
                cookies = pickle.load(f)
                session.cookies = cookies
            
            # Configure connection pooling
            adapter = HTTPAdapter(
                pool_connections=self.pool_connections,
                pool_maxsize=self.pool_maxsize
            )
            
            # Mount the adapter for both HTTP and HTTPS
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            # Store the session
            self.sessions[service_name] = session
            
            return True
        except Exception as e:
            self.logger.error(f"Error loading cookies for {service_name}: {str(e)}")
            return False
    
    def _get_cache_key(self, method: str, url: str, params: Any = None, data: Any = None) -> str:
        """Generate a cache key for a request."""
        # Create a string representation of params and data
        params_str = json.dumps(params, sort_keys=True) if params else ""
        data_str = json.dumps(data, sort_keys=True) if data else ""
        
        # Combine method, url, params, and data
        key_str = f"{method}:{url}:{params_str}:{data_str}"
        
        # Create a hash of the key string
        return hashlib.md5(key_str.encode('utf-8')).hexdigest()
    
    def request(self, service_name: str, method: str, url: str, 
              params: Any = None, data: Any = None, json: Any = None,
              headers: Dict[str, str] = None, timeout: Optional[int] = None,
              cache_ttl: int = 300, cache: bool = None,
              stream: bool = False, **kwargs) -> requests.Response:
        """
        Make a request using a service session with caching and duplicate prevention.
        
        Args:
            service_name: Name of the service
            method: HTTP method
            url: Request URL
            params: URL parameters
            data: Request data
            json: JSON data
            headers: Request headers
            timeout: Request timeout in seconds
            cache_ttl: Cache time-to-live in seconds
            cache: Whether to use cache (None = use default setting)
            stream: Whether to stream the response
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            requests.Response: Response object
        """
        # Determine if caching should be used
        use_cache = self.enable_cache if cache is None else cache
        
        # Generate cache key if caching is enabled
        cache_key = None
        if use_cache and method.upper() == 'GET':
            cache_key = self._get_cache_key(method, url, params, data)
            service_cache = self.cache.get(service_name, {})
            service_ttl = self.cache_ttl.get(service_name, {})
            
            # Check if response is in cache and not expired
            if cache_key in service_cache and cache_key in service_ttl:
                if service_ttl[cache_key] > time.time():
                    return service_cache[cache_key]
                else:
                    # Remove expired cache entry
                    del service_cache[cache_key]
                    del service_ttl[cache_key]
        
        # Check for duplicate requests
        request_id = f"{service_name}:{method}:{url}:{cache_key}"
        with self.lock:
            if request_id in self.active_requests:
                # Duplicate request detected, wait for the original to complete
                self.logger.info(f"Duplicate request detected: {request_id}")
                
                # Simple wait loop (could be improved with condition variables)
                max_wait = 30  # Maximum wait time in seconds
                wait_start = time.time()
                while request_id in self.active_requests and time.time() - wait_start < max_wait:
                    self.lock.release()
                    time.sleep(0.1)
                    self.lock.acquire()
                
                # Check if response is now in cache
                if use_cache and cache_key and service_name in self.cache and cache_key in self.cache.get(service_name, {}):
                    return self.cache[service_name][cache_key]
            
            # Add to active requests
            self.active_requests.add(request_id)
        
        try:
            # Get the session
            session = self.get_session(service_name)
            
            # Set default timeout if not provided
            if timeout is None:
                timeout = self.default_timeout
                
            # Make the request
            response = session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=headers,
                timeout=timeout,
                stream=stream,
                **kwargs
            )
            
            # Check if response needs to be cached
            if use_cache and cache_key and method.upper() == 'GET' and response.status_code == 200 and not stream:
                # Store response in cache
                if service_name not in self.cache:
                    self.cache[service_name] = {}
                if service_name not in self.cache_ttl:
                    self.cache_ttl[service_name] = {}
                
                # Cache the response
                self.cache[service_name][cache_key] = response
                self.cache_ttl[service_name][cache_key] = time.time() + cache_ttl
                
            # If it's a successful request, save cookies
            if response.status_code < 400 and self.storage_dir:
                self._save_cookies(service_name)
                
            return response
                
        finally:
            # Remove from active requests
            with self.lock:
                self.active_requests.discard(request_id)
    
    def clear_cache(self, service_name: Optional[str] = None) -> None:
        """
        Clear the request cache.
        
        Args:
            service_name: Name of the service (None = all services)
        """
        with self.lock:
            if service_name:
                # Clear cache for specific service
                if service_name in self.cache:
                    del self.cache[service_name]
                if service_name in self.cache_ttl:
                    del self.cache_ttl[service_name]
            else:
                # Clear all cache
                self.cache = {}
                self.cache_ttl = {}
    
    def get_default_retry_strategy(self, total_retries: int = 3,
                                 backoff_factor: float = 0.5,
                                 status_forcelist: Optional[List[int]] = None) -> Retry:
        """
        Get a default retry strategy for requests.
        
        Args:
            total_retries: Total number of retries
            backoff_factor: Backoff factor
            status_forcelist: List of status codes to retry on
            
        Returns:
            Retry: Retry strategy
        """
        if status_forcelist is None:
            status_forcelist = [429, 500, 502, 503, 504]
            
        return Retry(
            total=total_retries,
            read=total_retries,
            connect=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE", "PATCH"]
        )
    
    def purge_expired_cache(self) -> int:
        """
        Remove expired items from the cache.
        
        Returns:
            int: Number of expired items removed
        """
        with self.lock:
            now = time.time()
            removed = 0
            
            for service_name in list(self.cache_ttl.keys()):
                service_ttl = self.cache_ttl[service_name]
                service_cache = self.cache.get(service_name, {})
                
                for cache_key in list(service_ttl.keys()):
                    if service_ttl[cache_key] <= now:
                        # Remove expired item
                        del service_ttl[cache_key]
                        if cache_key in service_cache:
                            del service_cache[cache_key]
                        removed += 1
                        
            return removed
    
    def get_session_stats(self, service_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about the sessions and cache.
        
        Args:
            service_name: Name of the service (None = all services)
            
        Returns:
            Dict[str, Any]: Session statistics
        """
        with self.lock:
            stats = {
                "sessions_count": len(self.sessions),
                "active_requests": len(self.active_requests),
                "cache_size": sum(len(cache) for cache in self.cache.values()),
                "services": list(self.sessions.keys())
            }
            
            if service_name and service_name in self.sessions:
                session = self.sessions[service_name]
                
                stats["service"] = {
                    "name": service_name,
                    "cookies_count": len(session.cookies),
                    "headers": dict(session.headers),
                    "cache_entries": len(self.cache.get(service_name, {})),
                    "active_requests": sum(1 for req in self.active_requests if req.startswith(f"{service_name}:"))
                }
                
            return stats
