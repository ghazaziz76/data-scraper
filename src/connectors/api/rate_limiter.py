# src/connectors/api/rate_limiter.py

import time
import logging
import threading
from typing import Dict, List, Optional, Any, Callable
from collections import defaultdict, deque
import math


class RateLimiter:
    """
    A flexible rate limiter for API requests with various throttling strategies.
    
    Supports different rate limit windows (per second, minute, hour, day) and
    can handle multiple rate limit pools for different API endpoints or methods.
    """
    
    def __init__(self, default_limit: int = 60, default_window: int = 60):
        """
        Initialize the rate limiter.
        
        Args:
            default_limit: Default number of requests allowed in the window
            default_window: Default time window in seconds (60 = per minute)
        """
        self.default_limit = default_limit
        self.default_window = default_window
        self.logger = logging.getLogger(__name__)
        
        # Thread lock for rate limiting
        self.lock = threading.RLock()
        
        # Rate limit pools for different endpoints/methods
        # Format: {'pool_name': deque([timestamp1, timestamp2, ...])}
        self.request_pools = defaultdict(deque)
        
        # Rate limit configurations for different pools
        # Format: {'pool_name': {'limit': 100, 'window': 3600}}
        self.pool_configs = {}
        
        # Retry status tracking
        self.retry_counts = defaultdict(int)
        self.max_retries = 3
        self.backoff_factor = 2
        
        # Event hooks
        self.on_rate_limited = None
        self.on_retry = None
    
    def add_limit(self, pool_name: str, limit: int, window: int) -> None:
        """
        Add a rate limit configuration for a specific pool.
        
        Args:
            pool_name: Name of the rate limit pool
            limit: Number of requests allowed in the window
            window: Time window in seconds
        """
        with self.lock:
            self.pool_configs[pool_name] = {
                'limit': limit,
                'window': window
            }
    
    def get_limit_config(self, pool_name: str) -> Dict[str, int]:
        """
        Get the rate limit configuration for a pool.
        
        Args:
            pool_name: Name of the rate limit pool
            
        Returns:
            Dict[str, int]: Rate limit configuration
        """
        with self.lock:
            if pool_name in self.pool_configs:
                return self.pool_configs[pool_name]
            else:
                return {
                    'limit': self.default_limit,
                    'window': self.default_window
                }
    
    def check_limit(self, pool_name: str = "default") -> bool:
        """
        Check if a request would exceed the rate limit.
        
        Args:
            pool_name: Name of the rate limit pool
            
        Returns:
            bool: True if request is allowed, False if it would exceed the limit
        """
        with self.lock:
            config = self.get_limit_config(pool_name)
            limit = config['limit']
            window = config['window']
            
            now = time.time()
            
            # Clean up old timestamps
            while self.request_pools[pool_name] and now - self.request_pools[pool_name][0] > window:
                self.request_pools[pool_name].popleft()
            
            # Check if we've hit the limit
            return len(self.request_pools[pool_name]) < limit
    
    def add_request(self, pool_name: str = "default") -> None:
        """
        Record a request for rate limiting purposes.
        
        Args:
            pool_name: Name of the rate limit pool
        """
        with self.lock:
            now = time.time()
            self.request_pools[pool_name].append(now)
    
    def wait_if_needed(self, pool_name: str = "default") -> float:
        """
        Wait if necessary to comply with rate limits.
        
        Args:
            pool_name: Name of the rate limit pool
            
        Returns:
            float: Time waited in seconds (0 if no wait was needed)
        """
        with self.lock:
            if self.check_limit(pool_name):
                # No need to wait
                self.add_request(pool_name)
                return 0
            
            config = self.get_limit_config(pool_name)
            window = config['window']
            
            now = time.time()
            
            # Calculate wait time based on oldest request
            oldest = self.request_pools[pool_name][0]
            wait_time = oldest + window - now
            
            if wait_time > 0:
                if self.on_rate_limited:
                    self.on_rate_limited(pool_name, wait_time)
                else:
                    self.logger.info(f"Rate limit reached for pool '{pool_name}'. Waiting for {wait_time:.2f} seconds")
                
                time.sleep(wait_time)
                
                # Add the request after waiting
                self.add_request(pool_name)
                return wait_time
            else:
                # No need to wait (window has passed since checking)
                self.add_request(pool_name)
                return 0
    
    def reset_pool(self, pool_name: str) -> None:
        """
        Reset the request history for a rate limit pool.
        
        Args:
            pool_name: Name of the rate limit pool
        """
        with self.lock:
            self.request_pools[pool_name] = deque()
    
    def get_remaining_requests(self, pool_name: str = "default") -> int:
        """
        Get the number of remaining requests allowed in the current window.
        
        Args:
            pool_name: Name of the rate limit pool
            
        Returns:
            int: Number of remaining requests
        """
        with self.lock:
            config = self.get_limit_config(pool_name)
            limit = config['limit']
            window = config['window']
            
            now = time.time()
            
            # Clean up old timestamps
            while self.request_pools[pool_name] and now - self.request_pools[pool_name][0] > window:
                self.request_pools[pool_name].popleft()
            
            return max(0, limit - len(self.request_pools[pool_name]))
    
    def get_reset_time(self, pool_name: str = "default") -> float:
        """
        Get the time when the rate limit window will reset.
        
        Args:
            pool_name: Name of the rate limit pool
            
        Returns:
            float: Seconds until reset (0 if already reset)
        """
        with self.lock:
            if not self.request_pools[pool_name]:
                return 0
                
            config = self.get_limit_config(pool_name)
            window = config['window']
            
            now = time.time()
            oldest = self.request_pools[pool_name][0]
            
            return max(0, oldest + window - now)
    
    def set_retry_hook(self, callback: Callable[[str, int, float], None]) -> None:
        """
        Set a callback function for retry events.
        
        Args:
            callback: Function to call on retries, with parameters:
                     (pool_name, retry_count, wait_time)
        """
        self.on_retry = callback
    
    def set_rate_limit_hook(self, callback: Callable[[str, float], None]) -> None:
        """
        Set a callback function for rate limit events.
        
        Args:
            callback: Function to call when rate limited, with parameters:
                     (pool_name, wait_time)
        """
        self.on_rate_limited = callback
    
    def should_retry(self, pool_name: str, status_code: int) -> bool:
        """
        Determine if a request should be retried based on status code and retry count.
        
        Args:
            pool_name: Name of the rate limit pool
            status_code: HTTP status code of the failed request
            
        Returns:
            bool: True if the request should be retried
        """
        with self.lock:
            # Retry on common transient errors
            retryable_codes = [429, 500, 502, 503, 504]
            
            if status_code not in retryable_codes:
                return False
                
            if self.retry_counts[pool_name] >= self.max_retries:
                return False
                
            return True
    
    def get_retry_wait_time(self, pool_name: str, status_code: int) -> float:
        """
        Calculate wait time for a retry with exponential backoff.
        
        Args:
            pool_name: Name of the rate limit pool
            status_code: HTTP status code of the failed request
            
        Returns:
            float: Seconds to wait before retry
        """
        with self.lock:
            retry_count = self.retry_counts[pool_name]
            
            # Special case for rate limiting (429)
            if status_code == 429:
                # Use longer wait for rate limiting
                base_wait = 60  # 1 minute
            else:
                # Standard exponential backoff for other errors
                base_wait = 1  # 1 second
            
            # Calculate wait time with exponential backoff and jitter
            wait_time = base_wait * (self.backoff_factor ** retry_count)
            
            # Add some randomness (jitter) to prevent synchronized retries
            jitter = wait_time * 0.2  # 20% jitter
            wait_time = wait_time + (jitter * (2 * (0.5 - random.random())))
            
            # Increment retry counter
            self.retry_counts[pool_name] += 1
            
            if self.on_retry:
                self.on_retry(pool_name, retry_count, wait_time)
            else:
                self.logger.info(f"Retrying request for pool '{pool_name}' after {wait_time:.2f} seconds (attempt {retry_count + 1}/{self.max_retries})")
            
            return wait_time
    
    def reset_retries(self, pool_name: str) -> None:
        """
        Reset the retry counter for a pool.
        
        Args:
            pool_name: Name of the rate limit pool
        """
        with self.lock:
            self.retry_counts[pool_name] = 0
    
    def execute_with_rate_limit(self, func: Callable, pool_name: str = "default", *args, **kwargs) -> Any:
        """
        Execute a function with rate limiting and automatic retries.
        
        Args:
            func: Function to execute
            pool_name: Name of the rate limit pool
            *args: Arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            Any: Result of the function
        """
        # Reset retry counter
        self.reset_retries(pool_name)
        
        # Try the initial request
        self.wait_if_needed(pool_name)
        
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Check status code for retry
            status_code = getattr(e, 'status_code', 500)
            
            # Retry loop
            while self.should_retry(pool_name, status_code):
                # Calculate wait time
                wait_time = self.get_retry_wait_time(pool_name, status_code)
                
                # Wait before retry
                time.sleep(wait_time)
                
                # Perform rate limiting check again
                self.wait_if_needed(pool_name)
                
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Update status code for next retry decision
                    status_code = getattr(e, 'status_code', 500)
            
            # If we get here, we've exhausted retries
            raise
