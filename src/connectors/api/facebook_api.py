# src/connectors/api/facebook_api.py

import os
import requests
import json
import time
import logging
import urllib.parse
from typing import Dict, List, Any, Optional, Union, Tuple
import pandas as pd
import datetime


class FacebookAPI:
    """
    Facebook Graph API connector with authentication, rate limiting, and data extraction capabilities.
    """
    
    # Facebook API endpoints
    BASE_URL = "https://graph.facebook.com"
    AUTH_URL = "https://www.facebook.com/v17.0/dialog/oauth"
    TOKEN_URL = "https://graph.facebook.com/v17.0/oauth/access_token"
    
    # Default API version
    API_VERSION = "v17.0"
    
    def __init__(self, app_id: str = "", app_secret: str = "", 
                redirect_uri: str = "", 
                token_path: Optional[str] = None,
                rate_limit: int = 200,
                version: str = "v17.0"):
        """
        Initialize the Facebook API connector.
        
        Args:
            app_id: Facebook application ID
            app_secret: Facebook application secret
            redirect_uri: OAuth redirect URI
            token_path: Path to save/load authentication tokens
            rate_limit: Requests per hour rate limit
            version: API version
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.redirect_uri = redirect_uri
        self.token_path = token_path
        self.rate_limit = rate_limit
        self.version = version
        
        self.access_token = None
        self.token_expiry = None
        self.logger = logging.getLogger(__name__)
        
        # Rate limiting
        self.request_timestamps = []
        
        # If token path is provided, try to load existing token
        if token_path and os.path.exists(token_path):
            self.load_token()
    
    def auth_url(self, scopes: List[str]) -> str:
        """
        Generate the authorization URL for OAuth 2.0 flow.
        
        Args:
            scopes: List of permission scopes to request
            
        Returns:
            str: Authorization URL
        """
        params = {
            'client_id': self.app_id,
            'redirect_uri': self.redirect_uri,
            'scope': ','.join(scopes),
            'response_type': 'code',
            'state': 'facebook_auth'
        }
        
        query_string = urllib.parse.urlencode(params)
        return f"{self.AUTH_URL}?{query_string}"
    
    def get_token_from_code(self, code: str) -> bool:
        """
        Exchange authorization code for access token.
        
        Args:
            code: OAuth authorization code
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            params = {
                'client_id': self.app_id,
                'client_secret': self.app_secret,
                'redirect_uri': self.redirect_uri,
                'code': code
            }
            
            response = requests.get(self.TOKEN_URL, params=params)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('access_token')
                
                # Set token expiry if provided
                if 'expires_in' in data:
                    expires_in = data.get('expires_in', 0)
                    self.token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
                
                # Save token if path provided
                if self.token_path:
                    self.save_token()
                    
                return True
            else:
                self.logger.error(f"Error getting token: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error getting token: {str(e)}")
            return False
    
    def get_app_token(self) -> bool:
        """
        Get an app access token for server-to-server requests.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            params = {
                'client_id': self.app_id,
                'client_secret': self.app_secret,
                'grant_type': 'client_credentials'
            }
            
            response = requests.get(self.TOKEN_URL, params=params)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('access_token')
                
                # Set token expiry if provided
                if 'expires_in' in data:
                    expires_in = data.get('expires_in', 0)
                    self.token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
                
                # Save token if path provided
                if self.token_path:
                    self.save_token()
                    
                return True
            else:
                self.logger.error(f"Error getting app token: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error getting app token: {str(e)}")
            return False
    
    def extend_token(self) -> bool:
        """
        Extend the expiration time of a user access token.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.access_token:
            return False
            
        try:
            params = {
                'client_id': self.app_id,
                'client_secret': self.app_secret,
                'grant_type': 'fb_exchange_token',
                'fb_exchange_token': self.access_token
            }
            
            response = requests.get(f"{self.BASE_URL}/{self.version}/oauth/access_token", params=params)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('access_token')
                
                # Set token expiry if provided
                if 'expires_in' in data:
                    expires_in = data.get('expires_in', 0)
                    self.token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
                
                # Save token if path provided
                if self.token_path:
                    self.save_token()
                    
                return True
            else:
                self.logger.error(f"Error extending token: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Error extending token: {str(e)}")
            return False
    
    def save_token(self) -> bool:
        """
        Save the access token to a file.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.access_token or not self.token_path:
            return False
            
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(self.token_path)), exist_ok=True)
            
            token_data = {
                'access_token': self.access_token,
                'expiry': self.token_expiry.isoformat() if self.token_expiry else None
            }
            
            with open(self.token_path, 'w') as f:
                json.dump(token_data, f)
                
            return True
        except Exception as e:
            self.logger.error(f"Error saving token: {str(e)}")
            return False
    
    def load_token(self) -> bool:
        """
        Load the access token from a file.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.token_path or not os.path.exists(self.token_path):
            return False
            
        try:
            with open(self.token_path, 'r') as f:
                token_data = json.load(f)
                
            self.access_token = token_data.get('access_token')
            
            expiry_str = token_data.get('expiry')
            if expiry_str:
                self.token_expiry = datetime.datetime.fromisoformat(expiry_str)
                
                # Check if token is expired
                if self.token_expiry and self.token_expiry <= datetime.datetime.now():
                    self.logger.info("Token expired. Attempting to extend...")
                    return self.extend_token()
            
            return bool(self.access_token)
        except Exception as e:
            self.logger.error(f"Error loading token: {str(e)}")
            return False
    
    def _check_rate_limit(self) -> None:
        """Check rate limit and sleep if necessary."""
        now = time.time()
        
        # Remove timestamps older than 1 hour
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 3600]
        
        # If we've hit the rate limit, sleep until we can make another request
        if len(self.request_timestamps) >= self.rate_limit:
            sleep_time = 3600 - (now - self.request_timestamps[0])
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                # Update the current time after sleeping
                now = time.time()
        
        # Add the current timestamp
        self.request_timestamps.append(now)
    
    def _make_request(self, method: str, endpoint: str, 
                    params: Optional[Dict] = None, 
                    data: Optional[Dict] = None,
                    files: Optional[Dict] = None,
                    retries: int = 3) -> Tuple[bool, Any]:
        """
        Make an API request with rate limiting and retry logic.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request data
            files: Files to upload
            retries: Number of retry attempts
            
        Returns:
            Tuple[bool, Any]: (Success flag, Response data or error message)
        """
        if not self.access_token:
            return False, "Not authenticated"
            
        # Prepare the request
        url = f"{self.BASE_URL}/{self.version}/{endpoint.lstrip('/')}"
        
        # Add access token to params
        params = params or {}
        params['access_token'] = self.access_token
        
        attempts = 0
        while attempts < retries:
            try:
                # Check rate limit before making the request
                self._check_rate_limit()
                
                # Make the request
                if method.lower() == 'get':
                    response = requests.get(url, params=params)
                elif method.lower() == 'post':
                    response = requests.post(url, params=params, data=data, files=files)
                elif method.lower() == 'delete':
                    response = requests.delete(url, params=params)
                else:
                    return False, f"Unsupported method: {method}"
                
                # Handle response
                if response.status_code == 200:
                    try:
                        return True, response.json()
                    except ValueError:
                        return True, response.text
                elif response.status_code == 401 or response.status_code == 403:
                    # Unauthorized - try to extend token
                    self.logger.info("Token expired or insufficient permissions. Attempting to extend...")
                    if self.extend_token():
                        # Update the access token in params
                        params['access_token'] = self.access_token
                        attempts += 1
                        continue
                    else:
                        return False, "Authentication failed and token extension failed"
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    retry_after = int(response.headers.get('Retry-After', 3600))
                    self.logger.info(f"Rate limited. Waiting for {retry_after} seconds")
                    time.sleep(retry_after)
                    attempts += 1
                    continue
                else:
                    error_msg = f"API error: {response.status_code}"
                    try:
                        error_data = response.json()
                        error_msg = f"{error_msg} - {error_data}"
                    except:
                        pass
                    return False, error_msg
                    
            except Exception as e:
                self.logger.error(f"Request error: {str(e)}")
                attempts += 1
                if attempts < retries:
                    # Exponential backoff
                    wait_time = 2 ** attempts
                    self.logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    return False, str(e)
        
        return False, "Max retries exceeded"
    
    def get_user_profile(self, user_id: str = "me", fields: Optional[List[str]] = None) -> Tuple[bool, Dict]:
        """
        Get profile information for a user.
        
        Args:
            user_id: Facebook user ID or 'me' for the current user
            fields: List of fields to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Profile data or error message)
        """
        if not fields:
            fields = ["id", "name", "email", "picture", "link"]
            
        return self._make_request(
            method='get',
            endpoint=f'{user_id}',
            params={
                'fields': ','.join(fields)
            }
        )
    
    def get_page_info(self, page_id: str, fields: Optional[List[str]] = None) -> Tuple[bool, Dict]:
        """
        Get information about a Facebook Page.
        
        Args:
            page_id: Facebook Page ID
            fields: List of fields to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Page data or error message)
        """
        if not fields:
            fields = ["id", "name", "about", "description", "category", "fan_count", "website", "picture"]
            
        return self._make_request(
            method='get',
            endpoint=f'{page_id}',
            params={
                'fields': ','.join(fields)
            }
        )
    
    def get_page_posts(self, page_id: str, limit: int = 10, 
                    since: Optional[str] = None, 
                    until: Optional[str] = None) -> Tuple[bool, Dict]:
        """
        Get posts from a Facebook Page.
        
        Args:
            page_id: Facebook Page ID
            limit: Maximum number of posts to retrieve
            since: Start date (ISO format)
            until: End date (ISO format)
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Posts data or error message)
        """
        params = {
            'limit': limit,
            'fields': 'id,message,created_time,attachments,permalink_url,shares,likes.summary(true),comments.summary(true)'
        }
        
        if since:
            params['since'] = since
        if until:
            params['until'] = until
            
        return self._make_request(
            method='get',
            endpoint=f'{page_id}/posts',
            params=params
        )
    
    def get_post_details(self, post_id: str) -> Tuple[bool, Dict]:
        """
        Get detailed information about a post.
        
        Args:
            post_id: Facebook post ID
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Post data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'{post_id}',
            params={
                'fields': 'id,message,created_time,attachments,permalink_url,shares,likes.summary(true),comments.summary(true),reactions.summary(true)'
            }
        )
    
    def get_post_comments(self, post_id: str, limit: int = 25) -> Tuple[bool, Dict]:
        """
        Get comments from a post.
        
        Args:
            post_id: Facebook post ID
            limit: Maximum number of comments to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Comments data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'{post_id}/comments',
            params={
                'limit': limit,
                'fields': 'id,message,created_time,attachment,like_count,comment_count,from'
            }
        )
    
    def search_pages(self, query: str, limit: int = 10, fields: Optional[List[str]] = None) -> Tuple[bool, Dict]:
        """
        Search for Facebook Pages.
        
        Args:
            query: Search query
            limit: Maximum number of results
            fields: List of fields to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Search results or error message)
        """
        if not fields:
            fields = ["id", "name", "category", "link"]
            
        return self._make_request(
            method='get',
            endpoint='search',
            params={
                'q': query,
                'type': 'page',
                'limit': limit,
                'fields': ','.join(fields)
            }
        )
    
    def get_page_insights(self, page_id: str, metrics: List[str], 
                        period: str = "day", 
                        since: Optional[str] = None, 
                        until: Optional[str] = None) -> Tuple[bool, Dict]:
        """
        Get insights for a Facebook Page.
        
        Args:
            page_id: Facebook Page ID
            metrics: List of insight metrics to retrieve
            period: Time period ('day', 'week', 'month', etc.)
            since: Start date (ISO format)
            until: End date (ISO format)
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Insights data or error message)
        """
        params = {
            'metric': ','.join(metrics),
            'period': period
        }
        
        if since:
            params['since'] = since
        if until:
            params['until'] = until
            
        return self._make_request(
            method='get',
            endpoint=f'{page_id}/insights',
            params=params
        )
    
    def publish_page_post(self, page_id: str, message: str, 
                        link: Optional[str] = None, 
                        published: bool = True) -> Tuple[bool, Dict]:
        """
        Publish a post to a Facebook Page.
        
        Args:
            page_id: Facebook Page ID
            message: Post message
            link: URL to share
            published: Whether to publish immediately
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Response data or error message)
        """
        data = {
            'message': message,
            'published': json.dumps(published)
        }
        
        if link:
            data['link'] = link
            
        return self._make_request(
            method='post',
            endpoint=f'{page_id}/feed',
            data=data
        )
    
    def delete_post(self, post_id: str) -> Tuple[bool, Dict]:
        """
        Delete a Facebook post.
        
        Args:
            post_id: Facebook post ID
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Response data or error message)
        """
        return self._make_request(
            method='delete',
            endpoint=f'{post_id}'
        )
    
    def get_events(self, page_id: str, time_filter: str = "upcoming", 
                 limit: int = 10) -> Tuple[bool, Dict]:
        """
        Get events for a Facebook Page.
        
        Args:
            page_id: Facebook Page ID
            time_filter: Filter by time ('upcoming', 'past', or empty for all)
            limit: Maximum number of events to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Events data or error message)
        """
        params = {
            'limit': limit,
            'fields': 'id,name,description,start_time,end_time,place,attending_count,interested_count'
        }
        
        if time_filter:
            params['time_filter'] = time_filter
            
        return self._make_request(
            method='get',
            endpoint=f'{page_id}/events',
            params=params
        )
    
    def get_page_albums(self, page_id: str, limit: int = 10) -> Tuple[bool, Dict]:
        """
        Get photo albums for a Facebook Page.
        
        Args:
            page_id: Facebook Page ID
            limit: Maximum number of albums to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Albums data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'{page_id}/albums',
            params={
                'limit': limit,
                'fields': 'id,name,description,created_time,count,cover_photo{source}'
            }
        )
    
    def get_album_photos(self, album_id: str, limit: int = 25) -> Tuple[bool, Dict]:
        """
        Get photos from an album.
        
        Args:
            album_id: Facebook album ID
            limit: Maximum number of photos to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Photos data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'{album_id}/photos',
            params={
                'limit': limit,
                'fields': 'id,name,images,created_time,place,likes.summary(true),comments.summary(true)'
            }
        )
    
    def get_page_videos(self, page_id: str, limit: int = 10) -> Tuple[bool, Dict]:
        """
        Get videos from a Facebook Page.
        
        Args:
            page_id: Facebook Page ID
            limit: Maximum number of videos to retrieve
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Videos data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'{page_id}/videos',
            params={
                'limit': limit,
                'fields': 'id,title,description,created_time,thumbnail_url,permalink_url,views,likes.summary(true),comments.summary(true)'
            }
        )
    
    def pagination(self, response: Dict) -> Tuple[bool, Any]:
        """
        Get the next page of results using pagination.
        
        Args:
            response: Previous API response
            
        Returns:
            Tuple[bool, Any]: (Success flag, Next page data or error message)
        """
        if not response or 'paging' not in response or 'next' not in response['paging']:
            return False, "No next page available"
            
        next_url = response['paging']['next']
        
        try:
            # Check rate limit before making the request
            self._check_rate_limit()
            
            # Make the request
            response = requests.get(next_url)
            
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, f"API error: {response.status_code}"
        except Exception as e:
            return False, str(e)
    
    def get_all_results(self, endpoint: str, params: Dict, 
                      max_results: int = 100, 
                      max_pages: int = 10) -> Tuple[bool, List[Dict]]:
        """
        Get all paginated results from an API endpoint.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            max_results: Maximum number of results to return
            max_pages: Maximum number of pages to fetch
            
        Returns:
            Tuple[bool, List[Dict]]: (Success flag, Combined results or error message)
        """
        all_data = []
        page_count = 0
        
        # Make initial request
        success, response = self._make_request(
            method='get',
            endpoint=endpoint,
            params=params
        )
        
        if not success:
            return False, response
            
        # Extract data
        if 'data' in response:
            all_data.extend(response['data'])
            
        # Continue pagination until we reach max_results or max_pages
        while (len(all_data) < max_results and 
              page_count < max_pages and 
              'paging' in response and 
              'next' in response['paging']):
            
            # Get next page
            success, response = self.pagination(response)
            
            if not success:
                break
                
            # Extract data
            if 'data' in response:
                all_data.extend(response['data'])
                
            page_count += 1
            
        # Trim to max_results if needed
        if len(all_data) > max_results:
            all_data = all_data[:max_results]
            
        return True, all_data
    
    def results_to_dataframe(self, results: Dict, 
                          flatten: bool = True) -> pd.DataFrame:
        """
        Convert API results to a pandas DataFrame.
        
        Args:
            results: API response data
            flatten: Whether to flatten nested structures
            
        Returns:
            pd.DataFrame: Data in DataFrame format
        """
        try:
            if not results or 'data' not in results:
                return pd.DataFrame()
                
            data = results['data']
            
            if flatten:
                # Flatten nested structures
                flattened = []
                for item in data:
                    flat_item = {}
                    self._flatten_dict(item, flat_item)
                    flattened.append(flat_item)
                return pd.DataFrame(flattened)
            else:
                return pd.DataFrame(data)
        except Exception as e:
            self.logger.error(f"Error converting to DataFrame: {str(e)}")
            return pd.DataFrame()
    
    def _flatten_dict(self, nested_dict: Dict, flat_dict: Dict, prefix: str = '') -> None:
        """Recursively flatten a nested dictionary."""
        for key, value in nested_dict.items():
            if prefix:
                new_key = f"{prefix}_{key}"
            else:
                new_key = key
                
            if isinstance(value, dict):
                self._flatten_dict(value, flat_dict, new_key)
            elif isinstance(value, list):
                # For lists, create a string representation
                flat_dict[new_key] = str(value)
            else:
                flat_dict[new_key] = value
