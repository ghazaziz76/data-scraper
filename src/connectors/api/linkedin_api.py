# src/connectors/api/linkedin_api.py

import os
import requests
import json
import time
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
import pandas as pd
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient


class LinkedInAPI:
    """
    LinkedIn API connector with authentication, rate limiting, and data extraction capabilities.
    """
    
    # LinkedIn API endpoints
    BASE_URL = "https://api.linkedin.com/v2"
    AUTH_URL = "https://www.linkedin.com/oauth/v2/authorization"
    TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
    
    # Default API version
    API_VERSION = "v2"
    
    def __init__(self, client_id: str = "", client_secret: str = "", 
                redirect_uri: str = "", 
                token_path: Optional[str] = None,
                rate_limit: int = 60):
        """
        Initialize the LinkedIn API connector.
        
        Args:
            client_id: LinkedIn application client ID
            client_secret: LinkedIn application client secret
            redirect_uri: OAuth redirect URI
            token_path: Path to save/load authentication tokens
            rate_limit: Requests per minute rate limit
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_path = token_path
        self.rate_limit = rate_limit
        
        self.session = None
        self.token = None
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
        oauth = OAuth2Session(
            client_id=self.client_id,
            redirect_uri=self.redirect_uri,
            scope=scopes
        )
        
        auth_url, state = oauth.authorization_url(self.AUTH_URL)
        return auth_url
    
    def get_token_from_code(self, code: str) -> bool:
        """
        Exchange authorization code for access token.
        
        Args:
            code: OAuth authorization code
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            oauth = OAuth2Session(
                client_id=self.client_id,
                redirect_uri=self.redirect_uri
            )
            
            self.token = oauth.fetch_token(
                token_url=self.TOKEN_URL,
                client_secret=self.client_secret,
                code=code
            )
            
            # Create authenticated session
            self.session = oauth
            
            # Save token if path provided
            if self.token_path:
                self.save_token()
                
            return True
        except Exception as e:
            self.logger.error(f"Error getting token: {str(e)}")
            return False
    
    def get_token_from_client_credentials(self) -> bool:
        """
        Get access token using client credentials grant (for organization APIs).
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            client = BackendApplicationClient(client_id=self.client_id)
            oauth = OAuth2Session(client=client)
            
            self.token = oauth.fetch_token(
                token_url=self.TOKEN_URL,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            
            # Create authenticated session
            self.session = oauth
            
            # Save token if path provided
            if self.token_path:
                self.save_token()
                
            return True
        except Exception as e:
            self.logger.error(f"Error getting token: {str(e)}")
            return False
    
    def save_token(self) -> bool:
        """
        Save the access token to a file.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.token or not self.token_path:
            return False
            
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(self.token_path)), exist_ok=True)
            
            with open(self.token_path, 'w') as f:
                json.dump(self.token, f)
                
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
                self.token = json.load(f)
                
            # Create authenticated session
            self.session = OAuth2Session(
                client_id=self.client_id,
                token=self.token
            )
            
            return True
        except Exception as e:
            self.logger.error(f"Error loading token: {str(e)}")
            return False
    
    def refresh_token(self) -> bool:
        """
        Refresh the access token.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.token or 'refresh_token' not in self.token:
            return False
            
        try:
            # Create a new session for the refresh
            oauth = OAuth2Session(
                client_id=self.client_id,
                token=self.token
            )
            
            self.token = oauth.refresh_token(
                token_url=self.TOKEN_URL,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.token['refresh_token']
            )
            
            # Update the session
            self.session = oauth
            
            # Save the new token
            if self.token_path:
                self.save_token()
                
            return True
        except Exception as e:
            self.logger.error(f"Error refreshing token: {str(e)}")
            return False
    
    def _check_rate_limit(self) -> None:
        """Check rate limit and sleep if necessary."""
        now = time.time()
        
        # Remove timestamps older than 1 minute
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        # If we've hit the rate limit, sleep until we can make another request
        if len(self.request_timestamps) >= self.rate_limit:
            sleep_time = 60 - (now - self.request_timestamps[0])
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
                    headers: Optional[Dict] = None,
                    retries: int = 3) -> Tuple[bool, Any]:
        """
        Make an API request with rate limiting and retry logic.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request data
            headers: Additional headers
            retries: Number of retry attempts
            
        Returns:
            Tuple[bool, Any]: (Success flag, Response data or error message)
        """
        if not self.session or not self.token:
            return False, "Not authenticated"
            
        # Prepare the request
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = headers or {}
        
        # Add default headers
        if 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'
        
        attempts = 0
        while attempts < retries:
            try:
                # Check rate limit before making the request
                self._check_rate_limit()
                
                # Make the request
                if method.lower() == 'get':
                    response = self.session.get(url, params=params, headers=headers)
                elif method.lower() == 'post':
                    response = self.session.post(url, params=params, json=data, headers=headers)
                elif method.lower() == 'put':
                    response = self.session.put(url, params=params, json=data, headers=headers)
                elif method.lower() == 'delete':
                    response = self.session.delete(url, params=params, headers=headers)
                else:
                    return False, f"Unsupported method: {method}"
                
                # Handle response
                if response.status_code == 200:
                    try:
                        return True, response.json()
                    except ValueError:
                        return True, response.text
                elif response.status_code == 401:
                    # Unauthorized - try to refresh token
                    self.logger.info("Token expired. Attempting to refresh...")
                    if self.refresh_token():
                        attempts += 1
                        continue
                    else:
                        return False, "Authentication failed and token refresh failed"
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    retry_after = int(response.headers.get('Retry-After', 60))
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
    
    def get_profile(self) -> Tuple[bool, Dict]:
        """
        Get the current user's profile information.
        
        Returns:
            Tuple[bool, Dict]: (Success flag, Profile data or error message)
        """
        return self._make_request(
            method='get',
            endpoint='/me',
            params={
                'projection': '(id,firstName,lastName,profilePicture,headline,vanityName,emailAddress)'
            }
        )
    
    def get_company(self, company_id: str) -> Tuple[bool, Dict]:
        """
        Get information about a company.
        
        Args:
            company_id: LinkedIn company ID
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Company data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'/organizations/{company_id}',
            params={
                'projection': '(id,name,tagline,description,logoV2,vanityName,website)'
            }
        )
    
    def search_companies(self, keywords: str, 
                       start: int = 0, 
                       count: int = 10) -> Tuple[bool, Dict]:
        """
        Search for companies by keywords.
        
        Args:
            keywords: Search keywords
            start: Pagination start index
            count: Number of results per page
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Search results or error message)
        """
        return self._make_request(
            method='get',
            endpoint='/search/organizations',
            params={
                'keywords': keywords,
                'start': start,
                'count': count
            }
        )
    
    def get_company_updates(self, company_id: str, 
                          start: int = 0, 
                          count: int = 10) -> Tuple[bool, Dict]:
        """
        Get updates from a company.
        
        Args:
            company_id: LinkedIn company ID
            start: Pagination start index
            count: Number of results per page
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Updates data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'/organizations/{company_id}/updates',
            params={
                'start': start,
                'count': count
            }
        )
    
    def search_people(self, keywords: str, 
                    first_name: Optional[str] = None,
                    last_name: Optional[str] = None,
                    company_id: Optional[str] = None,
                    start: int = 0, 
                    count: int = 10) -> Tuple[bool, Dict]:
        """
        Search for people by keywords and filters.
        
        Args:
            keywords: Search keywords
            first_name: Filter by first name
            last_name: Filter by last name
            company_id: Filter by company ID
            start: Pagination start index
            count: Number of results per page
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Search results or error message)
        """
        params = {
            'keywords': keywords,
            'start': start,
            'count': count
        }
        
        if first_name:
            params['firstName'] = first_name
        if last_name:
            params['lastName'] = last_name
        if company_id:
            params['companyId'] = company_id
            
        return self._make_request(
            method='get',
            endpoint='/search/people',
            params=params
        )
    
    def get_connections(self, start: int = 0, 
                      count: int = 10) -> Tuple[bool, Dict]:
        """
        Get the current user's connections.
        
        Args:
            start: Pagination start index
            count: Number of results per page
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Connections data or error message)
        """
        return self._make_request(
            method='get',
            endpoint='/connections',
            params={
                'start': start,
                'count': count,
                'projection': '(id,firstName,lastName,headline)'
            }
        )
    
    def send_message(self, recipient_ids: List[str], 
                   subject: str, 
                   body: str) -> Tuple[bool, Dict]:
        """
        Send a message to connections.
        
        Args:
            recipient_ids: List of recipient LinkedIn IDs
            subject: Message subject
            body: Message body
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Response data or error message)
        """
        data = {
            'recipients': {
                'values': [{'person': {'id': rid}} for rid in recipient_ids]
            },
            'subject': subject,
            'body': body
        }
        
        return self._make_request(
            method='post',
            endpoint='/messages',
            data=data
        )
    
    def share_update(self, text: str, 
                   visibility: str = "PUBLIC") -> Tuple[bool, Dict]:
        """
        Share an update on LinkedIn.
        
        Args:
            text: Update text
            visibility: Post visibility ('PUBLIC', 'CONNECTIONS', or 'PRIVATE')
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Response data or error message)
        """
        data = {
            'owner': f'urn:li:person:{self.get_profile()[1].get("id")}',
            'text': {
                'text': text
            },
            'distribution': {
                'linkedInDistributionTarget': {
                    'visibleToGuest': visibility == 'PUBLIC',
                    'visibleToConnections': visibility != 'PRIVATE'
                }
            }
        }
        
        return self._make_request(
            method='post',
            endpoint='/shares',
            data=data
        )
    
    def get_company_followers(self, company_id: str, 
                           start: int = 0, 
                           count: int = 10) -> Tuple[bool, Dict]:
        """
        Get followers of a company.
        
        Args:
            company_id: LinkedIn company ID
            start: Pagination start index
            count: Number of results per page
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Followers data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'/organizations/{company_id}/followers',
            params={
                'start': start,
                'count': count
            }
        )
    
    def search_jobs(self, keywords: str, 
                  location: Optional[str] = None,
                  company_id: Optional[str] = None,
                  job_type: Optional[str] = None,
                  start: int = 0, 
                  count: int = 10) -> Tuple[bool, Dict]:
        """
        Search for jobs by keywords and filters.
        
        Args:
            keywords: Search keywords
            location: Job location
            company_id: Filter by company ID
            job_type: Job type (e.g. 'F' for full-time)
            start: Pagination start index
            count: Number of results per page
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Search results or error message)
        """
        params = {
            'keywords': keywords,
            'start': start,
            'count': count
        }
        
        if location:
            params['location'] = location
        if company_id:
            params['companyId'] = company_id
        if job_type:
            params['jobType'] = job_type
            
        return self._make_request(
            method='get',
            endpoint='/jobs/search',
            params=params
        )
    
    def get_job_details(self, job_id: str) -> Tuple[bool, Dict]:
        """
        Get details for a specific job.
        
        Args:
            job_id: LinkedIn job ID
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Job details or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'/jobs/{job_id}'
        )
    
    def get_user_profile(self, user_id: str) -> Tuple[bool, Dict]:
        """
        Get profile information for a specific user.
        
        Args:
            user_id: LinkedIn user ID
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Profile data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'/people/{user_id}',
            params={
                'projection': '(id,firstName,lastName,profilePicture,headline,vanityName)'
            }
        )
    
    def get_member_profile(self, vanity_name: str) -> Tuple[bool, Dict]:
        """
        Get profile information using a member's vanity name.
        
        Args:
            vanity_name: LinkedIn vanity name (profile URL identifier)
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Profile data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'/people/vanity={vanity_name}',
            params={
                'projection': '(id,firstName,lastName,profilePicture,headline,vanityName)'
            }
        )
    
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
            if not results or 'elements' not in results:
                return pd.DataFrame()
                
            elements = results['elements']
            
            if flatten:
                # Flatten nested structures
                flattened = []
                for element in elements:
                    flat_element = {}
                    self._flatten_dict(element, flat_element)
                    flattened.append(flat_element)
                return pd.DataFrame(flattened)
            else:
                return pd.DataFrame(elements)
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
    
    def get_company_employees(self, company_id: str, 
                           start: int = 0, 
                           count: int = 10) -> Tuple[bool, Dict]:
        """
        Get employees of a company.
        
        Args:
            company_id: LinkedIn company ID
            start: Pagination start index
            count: Number of results per page
            
        Returns:
            Tuple[bool, Dict]: (Success flag, Employees data or error message)
        """
        return self._make_request(
            method='get',
            endpoint=f'/organizations/{company_id}/employees',
            params={
                'start': start,
                'count': count
            }
        )
    
    def get_profile_network_info(self) -> Tuple[bool, Dict]:
        """
        Get network information for the current user.
        
        Returns:
            Tuple[bool, Dict]: (Success flag, Network data or error message)
        """
        return self._make_request(
            method='get',
            endpoint='/networkinfo'
        )
    
    def get_pagination_results(self, endpoint: str, 
                            params: Dict, 
                            max_results: int = 100) -> Tuple[bool, List[Dict]]:
        """
        Get paginated results from an API endpoint.
        
        Args:
            endpoint: API endpoint
            params: Base parameters for the request
            max_results: Maximum number of results to return
            
        Returns:
            Tuple[bool, List[Dict]]: (Success flag, Combined results or error message)
        """
        all_results = []
        start = params.get('start', 0)
        count = min(params.get('count', 10), 100)  # LinkedIn max is 100 per page
        
        while len(all_results) < max_results:
            # Update pagination parameters
            current_params = params.copy()
            current_params['start'] = start
            current_params['count'] = min(count, max_results - len(all_results))
            
            # Make the request
            success, response = self._make_request(
                method='get',
                endpoint=endpoint,
                params=current_params
            )
            
            if not success:
                return False, response
                
            # Extract and append elements
            if 'elements' in response:
                elements = response['elements']
                all_results.extend(elements)
                
                # Check if we've reached the end
                if len(elements) < count:
                    break
                    
                # Update start for next page
                start += len(elements)
            else:
                # No elements found
                break
        
        return True, all_results
