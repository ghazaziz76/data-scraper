# src/connectors/api/auth_manager.py

import os
import json
import time
import logging
import datetime
from typing import Dict, List, Optional, Any, Union, Callable
import requests
from urllib.parse import urlencode
import base64
import hashlib
import secrets
import threading


class AuthenticationManager:
    """
    Unified authentication manager to handle credentials, tokens, and authentication
    flows for multiple APIs and services.
    """
    
    def __init__(self, storage_dir: str = None, encrypt_tokens: bool = False):
        """
        Initialize the authentication manager.
        
        Args:
            storage_dir: Directory to store authentication tokens
            encrypt_tokens: Whether to encrypt stored tokens
        """
        self.storage_dir = storage_dir
        self.encrypt_tokens = encrypt_tokens
        self.logger = logging.getLogger(__name__)
        
        # Dictionary to store credentials for different services
        self.credentials = {}
        
        # Dictionary to store authentication tokens for different services
        self.tokens = {}
        
        # Dictionary to store auth-related configurations
        self.auth_configs = {}
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        # Create storage directory if it doesn't exist
        if storage_dir:
            os.makedirs(storage_dir, exist_ok=True)
    
    def register_service(self, service_name: str, credentials: Dict[str, str], 
                       auth_config: Dict[str, Any]) -> bool:
        """
        Register a service with its credentials and authentication configuration.
        
        Args:
            service_name: Name of the service
            credentials: Dictionary of credentials (client_id, client_secret, etc.)
            auth_config: Authentication configuration (auth_url, token_url, scopes, etc.)
            
        Returns:
            bool: True if registration was successful
        """
        with self.lock:
            try:
                self.credentials[service_name] = credentials
                self.auth_configs[service_name] = auth_config
                
                # Try to load existing token
                self._load_token(service_name)
                
                return True
            except Exception as e:
                self.logger.error(f"Error registering service {service_name}: {str(e)}")
                return False
    
    def get_auth_url(self, service_name: str, scopes: Optional[List[str]] = None, 
                   state: Optional[str] = None, redirect_uri: Optional[str] = None) -> str:
        """
        Generate an authorization URL for OAuth flow.
        
        Args:
            service_name: Name of the service
            scopes: List of permission scopes to request (overrides config)
            state: State parameter for CSRF protection
            redirect_uri: Redirect URI (overrides config)
            
        Returns:
            str: Authorization URL
        """
        with self.lock:
            if service_name not in self.auth_configs:
                raise ValueError(f"Service {service_name} not registered")
                
            config = self.auth_configs[service_name]
            creds = self.credentials[service_name]
            
            if not state:
                # Generate a random state for CSRF protection
                state = secrets.token_urlsafe(16)
            
            # Get the authorization endpoint
            auth_url = config.get('auth_url')
            if not auth_url:
                raise ValueError(f"Service {service_name} has no auth_url configured")
                
            # Use provided scopes or fall back to config
            if not scopes:
                scopes = config.get('scopes', [])
                
            # Use provided redirect_uri or fall back to config
            if not redirect_uri:
                redirect_uri = config.get('redirect_uri')
            
            # Build parameters based on the OAuth flow type
            params = {
                'response_type': 'code',
                'client_id': creds.get('client_id'),
                'redirect_uri': redirect_uri,
                'state': state
            }
            
            # Add scopes if specified
            if scopes:
                # Different services format scopes differently
                scope_format = config.get('scope_format', 'space')
                if scope_format == 'space':
                    params['scope'] = ' '.join(scopes)
                elif scope_format == 'comma':
                    params['scope'] = ','.join(scopes)
                elif scope_format == 'multiple':
                    # Some APIs use multiple scope parameters
                    for scope in scopes:
                        if 'scope' not in params:
                            params['scope'] = scope
                        else:
                            params['scope'] += ' ' + scope
            
            # Add service-specific parameters
            additional_params = config.get('auth_params', {})
            params.update(additional_params)
            
            # Build the URL
            query_string = urlencode(params)
            return f"{auth_url}?{query_string}"
    
    def get_token_from_code(self, service_name: str, code: str, 
                          redirect_uri: Optional[str] = None) -> bool:
        """
        Exchange an authorization code for an access token.
        
        Args:
            service_name: Name of the service
            code: Authorization code
            redirect_uri: Redirect URI (overrides config)
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name not in self.auth_configs:
                raise ValueError(f"Service {service_name} not registered")
                
            config = self.auth_configs[service_name]
            creds = self.credentials[service_name]
            
            # Get the token endpoint
            token_url = config.get('token_url')
            if not token_url:
                raise ValueError(f"Service {service_name} has no token_url configured")
                
            # Use provided redirect_uri or fall back to config
            if not redirect_uri:
                redirect_uri = config.get('redirect_uri')
            
            # Build the request data
            data = {
                'grant_type': 'authorization_code',
                'code': code,
                'client_id': creds.get('client_id'),
                'client_secret': creds.get('client_secret'),
                'redirect_uri': redirect_uri
            }
            
            # Add service-specific parameters
            additional_params = config.get('token_params', {})
            data.update(additional_params)
            
            # Handle different auth methods
            auth_method = config.get('auth_method', 'params')
            headers = {}
            
            if auth_method == 'basic':
                # HTTP Basic Auth
                auth_string = f"{creds.get('client_id')}:{creds.get('client_secret')}"
                encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
                headers['Authorization'] = f"Basic {encoded_auth}"
                # Remove from data when using basic auth
                if 'client_secret' in data:
                    del data['client_secret']
                if 'client_id' in data:
                    del data['client_id']
            
            # Make the request
            try:
                response = requests.post(token_url, data=data, headers=headers)
                response.raise_for_status()
                
                token_data = response.json()
                
                # Store the token
                self._process_and_store_token(service_name, token_data)
                
                return True
            except Exception as e:
                self.logger.error(f"Error getting token for {service_name}: {str(e)}")
                return False
    
    def get_client_credentials_token(self, service_name: str) -> bool:
        """
        Get an access token using the client credentials flow.
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name not in self.auth_configs:
                raise ValueError(f"Service {service_name} not registered")
                
            config = self.auth_configs[service_name]
            creds = self.credentials[service_name]
            
            # Get the token endpoint
            token_url = config.get('token_url')
            if not token_url:
                raise ValueError(f"Service {service_name} has no token_url configured")
            
            # Build the request data
            data = {
                'grant_type': 'client_credentials',
                'client_id': creds.get('client_id'),
                'client_secret': creds.get('client_secret')
            }
            
            # Add scopes if specified
            scopes = config.get('scopes', [])
            if scopes:
                # Different services format scopes differently
                scope_format = config.get('scope_format', 'space')
                if scope_format == 'space':
                    data['scope'] = ' '.join(scopes)
                elif scope_format == 'comma':
                    data['scope'] = ','.join(scopes)
            
            # Add service-specific parameters
            additional_params = config.get('token_params', {})
            data.update(additional_params)
            
            # Handle different auth methods
            auth_method = config.get('auth_method', 'params')
            headers = {}
            
            if auth_method == 'basic':
                # HTTP Basic Auth
                auth_string = f"{creds.get('client_id')}:{creds.get('client_secret')}"
                encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
                headers['Authorization'] = f"Basic {encoded_auth}"
                # Remove from data when using basic auth
                if 'client_secret' in data:
                    del data['client_secret']
                if 'client_id' in data:
                    del data['client_id']
            
            # Make the request
            try:
                response = requests.post(token_url, data=data, headers=headers)
                response.raise_for_status()
                
                token_data = response.json()
                
                # Store the token
                self._process_and_store_token(service_name, token_data)
                
                return True
            except Exception as e:
                self.logger.error(f"Error getting client credentials token for {service_name}: {str(e)}")
                return False
    
    def refresh_token(self, service_name: str) -> bool:
        """
        Refresh an access token using a refresh token.
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name not in self.auth_configs or service_name not in self.tokens:
                return False
                
            config = self.auth_configs[service_name]
            creds = self.credentials[service_name]
            token_data = self.tokens[service_name]
            
            # Check if refresh token exists
            refresh_token = token_data.get('refresh_token')
            if not refresh_token:
                self.logger.warning(f"No refresh token available for {service_name}")
                return False
                
            # Get the token endpoint
            token_url = config.get('token_url')
            if not token_url:
                raise ValueError(f"Service {service_name} has no token_url configured")
            
            # Build the request data
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': creds.get('client_id'),
                'client_secret': creds.get('client_secret')
            }
            
            # Add service-specific parameters
            additional_params = config.get('refresh_params', {})
            data.update(additional_params)
            
            # Handle different auth methods
            auth_method = config.get('auth_method', 'params')
            headers = {}
            
            if auth_method == 'basic':
                # HTTP Basic Auth
                auth_string = f"{creds.get('client_id')}:{creds.get('client_secret')}"
                encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
                headers['Authorization'] = f"Basic {encoded_auth}"
                # Remove from data when using basic auth
                if 'client_secret' in data:
                    del data['client_secret']
                if 'client_id' in data:
                    del data['client_id']
            
            # Make the request
            try:
                response = requests.post(token_url, data=data, headers=headers)
                response.raise_for_status()
                
                new_token_data = response.json()
                
                # Some services don't return the refresh token again, preserve it
                if 'refresh_token' not in new_token_data and refresh_token:
                    new_token_data['refresh_token'] = refresh_token
                
                # Store the token
                self._process_and_store_token(service_name, new_token_data)
                
                return True
            except Exception as e:
                self.logger.error(f"Error refreshing token for {service_name}: {str(e)}")
                return False
    
    def get_access_token(self, service_name: str, auto_refresh: bool = True) -> Optional[str]:
        """
        Get the current access token for a service.
        
        Args:
            service_name: Name of the service
            auto_refresh: Whether to automatically refresh if expired
            
        Returns:
            Optional[str]: Access token or None if not available
        """
        with self.lock:
            if service_name not in self.tokens:
                return None
                
            token_data = self.tokens[service_name]
            
            # Check if token is expired
            if 'expires_at' in token_data:
                expires_at = token_data['expires_at']
                if isinstance(expires_at, str):
                    expires_at = datetime.datetime.fromisoformat(expires_at)
                
                # Check if token is expired or about to expire (within 5 minutes)
                if expires_at <= (datetime.datetime.now() + datetime.timedelta(minutes=5)):
                    if auto_refresh and 'refresh_token' in token_data:
                        success = self.refresh_token(service_name)
                        if not success:
                            self.logger.warning(f"Failed to refresh token for {service_name}")
                            return None
                    else:
                        self.logger.warning(f"Token for {service_name} is expired")
                        return None
            
            return token_data.get('access_token')
    
    def get_full_token(self, service_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the full token data for a service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            Optional[Dict[str, Any]]: Token data or None if not available
        """
        with self.lock:
            if service_name not in self.tokens:
                return None
                
            return self.tokens[service_name].copy()
    
    def is_authenticated(self, service_name: str) -> bool:
        """
        Check if a service is authenticated.
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if authenticated
        """
        with self.lock:
            # Check if the service has a token
            if service_name not in self.tokens:
                return False
                
            token_data = self.tokens[service_name]
            
            # Check if token is expired
            if 'expires_at' in token_data:
                expires_at = token_data['expires_at']
                if isinstance(expires_at, str):
                    expires_at = datetime.datetime.fromisoformat(expires_at)
                
                if expires_at <= datetime.datetime.now():
                    return False
            
            # Check if token has an access token
            if 'access_token' not in token_data or not token_data['access_token']:
                return False
                
            return True
    
    def logout(self, service_name: str) -> bool:
        """
        Log out a service by removing its token.
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name in self.tokens:
                del self.tokens[service_name]
                
                # Remove token file if it exists
                if self.storage_dir:
                    token_path = os.path.join(self.storage_dir, f"{service_name}_token.json")
                    if os.path.exists(token_path):
                        try:
                            os.remove(token_path)
                        except Exception as e:
                            self.logger.error(f"Error removing token file for {service_name}: {str(e)}")
                
                return True
            return False
    
    def _process_and_store_token(self, service_name: str, token_data: Dict[str, Any]) -> None:
        """Process and store a token."""
        # Calculate expiration time if not provided
        if 'expires_at' not in token_data and 'expires_in' in token_data:
            expires_in = token_data['expires_in']
            expires_at = datetime.datetime.now() + datetime.timedelta(seconds=expires_in)
            token_data['expires_at'] = expires_at
        
        # Store the token
        self.tokens[service_name] = token_data
        
        # Save to file if storage directory is specified
        if self.storage_dir:
            self._save_token(service_name)
    
    def _save_token(self, service_name: str) -> bool:
        """Save a token to a file."""
        if not self.storage_dir or service_name not in self.tokens:
            return False
            
        try:
            token_path = os.path.join(self.storage_dir, f"{service_name}_token.json")
            
            # Make a copy of the token data for serialization
            token_data = self.tokens[service_name].copy()
            
            # Convert datetime objects to ISO format strings for JSON serialization
            for key, value in token_data.items():
                if isinstance(value, datetime.datetime):
                    token_data[key] = value.isoformat()
            
            # Encrypt token if configured
            if self.encrypt_tokens:
                token_data = self._encrypt_token(service_name, token_data)
                
            with open(token_path, 'w') as f:
                json.dump(token_data, f, indent=2)
                
            return True
        except Exception as e:
            self.logger.error(f"Error saving token for {service_name}: {str(e)}")
            return False
    
    def _load_token(self, service_name: str) -> bool:
        """Load a token from a file."""
        if not self.storage_dir:
            return False
            
        token_path = os.path.join(self.storage_dir, f"{service_name}_token.json")
        
        if not os.path.exists(token_path):
            return False
            
        try:
            with open(token_path, 'r') as f:
                token_data = json.load(f)
                
            # Decrypt token if configured
            if self.encrypt_tokens:
                token_data = self._decrypt_token(service_name, token_data)
                
            # Convert ISO format strings to datetime objects
            for key, value in token_data.items():
                if key == 'expires_at' and isinstance(value, str):
                    try:
                        token_data[key] = datetime.datetime.fromisoformat(value)
                    except ValueError:
                        # If parsing fails, leave as string
                        pass
            
            # Store the token
            self.tokens[service_name] = token_data
            
            return True
        except Exception as e:
            self.logger.error(f"Error loading token for {service_name}: {str(e)}")
            return False
    
    def _encrypt_token(self, service_name: str, token_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt token data.
        
        Note: This is a placeholder. For a production system, implement proper encryption.
        
        Args:
            service_name: Name of the service
            token_data: Token data to encrypt
            
        Returns:
            Dict[str, Any]: Encrypted token data
        """
        # This is a simple placeholder. In a real implementation,
        # use a proper encryption library like cryptography.
        # This example just indicates that encryption is needed.
        
        # WARNING: Do not use this in production!
        self.logger.warning("Token encryption not properly implemented. Using placeholder.")
        
        encrypted_data = {
            "encrypted": True,
            "data": base64.b64encode(json.dumps(token_data).encode('utf-8')).decode('utf-8'),
            "service": service_name,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        return encrypted_data
    
    def _decrypt_token(self, service_name: str, encrypted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decrypt token data.
        
        Note: This is a placeholder. For a production system, implement proper decryption.
        
        Args:
            service_name: Name of the service
            encrypted_data: Encrypted token data
            
        Returns:
            Dict[str, Any]: Decrypted token data
        """
        # This is a simple placeholder. In a real implementation,
        # use a proper encryption library like cryptography.
        # This example just indicates that decryption is needed.
        
        # WARNING: Do not use this in production!
        self.logger.warning("Token decryption not properly implemented. Using placeholder.")
        
        if encrypted_data.get("encrypted", False) and "data" in encrypted_data:
            try:
                decrypted_json = base64.b64decode(encrypted_data["data"].encode('utf-8')).decode('utf-8')
                return json.loads(decrypted_json)
            except Exception as e:
                self.logger.error(f"Error decrypting token: {str(e)}")
                return {}
        
        # If not encrypted or decryption fails, return as is
        return encrypted_data
    
    def get_api_header(self, service_name: str, header_name: str = "Authorization") -> Dict[str, str]:
        """
        Get an API authentication header.
        
        Args:
            service_name: Name of the service
            header_name: Name of the header
            
        Returns:
            Dict[str, str]: Header dictionary
        """
        token = self.get_access_token(service_name)
        if not token:
            return {}
            
        config = self.auth_configs.get(service_name, {})
        header_format = config.get('header_format', 'Bearer {token}')
        
        header_value = header_format.format(token=token)
        return {header_name: header_value}
    
    def revoke_token(self, service_name: str) -> bool:
        """
        Actively revoke an access token (if supported by the service).
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if successful
        """
        with self.lock:
            if service_name not in self.auth_configs or service_name not in self.tokens:
                return False
                
            config = self.auth_configs[service_name]
            creds = self.credentials[service_name]
            token_data = self.tokens[service_name]
            
            # Check if service supports token revocation
            revoke_url = config.get('revoke_url')
            if not revoke_url:
                return self.logout(service_name)  # Just remove locally if revocation not supported
            
            # Get the token to revoke
            token = token_data.get('access_token')
            if not token:
                return False
                
            # Build the request data
            data = {
                'token': token,
                'client_id': creds.get('client_id'),
                'client_secret': creds.get('client_secret'),
            }
            
            # Add service-specific parameters
            additional_params = config.get('revoke_params', {})
            data.update(additional_params)
            
            try:
                response = requests.post(revoke_url, data=data)
                success = response.status_code == 200
                
                # Remove the token locally regardless of server response
                self.logout(service_name)
                
                return success
            except Exception as e:
                self.logger.error(f"Error revoking token for {service_name}: {str(e)}")
                
                # Still remove the token locally on error
                self.logout(service_name)
                
                return False
    
    def get_registered_services(self) -> List[str]:
        """
        Get a list of registered services.
        
        Returns:
            List[str]: List of service names
        """
        return list(self.auth_configs.keys())
    
    def is_token_expired(self, service_name: str) -> bool:
        """
        Check if a token is expired.
        
        Args:
            service_name: Name of the service
            
        Returns:
            bool: True if expired or no token exists
        """
        with self.lock:
            if service_name not in self.tokens:
                return True
                
            token_data = self.tokens[service_name]
            
            # Check if token has expiration info
            if 'expires_at' not in token_data:
                # Can't determine expiration, assume not expired
                return False
                
            expires_at = token_data['expires_at']
            if isinstance(expires_at, str):
                expires_at = datetime.datetime.fromisoformat(expires_at)
                
            # Check if token is expired
            return expires_at <= datetime.datetime.now()
