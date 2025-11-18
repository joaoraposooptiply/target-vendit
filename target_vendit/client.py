"""Vendit API client for target operations."""

import requests
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class VenditTargetClient:
    """Client for interacting with Vendit API for target operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the client with configuration."""
        logger.info("=" * 80)
        logger.info("Initializing VenditTargetClient")
        logger.info(f"Config keys: {list(config.keys())}")
        self.api_url = config.get("api_url", "https://api2.vendit.online")
        self.oauth_url = config.get("oauth_url", "https://oauth.vendit.online/api/GetToken")
        self.api_key = config.get("vendit_api_key")
        self.username = config.get("username")
        self.password = config.get("password")
        logger.info(f"API URL: {self.api_url}")
        logger.info(f"OAuth URL: {self.oauth_url}")
        logger.info(f"API Key present: {bool(self.api_key)}")
        logger.info(f"Username present: {bool(self.username)}")
        logger.info(f"Password present: {bool(self.password)}")
        self.session = requests.Session()
        self._auth_token = None
        logger.info("VenditTargetClient initialized")
        logger.info("=" * 80)
        
    def authenticate(self) -> str:
        """Authenticate with Vendit API and return auth token."""
        logger.info("=" * 80)
        logger.info("AUTHENTICATE called")
        logger.info(f"OAuth URL: {self.oauth_url}")
        logger.info(f"API Key present: {bool(self.api_key)}")
        logger.info(f"Username present: {bool(self.username)}")
        logger.info(f"Password present: {bool(self.password)}")
        
        # Use the configurable OAuth endpoint for authentication
        auth_url = self.oauth_url
        
        # Use query parameters instead of JSON body
        auth_params = {
            "username": self.username,
            "password": self.password,
            "apiKey": self.api_key
        }
        
        # Set headers for authentication request
        auth_headers = {
            "Content-Type": "application/json",
            "ApiKey": self.api_key
        }
        
        logger.info(f"Sending POST request to: {auth_url}")
        logger.info(f"Auth params keys: {list(auth_params.keys())}")
        logger.info(f"Auth headers keys: {list(auth_headers.keys())}")
        
        try:
            response = self.session.post(auth_url, params=auth_params, headers=auth_headers)
            logger.info(f"Authentication response status: {response.status_code}")
            response.raise_for_status()
            
            auth_data = response.json()
            logger.info(f"Auth response keys: {list(auth_data.keys()) if isinstance(auth_data, dict) else 'not a dict'}")
            self._auth_token = auth_data.get("token")
            
            if not self._auth_token:
                logger.error("No auth token in response")
                logger.error(f"Full response: {auth_data}")
                raise ValueError("No auth token received from API")
            
            logger.info(f"Auth token received (length: {len(self._auth_token) if self._auth_token else 0})")
                
            # Set authorization header for future requests
            self.session.headers.update({
                "Token": self._auth_token,
                "ApiKey": self.api_key,
                "Content-Type": "application/json"
            })
            
            logger.info("Successfully authenticated with Vendit API")
            logger.info("=" * 80)
            return self._auth_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response headers: {dict(e.response.headers)}")
                logger.error(f"Response content: {e.response.text}")
            logger.info("=" * 80)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during authentication: {e}", exc_info=True)
            logger.info("=" * 80)
            raise
    
    def import_pre_purchase_orders(self, pre_purchase_orders: list) -> Dict[str, Any]:
        """Import pre-purchase orders to Vendit API."""
        logger.info(f"import_pre_purchase_orders called with {len(pre_purchase_orders)} items")
        logger.info(f"First item sample: {pre_purchase_orders[0] if pre_purchase_orders else 'empty'}")
        
        if not self._auth_token:
            logger.info("No auth token, authenticating...")
            self.authenticate()
        else:
            logger.info("Using existing auth token")
            
        import_url = f"{self.api_url}/VenditPublicApi/PrePurchaseOrders/Import"
        logger.info(f"Import URL: {import_url}")
        
        # Transform the data to match the API schema
        payload = {
            "items": pre_purchase_orders
        }
        
        logger.info(f"Sending PUT request with payload containing {len(pre_purchase_orders)} items")
        try:
            response = self.session.put(import_url, json=payload)
            logger.info(f"Response status code: {response.status_code}")
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Successfully imported {len(pre_purchase_orders)} pre-purchase orders. Response: {result}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to import pre-purchase orders: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during import: {e}", exc_info=True)
            raise
    
    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()
