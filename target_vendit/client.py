"""Vendit API client for target operations."""

import requests
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class VenditTargetClient:
    """Client for interacting with Vendit API for target operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the client with configuration."""
        self.api_url = config.get("api_url", "https://api.staging.vendit.online")
        self.api_key = config["vendit_api_key"]
        self.username = config["username"]
        self.password = config["password"]
        self.session = requests.Session()
        self._auth_token = None
        
    def authenticate(self) -> str:
        """Authenticate with Vendit API and return auth token."""
        # Use the correct OAuth endpoint for authentication
        auth_url = "https://oauth.staging.vendit.online/api/GetToken"
        
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
        
        try:
            response = self.session.post(auth_url, params=auth_params, headers=auth_headers)
            response.raise_for_status()
            
            auth_data = response.json()
            self._auth_token = auth_data.get("token")
            
            if not self._auth_token:
                raise ValueError("No auth token received from API")
                
            # Set authorization header for future requests
            self.session.headers.update({
                "Token": self._auth_token,
                "ApiKey": self.api_key,
                "Content-Type": "application/json"
            })
            
            logger.info("Successfully authenticated with Vendit API")
            return self._auth_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during authentication: {e}")
            raise
    
    def import_pre_purchase_orders(self, pre_purchase_orders: list) -> Dict[str, Any]:
        """Import pre-purchase orders to Vendit API."""
        if not self._auth_token:
            self.authenticate()
            
        import_url = f"{self.api_url}/VenditPublicApi/PrePurchaseOrders/Import"
        
        # Transform the data to match the API schema
        payload = {
            "items": pre_purchase_orders
        }
        
        try:
            response = self.session.put(import_url, json=payload)
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Successfully imported {len(pre_purchase_orders)} pre-purchase orders")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to import pre-purchase orders: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during import: {e}")
            raise
    
    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()
