"""Vendit API client for target operations."""

import requests
from typing import Dict, Any, Optional, List
import logging
from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueSink

from .auth import VenditAuthenticator

logger = logging.getLogger(__name__)


class VenditSink(HotglueSink):
    """Base sink class for Vendit API operations."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)
        self.auth_state = {}

    @property
    def authenticator(self) -> VenditAuthenticator:
        """Return the authenticator instance."""
        oauth_url = self.config.get("oauth_url", "https://oauth.vendit.online/api/GetToken")
        return VenditAuthenticator(
            self._target, self.auth_state, oauth_url
        )

    @property
    def http_headers(self) -> Dict[str, str]:
        """Return the HTTP headers needed for API requests."""
        return self.authenticator.auth_headers

    @property
    def api_url(self) -> str:
        """Return the API base URL."""
        return self.config.get("api_url", "https://api2.vendit.online")

    def request_api(
        self,
        method: str,
        endpoint: str,
        request_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Make an API request.

        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            endpoint: API endpoint path
            request_data: Request body data
            params: Query parameters
            headers: Additional headers

        Returns:
            Response object
        """
        url = f"{self.api_url}{endpoint}"
        
        if headers is None:
            headers = self.http_headers
        else:
            headers.update(self.http_headers)
        
        self.logger.info(f"Making {method} request to: {url}")
        if params:
            self.logger.info(f"Params: {params}")
        if request_data:
            self.logger.info(f"Request data keys: {list(request_data.keys())}")
        
        try:
            response = requests.request(
                method=method,
                url=url,
                json=request_data,
                params=params,
                headers=headers
            )
            
            self.logger.info(f"Response status code: {response.status_code}")
            response.raise_for_status()
            
            return response
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during API request: {e}", exc_info=True)
            raise
