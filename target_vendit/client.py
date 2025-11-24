"""Vendit target sink base class."""

from typing import Dict, Optional, List

import requests
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueSink

from target_vendit.auth import VenditAuthenticator


class VenditSink(HotglueSink):
    """Vendit target sink base class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        super().__init__(target, stream_name, schema, key_properties)

        try:
            # Initialize authenticator (lazy - won't fetch token until needed)
            self._authenticator = VenditAuthenticator(self.config)
        except Exception as e:
            self.logger.warning(f"Failed to initialize authenticator: {e}. Will retry when making requests.")
            self._authenticator = None
        
        self.logger.info(f"Initialized {self.__class__.__name__} sink for stream '{stream_name}'")

    @property
    def base_url(self) -> str:
        """Get the base URL for the Vendit API."""
        api_url = self.config.get("api_url", "https://api2.vendit.online")
        api_url = api_url.rstrip("/")
        return f"{api_url}/VenditPublicApi"

    @property
    def http_headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        # Initialize authenticator if not already done
        if self._authenticator is None:
            self._authenticator = VenditAuthenticator(self.config)
        
        # Get headers from authenticator
        return self._authenticator.get_headers()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess record before sending."""
        return record

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record, handling None from preprocess_record."""
        # Preprocess the record
        preprocessed = self.preprocess_record(record, context)
        
        # If preprocess_record returns None, skip this record
        if preprocessed is None:
            return
        
        # Call parent's process_record with the preprocessed record
        super().process_record(preprocessed, context)

    def request_api(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        request_data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> requests.Response:
        """Make an API request with correct URL construction for Vendit API."""
        # Ensure endpoint doesn't have leading slash to avoid double slashes
        endpoint = endpoint.lstrip("/")
        # Construct full URL: base_url already includes /VenditPublicApi
        full_url = f"{self.base_url}/{endpoint}"
        
        # Get headers (merge with any provided headers)
        request_headers = self.http_headers.copy()
        if headers:
            request_headers.update(headers)
        
        # Make the request directly to ensure correct URL
        try:
            if method.upper() == "GET":
                response = requests.get(full_url, params=params, headers=request_headers, json=request_data)
            elif method.upper() == "POST":
                response = requests.post(full_url, params=params, headers=request_headers, json=request_data)
            elif method.upper() == "PUT":
                response = requests.put(full_url, params=params, headers=request_headers, json=request_data)
            elif method.upper() == "PATCH":
                response = requests.patch(full_url, params=params, headers=request_headers, json=request_data)
            elif method.upper() == "DELETE":
                response = requests.delete(full_url, params=params, headers=request_headers, json=request_data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Validate response (this will raise FatalAPIError if needed)
            self.validate_response(response)
            
            return response
            
        except requests.exceptions.RequestException as e:
            raise FatalAPIError(f"Request to {full_url} failed: {e}") from e

    def validate_response(self, response: requests.Response) -> None:
        """Validate API response and raise FatalAPIError if needed."""
        # Call parent's validate_response which will raise FatalAPIError if needed
        super().validate_response(response)

