"""Authentication handling for the Vendit API."""

import json
import logging
from typing import Dict, Optional

import requests
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class VenditAuthenticator:
    """Handles authentication for Vendit API including OAuth token retrieval."""

    def __init__(self, config: Dict) -> None:
        """Initialize the authenticator.
        
        Args:
            config: Configuration dictionary containing credentials
        """
        self.config = config
        self._token = None
        self._api_key = None

    @property
    def api_key(self) -> str:
        """Get the API key from config."""
        if self._api_key is None:
            self._api_key = (
                self.config.get("api_key") or self.config.get("vendit_api_key")
            )
            if not self._api_key:
                raise ValueError(
                    "Missing required 'api_key' or 'vendit_api_key' in config."
                )
        return self._api_key

    @property
    def token(self) -> str:
        """Get the OAuth token, fetching it if necessary."""
        if self._token is None:
            # First check if token is provided directly
            self._token = self.config.get("token")
            if not self._token:
                # Try to get token via OAuth
                self._token = self._get_oauth_token()
        return self._token

    def _get_oauth_token(self) -> str:
        """Get OAuth token from Vendit API.
        
        Returns:
            OAuth token string
            
        Raises:
            ValueError: If OAuth credentials are missing or token request fails
        """
        username = self.config.get("username")
        password = self.config.get("password")
        oauth_url = (
            self.config.get("oauth_url")
            or "https://oauth.vendit.online/Api/GetToken"
        )

        if not username or not password:
            raise ValueError(
                "Missing OAuth credentials. Either provide 'token' directly, "
                "or provide 'username' and 'password' to obtain token via OAuth."
            )

        # Build OAuth URL with credentials as query parameters
        url = (
            f"{oauth_url}?"
            f"apiKey={self.api_key}"
            f"&username={username}"
            f"&password={password}"
        )

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ApiKey": self.api_key,
        }

        logger.info(f"Getting OAuth token from {oauth_url}")
        try:
            response = requests.post(url, headers=headers)
            response.raise_for_status()
            token_data = response.json()

            if not isinstance(token_data, dict) or "token" not in token_data:
                raise ValueError(
                    "Invalid token response format from OAuth endpoint"
                )

            token = token_data["token"]
            logger.info("Successfully obtained OAuth token")
            return token

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get OAuth token: {e}")
            raise ValueError(f"OAuth token request failed: {e}")

    def get_headers(self) -> Dict[str, str]:
        """Get headers for API requests.
        
        Returns:
            Dictionary with Token and ApiKey headers
        """
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Token": self.token,
            "ApiKey": self.api_key,
        }

