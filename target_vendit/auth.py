"""Vendit API authentication."""

import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class VenditAuthenticator:
    """Authenticator for Vendit API OAuth token-based authentication."""

    def __init__(
        self,
        target,
        state: Dict[str, Any],
        oauth_url: Optional[str] = None,
    ) -> None:
        """Initialize authenticator.

        Args:
            target: The target instance.
            state: State dictionary for storing auth info.
            oauth_url: OAuth endpoint URL.
        """
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target.config
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._oauth_url = oauth_url or self._config.get("oauth_url", "https://oauth.vendit.online/api/GetToken")
        self._target = target
        self.state = state
        self._auth_token: Optional[str] = None

    @property
    def auth_headers(self) -> Dict[str, str]:
        """Return authentication headers."""
        if not self._auth_token:
            self.update_access_token()
        
        return {
            "Token": self._auth_token,
            "ApiKey": self._config.get("vendit_api_key"),
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    @property
    def oauth_request_params(self) -> Dict[str, str]:
        """Return OAuth request parameters."""
        return {
            "username": self._config.get("username"),
            "password": self._config.get("password"),
            "apiKey": self._config.get("vendit_api_key")
        }

    def is_token_valid(self) -> bool:
        """Check if the current auth token is valid."""
        # Vendit tokens don't expire, so if we have one, it's valid
        return self._auth_token is not None

    def update_access_token(self) -> None:
        """Update the access token by authenticating with Vendit API."""
        self.logger.info("=" * 80)
        self.logger.info("AUTHENTICATE called")
        self.logger.info(f"OAuth URL: {self._oauth_url}")
        self.logger.info(f"API Key present: {bool(self._config.get('vendit_api_key'))}")
        self.logger.info(f"Username present: {bool(self._config.get('username'))}")
        self.logger.info(f"Password present: {bool(self._config.get('password'))}")
        
        auth_headers = {
            "Content-Type": "application/json",
            "ApiKey": self._config.get("vendit_api_key")
        }
        
        self.logger.info(f"Sending POST request to: {self._oauth_url}")
        self.logger.info(f"Auth params keys: {list(self.oauth_request_params.keys())}")
        
        try:
            response = requests.post(
                self._oauth_url,
                params=self.oauth_request_params,
                headers=auth_headers
            )
            
            self.logger.info(f"Authentication response status: {response.status_code}")
            response.raise_for_status()
            
            auth_data = response.json()
            self.logger.info(f"Auth response keys: {list(auth_data.keys()) if isinstance(auth_data, dict) else 'not a dict'}")
            
            self._auth_token = auth_data.get("token")
            
            if not self._auth_token:
                self.logger.error("No auth token in response")
                self.logger.error(f"Full response: {auth_data}")
                raise ValueError("No auth token received from API")
            
            self.logger.info(f"Auth token received (length: {len(self._auth_token) if self._auth_token else 0})")
            self.logger.info("Successfully authenticated with Vendit API")
            self.logger.info("=" * 80)
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Authentication failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response headers: {dict(e.response.headers)}")
                self.logger.error(f"Response content: {e.response.text}")
            self.logger.info("=" * 80)
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during authentication: {e}", exc_info=True)
            self.logger.info("=" * 80)
            raise

