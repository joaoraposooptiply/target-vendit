"""Vendit target sink base class."""

from typing import Dict, Optional, List

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
        self._target = target
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

