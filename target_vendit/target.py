"""Vendit target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_vendit.sinks import PrePurchaseOrders, BuyOrders


class TargetVendit(TargetHotglue):
    SINK_TYPES = [
        PrePurchaseOrders,
        BuyOrders,
    ]
    name = "target-vendit"
    
    def __init__(self, *args, **kwargs):
        """Initialize the target with logging."""
        super().__init__(*args, **kwargs)
        self._records_processed = 0
        self.logger.info(f"Target '{self.name}' initialized and ready to receive input")
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            default="https://api2.vendit.online",
            description="The base URL for the Vendit API service",
        ),
        th.Property("token", th.StringType, required=False),
        th.Property("api_key", th.StringType, required=False),
        # Support alternative config field names from tap config
        th.Property("vendit_api_key", th.StringType, required=False),
        th.Property("username", th.StringType, required=False),
        th.Property("password", th.StringType, required=False),
        th.Property(
            "oauth_url",
            th.StringType,
            required=False,
            description="The url for the Vendit OAuth token endpoint (optional, defaults to production if not provided)",
        ),
    ).to_dict()

    def validate_config(self) -> None:
        """Validate the configuration."""
        super().validate_config()

        # Validate API URL format
        api_url = self.config.get("api_url", "https://api2.vendit.online")
        if not api_url.startswith(("http://", "https://")):
            raise ValueError("api_url must start with http:// or https://")

    def _process_singer_message(self, message: dict) -> None:
        """Process a singer message and track record count."""
        # Track RECORD messages
        if message.get("type") == "RECORD":
            self._records_processed += 1
        super()._process_singer_message(message)

    def _process_lines(self, file_input) -> None:
        """Process input lines and validate that data was received."""
        import sys
        
        # Log input source for debugging
        if file_input is sys.stdin:
            if sys.stdin.isatty():
                self.logger.warning("Reading from interactive terminal (stdin is a TTY). Expected input from pipe or file.")
            else:
                self.logger.info("Reading from stdin (piped input)")
        else:
            self.logger.info(f"Reading from file-like object: {type(file_input).__name__}")
        
        try:
            super()._process_lines(file_input)
        finally:
            # After processing, check if any records were received
            if self._records_processed == 0:
                error_msg = (
                    "No singer data received. The input is empty or contains no RECORD messages. "
                    "This usually means:\n"
                    "1. The singer file was not piped to the target (e.g., 'cat data.singer | target-vendit --config config.json')\n"
                    "2. The singer file exists but is empty\n"
                    "3. The singer file contains only SCHEMA/STATE messages but no RECORD messages\n\n"
                    f"Records processed: {self._records_processed}"
                )
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            else:
                self.logger.info(f"Successfully processed {self._records_processed} record(s)")


if __name__ == "__main__":
    TargetVendit.cli()

