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


if __name__ == "__main__":
    TargetVendit.cli()

