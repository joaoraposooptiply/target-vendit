"""Vendit target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_vendit.sinks import PrePurchaseOrders


class TargetVendit(TargetHotglue):
    SINK_TYPES = [
        PrePurchaseOrders,
    ]
    name = "target-vendit"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            default="https://api2.vendit.online",
            description="The base URL for the Vendit API service",
        ),
        th.Property("token", th.StringType, required=True, secret=True),
        th.Property("api_key", th.StringType, required=True, secret=True),
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

