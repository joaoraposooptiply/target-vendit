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
    
    #config schema
    config_jsonschema = th.PropertiesList(
        th.Property("api_url", th.StringType, default="https://api2.vendit.online"),
        th.Property("token", th.StringType, required=False),
        th.Property("api_key", th.StringType, required=False),
        th.Property("vendit_api_key", th.StringType, required=False),
        th.Property("username", th.StringType, required=False),
        th.Property("password", th.StringType, required=False),
        th.Property("oauth_url", th.StringType, required=False),
    ).to_dict()

if __name__ == "__main__":
    TargetVendit.cli()