"""Target Vendit - Singer target for Vendit API."""

from typing import Dict, Any, List, Optional, Type
import logging
from singer_sdk import Target
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, NumberType, BooleanType, DateTimeType
from singer_sdk.sinks import Sink

from .sinks import PrePurchaseOrdersSink

logger = logging.getLogger(__name__)


class TargetVendit(Target):
    """Target for Vendit API."""
    
    name = "target-vendit"
    
    config_jsonschema = PropertiesList(
        Property("vendit_api_key", StringType, required=True, description="Vendit API key"),
        Property("username", StringType, required=True, description="Vendit username"),
        Property("password", StringType, required=True, description="Vendit password"),
        Property("api_url", StringType, default="https://api2.vendit.online", description="Vendit API URL"),
        Property("oauth_url", StringType, default="https://oauth.vendit.online/api/GetToken", description="Vendit OAuth URL for authentication"),
        Property("batch_size", IntegerType, default=100, description="Batch size for processing records"),
    ).to_dict()
    
    SINK_TYPES = [PrePurchaseOrdersSink]
    
    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Return the sink class for a given stream name."""
        logger.info(f"get_sink_class called for stream: {stream_name}")
        # Map stream names to sink classes
        if stream_name in ["BuyOrders", "pre_purchase_orders"]:
            return PrePurchaseOrdersSink
        # If using SINK_TYPES, the parent should handle it, but we'll be explicit
        for sink_class in self.SINK_TYPES:
            if sink_class.name == stream_name:
                logger.info(f"Found sink class {sink_class.__name__} for stream {stream_name}")
                return sink_class
        # Fallback to first sink type if stream name matches
        if self.SINK_TYPES:
            logger.info(f"Using default sink class {self.SINK_TYPES[0].__name__} for stream {stream_name}")
            return self.SINK_TYPES[0]
        raise ValueError(f"No sink class found for stream: {stream_name}")
