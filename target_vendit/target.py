"""Target Vendit - Singer target for Vendit API."""

from typing import Dict, Any, List, Optional
import logging
from singer_sdk import Target
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, NumberType, BooleanType, DateTimeType

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
