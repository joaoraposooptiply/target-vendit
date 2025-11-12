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
        Property("api_url", StringType, default="https://api.staging.vendit.online", description="Vendit API URL"),
        Property("batch_size", IntegerType, default=100, description="Batch size for processing records"),
    ).to_dict()
    
    def __init__(self, *, config: Dict[str, Any], parse_env_config: bool = False, validate_config: bool = True):
        """Initialize the target."""
        super().__init__(config=config, parse_env_config=parse_env_config, validate_config=validate_config)
        self._sinks: Dict[str, PrePurchaseOrdersSink] = {}
    
    def get_sink(self, stream_name: str, schema: Dict[str, Any], key_properties: List[str]) -> PrePurchaseOrdersSink:
        """Get a sink for the given stream."""
        if stream_name in ["BuyOrders", "pre_purchase_orders"]:
            return PrePurchaseOrdersSink(self, stream_name, schema, key_properties)
        else:
            raise ValueError(f"Unknown stream: {stream_name}")
    
    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Process a single record."""
        stream_name = record.get("stream")
        if not stream_name:
            self.logger.warning("Record missing stream name, skipping")
            return
            
        # Get or create sink for this stream
        if stream_name not in self._sinks:
            # Create a default schema for BuyOrders/pre_purchase_orders if not provided
            if stream_name in ["BuyOrders", "pre_purchase_orders"]:
                schema = self._get_default_pre_purchase_orders_schema()
                key_properties = ["optiplyId"]
                self._sinks[stream_name] = self.get_sink(stream_name, schema, key_properties)
            else:
                raise ValueError(f"Unknown stream: {stream_name}")
        
        sink = self._sinks[stream_name]
        sink.process_record(record, context)
    
    def _process_schema_message(self, message: Dict[str, Any]) -> None:
        """Process a schema message."""
        stream_name = message.get("stream")
        if stream_name in ["BuyOrders", "pre_purchase_orders"]:
            # Create sink for this stream if it doesn't exist
            if stream_name not in self._sinks:
                schema = message.get("schema", self._get_default_pre_purchase_orders_schema())
                key_properties = message.get("key_properties", ["optiplyId"])
                self._sinks[stream_name] = self.get_sink(stream_name, schema, key_properties)
                self.logger.info(f"Created sink for stream: {stream_name}")
        else:
            self.logger.warning(f"Unknown stream in schema message: {stream_name}")
    
    def _assert_sink_exists(self, stream_name: str) -> None:
        """Override to handle BuyOrders stream."""
        if stream_name in ["BuyOrders", "pre_purchase_orders"]:
            if stream_name not in self._sinks:
                # Create sink automatically for supported streams
                schema = self._get_default_pre_purchase_orders_schema()
                key_properties = ["optiplyId"]
                self._sinks[stream_name] = self.get_sink(stream_name, schema, key_properties)
                self.logger.info(f"Auto-created sink for stream: {stream_name}")
        else:
            # Use the parent method for other streams
            super()._assert_sink_exists(stream_name)
    
    def _process_record_message(self, message: Dict[str, Any]) -> None:
        """Override to handle BuyOrders stream directly."""
        stream_name = message.get("stream")
        if stream_name in ["BuyOrders", "pre_purchase_orders"]:
            # Process directly without stream mapping
            self.process_record(message, {})
        else:
            # Use the parent method for other streams
            super()._process_record_message(message)
    
    def _get_default_pre_purchase_orders_schema(self) -> Dict[str, Any]:
        """Get the default schema for pre-purchase orders."""
        return {
            "type": "object",
            "properties": {
                "productPreorderId": {"type": ["integer", "null"]},
                "isManual": {"type": ["boolean", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "employeeId": {"type": ["integer", "null"]},
                "productId": {"type": ["integer", "null"]},
                "productSizeColorId": {"type": ["integer", "null"]},
                "supplierProductNumber": {"type": ["string", "null"]},
                "productNumber": {"type": ["string", "null"]},
                "productType": {"type": ["string", "null"]},
                "productDescription": {"type": ["string", "null"]},
                "productSubdescription": {"type": ["string", "null"]},
                "productExtraInfo": {"type": ["string", "null"]},
                "targetSupplierId": {"type": ["integer", "null"]},
                "targetOfficeId": {"type": ["integer", "null"]},
                "amount": {"type": ["integer", "null"]},
                "purchasePriceEx": {"type": ["number", "null"]},
                "onetimePurchasePrice": {"type": ["boolean", "null"]},
                "orderReference": {"type": ["string", "null"]},
                "minOrderQuantity": {"type": ["integer", "null"]},
                "expectedDeliveryWeek": {"type": ["integer", "null"]},
                "expectedDeliveryDate": {"type": ["string", "null"], "format": "date-time"},
                "extraPriceInfo": {"type": ["string", "null"]},
                "bebat": {"type": ["number", "null"]},
                "brutoPurchasePriceEx": {"type": ["number", "null"]},
                "useFormula": {"type": ["boolean", "null"]},
                "promotionProductId": {"type": ["integer", "null"]},
                "orderAutomatically": {"type": ["boolean", "null"]},
                "lineId": {"type": ["string", "null"]},
                "creationDatetime": {"type": ["string", "null"], "format": "date-time"},
                "serialNumber": {"type": ["string", "null"]},
                "frameNumber": {"type": ["string", "null"]},
                "imeiNumber": {"type": ["string", "null"]},
                "certificateNumber": {"type": ["string", "null"]},
                "optiplyId": {"type": ["string", "null"]},
            },
            "additionalProperties": True
        }
    
    def process_batch(self, context: Dict[str, Any]) -> None:
        """Process any remaining records in all sinks."""
        for sink in self._sinks.values():
            sink.process_batch(context)
    
    def clean_up(self) -> None:
        """Clean up resources."""
        for sink in self._sinks.values():
            sink.clean_up()
