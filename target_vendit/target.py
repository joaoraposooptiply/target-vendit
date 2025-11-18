"""Target Vendit - Singer target for Vendit API."""

from typing import Dict, Any, List, Optional, Type
import logging
import json
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
    
    def __init__(self, *args, **kwargs):
        """Initialize the target."""
        logger.info("=" * 80)
        logger.info("Initializing TargetVendit")
        config = kwargs.get('config', {})
        if isinstance(config, dict):
            logger.info(f"Config keys: {list(config.keys())}")
        else:
            logger.info(f"Config type: {type(config)}, value: {config}")
        logger.info(f"Args count: {len(args)}, Kwargs keys: {list(kwargs.keys())}")
        super().__init__(*args, **kwargs)
        logger.info("TargetVendit initialized successfully")
        if hasattr(self, 'config') and isinstance(self.config, dict):
            logger.info(f"Final config keys: {list(self.config.keys())}")
        logger.info("=" * 80)
    
    def _process_line(self, line: str) -> None:
        """Process a single line of input."""
        logger.info(f"_process_line called with line length: {len(line)}")
        logger.info(f"Line content (first 500 chars): {line[:500]}")
        try:
            super()._process_line(line)
        except Exception as e:
            logger.error(f"Error processing line: {e}", exc_info=True)
            logger.error(f"Problematic line: {line[:500]}")
            raise
    
    def _process_schema_message(self, message: Dict[str, Any]) -> None:
        """Process a SCHEMA message."""
        stream_name = message.get("stream", "unknown")
        logger.info("=" * 80)
        logger.info(f"PROCESSING SCHEMA MESSAGE for stream: {stream_name}")
        logger.info(f"Schema message keys: {list(message.keys())}")
        logger.info(f"Schema properties: {list(message.get('schema', {}).get('properties', {}).keys())}")
        try:
            super()._process_schema_message(message)
            logger.info(f"Successfully processed SCHEMA message for stream: {stream_name}")
        except Exception as e:
            logger.error(f"Error processing SCHEMA message for stream {stream_name}: {e}", exc_info=True)
            raise
        finally:
            logger.info("=" * 80)
    
    def _process_record_message(self, message: Dict[str, Any]) -> None:
        """Process a RECORD message."""
        stream_name = message.get("stream", "unknown")
        record = message.get("record", {})
        logger.info("=" * 80)
        logger.info(f"PROCESSING RECORD MESSAGE for stream: {stream_name}")
        logger.info(f"Record message keys: {list(message.keys())}")
        logger.info(f"Record keys: {list(record.keys()) if isinstance(record, dict) else 'not a dict'}")
        logger.info(f"Record id: {record.get('id', 'N/A') if isinstance(record, dict) else 'N/A'}")
        logger.info(f"Record has line_items: {'line_items' in record if isinstance(record, dict) else False}")
        
        # Check if we have a sink for this stream (if _sinks exists)
        if hasattr(self, '_sinks') and self._sinks:
            sink = self._sinks.get(stream_name)
            if sink:
                logger.info(f"Found sink for stream {stream_name}: {type(sink).__name__}")
            else:
                logger.warning(f"No sink found for stream {stream_name}. Available sinks: {list(self._sinks.keys())}")
        else:
            logger.info(f"_sinks not yet initialized (will be created by parent class)")
        
        try:
            super()._process_record_message(message)
            logger.info(f"Successfully processed RECORD message for stream: {stream_name}")
        except Exception as e:
            logger.error(f"Error processing RECORD message for stream {stream_name}: {e}", exc_info=True)
            logger.error(f"Record that caused error: {json.dumps(record, default=str)[:500]}")
            raise
        finally:
            logger.info("=" * 80)
    
    def _process_state_message(self, message: Dict[str, Any]) -> None:
        """Process a STATE message."""
        logger.info("=" * 80)
        logger.info("PROCESSING STATE MESSAGE")
        logger.info(f"State message keys: {list(message.keys())}")
        logger.info(f"State value: {message.get('value', {})}")
        try:
            super()._process_state_message(message)
            logger.info("Successfully processed STATE message")
        except Exception as e:
            logger.error(f"Error processing STATE message: {e}", exc_info=True)
            raise
        finally:
            logger.info("=" * 80)
    
    def _assert_sink_exists(self, stream_name: str) -> Sink:
        """Assert that a sink exists for the given stream name."""
        logger.info(f"_assert_sink_exists called for stream: {stream_name}")
        if hasattr(self, '_sinks') and self._sinks:
            logger.info(f"Current sinks: {list(self._sinks.keys())}")
        else:
            logger.info("_sinks not yet initialized")
        try:
            sink = super()._assert_sink_exists(stream_name)
            logger.info(f"Successfully retrieved sink for stream {stream_name}: {type(sink).__name__}")
            if hasattr(self, '_sinks') and self._sinks:
                logger.info(f"Available sinks after retrieval: {list(self._sinks.keys())}")
            return sink
        except Exception as e:
            logger.error(f"Failed to get sink for stream {stream_name}: {e}", exc_info=True)
            if hasattr(self, '_sinks') and self._sinks:
                logger.error(f"Available sinks: {list(self._sinks.keys())}")
            raise
    
    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Return the sink class for a given stream name."""
        logger.info(f"get_sink_class called for stream: {stream_name}")
        # Map stream names to sink classes
        if stream_name in ["BuyOrders", "pre_purchase_orders"]:
            logger.info(f"Mapped stream {stream_name} to PrePurchaseOrdersSink")
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
        logger.error(f"No sink class found for stream: {stream_name}")
        raise ValueError(f"No sink class found for stream: {stream_name}")
