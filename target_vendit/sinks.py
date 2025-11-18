"""Singer sinks for Vendit target."""

from typing import Dict, Any, List, Optional
import logging
from singer_sdk import Sink
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, NumberType, BooleanType, DateTimeType

from .client import VenditTargetClient

logger = logging.getLogger(__name__)


class PrePurchaseOrdersSink(Sink):
    """Sink for pre-purchase orders."""
    
    name = "BuyOrders"
    
    def __init__(self, target, stream_name: str, schema: Dict[str, Any], key_properties: List[str]):
        """Initialize the sink."""
        logger.info(f"Initializing PrePurchaseOrdersSink for stream: {stream_name}")
        super().__init__(target, stream_name, schema, key_properties)
        logger.info(f"Sink initialized, creating client...")
        self.client = VenditTargetClient(target.config)
        self.batch: List[Dict[str, Any]] = []
        self.batch_size = target.config.get("batch_size", 100)
        logger.info(f"Sink fully initialized with batch_size: {self.batch_size}")
    
    @property
    def max_size(self) -> int:
        """Return the maximum batch size."""
        return self.batch_size
    
    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Process a single record."""
        logger.info(f"process_record called for stream {self.stream_name}, record keys: {list(record.keys()) if isinstance(record, dict) else 'not a dict'}")
        
        # Preprocess the record to transform it into the correct format
        preprocessed_record = self.preprocess_record(record, context)
        logger.info(f"Preprocessed record: {preprocessed_record}")
        
        if preprocessed_record and preprocessed_record.get("items"):
            self.batch.append(preprocessed_record)
            logger.info(f"Added record to batch. Batch size: {len(self.batch)}, Items in record: {len(preprocessed_record.get('items', []))}")
            
            if len(self.batch) >= self.max_size:
                logger.info(f"Batch size reached {self.max_size}, processing batch")
                self._process_batch()
        else:
            logger.warning(f"Preprocessed record is empty or has no items, skipping. Preprocessed: {preprocessed_record}")
    
    def preprocess_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess a record to match the Vendit API schema.
        
        This method transforms the incoming record into the format expected by the Vendit API.
        Handles BuyOrders with line_items by extracting each line item separately.
        """
        logger.info(f"preprocess_record called with record type: {type(record)}, keys: {list(record.keys()) if isinstance(record, dict) else 'not a dict'}")
        
        # The Singer SDK passes the record data directly (not wrapped in a message)
        # Check if this is a BuyOrder with line_items (primary use case)
        if "line_items" in record and record.get("line_items"):
            logger.info(f"Found line_items in record, processing buy order with lines")
            # Check if we have an id (BuyOrder id)
            if "id" in record or "buyOrderId" in record:
                # This is a buy order with line items - extract each line
                result = self._preprocess_buy_order_with_lines(record)
                logger.info(f"Preprocessed buy order with lines, got {len(result.get('items', []))} items")
                return result
        
        # Fallback: direct pre-purchase order record
        logger.info("Processing as direct pre-purchase order record")
        result = self._preprocess_direct_record(record)
        logger.info(f"Preprocessed direct record, got {len(result.get('items', []))} items")
        return result
    
    def _preprocess_buy_order_with_lines(self, record_data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess a buy order with line items into Vendit format.
        
        Extracts each line item and creates a separate item for the API.
        Uses the BuyOrder id as optiplyId for all line items.
        """
        line_items = self._parse_json(record_data.get("line_items", []))
        
        # Handle different date field names - use transaction_date from the Singer data
        transaction_date = self._convert_datetime(record_data.get("transaction_date"))
        
        # Get the BuyOrder id - this goes into optiplyId for all line items
        buy_order_id = record_data.get("buyOrderId") or record_data.get("id")
        
        if not buy_order_id:
            logger.warning("BuyOrder record missing id, skipping")
            return {"items": []}
        
        # Create items array from line items - one item per line
        items = []
        for line in line_items:
            product_id = line.get("product_remoteId")
            quantity = line.get("quantity")
            
            # Skip line items missing required fields
            if not product_id or quantity is None:
                logger.warning(f"Skipping line item missing productId or quantity: {line}")
                continue
            
            item = {
                "productId": product_id,
                "amount": quantity,
                "optiplyId": str(buy_order_id)  # Use BuyOrder id as optiplyId
            }
            
            # Add creationDatetime if available
            if transaction_date:
                item["creationDatetime"] = transaction_date
                
            items.append(item)
        
        return {
            "items": items
        }
    
    def _preprocess_direct_record(self, record_data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess a direct pre-purchase order record."""
        # Handle direct records that already have the right structure
        if "items" in record_data:
            # Already in the correct format
            return record_data
        
        # Transform single record into items array format
        item = {}
        
        # Required fields
        if "productId" in record_data:
            item["productId"] = record_data["productId"]
        if "amount" in record_data:
            item["amount"] = record_data["amount"]
        if "optiplyId" in record_data:
            item["optiplyId"] = str(record_data["optiplyId"])
            
        # Optional fields
        optional_fields = [
            "productPreorderId", "isManual", "officeId", "employeeId", 
            "productSizeColorId", "supplierProductNumber", "productNumber", 
            "productType", "productDescription", "productSubdescription", 
            "productExtraInfo", "targetSupplierId", "targetOfficeId", 
            "purchasePriceEx", "onetimePurchasePrice", "orderReference", 
            "minOrderQuantity", "expectedDeliveryWeek", "expectedDeliveryDate", 
            "extraPriceInfo", "bebat", "brutoPurchasePriceEx", "useFormula", 
            "promotionProductId", "orderAutomatically", "lineId", 
            "serialNumber", "frameNumber", "imeiNumber", "certificateNumber"
        ]
        
        for field in optional_fields:
            if field in record_data:
                item[field] = record_data[field]
        
        # Handle creationDatetime
        if "creationDatetime" in record_data:
            item["creationDatetime"] = self._convert_datetime(record_data["creationDatetime"])
        
        return {
            "items": [item]
        }
    
    def _parse_json(self, data) -> List[Dict[str, Any]]:
        """Parse JSON data, handling both string and object formats."""
        if isinstance(data, str):
            try:
                import json
                return json.loads(data)
            except json.JSONDecodeError:
                return []
        elif isinstance(data, list):
            return data
        else:
            return []
    
    def _convert_datetime(self, datetime_value) -> Optional[str]:
        """Convert datetime value to ISO format string with milliseconds precision.
        
        Converts datetime to format: YYYY-MM-DDTHH:mm:ss.sssZ
        Normalizes microseconds (6 digits) to milliseconds (3 digits) if needed.
        """
        if not datetime_value:
            return None
        
        from datetime import datetime
        import re
        
        try:
            if isinstance(datetime_value, str):
                # Handle common case: YYYY-MM-DDTHH:mm:ss.XXXXXXZ
                # Truncate microseconds to milliseconds
                if datetime_value.endswith('Z') and '.' in datetime_value:
                    # Pattern: YYYY-MM-DDTHH:mm:ss.XXXXXXZ
                    match = re.match(r'(.+?)\.(\d{1,9})(Z|[\+\-]\d{2}:\d{2})$', datetime_value)
                    if match:
                        base, fractional, tz = match.groups()
                        # Truncate to 3 digits (milliseconds)
                        fractional = fractional[:3]
                        return f"{base}.{fractional}Z" if tz == 'Z' else f"{base}.{fractional}{tz}"
                
                # Parse and reformat if needed
                dt_str = datetime_value.replace('Z', '+00:00')
                dt = datetime.fromisoformat(dt_str)
            else:
                # Assume it's a datetime object
                dt = datetime_value
            
            # Format to ISO
            iso_str = dt.isoformat()
            
            # Truncate microseconds to milliseconds if present
            if '.' in iso_str:
                parts = iso_str.split('.', 1)
                fractional = parts[1]
                
                # Handle timezone in fractional part
                if '+' in fractional:
                    fractional_sec, tz_part = fractional.split('+', 1)
                    iso_str = f"{parts[0]}.{fractional_sec[:3]}+{tz_part}"
                elif '-' in fractional and len(fractional) > 3:
                    # Check if there's a timezone (after position 3 to avoid date separators)
                    tz_match = re.search(r'([\+\-]\d{2}:\d{2})$', fractional)
                    if tz_match:
                        fractional_sec = fractional[:tz_match.start()]
                        tz_part = tz_match.group()
                        iso_str = f"{parts[0]}.{fractional_sec[:3]}{tz_part}"
                    else:
                        iso_str = f"{parts[0]}.{fractional[:3]}"
                else:
                    iso_str = f"{parts[0]}.{fractional[:3]}"
            
            # Normalize UTC timezone to Z format
            if '+00:00' in iso_str:
                iso_str = iso_str.replace('+00:00', 'Z')
            elif iso_str.endswith('+00:00'):
                iso_str = iso_str[:-6] + 'Z'
            
            return iso_str
            
        except Exception as e:
            logger.warning(f"Failed to convert datetime {datetime_value}: {e}, using as-is")
            # Fallback: return as string if conversion fails
            return str(datetime_value) if datetime_value else None
    
    def _process_batch(self) -> None:
        """Process the current batch of records."""
        if not self.batch:
            logger.warning("_process_batch called but batch is empty")
            return
            
        try:
            logger.info(f"Processing batch of {len(self.batch)} records")
            # Flatten all items from all records in the batch
            all_items = []
            for record in self.batch:
                if "items" in record:
                    all_items.extend(record["items"])
            
            logger.info(f"Total items to send: {len(all_items)}")
            if all_items:
                # Send all items as a single batch to the API
                logger.info(f"Calling client.import_pre_purchase_orders with {len(all_items)} items")
                result = self.client.import_pre_purchase_orders(all_items)
                logger.info(f"Successfully processed batch of {len(all_items)} items from {len(self.batch)} records. Result: {result}")
            else:
                logger.warning("No items to send in batch")
            
            self.batch = []
        except Exception as e:
            logger.error(f"Failed to process batch: {e}", exc_info=True)
            raise
    
    def process_batch(self, context: Dict[str, Any]) -> None:
        """Process any remaining records in the batch."""
        logger.info(f"process_batch called, current batch size: {len(self.batch)}")
        self._process_batch()
    
    def clean_up(self) -> None:
        """Clean up resources."""
        logger.info(f"clean_up called, current batch size: {len(self.batch)}")
        if self.batch:
            logger.info("Processing remaining batch in clean_up")
            self._process_batch()
        self.client.close()
