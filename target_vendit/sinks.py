"""Singer sinks for Vendit target."""

from typing import Dict, Any, List, Optional
import logging
from singer_sdk import Sink
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, NumberType, BooleanType, DateTimeType

from .client import VenditTargetClient

logger = logging.getLogger(__name__)


class PrePurchaseOrdersSink(Sink):
    """Sink for pre-purchase orders."""
    
    name = "pre_purchase_orders"
    
    def __init__(self, target, stream_name: str, schema: Dict[str, Any], key_properties: List[str]):
        """Initialize the sink."""
        super().__init__(target, stream_name, schema, key_properties)
        self.client = VenditTargetClient(target.config)
        self.batch: List[Dict[str, Any]] = []
        self.batch_size = target.config.get("batch_size", 100)
    
    @property
    def max_size(self) -> int:
        """Return the maximum batch size."""
        return self.batch_size
    
    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Process a single record."""
        # Preprocess the record to transform it into the correct format
        preprocessed_record = self.preprocess_record(record, context)
        if preprocessed_record:
            self.batch.append(preprocessed_record)
            
            if len(self.batch) >= self.max_size:
                self._process_batch()
    
    def preprocess_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess a record to match the Vendit API schema.
        
        This method transforms the incoming record into the format expected by the Vendit API.
        Based on your example, it should handle cases like:
        - Buy orders with line items
        - Direct pre-purchase order records
        - Various data structures from different sources
        """
        # Extract the actual record data from Singer format
        record_data = record.get("record", record)
        
        # Handle different record structures
        if "line_items" in record_data and ("buyOrderId" in record_data or "id" in record_data):
            # This looks like a buy order with line items (similar to your example)
            return self._preprocess_buy_order_with_lines(record_data)
        else:
            # This looks like a direct pre-purchase order record
            return self._preprocess_direct_record(record_data)
    
    def _preprocess_buy_order_with_lines(self, record_data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess a buy order with line items into Vendit format."""
        line_items = self._parse_json(record_data.get("line_items", []))
        
        # Handle different date field names - use transaction_date from the Singer data
        transaction_date = self._convert_datetime(record_data.get("transaction_date"))
        
        # Handle different ID field names
        buy_order_id = record_data.get("buyOrderId") or record_data.get("id")
        
        # Create items array from line items
        items = []
        for line in line_items:
            item = {
                "productId": line.get("product_remoteId"),
                "amount": line.get("quantity"),
                "optiplyId": str(buy_order_id)  # Convert to string as required by API
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
        """Convert datetime value to ISO format string."""
        if not datetime_value:
            return None
            
        if isinstance(datetime_value, str):
            # Already a string, return as-is if it looks like ISO format
            if "T" in datetime_value and ("Z" in datetime_value or "+" in datetime_value):
                return datetime_value
            # Try to parse and reformat
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(datetime_value.replace('Z', '+00:00'))
                return dt.isoformat().replace('+00:00', 'Z')
            except:
                return datetime_value
        
        # Handle datetime objects
        try:
            return datetime_value.isoformat().replace('+00:00', 'Z')
        except:
            return str(datetime_value)
    
    def _process_batch(self) -> None:
        """Process the current batch of records."""
        if not self.batch:
            return
            
        try:
            # Flatten all items from all records in the batch
            all_items = []
            for record in self.batch:
                if "items" in record:
                    all_items.extend(record["items"])
            
            if all_items:
                # Send all items as a single batch to the API
                result = self.client.import_pre_purchase_orders(all_items)
                logger.info(f"Successfully processed batch of {len(all_items)} items from {len(self.batch)} records")
            
            self.batch = []
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            raise
    
    def process_batch(self, context: Dict[str, Any]) -> None:
        """Process any remaining records in the batch."""
        self._process_batch()
    
    def clean_up(self) -> None:
        """Clean up resources."""
        if self.batch:
            self._process_batch()
        self.client.close()
