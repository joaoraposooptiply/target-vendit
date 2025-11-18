"""Singer sinks for Vendit target."""

from typing import Dict, Any, List, Optional
import logging
import json
import re
from datetime import datetime

from .client import VenditSink

logger = logging.getLogger(__name__)


class PrePurchaseOrdersSink(VenditSink):
    """Sink for pre-purchase orders."""
    
    name = "BuyOrders"
    endpoint = "/VenditPublicApi/PrePurchaseOrders/Import"
    
    def __init__(self, target, stream_name: str, schema: Dict[str, Any], key_properties: List[str]):
        """Initialize the sink."""
        logger.info("=" * 80)
        logger.info(f"Initializing PrePurchaseOrdersSink for stream: {stream_name}")
        logger.info(f"Schema keys: {list(schema.keys()) if isinstance(schema, dict) else 'not a dict'}")
        logger.info(f"Schema properties: {list(schema.get('properties', {}).keys()) if isinstance(schema, dict) else 'N/A'}")
        logger.info(f"Key properties: {key_properties}")
        logger.info(f"Target config keys: {list(target.config.keys())}")
        
        # Initialize base class (VenditSink which extends Sink)
        super().__init__(target, stream_name, schema, key_properties)
        
        logger.info(f"Sink fully initialized")
        logger.info("=" * 80)
    
    def preprocess_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Preprocess a record to match the Vendit API schema.
        
        This method transforms the incoming record into the format expected by the Vendit API.
        Handles BuyOrders with line_items by extracting each line item separately.
        Returns None if the record should be skipped.
        """
        logger.info("=" * 80)
        logger.info(f"PREPROCESS_RECORD called for stream: {self.stream_name}")
        logger.info(f"Record type: {type(record)}")
        logger.info(f"Record keys: {list(record.keys()) if isinstance(record, dict) else 'not a dict'}")
        
        if isinstance(record, dict):
            logger.info(f"Record id: {record.get('id', 'N/A')}")
            logger.info(f"Record has line_items: {'line_items' in record}")
            if "line_items" in record:
                logger.info(f"line_items value type: {type(record.get('line_items'))}")
                logger.info(f"line_items value (first 200 chars): {str(record.get('line_items'))[:200]}")
        
        # Check if this is a BuyOrder with line_items (primary use case)
        if "line_items" in record and record.get("line_items"):
            logger.info(f"Found line_items in record, processing buy order with lines")
            # Check if we have an id (BuyOrder id)
            if "id" in record or "buyOrderId" in record:
                # This is a buy order with line items - extract each line
                result = self._preprocess_buy_order_with_lines(record)
                if not result or not result.get("items"):
                    logger.warning("No items extracted from buy order, skipping")
                    logger.info("=" * 80)
                    return None
                logger.info(f"Preprocessed buy order with lines, got {len(result.get('items', []))} items")
                if result.get('items'):
                    logger.info(f"First item sample: {result['items'][0]}")
                logger.info("=" * 80)
                return result
            else:
                logger.warning("Record has line_items but no id or buyOrderId field")
        
        # Fallback: direct pre-purchase order record
        logger.info("Processing as direct pre-purchase order record")
        result = self._preprocess_direct_record(record)
        if not result or not result.get("items"):
            logger.warning("No items in direct record, skipping")
            logger.info("=" * 80)
            return None
        logger.info(f"Preprocessed direct record, got {len(result.get('items', []))} items")
        if result.get('items'):
            logger.info(f"First item sample: {result['items'][0]}")
        logger.info("=" * 80)
        return result
    
    def _preprocess_buy_order_with_lines(self, record_data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess a buy order with line items into Vendit format.
        
        Extracts each line item and creates a separate item for the API.
        Uses the BuyOrder id as optiplyId for all line items.
        """
        logger.info(f"_preprocess_buy_order_with_lines called")
        logger.info(f"Record data keys: {list(record_data.keys())}")
        
        line_items_raw = record_data.get("line_items", [])
        logger.info(f"Raw line_items type: {type(line_items_raw)}")
        logger.info(f"Raw line_items value (first 500 chars): {str(line_items_raw)[:500]}")
        
        line_items = self._parse_json(line_items_raw)
        logger.info(f"Parsed line_items count: {len(line_items)}")
        logger.info(f"Parsed line_items: {line_items}")
        
        # Handle different date field names - use transaction_date from the Singer data
        transaction_date_raw = record_data.get("transaction_date")
        logger.info(f"Raw transaction_date: {transaction_date_raw}")
        transaction_date = self._convert_datetime(transaction_date_raw)
        logger.info(f"Converted transaction_date: {transaction_date}")
        
        # Get the BuyOrder id - this goes into optiplyId for all line items
        buy_order_id = record_data.get("buyOrderId") or record_data.get("id")
        logger.info(f"BuyOrder id: {buy_order_id}")
        
        if not buy_order_id:
            logger.warning("BuyOrder record missing id, skipping")
            return {"items": []}
        
        # Create items array from line items - one item per line
        items = []
        for idx, line in enumerate(line_items):
            logger.info(f"Processing line item {idx + 1}/{len(line_items)}: {line}")
            product_id = line.get("product_remoteId")
            quantity = line.get("quantity")
            
            logger.info(f"Line {idx + 1} - product_remoteId: {product_id}, quantity: {quantity}")
            
            # Skip line items missing required fields
            if not product_id or quantity is None:
                logger.warning(f"Skipping line item {idx + 1} missing productId or quantity: {line}")
                continue
            
            item = {
                "productId": product_id,
                "amount": quantity,
                "optiplyId": str(buy_order_id)  # Use BuyOrder id as optiplyId
            }
            
            # Add creationDatetime if available
            if transaction_date:
                item["creationDatetime"] = transaction_date
                
            logger.info(f"Created item {idx + 1}: {item}")
            items.append(item)
        
        logger.info(f"Created {len(items)} items from {len(line_items)} line items")
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
                return json.loads(data)
            except json.JSONDecodeError:
                return []
        elif isinstance(data, list):
            return data
        else:
            return []
    
    def _convert_datetime(self, datetime_value) -> Optional[str]:
        """Convert datetime value to ISO format string with milliseconds precision."""
        if not datetime_value:
            return None
        
        try:
            if isinstance(datetime_value, str):
                # Handle common case: YYYY-MM-DDTHH:mm:ss.XXXXXXZ
                if datetime_value.endswith('Z') and '.' in datetime_value:
                    match = re.match(r'(.+?)\.(\d{1,9})(Z|[\+\-]\d{2}:\d{2})$', datetime_value)
                    if match:
                        base, fractional, tz = match.groups()
                        fractional = fractional[:3]
                        return f"{base}.{fractional}Z" if tz == 'Z' else f"{base}.{fractional}{tz}"
                
                dt_str = datetime_value.replace('Z', '+00:00')
                dt = datetime.fromisoformat(dt_str)
            else:
                dt = datetime_value
            
            iso_str = dt.isoformat()
            
            # Truncate microseconds to milliseconds if present
            if '.' in iso_str:
                parts = iso_str.split('.', 1)
                fractional = parts[1]
                
                if '+' in fractional:
                    fractional_sec, tz_part = fractional.split('+', 1)
                    iso_str = f"{parts[0]}.{fractional_sec[:3]}+{tz_part}"
                elif '-' in fractional and len(fractional) > 3:
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
            return str(datetime_value) if datetime_value else None
    
    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Process a single record.
        
        The SDK calls preprocess_record first and passes the result here.
        We then call upsert_record to send it to the API.
        """
        logger.info("=" * 80)
        logger.info(f"PROCESS_RECORD called for stream: {self.stream_name}")
        logger.info(f"Record keys: {list(record.keys()) if isinstance(record, dict) else 'not a dict'}")
        
        # The record is already preprocessed by the SDK calling preprocess_record
        # If preprocess_record returned None, record will be None and we skip
        if record:
            self.upsert_record(record, context)
        else:
            logger.warning("Record is None (skipped by preprocess_record)")
        logger.info("=" * 80)
    
    def upsert_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> tuple:
        """Upsert a record to the Vendit API.
        
        Args:
            record: The preprocessed record containing items
            context: Context dictionary
            
        Returns:
            Tuple of (record_id, success, state_updates)
        """
        logger.info("=" * 80)
        logger.info(f"UPSERT_RECORD called for stream: {self.stream_name}")
        logger.info(f"Record keys: {list(record.keys()) if isinstance(record, dict) else 'not a dict'}")
        
        if not record or not record.get("items"):
            logger.warning("Record is empty or has no items, skipping")
            logger.info("=" * 80)
            return None, False, {}
        
        items = record.get("items", [])
        logger.info(f"Sending {len(items)} items to Vendit API")
        logger.info(f"Sample items (first 3): {items[:3]}")
        
        # Prepare the payload
        payload = {
            "items": items
        }
        
        # Make the API call
        try:
            params = {}
            response = self.request_api(
                "PUT",
                endpoint=self.endpoint,
                request_data=payload,
                params=params
            )
            
            result = response.json()
            logger.info(f"Successfully imported {len(items)} pre-purchase orders. Response: {result}")
            
            # Extract the first optiplyId as the record ID (all items share the same optiplyId)
            record_id = items[0].get("optiplyId") if items else None
            
            logger.info("=" * 80)
            return record_id, True, {}
            
        except Exception as e:
            logger.error(f"Failed to upsert record: {e}", exc_info=True)
            logger.info("=" * 80)
            return None, False, {}
