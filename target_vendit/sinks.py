"""Vendit target sink classes, which handle writing streams."""

import json
from datetime import datetime, timezone

from singer_sdk.exceptions import FatalAPIError
from target_vendit.client import VenditSink


class PrePurchaseOrders(VenditSink):
    """PrePurchaseOrders sink for Vendit API."""

    endpoint = "PrePurchaseOrders/Import"
    name = "PrePurchaseOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Build the payload for PrePurchaseOrders."""
        # If this has line_items, it's actually a BuyOrders record - skip it
        # (BuyOrders sink will handle it)
        if "line_items" in record and record.get("line_items"):
            return None
        
        items = []

        # Get productId
        product_id = (
            record.get("productId")
            or record.get("product_id")
            or record.get("product_remoteId")
        )
        if not product_id:
            self.logger.info("Skipping record with no productId")
            return None

        # Get amount/quantity
        amount = (
            record.get("amount")
            or record.get("quantity")
            or record.get("qty")
        )
        if not amount:
            self.logger.info("Skipping record with no amount/quantity")
            return None

        # Get creationDatetime
        creation_datetime = (
            record.get("creationDatetime")
            or record.get("creation_datetime")
            or record.get("transaction_date")
            or record.get("created_at")
        )
        
        # Normalize datetime to API format: "2025-08-18T13:35:51.885Z"
        if isinstance(creation_datetime, datetime):
            # Convert datetime to UTC and format with milliseconds
            if creation_datetime.tzinfo:
                creation_datetime = creation_datetime.astimezone(timezone.utc).replace(tzinfo=None)
            creation_datetime = creation_datetime.isoformat(timespec='milliseconds') + "Z"
        elif isinstance(creation_datetime, str):
            # Parse string datetime and normalize to API format
            try:
                # Try parsing with fromisoformat first (handles most ISO formats)
                dt = datetime.fromisoformat(creation_datetime.replace('Z', '+00:00'))
                # Convert to UTC if timezone-aware, then format with milliseconds
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                creation_datetime = dt.isoformat(timespec='milliseconds') + "Z"
            except (ValueError, AttributeError):
                # Fallback: try manual parsing
                try:
                    # Clean up the string - remove trailing Z, handle timezone
                    dt_str = creation_datetime.replace('Z', '').strip()
                    # Remove timezone offset if present
                    if '+' in dt_str:
                        dt_str = dt_str.split('+')[0]
                    elif dt_str.count('-') > 2:  # Has timezone like -05:00
                        parts = dt_str.rsplit('-', 2)
                        if len(parts) == 3 and ':' in parts[2]:
                            dt_str = '-'.join(parts[:2])
                    # Replace space with T if needed for ISO format
                    if ' ' in dt_str and 'T' not in dt_str:
                        dt_str = dt_str.replace(' ', 'T')
                    # Parse ISO format
                    dt = datetime.fromisoformat(dt_str)
                    # Convert to UTC and format with milliseconds
                    creation_datetime = dt.isoformat(timespec='milliseconds') + "Z"
                except Exception as e:
                    self.logger.warning(f"Failed to parse creationDatetime '{creation_datetime}': {e}, using current time")
                    creation_datetime = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"
        elif not creation_datetime:
            creation_datetime = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"

        # Get optiplyId
        optiply_id = (
            record.get("optiplyId")
            or record.get("optiply_id")
            or record.get("id")
        )

        item = {
            "productId": int(product_id),
            "amount": int(amount),
            "creationDatetime": creation_datetime,
        }
        if optiply_id:
            item["optiplyId"] = str(optiply_id)

        items.append(item)

        return {"items": items}

    def upsert_record(self, record: dict, context: dict):
        """Send the record to Vendit API."""
        state_updates = dict()

        if not record:
            return None, False, state_updates

        response = self.request_api(
            "PUT",
            endpoint=self.endpoint,
            request_data=record
        )

        # Extract response ID
        response_id = None
        if response.status_code in [200, 201, 204]:
            try:
                response_data = response.json()
                response_id = response_data.get("id")
            except (json.JSONDecodeError, AttributeError):
                pass

            # Fallback to optiplyId from first item
            if not response_id and record.get("items") and len(record["items"]) > 0:
                response_id = record["items"][0].get("optiplyId")

        return response_id, True, state_updates


class BuyOrders(VenditSink):
    """BuyOrders sink for Vendit API."""

    endpoint = "PrePurchaseOrders/Import"
    name = "BuyOrders"

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record by splitting line_items and sending each as a separate request."""
        # If record already has 'items' (single item), process it directly
        if "items" in record and "line_items" not in record:
            super().process_record(record, context)
            return
        
        # Parse and split line_items into individual requests
        line_items = record.get("line_items")
        
        if line_items is None:
            self.logger.info(f"Skipping order with no line_items field. Record keys: {list(record.keys())}")
            return
        
        # Parse if it's a string
        if isinstance(line_items, str):
            if not line_items.strip():
                return
            try:
                line_items = json.loads(line_items)
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse line_items JSON: {e}")
                return

        # Check if line_items is empty after parsing
        if not line_items or (isinstance(line_items, list) and len(line_items) == 0):
            self.logger.info(f"Skipping order with empty line_items after parsing")
            return

        # Ensure line_items is a list
        if not isinstance(line_items, list):
            line_items = [line_items]

        # Get creationDatetime from buy order
        creation_datetime = (
            record.get("creationDatetime")
            or record.get("creation_datetime")
            or record.get("transaction_date")
            or record.get("created_at")
        )
        
        # Normalize datetime to API format
        if isinstance(creation_datetime, datetime):
            if creation_datetime.tzinfo:
                creation_datetime = creation_datetime.astimezone(timezone.utc).replace(tzinfo=None)
            creation_datetime = creation_datetime.isoformat(timespec='milliseconds') + "Z"
        elif isinstance(creation_datetime, str):
            try:
                dt = datetime.fromisoformat(creation_datetime.replace('Z', '+00:00'))
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                creation_datetime = dt.isoformat(timespec='milliseconds') + "Z"
            except (ValueError, AttributeError):
                try:
                    dt_str = creation_datetime.replace('Z', '').strip()
                    if '+' in dt_str:
                        dt_str = dt_str.split('+')[0]
                    elif dt_str.count('-') > 2:
                        parts = dt_str.rsplit('-', 2)
                        if len(parts) == 3 and ':' in parts[2]:
                            dt_str = '-'.join(parts[:2])
                    if ' ' in dt_str and 'T' not in dt_str:
                        dt_str = dt_str.replace(' ', 'T')
                    dt = datetime.fromisoformat(dt_str)
                    creation_datetime = dt.isoformat(timespec='milliseconds') + "Z"
                except Exception as e:
                    self.logger.warning(f"Failed to parse creationDatetime '{creation_datetime}': {e}, using current time")
                    creation_datetime = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"
        elif not creation_datetime:
            creation_datetime = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"

        # Get optiplyId from buy order
        optiply_id = (
            record.get("optiplyId")
            or record.get("optiply_id")
            or record.get("id")
        )

        # Get target supplier ID from buy order
        target_supplier_id = (
            record.get("targetSupplierId")
            or record.get("target_supplier_id")
            or record.get("supplier_remoteId")
        )

        # Process each line item as a separate request
        for line_item in line_items:
            # Get productId
            product_id = (
                line_item.get("productId")
                or line_item.get("product_id")
                or line_item.get("product_remoteId")
            )
            if not product_id:
                self.logger.warning(f"Line item missing productId, skipping: {line_item}")
                continue

            # Get amount/quantity
            amount = (
                line_item.get("amount")
                or line_item.get("quantity")
                or line_item.get("qty")
            )
            if not amount:
                self.logger.warning(f"Line item missing amount/quantity, skipping: {line_item}")
                continue

            # Create payload with single item
            item = {
                "productId": int(product_id),
                "amount": int(amount),
                "creationDatetime": creation_datetime,
            }
            if optiply_id:
                item["optiplyId"] = str(optiply_id)
            
            # Add target supplier ID
            if target_supplier_id:
                item["targetSupplierId"] = int(target_supplier_id)

            # Add office ID if configured (default warehouse for buy order export)
            office_id = self.config.get("default_export_buyOrder_warehouseId")
            if office_id is not None:
                item["officeId"] = int(office_id)

            # Send each line item as a separate request
            single_item_payload = {"items": [item]}
            super().process_record(single_item_payload, context)

    def upsert_record(self, record: dict, context: dict):
        """Send the record to Vendit API."""
        state_updates = dict()

        if not record:
            return None, False, state_updates

        try:
            response = self.request_api(
                "PUT",
                endpoint=self.endpoint,
                request_data=record
            )

            # Extract response ID
            response_id = None
            if response.status_code in [200, 201, 204]:
                try:
                    response_data = response.json()
                    response_id = response_data.get("id")
                except (json.JSONDecodeError, AttributeError):
                    pass

                # Fallback to optiplyId from first item
                if not response_id and record.get("items") and len(record["items"]) > 0:
                    response_id = record["items"][0].get("optiplyId")

            return response_id, True, state_updates
            
        except FatalAPIError as e:
            state_updates["error"] = str(e)
            return None, False, state_updates
        except Exception as e:
            self.logger.error(f"[BuyOrders] Unexpected error: {type(e).__name__}: {str(e)}")
            state_updates["error"] = str(e)
            return None, False, state_updates

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess record - returns as-is since process_record handles splitting."""
        # If record already has 'items', return as-is (already processed)
        if "items" in record and "line_items" not in record:
            return record
        # Otherwise return as-is - process_record will handle the splitting
        return record
