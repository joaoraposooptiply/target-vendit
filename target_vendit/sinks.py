"""Vendit target sink classes, which handle writing streams."""

import json
from datetime import datetime

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
        if isinstance(creation_datetime, datetime):
            creation_datetime = creation_datetime.isoformat() + "Z"
        elif not creation_datetime:
            creation_datetime = datetime.utcnow().isoformat() + "Z"

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

    def upsert_record(self, record: dict, context: dict):
        """Send the record to Vendit API."""
        state_updates = dict()

        if not record:
            return None, False, state_updates

        try:
            self.logger.info(f"[BuyOrders] Sending request to {self.endpoint} with payload: {json.dumps(record, indent=2, default=str)}")
            
            response = self.request_api(
                "PUT",
                endpoint=self.endpoint,
                request_data=record
            )

            self.logger.info(f"[BuyOrders] API response status: {response.status_code}")
            self.logger.info(f"[BuyOrders] API response body: {response.text[:500]}")  # First 500 chars

            # Extract response ID
            response_id = None
            if response.status_code in [200, 201, 204]:
                try:
                    response_data = response.json()
                    response_id = response_data.get("id")
                    self.logger.info(f"[BuyOrders] Extracted response_id: {response_id}")
                except (json.JSONDecodeError, AttributeError):
                    pass

                # Fallback to optiplyId from first item
                if not response_id and record.get("items") and len(record["items"]) > 0:
                    response_id = record["items"][0].get("optiplyId")
                    self.logger.info(f"[BuyOrders] Using optiplyId as response_id: {response_id}")

            return response_id, True, state_updates
            
        except Exception as e:
            self.logger.error(f"[BuyOrders] Error sending record to API: {type(e).__name__}: {str(e)}")
            import traceback
            self.logger.error(f"[BuyOrders] Traceback: {traceback.format_exc()}")
            state_updates["error"] = str(e)
            return None, False, state_updates

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Build the payload for BuyOrders from line_items."""
        # If record already has 'items', it's already been preprocessed - return as-is
        if "items" in record and "line_items" not in record:
            self.logger.info("[BuyOrders] Record already preprocessed, returning as-is")
            return record
        
        # Log record safely (handle datetime objects)
        try:
            record_str = json.dumps(record, indent=2, default=str)
            self.logger.info(f"[BuyOrders] Received record: {record_str}")
        except Exception as e:
            self.logger.info(f"[BuyOrders] Received record (keys: {list(record.keys())})")
        
        items = []

        # Parse line_items
        line_items = record.get("line_items")
        self.logger.info(f"[BuyOrders] line_items value: {line_items}, type: {type(line_items)}")
        
        # Check if line_items exists
        if line_items is None:
            self.logger.info(f"Skipping order with no line_items field. Record keys: {list(record.keys())}")
            return None
        
        # Parse if it's a string
        if isinstance(line_items, str):
            if not line_items.strip():  # Empty string
                self.logger.info("Skipping order with empty line_items string")
                return None
            try:
                line_items = json.loads(line_items)
                self.logger.info(f"[BuyOrders] Parsed line_items: {json.dumps(line_items, indent=2)}")
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse line_items JSON: {e}")
                return None

        # Check if line_items is empty after parsing
        if not line_items or (isinstance(line_items, list) and len(line_items) == 0):
            self.logger.info(f"Skipping order with empty line_items after parsing")
            return None

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
        if isinstance(creation_datetime, datetime):
            creation_datetime = creation_datetime.isoformat() + "Z"
        elif not creation_datetime:
            creation_datetime = datetime.utcnow().isoformat() + "Z"

        # Get optiplyId from buy order
        optiply_id = (
            record.get("optiplyId")
            or record.get("optiply_id")
            or record.get("id")
        )

        # Process each line item
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

            item = {
                "productId": int(product_id),
                "amount": int(amount),
                "creationDatetime": creation_datetime,
            }
            if optiply_id:
                item["optiplyId"] = str(optiply_id)

            items.append(item)

        if not items:
            self.logger.info("Skipping order with no valid line items")
            return None

        return {"items": items}
