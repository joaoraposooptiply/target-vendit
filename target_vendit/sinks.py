"""Vendit target sink classes, which handle writing streams."""

import json
from datetime import datetime

from target_vendit.client import VenditSink


class PrePurchaseOrders(VenditSink):
    """PrePurchaseOrders sink for Vendit API."""

    endpoint = "PrePurchaseOrders/Import"
    name = "buyOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Build the payload for PrePurchaseOrders."""
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

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Build the payload for BuyOrders from line_items."""
        items = []

        # Parse line_items
        line_items = record.get("line_items")
        if isinstance(line_items, str):
            try:
                line_items = json.loads(line_items)
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse line_items JSON: {e}")
                return None

        if not line_items:
            self.logger.info("Skipping order with no line_items")
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
