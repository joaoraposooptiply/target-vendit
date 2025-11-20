"""Vendit target sink class, which handles writing streams."""

import json
from typing import Dict, List, Optional
from datetime import datetime

import requests
from singer_sdk.plugin_base import PluginBase

from target_hotglue.client import HotglueSink
from target_vendit.auth import VenditAuthenticator


class VenditSink(HotglueSink):
    """Vendit target sink base class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        try:
            # Initialize authenticator (lazy - won't fetch token until needed)
            self._authenticator = VenditAuthenticator(self.config)
        except Exception as e:
            self.logger.warning(f"Failed to initialize authenticator: {e}. Will retry when making requests.")
            self._authenticator = None
        
        self.logger.info(f"Initialized {self.__class__.__name__} sink for stream '{stream_name}'")

    @property
    def base_url(self) -> str:
        """Get the base URL for the Vendit API."""
        api_url = self.config.get("api_url", "https://api2.vendit.online")
        api_url = api_url.rstrip("/")
        return f"{api_url}/VenditPublicApi"

    @property
    def http_headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        # Initialize authenticator if not already done
        if self._authenticator is None:
            self._authenticator = VenditAuthenticator(self.config)
        
        # Get headers from authenticator
        return self._authenticator.get_headers()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess record before sending."""
        return record


class PrePurchaseOrders(VenditSink):
    """PrePurchaseOrders sink for Vendit API.
    
    Handles both PrePurchaseOrders and BuyOrders streams.
    For BuyOrders, parses line_items JSON and creates items for each line.
    """

    endpoint = "PrePurchaseOrders/Import"
    name = "PrePurchaseOrders"

    def _format_item_from_line_item(self, line_item: dict, buy_order: dict) -> dict:
        """Format a line item from BuyOrders into a Vendit API item format."""
        item = {}

        # Map productId from line_item
        product_id = (
            line_item.get("productId")
            or line_item.get("product_id")
            or line_item.get("product_remoteId")
        )
        if product_id:
            item["productId"] = int(product_id)

        # Map amount/quantity from line_item
        amount = (
            line_item.get("amount")
            or line_item.get("quantity")
            or line_item.get("qty")
        )
        if amount:
            item["amount"] = int(amount)

        # Map creationDatetime from buy order
        creation_datetime = (
            buy_order.get("creationDatetime")
            or buy_order.get("creation_datetime")
            or buy_order.get("transaction_date")
            or buy_order.get("created_at")
        )
        if creation_datetime:
            if isinstance(creation_datetime, str):
                item["creationDatetime"] = creation_datetime
            else:
                item["creationDatetime"] = creation_datetime.isoformat()
        else:
            item["creationDatetime"] = datetime.utcnow().isoformat() + "Z"

        # Map optiplyId from buy order id
        optiply_id = (
            buy_order.get("optiplyId")
            or buy_order.get("optiply_id")
            or buy_order.get("id")
        )
        if optiply_id:
            item["optiplyId"] = str(optiply_id)

        return item

    def _format_item(self, record: dict) -> dict:
        """Format a record into a Vendit API item format."""
        item = {}

        # Map productId - could be from productId, product_id, or product_remoteId
        product_id = (
            record.get("productId")
            or record.get("product_id")
            or record.get("product_remoteId")
        )
        if product_id:
            item["productId"] = int(product_id)

        # Map amount - could be from amount, quantity, or qty
        amount = (
            record.get("amount")
            or record.get("quantity")
            or record.get("qty")
        )
        if amount:
            item["amount"] = int(amount)

        # Map creationDatetime - use current time if not provided
        creation_datetime = record.get("creationDatetime") or record.get(
            "creation_datetime"
        )
        if creation_datetime:
            # Ensure it's in ISO format
            if isinstance(creation_datetime, str):
                item["creationDatetime"] = creation_datetime
            else:
                item["creationDatetime"] = creation_datetime.isoformat()
        else:
            item["creationDatetime"] = datetime.utcnow().isoformat() + "Z"

        # Map optiplyId - could be from optiplyId, optiply_id, or id
        optiply_id = (
            record.get("optiplyId")
            or record.get("optiply_id")
            or record.get("id")
        )
        if optiply_id:
            item["optiplyId"] = str(optiply_id)

        return item

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess record before sending.
        
        Builds the payload that will be sent to the API.
        Returns the payload dict.
        """
        self.logger.info(f"[PREPROCESS] Received record for {self.name}: {json.dumps(record, indent=2)}")
        
        items = []
        
        # Check if this is a BuyOrders record with line_items
        if "line_items" in record and record.get("line_items"):
            # Parse line_items JSON string
            line_items_str = record.get("line_items")
            if isinstance(line_items_str, str):
                try:
                    line_items = json.loads(line_items_str)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse line_items JSON: {e}")
                    # Return skip marker
                    return {"_skip": True, "error": f"Invalid line_items JSON: {e}", "id": record.get("id")}
            else:
                line_items = line_items_str
            
            # Create an item for each line_item
            if isinstance(line_items, list):
                for line_item in line_items:
                    item = self._format_item_from_line_item(line_item, record)
                    if "productId" in item and "amount" in item:
                        items.append(item)
                    else:
                        self.logger.warning(
                            f"Line item missing required fields, skipping: {line_item}"
                        )
            else:
                # Single line item
                item = self._format_item_from_line_item(line_items, record)
                if "productId" in item and "amount" in item:
                    items.append(item)
        else:
            # Regular PrePurchaseOrders format
            item = self._format_item(record)
            if "productId" in item and "amount" in item:
                items.append(item)

        # Validate we have at least one valid item
        if not items:
            self.logger.warning(
                f"Record missing required fields (productId, amount), skipping: {record}"
            )
            # Return skip marker
            return {"_skip": True, "error": "Missing productId or amount", "id": record.get("id")}

        # Return payload with items array
        payload = {"items": items}
        self.logger.info(f"[PREPROCESS] Built payload for {self.name}: {json.dumps(payload, indent=2)}")
        return payload

    def upsert_record(self, record: dict, context: dict):
        """Upsert a record to Vendit API.
        
        Receives the payload from preprocess_record and sends it to the API.
        Returns: (id, status, state_updates)
        """
        self.logger.info(f"[UPSERT] Processing {self.name} record")
        self.logger.info(f"[UPSERT] Payload received: {json.dumps(record, indent=2)}")
        self.logger.info(f"[UPSERT] Endpoint: {self.endpoint}")
        self.logger.info(f"[UPSERT] Base URL: {self.base_url}")
        self.logger.info(f"[UPSERT] Headers: {json.dumps(self.http_headers, indent=2)}")
        
        state_updates = dict()
        
        try:
            # Send PUT request to Vendit API using Hotglue's request_api method
            self.logger.info(f"[UPSERT] Sending PUT request to {self.base_url}/{self.endpoint}")
            response = self.request_api(
                "PUT",
                endpoint=self.endpoint,
                request_data=record
            )
            self.logger.info(f"[UPSERT] Response status: {response.status_code}")
            self.logger.info(f"[UPSERT] Response body: {response.text}")
            
            # Extract response ID if available
            response_id = None
            if response.status_code in [200, 201, 204]:
                try:
                    response_data = response.json()
                    self.logger.info(f"[UPSERT] Response JSON: {json.dumps(response_data, indent=2)}")
                    # Try to get ID from response, or from the first item's optiplyId
                    response_id = response_data.get("id")
                    if not response_id and isinstance(record.get("items"), list) and len(record.get("items", [])) > 0:
                        response_id = record["items"][0].get("optiplyId")
                except (json.JSONDecodeError, AttributeError):
                    self.logger.warning(f"[UPSERT] Could not parse response as JSON")
                    if isinstance(record.get("items"), list) and len(record.get("items", [])) > 0:
                        response_id = record["items"][0].get("optiplyId")
            
            self.logger.info(f"[UPSERT] Returning: id={response_id}, status=True, state={state_updates}")
            return response_id, True, state_updates

        except requests.exceptions.RequestException as e:
            self.logger.error(f"[UPSERT] RequestException sending record to Vendit: {e}")
            self.logger.error(f"[UPSERT] Exception type: {type(e).__name__}")
            state_updates["error"] = str(e)
            return None, False, state_updates
        except Exception as e:
            self.logger.error(f"[UPSERT] Unexpected error processing record: {e}")
            self.logger.error(f"[UPSERT] Exception type: {type(e).__name__}")
            import traceback
            self.logger.error(f"[UPSERT] Traceback: {traceback.format_exc()}")
            state_updates["error"] = str(e)
            return None, False, state_updates


class BuyOrders(PrePurchaseOrders):
    """BuyOrders sink for Vendit API.
    
    This is an alias for PrePurchaseOrders that handles the BuyOrders stream.
    The logic is the same - it parses line_items and sends to PrePurchaseOrders endpoint.
    """

    endpoint = "PrePurchaseOrders/Import"
    name = "BuyOrders"

