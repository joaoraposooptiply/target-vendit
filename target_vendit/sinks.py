"""Vendit target sink class, which handles writing streams."""

import json
from typing import Dict, List, Optional
from datetime import datetime

import requests
from singer_sdk.plugin_base import PluginBase

from target_hotglue.client import HotglueSink


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

        # Support both direct config and tap config field names
        # If token is not provided, try using vendit_api_key as token
        self.token = self.config.get("token") or self.config.get("vendit_api_key")
        self.api_key = self.config.get("api_key") or self.config.get("vendit_api_key")
        
        # Validate we have required credentials
        if not self.token:
            raise ValueError(
                "Missing required 'token' or 'vendit_api_key' in config. "
                "One of these must be provided."
            )
        if not self.api_key:
            raise ValueError(
                "Missing required 'api_key' or 'vendit_api_key' in config. "
                "One of these must be provided."
            )
        # Get api_url from config, default to production
        api_url = self.config.get("api_url", "https://api2.vendit.online")
        # Ensure it doesn't have trailing slash and append the API path
        api_url = api_url.rstrip("/")
        self._api_base_url = f"{api_url}/VenditPublicApi"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> requests.Response:
        """Make a request to the Vendit API."""
        url = f"{self._api_base_url}/{endpoint}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Token": self.token,
            "ApiKey": self.api_key,
        }

        self.logger.info(f"{method} {url}")
        if data:
            self.logger.info(f"Payload: {json.dumps(data, indent=2)}")

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
        )

        self.logger.info(f"Response Status: {response.status_code}")
        if response.status_code not in [200, 201, 204]:
            self.logger.error(f"Error response: {response.text}")
            response.raise_for_status()

        return response

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

    def upsert_record(self, record: dict, context: dict):
        """Upsert a record to Vendit API."""
        status = True
        state_updates = dict()

        try:
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
                        state_updates["success"] = False
                        state_updates["error"] = f"Invalid line_items JSON: {e}"
                        return None, False, state_updates
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
                state_updates["success"] = False
                state_updates["error"] = "Missing productId or amount"
                return None, False, state_updates

            # Prepare payload with items array
            payload = {"items": items}

            # Send PUT request to Vendit API
            response = self._make_request("PUT", self.endpoint, data=payload)

            # Extract response ID if available
            response_id = None
            if response.status_code in [200, 201, 204]:
                try:
                    response_data = response.json()
                    response_id = response_data.get("id") or items[0].get("optiplyId")
                except (json.JSONDecodeError, AttributeError):
                    response_id = items[0].get("optiplyId") if items else None

            state_updates["success"] = True
            return response_id, status, state_updates

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending record to Vendit: {e}")
            state_updates["success"] = False
            state_updates["error"] = str(e)
            status = False
            return None, status, state_updates
        except Exception as e:
            self.logger.error(f"Unexpected error processing record: {e}")
            state_updates["success"] = False
            state_updates["error"] = str(e)
            status = False
            return None, status, state_updates


class BuyOrders(PrePurchaseOrders):
    """BuyOrders sink for Vendit API.
    
    This is an alias for PrePurchaseOrders that handles the BuyOrders stream.
    The logic is the same - it parses line_items and sends to PrePurchaseOrders endpoint.
    """

    endpoint = "PrePurchaseOrders/Import"
    name = "BuyOrders"

