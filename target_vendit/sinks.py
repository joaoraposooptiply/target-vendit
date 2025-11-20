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

        self.token = self.config.get("token")
        self.api_key = self.config.get("api_key")
        # Get api_url from config, default to production
        api_url = self.config.get("api_url", "https://api2.vendit.online")
        # Ensure it doesn't have trailing slash and append the API path
        api_url = api_url.rstrip("/")
        self.base_url = f"{api_url}/VenditPublicApi"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> requests.Response:
        """Make a request to the Vendit API."""
        url = f"{self.base_url}/{endpoint}"

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
    """PrePurchaseOrders sink for Vendit API."""

    endpoint = "PrePurchaseOrders/Import"
    name = "PrePurchaseOrders"

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
            # Format the record as an item
            item = self._format_item(record)

            # Validate required fields
            if "productId" not in item:
                self.logger.warning(
                    f"Record missing productId, skipping: {record}"
                )
                state_updates["success"] = False
                state_updates["error"] = "Missing productId"
                return None, False, state_updates

            if "amount" not in item:
                self.logger.warning(
                    f"Record missing amount, skipping: {record}"
                )
                state_updates["success"] = False
                state_updates["error"] = "Missing amount"
                return None, False, state_updates

            # Prepare payload with items array
            payload = {"items": [item]}

            # Send PUT request to Vendit API
            response = self._make_request("PUT", self.endpoint, data=payload)

            # Extract response ID if available
            response_id = None
            if response.status_code in [200, 201, 204]:
                try:
                    response_data = response.json()
                    response_id = response_data.get("id") or item.get("optiplyId")
                except (json.JSONDecodeError, AttributeError):
                    response_id = item.get("optiplyId")

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

