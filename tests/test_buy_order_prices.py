import importlib
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class _FakeVenditSink:
    def __init__(self):
        self.config = {}
        self.logger = _Logger()
        self.payloads = []

    def process_record(self, record, context):
        self.payloads.append(record)


singer_sdk = types.ModuleType("singer_sdk")
singer_sdk_exceptions = types.ModuleType("singer_sdk.exceptions")
singer_sdk_exceptions.FatalAPIError = type("FatalAPIError", (Exception,), {})
singer_sdk.exceptions = singer_sdk_exceptions
sys.modules["singer_sdk"] = singer_sdk
sys.modules["singer_sdk.exceptions"] = singer_sdk_exceptions

fake_client = types.ModuleType("target_vendit.client")
fake_client.VenditSink = _FakeVenditSink
sys.modules["target_vendit.client"] = fake_client

sinks = importlib.import_module("target_vendit.sinks")


def test_buy_orders_maps_line_item_unit_price_to_vendit_purchase_price_ex():
    sink = sinks.BuyOrders()
    sink.process_record(
        {
            "id": "bo-123",
            "targetSupplierId": 456,
            "creationDatetime": "2026-05-13T10:15:00Z",
            "line_items": [
                {"productId": 27043329, "quantity": 2, "unit_price": "145.00"},
                {"productId": 27043156, "quantity": 4, "unit_price": 3.5},
            ],
        },
        {},
    )

    assert [payload["items"][0]["purchasePriceEx"] for payload in sink.payloads] == [145.0, 3.5]
    assert sink.payloads[0]["items"][0]["targetSupplierId"] == 456


def test_pre_purchase_orders_maps_unit_price_to_vendit_purchase_price_ex():
    sink = sinks.PrePurchaseOrders()
    payload = sink.preprocess_record(
        {
            "id": "bol-123",
            "productId": 27043329,
            "quantity": 1,
            "unit_price": "0",
            "creationDatetime": "2026-05-13T10:15:00Z",
        },
        {},
    )

    assert payload["items"][0]["purchasePriceEx"] == 0.0
