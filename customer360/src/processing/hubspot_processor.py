from __future__ import annotations

from typing import Any, Dict, Iterable, List


class HubSpotDataProcessor:
    """Convert raw HubSpot payloads into cleaned Customer 360 records."""

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def process_contacts(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed: List[Dict[str, Any]] = []
        for record in records:
            properties = record.get("properties", {})
            processed.append(
                {
                    "customer_id": record.get("id"),
                    "first_name": properties.get("firstname"),
                    "last_name": properties.get("lastname"),
                    "email": properties.get("email"),
                    "phone": properties.get("phone"),
                    "customer_stage": properties.get("lifecyclestage"),
                    "created_at": properties.get("createdate"),
                    "updated_at": properties.get("lastmodifieddate"),
                    "owner_id": properties.get("hubspot_owner_id"),
                }
            )
        return processed

    def process_orders(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed: List[Dict[str, Any]] = []
        for record in records:
            properties = record.get("properties", {})
            contacts = record.get("associations", {}).get("contacts", {}).get("results", [])
            processed.append(
                {
                    "order_id": record.get("id") or properties.get("hs_object_id"),
                    "customer_id": contacts[0].get("id") if contacts else None,
                    "order_ts": properties.get("hs_createdate") or properties.get("createdate"),
                    "order_status": properties.get("order_status") or properties.get("hs_order_status"),
                    "subtotal": self._to_float(properties.get("subtotal") or properties.get("hs_subtotal")),
                    "tax": self._to_float(properties.get("tax") or properties.get("hs_tax")),
                    "shipping": self._to_float(properties.get("shipping") or properties.get("hs_shipping_cost")),
                    "discount": self._to_float(properties.get("discount") or properties.get("hs_discount")),
                    "net_total": self._to_float(properties.get("net_total") or properties.get("hs_total_price") or properties.get("amount")),
                    "currency": properties.get("currency") or properties.get("hs_currency_code"),
                    "channel": properties.get("channel") or properties.get("hs_channel"),
                    "updated_at": properties.get("lastmodifieddate"),
                }
            )
        return processed

    def process_products(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed: List[Dict[str, Any]] = []
        for record in records:
            properties = record.get("properties", {})
            processed.append(
                {
                    "product_id": record.get("id") or properties.get("hs_object_id"),
                    "sku": properties.get("hs_sku") or properties.get("sku"),
                    "name": properties.get("name"),
                    "category": properties.get("hs_product_category") or properties.get("product_category"),
                    "brand": properties.get("brand") or properties.get("hs_brand"),
                    "price": self._to_float(properties.get("price") or properties.get("hs_price")),
                    "cost": self._to_float(properties.get("cost") or properties.get("hs_cost_of_goods_sold")),
                    "status": properties.get("hs_product_status") or properties.get("product_status"),
                    "launch_date": properties.get("hs_date_entered_market") or properties.get("launch_date"),
                    "updated_at": properties.get("lastmodifieddate"),
                }
            )
        return processed
