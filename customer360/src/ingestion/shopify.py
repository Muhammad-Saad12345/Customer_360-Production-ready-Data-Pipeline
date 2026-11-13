"""Shopify Admin GraphQL adapter: the ``api`` mode for OMS (orders) and PIM (products).

Status: implemented and unit-tested against recorded/mocked responses only.
It has NOT been run against a live store from this repository yet; the
first live run is step M18 in docs/OMS_PIM_API_INTEGRATION_GUIDE.md.

Secret ``customer360/<env>/oms`` (and ``.../pim``) is a JSON object:

    {"shop_domain": "your-store.myshopify.com",
     "client_id": "...", "client_secret": "...",     # Dev Dashboard app, or
     "access_token": "shpat_...",                     # a legacy custom-app token
     "api_version": "2026-10"}

Apps created in the Dev Dashboard (the only option for new apps since
1 Jan 2026) get a short-lived token through the client-credentials grant;
the token is requested once per Lambda invocation and never stored.

Scopes: read_orders (+ read_all_orders for history older than 60 days),
read_products, read_inventory (unit cost), read_customers (customer email).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

from src.errors import SourceModeError, SourceRequestError, TransientError
from src.ingestion.http_client import SourceHttpClient

logger = logging.getLogger("customer360.shopify")

DEFAULT_API_VERSION = "2026-10"
PAGE_SIZE = 100
MAX_PAGES = 500

ORDERS_QUERY = """
query Orders($first: Int!, $after: String, $query: String) {
  orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes {
      legacyResourceId name createdAt updatedAt cancelledAt
      displayFinancialStatus displayFulfillmentStatus currencyCode sourceName
      customer { legacyResourceId email phone }
      subtotalPriceSet { shopMoney { amount } }
      totalTaxSet { shopMoney { amount } }
      totalShippingPriceSet { shopMoney { amount } }
      totalDiscountsSet { shopMoney { amount } }
      totalPriceSet { shopMoney { amount } }
    }
  }
}
"""

PRODUCTS_QUERY = """
query Products($first: Int!, $after: String, $query: String) {
  products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes {
      legacyResourceId title vendor productType status createdAt publishedAt updatedAt
      variants(first: 1) { nodes { sku price inventoryItem { unitCost { amount } } } }
    }
  }
}
"""


def _money(node: Optional[Dict[str, Any]]) -> Optional[float]:
    try:
        return float(((node or {}).get("shopMoney") or {}).get("amount"))
    except (TypeError, ValueError):
        return None


def map_order_status(order: Dict[str, Any]) -> str:
    if order.get("cancelledAt"):
        return "cancelled"
    financial = (order.get("displayFinancialStatus") or "").upper()
    fulfillment = (order.get("displayFulfillmentStatus") or "").upper()
    if financial in ("REFUNDED", "PARTIALLY_REFUNDED"):
        return "refunded"
    if financial in ("PENDING", "AUTHORIZED", "EXPIRED"):
        return "pending"
    if financial == "VOIDED":
        return "failed"
    if fulfillment == "FULFILLED":
        return "fulfilled"
    if fulfillment in ("PARTIALLY_FULFILLED", "IN_PROGRESS", "ON_HOLD", "SCHEDULED"):
        return "processing"
    if financial in ("PAID", "PARTIALLY_PAID"):
        return "paid"
    return "processing"


def map_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """Shopify order -> the OMS raw contract validated by the silver gate."""
    customer = order.get("customer") or {}
    customer_id = f"shopify-{customer['legacyResourceId']}" if customer.get("legacyResourceId") else ""
    return {
        "order_id": f"shopify-{order.get('legacyResourceId')}",
        "order_name": order.get("name"),
        "customer_id": customer_id,
        "customer_email": customer.get("email"),
        "customer_phone": customer.get("phone"),
        "order_ts": order.get("createdAt"),
        "order_status": map_order_status(order),
        "subtotal": _money(order.get("subtotalPriceSet")),
        "tax": _money(order.get("totalTaxSet")),
        "shipping": _money(order.get("totalShippingPriceSet")),
        "discount": _money(order.get("totalDiscountsSet")),
        "net_total": _money(order.get("totalPriceSet")),
        "currency": order.get("currencyCode"),
        "channel": (order.get("sourceName") or "web").lower(),
        "updated_at": order.get("updatedAt"),
        "source_type": "shopify",
    }


def map_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """Shopify product -> the PIM raw contract validated by the silver gate."""
    variants = ((product.get("variants") or {}).get("nodes")) or [{}]
    variant = variants[0] or {}
    unit_cost = ((variant.get("inventoryItem") or {}).get("unitCost") or {}).get("amount")
    try:
        price = float(variant.get("price")) if variant.get("price") is not None else None
    except (TypeError, ValueError):
        price = None
    try:
        cost = float(unit_cost) if unit_cost is not None else None
    except (TypeError, ValueError):
        cost = None
    return {
        "product_id": f"shopify-{product.get('legacyResourceId')}",
        "sku": variant.get("sku") or "",
        "name": product.get("title"),
        "category": product.get("productType") or "",
        "brand": product.get("vendor") or "",
        "price": price,
        "cost": cost,
        "status": (product.get("status") or "").lower(),
        "launch_date": product.get("publishedAt") or product.get("createdAt"),
        "updated_at": product.get("updatedAt"),
        "attributes": {},
        "source_type": "shopify",
    }


class ShopifyClient:
    def __init__(
        self,
        credentials: Dict[str, Any],
        http: Optional[SourceHttpClient] = None,
        sleep: Callable[[float], None] = time.sleep,
        source: str = "shopify",
    ):
        shop = (credentials.get("shop_domain") or "").strip().lower()
        if not shop or not shop.endswith(".myshopify.com"):
            raise SourceModeError("Shopify secret needs shop_domain like 'your-store.myshopify.com'")
        self.shop = shop
        self.api_version = credentials.get("api_version") or DEFAULT_API_VERSION
        self.credentials = credentials
        self.source = source
        self._sleep = sleep
        self.http = http or SourceHttpClient(source=source, sleep=sleep)
        self.graphql_url = f"https://{self.shop}/admin/api/{self.api_version}/graphql.json"

    def authenticate(self) -> None:
        token = self.credentials.get("access_token")
        if not token:
            client_id = self.credentials.get("client_id")
            client_secret = self.credentials.get("client_secret")
            if not client_id or not client_secret:
                raise SourceModeError("Shopify secret needs access_token, or client_id + client_secret")
            body = self.http.request_json(
                "POST",
                f"https://{self.shop}/admin/oauth/access_token",
                data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
            )
            token = body.get("access_token")
            if not token:
                raise SourceRequestError("Shopify token response did not contain access_token")
        self.http.headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    def _graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(1, 6):
            body = self.http.post_json(self.graphql_url, {"query": query, "variables": variables})
            errors = body.get("errors") or []
            if not errors:
                return body.get("data") or {}
            throttled = any(((e.get("extensions") or {}).get("code") == "THROTTLED") for e in errors if isinstance(e, dict))
            if throttled and attempt < 5:
                self._sleep(min(2 ** attempt, 30))
                continue
            if throttled:
                raise TransientError(f"{self.source}: Shopify GraphQL throttled")
            messages = "; ".join(str(e.get("message", e)) for e in errors if isinstance(e, dict))
            raise SourceRequestError(f"{self.source}: Shopify GraphQL error: {messages[:500]}")
        raise TransientError(f"{self.source}: Shopify GraphQL retries exhausted")

    def _paginate(self, query: str, root: str, updated_after: Optional[str]) -> Iterator[Dict[str, Any]]:
        if "X-Shopify-Access-Token" not in self.http.headers:
            self.authenticate()
        search = f"updated_at:>='{updated_after}'" if updated_after else None
        after: Optional[str] = None
        for _ in range(MAX_PAGES):
            data = self._graphql(query, {"first": PAGE_SIZE, "after": after, "query": search})
            connection = data.get(root) or {}
            for node in connection.get("nodes") or []:
                yield node
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return
            after = page_info.get("endCursor")
        raise SourceRequestError(f"{self.source}: more than {MAX_PAGES} pages; narrow the window")

    def fetch_orders(self, updated_after: Optional[str]) -> List[Dict[str, Any]]:
        return [map_order(o) for o in self._paginate(ORDERS_QUERY, "orders", updated_after)]

    def fetch_products(self, updated_after: Optional[str]) -> List[Dict[str, Any]]:
        return [map_product(p) for p in self._paginate(PRODUCTS_QUERY, "products", updated_after)]
