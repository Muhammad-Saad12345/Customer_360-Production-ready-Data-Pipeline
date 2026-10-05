from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query

app = FastAPI(title="Mock OMS API")


def _order_record(order_id: str, customer_id: str, updated_at: datetime, status: str = "paid") -> Dict[str, Any]:
    subtotal = 150.0 if order_id.endswith("1") else 250.0
    tax = subtotal * 0.1
    shipping = 8.0 if status != "cancelled" else 0.0
    discount = 10.0 if customer_id.startswith("C") else 0.0
    net_total = subtotal + tax + shipping - discount
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_ts": (updated_at - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "order_status": status,
        "subtotal": subtotal,
        "tax": tax,
        "shipping": shipping,
        "discount": discount,
        "net_total": net_total,
        "currency": "USD",
        "channel": "web",
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    }


def _build_orders() -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    records = [
        _order_record("OMS-1001", "C-1001", now - timedelta(days=1, hours=2), "paid"),
        _order_record("OMS-1002", "C-1002", now - timedelta(days=1), "shipped"),
        _order_record("OMS-1003", "C-1001", now - timedelta(hours=20), "paid"),
        _order_record("OMS-1004", "C-1003", now - timedelta(hours=12), "fulfilled"),
        _order_record("OMS-1005", "C-1002", now - timedelta(hours=9), "cancelled"),
        _order_record("OMS-1006", "C-1004", now - timedelta(days=2), "paid"),
        _order_record("OMS-1007", "C-1005", now - timedelta(hours=2), "paid"),
        _order_record("OMS-1008", "C-1001", now - timedelta(minutes=30), "paid"),
        _order_record("OMS-1009", "C-1009", now - timedelta(minutes=10), "paid"),
        _order_record("OMS-1010", "C-1006", now - timedelta(minutes=5), "paid"),
        {"order_id": "OMS-INVALID-1", "customer_id": "", "order_ts": "bad-ts", "order_status": "unknown", "subtotal": "bad", "tax": 0, "shipping": 0, "discount": 0, "net_total": 0, "currency": "USD", "channel": "web", "updated_at": now.isoformat().replace("+00:00", "Z")},
        {"order_id": "OMS-DUP-1008", "customer_id": "C-1001", "order_ts": (now - timedelta(minutes=20)).isoformat().replace("+00:00", "Z"), "order_status": "paid", "subtotal": 170, "tax": 17, "shipping": 7, "discount": 0, "net_total": 194, "currency": "USD", "channel": "web", "updated_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")},
        {"order_id": "OMS-MISSING-CUST", "customer_id": "", "order_ts": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z"), "order_status": "paid", "subtotal": 99, "tax": 9, "shipping": 4, "discount": 0, "net_total": 112, "currency": "USD", "channel": "web", "updated_at": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z")},
    ]
    return records


ALL_ORDERS = _build_orders()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "mock-oms"}


@app.get("/api/v1/orders")
def list_orders(page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=200), updated_after: Optional[str] = None) -> Dict[str, Any]:
    items = ALL_ORDERS
    if updated_after:
        items = [
            r for r in items if (r.get("updated_at") or r.get("order_ts") or "1970-01-01T00:00:00Z") >= updated_after
        ]
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    next_page = page + 1 if end < total else None
    return {"page": page, "page_size": page_size, "total": total, "next_page": next_page, "items": page_items}


@app.get("/api/v1/orders/{order_id}")
def get_order(order_id: str) -> Dict[str, Any]:
    item = next((r for r in ALL_ORDERS if r.get("order_id") == order_id), None)
    if item is None:
        return {"detail": "Order not found"}
    return item
