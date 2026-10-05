from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query

app = FastAPI(title="Mock PIM API")


def _product_record(product_id: str, sku: str, name: str, category: str, brand: str, price: float, cost: float, status: str, launch_date: datetime, updated_at: datetime) -> Dict[str, Any]:
    return {
        "product_id": product_id,
        "sku": sku,
        "name": name,
        "category": category,
        "brand": brand,
        "price": price,
        "cost": cost,
        "attributes": {"color": "black", "size": "M"},
        "status": status,
        "launch_date": launch_date.isoformat().replace("+00:00", "Z"),
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    }


def _build_products() -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    return [
        _product_record("PIM-1001", "SKU-1001", "Aero Smartwatch", "Wearables", "Aero", 299.0, 180.0, "active", now - timedelta(days=120), now - timedelta(days=18)),
        _product_record("PIM-1002", "SKU-1002", "Nova Headphones", "Audio", "Nova", 199.0, 120.0, "active", now - timedelta(days=90), now - timedelta(days=7)),
        _product_record("PIM-1003", "SKU-1003", "Prime Laptop", "Computing", "Prime", 999.0, 750.0, "active", now - timedelta(days=150), now - timedelta(days=2)),
        _product_record("PIM-1004", "SKU-1004", "Flex Keyboard", "Accessories", "Flex", 89.0, 40.0, "archived", now - timedelta(days=200), now - timedelta(hours=5)),
        _product_record("PIM-1005", "SKU-1005", "Pulse Charger", "Accessories", "Pulse", 49.0, 28.0, "active", now - timedelta(days=30), now - timedelta(minutes=40)),
        {"product_id": "PIM-INVALID-1", "sku": "SKU-INVALID-1", "name": "Bad Product", "category": "Phones", "brand": "Acme", "price": -10, "cost": 50, "attributes": {"color": "black"}, "status": "invalid-status", "launch_date": "not-a-date", "updated_at": (now - timedelta(minutes=3)).isoformat().replace("+00:00", "Z")},
        {"product_id": "", "sku": "SKU-DUPLICATE-1", "name": "Missing Product ID", "category": "Phones", "brand": "Acme", "price": 120.0, "cost": 70, "attributes": {"color": "blue"}, "status": "active", "launch_date": (now - timedelta(days=10)).isoformat().replace("+00:00", "Z"), "updated_at": (now - timedelta(minutes=20)).isoformat().replace("+00:00", "Z")},
        {"product_id": "PIM-1006", "sku": "SKU-1001", "name": "Duplicate SKU", "category": "Phones", "brand": "Acme", "price": 159.0, "cost": 100.0, "attributes": {"color": "red"}, "status": "active", "launch_date": (now - timedelta(days=12)).isoformat().replace("+00:00", "Z"), "updated_at": (now - timedelta(minutes=15)).isoformat().replace("+00:00", "Z")},
    ]


ALL_PRODUCTS = _build_products()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "mock-pim"}


@app.get("/api/v1/products")
def list_products(page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=200), updated_after: Optional[str] = None) -> Dict[str, Any]:
    items = ALL_PRODUCTS
    if updated_after:
        items = [r for r in items if (r.get("updated_at") or r.get("launch_date") or "1970-01-01T00:00:00Z") >= updated_after]
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    next_page = page + 1 if end < total else None
    return {"page": page, "page_size": page_size, "total": total, "next_page": next_page, "items": page_items}


@app.get("/api/v1/products/{product_id}")
def get_product(product_id: str) -> Dict[str, Any]:
    item = next((r for r in ALL_PRODUCTS if r.get("product_id") == product_id), None)
    if item is None:
        return {"detail": "Product not found"}
    return item
