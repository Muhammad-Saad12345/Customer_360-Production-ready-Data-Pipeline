from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query

app = FastAPI(title="Mock Loyalty API")


def _loyalty_record(loyalty_id: str, customer_id: str, tier: str, points_balance: int, updated_at: datetime) -> Dict[str, Any]:
    return {
        "loyalty_id": loyalty_id,
        "customer_id": customer_id,
        "tier": tier,
        "points_balance": points_balance,
        "earn_ts": (updated_at - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "redeem_ts": (updated_at - timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
        "benefit_code": "VIP10" if tier == "gold" else "STANDARD",
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    }


def _build_loyalty() -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    return [
        _loyalty_record("LOY-1001", "C-1001", "gold", 3400, now - timedelta(days=5)),
        _loyalty_record("LOY-1002", "C-1002", "silver", 1200, now - timedelta(days=3)),
        _loyalty_record("LOY-1003", "C-1003", "gold", 2800, now - timedelta(days=1)),
        _loyalty_record("LOY-1004", "C-1004", "bronze", 800, now - timedelta(hours=7)),
        _loyalty_record("LOY-1005", "C-1001", "gold", 4200, now - timedelta(hours=2)),
        {"loyalty_id": "LOY-INVALID-1", "customer_id": "", "tier": "diamond", "points_balance": -5, "earn_ts": "2026-01-02T00:10:00Z", "redeem_ts": "not-a-date", "benefit_code": "VIP", "updated_at": (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")},
        {"loyalty_id": "LOY-1006", "customer_id": "C-1006", "tier": "camp", "points_balance": 700, "earn_ts": "2026-01-02T00:10:00Z", "redeem_ts": "2026-01-02T00:15:00Z", "benefit_code": "STANDARD", "updated_at": (now - timedelta(minutes=8)).isoformat().replace("+00:00", "Z")},
        {"loyalty_id": "LOY-1001", "customer_id": "C-1001", "tier": "gold", "points_balance": 3500, "earn_ts": "2026-01-02T00:35:00Z", "redeem_ts": "2026-01-02T00:45:00Z", "benefit_code": "VIP10", "updated_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")},
    ]


ALL_LOYALTY = _build_loyalty()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "mock-loyalty"}


@app.get("/api/v1/loyalty")
def list_loyalty(page: int = Query(1, ge=1), page_size: int = Query(100, ge=1, le=200), updated_after: Optional[str] = None) -> Dict[str, Any]:
    items = ALL_LOYALTY
    if updated_after:
        items = [r for r in items if (r.get("updated_at") or r.get("earn_ts") or "1970-01-01T00:00:00Z") >= updated_after]
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    next_page = page + 1 if end < total else None
    return {"page": page, "page_size": page_size, "total": total, "next_page": next_page, "items": page_items}


@app.get("/api/v1/loyalty/{loyalty_id}")
def get_loyalty(loyalty_id: str) -> Dict[str, Any]:
    item = next((r for r in ALL_LOYALTY if r.get("loyalty_id") == loyalty_id), None)
    if item is None:
        return {"detail": "Loyalty record not found"}
    return item
