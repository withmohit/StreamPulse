"""
StreamPulse Event Simulator
Generates realistic e-commerce events and sends to ingest API.

Usage:
    python simulator/generate.py                     # default 10 events/sec
    python simulator/generate.py --rate 50           # 50 events/sec
    python simulator/generate.py --rate 5 --bad 0.2  # 5/sec, 20% malformed
"""

import argparse
import random
import time
import uuid
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────

INGEST_URL = "http://localhost:8000/ingest"

TENANTS = ["flipkart", "meesho", "myntra", "nykaa", "zepto"]

REGIONS = ["North", "South", "East", "West", "Central"]

SKUS = [
    "SKU_TSHIRT_001", "SKU_JEANS_002", "SKU_SHOES_003",
    "SKU_LAPTOP_004", "SKU_PHONE_005", "SKU_EARBUDS_006",
    "SKU_KURTA_007", "SKU_SAREE_008", "SKU_WATCH_009",
    "SKU_BAG_010",   "SKU_CREAM_011", "SKU_SHAMPOO_012",
]

CATEGORIES = ["fashion", "electronics", "beauty", "grocery", "home"]

PAGES = [
    "/home", "/search", "/product/detail",
    "/cart", "/checkout", "/order/confirm",
    "/account", "/wishlist", "/offers",
]

ERROR_TYPES = [
    "payment_gateway_timeout",
    "out_of_stock_race_condition",
    "invalid_coupon_applied",
    "address_validation_failed",
    "session_expired",
    "cart_sync_failed",
]

PAYMENT_METHODS = ["upi", "card", "netbanking", "cod", "wallet"]

DEVICES = ["android", "ios", "web_desktop", "web_mobile"]

# ── Event Generators ──────────────────────────────────────────────────────────

def make_pageview_event(tenant: str) -> dict:
    """User viewed a page — most frequent event type."""
    return {
        "event_type": "pageview",
        "tenant_id": tenant,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {
            "user_id": f"user_{random.randint(1000, 99999)}",
            "session_id": str(uuid.uuid4()),
            "page": random.choice(PAGES),
            "sku": random.choice(SKUS) if random.random() > 0.4 else None,
            "category": random.choice(CATEGORIES),
            "region": random.choice(REGIONS),
            "device": random.choice(DEVICES),
            "load_time_ms": random.randint(80, 3500),
            "referrer": random.choice([
                "google_search", "instagram_ad",
                "direct", "email_campaign", None
            ]),
        }
    }


def make_purchase_event(tenant: str) -> dict:
    """User completed a purchase — high value event."""
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(99, 12999), 2)
    return {
        "event_type": "purchase",
        "tenant_id": tenant,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {
            "user_id": f"user_{random.randint(1000, 99999)}",
            "order_id": f"ORD_{uuid.uuid4().hex[:10].upper()}",
            "sku": random.choice(SKUS),
            "category": random.choice(CATEGORIES),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "currency": "INR",
            "payment_method": random.choice(PAYMENT_METHODS),
            "region": random.choice(REGIONS),
            "device": random.choice(DEVICES),
            "is_first_order": random.random() < 0.15,   # 15% new customers
            "coupon_applied": random.random() < 0.3,    # 30% used a coupon
            "discount_amount": round(random.uniform(0, 500), 2),
        }
    }


def make_click_event(tenant: str) -> dict:
    """User clicked something — useful for funnel analysis."""
    return {
        "event_type": "click",
        "tenant_id": tenant,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {
            "user_id": f"user_{random.randint(1000, 99999)}",
            "session_id": str(uuid.uuid4()),
            "element": random.choice([
                "add_to_cart", "buy_now", "add_to_wishlist",
                "apply_coupon", "proceed_to_checkout",
                "product_image", "review_section",
                "size_selector", "color_selector",
            ]),
            "sku": random.choice(SKUS),
            "page": random.choice(PAGES),
            "region": random.choice(REGIONS),
            "device": random.choice(DEVICES),
            "position_on_page": random.randint(1, 20),  # scroll depth proxy
        }
    }


def make_error_event(tenant: str) -> dict:
    """Something went wrong — critical for reliability monitoring."""
    return {
        "event_type": "error",
        "tenant_id": tenant,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {
            "user_id": f"user_{random.randint(1000, 99999)}",
            "error_type": random.choice(ERROR_TYPES),
            "error_code": random.choice([400, 402, 404, 408, 429, 500, 502, 503]),
            "page": random.choice(PAGES),
            "sku": random.choice(SKUS) if random.random() > 0.5 else None,
            "region": random.choice(REGIONS),
            "device": random.choice(DEVICES),
            "retry_count": random.randint(0, 3),
            "order_id": f"ORD_{uuid.uuid4().hex[:10].upper()}" if random.random() > 0.5 else None,
            "stack_trace_hash": uuid.uuid4().hex[:16],  # anonymized, not raw trace
        }
    }


# ── Malformed Event Generators (for DLQ testing) ─────────────────────────────

def make_bad_event() -> dict:
    """Intentionally malformed — tests your DLQ and validation layer."""
    bad_variants = [
        # Missing required field
        {"event_type": "purchase", "tenant_id": "flipkart"},

        # Invalid event_type
        {
            "event_type": "invalid_unknown_type",
            "tenant_id": "meesho",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {}
        },

        # Empty tenant_id
        {
            "event_type": "pageview",
            "tenant_id": "   ",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"page": "/home"}
        },

        # Completely garbage payload
        {"broken": True, "random_key": "random_value"},

        # Wrong timestamp format
        {
            "event_type": "click",
            "tenant_id": "nykaa",
            "timestamp": "not-a-real-timestamp",
            "data": {"element": "buy_now"}
        },
    ]
    return random.choice(bad_variants)


# ── Event Router ─────────────────────────────────────────────────────────────

# Realistic distribution: pageviews >> clicks >> errors >> purchases
EVENT_WEIGHTS = {
    "pageview": 0.50,   # 50% of traffic
    "click":    0.30,   # 30%
    "error":    0.12,   # 12%
    "purchase": 0.08,   # 8% — purchase funnel is narrow
}

GENERATORS = {
    "pageview": make_pageview_event,
    "click":    make_click_event,
    "error":    make_error_event,
    "purchase": make_purchase_event,
}

def generate_event(bad_rate: float = 0.10) -> dict:
    """Generate one event. bad_rate controls % of malformed events."""
    if random.random() < bad_rate:
        return make_bad_event()

    tenant = random.choice(TENANTS)
    event_type = random.choices(
        list(EVENT_WEIGHTS.keys()),
        weights=list(EVENT_WEIGHTS.values()),
        k=1
    )[0]
    return GENERATORS[event_type](tenant)


# ── Sender ────────────────────────────────────────────────────────────────────

def send_event(event: dict) -> tuple[bool, str]:
    """Send one event to ingest API. Returns (success, message)."""
    try:
        resp = requests.post(INGEST_URL, json=event, timeout=2)
        if resp.status_code == 200:
            return True, resp.json().get("event_id", "?")
        else:
            return False, f"HTTP {resp.status_code}: {resp.text[:80]}"
    except requests.exceptions.ConnectionError:
        return False, "Connection refused — is the ingest API running?"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)


# ── Main Loop ─────────────────────────────────────────────────────────────────

def run(rate: float, bad_rate: float):
    interval = 1.0 / rate
    sent = 0
    failed = 0
    bad_sent = 0

    print(f"\n StreamPulse Simulator")
    print(f" Rate      : {rate} events/sec")
    print(f" Bad rate  : {bad_rate * 100:.0f}% malformed")
    print(f" Target    : {INGEST_URL}")
    print(f" Tenants   : {', '.join(TENANTS)}")
    print(f"\n{'─' * 55}")
    print(f" {'#':<6} {'TYPE':<12} {'TENANT':<12} {'STATUS':<10} DETAIL")
    print(f"{'─' * 55}")

    while True:
        event = generate_event(bad_rate)
        is_bad = "event_type" not in event or event.get("tenant_id", "").strip() == ""

        success, detail = send_event(event)

        if success:
            sent += 1
            if is_bad:
                bad_sent += 1
            status = "✓ OK"
        else:
            failed += 1
            status = "✗ FAIL"

        event_type = event.get("event_type", "MALFORMED")
        tenant     = event.get("tenant_id", "?")[:10]

        print(f" {sent + failed:<6} {event_type:<12} {tenant:<12} {status:<10} {detail[:30]}")

        # Print summary every 50 events
        if (sent + failed) % 50 == 0:
            total = sent + failed
            print(f"\n{'─' * 55}")
            print(f" SUMMARY: {total} sent | {sent} ok | {failed} api-errors | ~{bad_sent} to DLQ")
            print(f"{'─' * 55}\n")

        time.sleep(interval)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StreamPulse event simulator")
    parser.add_argument(
        "--rate", type=float, default=10.0,
        help="Events per second (default: 10)"
    )
    parser.add_argument(
        "--bad", type=float, default=0.10,
        help="Fraction of malformed events (default: 0.10 = 10%%)"
    )
    args = parser.parse_args()

    try:
        run(rate=args.rate, bad_rate=args.bad)
    except KeyboardInterrupt:
        print("\n\n Simulator stopped.")