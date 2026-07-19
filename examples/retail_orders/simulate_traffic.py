"""
Simulates N concurrent shoppers, each a persistent customer_id moving
through a real funnel over time — not independent random events:

  IDLE -> (start session, pick channel/region) -> IN_CART
  IN_CART -> add item | remove item | abandon cart -> IDLE | checkout -> DONE
  DONE -> (maybe) cancel | refund shortly after purchase -> IDLE

Every event a given customer publishes during a session carries the same
customer_id/channel/region, so enriched.customer_360 (keyed by customer_id)
and enriched.order_summary_5min (keyed by channel/region) both aggregate a
believable, causally-connected stream rather than uncorrelated noise.

Needs google-cloud-pubsub, already pulled in by the `gcp` extra
(`pip install streaming-pipeline-framework[gcp]`) that running the pipeline
itself requires.

Usage:
  python -m examples.retail_orders.simulate_traffic \\
    --project <gcp-project> --customers 1000 --duration 60
"""

from __future__ import annotations

import argparse
import json
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from google.cloud import pubsub_v1

CHANNELS = ["web", "mobile", "store"]
REGIONS = ["us-west", "us-east", "eu-west"]

# Per-tick transition probabilities while IN_CART (must sum to 1.0).
P_ADD_ITEM = 0.50
P_REMOVE_ITEM = 0.15
P_ABANDON = 0.20
P_CHECKOUT = 0.15

# Post-purchase follow-up probabilities (mutually exclusive).
P_CANCEL_AFTER_PURCHASE = 0.15
P_REFUND_AFTER_PURCHASE = 0.10


@dataclass
class Shopper:
    customer_id: str
    state: str = "IDLE"  # IDLE | IN_CART | DONE
    channel: str = ""
    region: str = ""
    cart_items: int = 0
    cart_value: float = 0.0
    order_total: float = 0.0
    next_action_at: float = field(default_factory=lambda: time.monotonic() + random.uniform(0, 2))


def _envelope(event_type: str, shopper: Shopper, order_total: float) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source_system": "load-simulator",
        "domain": "orders",
        "public_id": f"PUB-{uuid.uuid4().hex[:8]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "order_id": f"ORD-{uuid.uuid4().hex[:8]}",
            "channel": shopper.channel,
            "region": shopper.region,
            "order_total": round(order_total, 2),
            "customer_id": shopper.customer_id,
        },
    }


def step(shopper: Shopper, min_interval: float, max_interval: float) -> dict | None:
    """Advances one shopper by one action and returns the event to publish."""
    event = None

    if shopper.state == "IDLE":
        shopper.channel = random.choice(CHANNELS)
        shopper.region = random.choice(REGIONS)
        shopper.cart_items = 1
        shopper.cart_value = round(random.uniform(10, 100), 2)
        shopper.state = "IN_CART"
        event = _envelope("cart.item_added", shopper, shopper.cart_value)

    elif shopper.state == "IN_CART":
        roll = random.random()
        if roll < P_ADD_ITEM:
            item_value = round(random.uniform(10, 100), 2)
            shopper.cart_items += 1
            shopper.cart_value += item_value
            event = _envelope("cart.item_added", shopper, shopper.cart_value)
        elif roll < P_ADD_ITEM + P_REMOVE_ITEM and shopper.cart_items > 1:
            item_value = round(random.uniform(10, 100), 2)
            shopper.cart_items -= 1
            shopper.cart_value = max(0.0, shopper.cart_value - item_value)
            event = _envelope("cart.item_removed", shopper, 0.0)
        elif roll < P_ADD_ITEM + P_REMOVE_ITEM + P_ABANDON:
            event = _envelope("cart.abandoned", shopper, 0.0)
            shopper.state = "IDLE"
        else:
            shopper.order_total = shopper.cart_value
            event = _envelope("order.created", shopper, shopper.order_total)
            shopper.state = "DONE"

    elif shopper.state == "DONE":
        roll = random.random()
        if roll < P_CANCEL_AFTER_PURCHASE:
            event = _envelope("order.cancelled", shopper, 0.0)
        elif roll < P_CANCEL_AFTER_PURCHASE + P_REFUND_AFTER_PURCHASE:
            event = _envelope("order.refunded", shopper, shopper.order_total)
        shopper.state = "IDLE"

    shopper.next_action_at = time.monotonic() + random.uniform(min_interval, max_interval)
    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--topic", default="order-events")
    parser.add_argument("--customers", type=int, default=1000, help="concurrent shoppers")
    parser.add_argument("--duration", type=int, default=60, help="seconds")
    parser.add_argument("--min-interval", type=float, default=1.0, help="min seconds between one shopper's actions")
    parser.add_argument("--max-interval", type=float, default=3.0, help="max seconds between one shopper's actions")
    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    shoppers = [Shopper(customer_id=f"CUST-{i:05d}") for i in range(args.customers)]

    sent = 0
    errors = 0
    lock = threading.Lock()

    def on_done(fut):
        nonlocal errors
        try:
            fut.result()
        except Exception:
            with lock:
                errors += 1

    print(f"Simulating {args.customers} concurrent shoppers against {topic_path} for {args.duration}s...")
    start = time.monotonic()
    end = start + args.duration
    last_report = start

    while time.monotonic() < end:
        now = time.monotonic()
        for shopper in shoppers:
            if shopper.next_action_at > now:
                continue
            event = step(shopper, args.min_interval, args.max_interval)
            if event is None:
                continue
            future = publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
            future.add_done_callback(on_done)
            sent += 1
        if now - last_report >= 5:
            print(f"  {sent} events sent in {now - start:.0f}s ({sent / (now - start):.0f}/sec avg)")
            last_report = now
        time.sleep(0.05)

    elapsed = time.monotonic() - start
    print(f"Done. Sent {sent} events in {elapsed:.1f}s ({sent / elapsed:.0f}/sec avg), {errors} publish errors.")


if __name__ == "__main__":
    main()
