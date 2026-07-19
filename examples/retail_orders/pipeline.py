"""
Example: e-commerce order events on the streaming-pipeline-framework.

Proves the framework is domain-agnostic — this has nothing to do with the
insurance use case the framework was extracted from. Swap in your own
DomainSpec, aggregator, and alert rules the same way.

Models a retail funnel, not just completed orders: cart.item_added,
cart.item_removed, cart.abandoned, order.created, order.cancelled,
order.refunded. All six share the same envelope/payload shape — for cart
events, payload.order_id doubles as a cart/session identifier and
payload.order_total as the cart's value at that point (0 is fine for
removal/abandonment events that don't carry a meaningful total).

Two DomainSpecs consume the same "order-events" topic: ORDERS_DOMAIN (the
operational (channel, region) view above) and CUSTOMER_360_DOMAIN (a
per-customer view keyed by payload.customer_id). This is the framework's
pattern for multiple derived views over one event stream — see
DomainSpec.raw_table and .enforce_domain_match's docstrings in framework.py
for why CUSTOMER_360_DOMAIN sets raw_table=None and
enforce_domain_match=False. Note "360" here means "everything about this
customer within the current 5-minute window," not an unbounded lifetime
profile — this framework's aggregation is fundamentally windowed; a true
lifetime CDP-style profile needs a keyed store (Bigtable, Firestore) or a
periodic BigQuery MERGE, not FixedWindows.

Run locally (DirectRunner, needs real Pub/Sub + BigQuery):
  python -m examples.retail_orders.pipeline --project <gcp-project> --runner DirectRunner

Deploy to Dataflow:
  python -m examples.retail_orders.pipeline \\
    --project <gcp-project> --region us-central1 --runner DataflowRunner \\
    --temp-location gs://<bucket>/tmp \\
    --service-account-email <dataflow-sa>@<gcp-project>.iam.gserviceaccount.com
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from streaming_pipeline_framework import DomainSpec, beam
from streaming_pipeline_framework.cli import main as cli_main

ENVELOPE_REQUIRED = frozenset(
    {"event_id", "event_type", "source_system", "domain", "public_id", "timestamp"}
)
PAYLOAD_REQUIRED = frozenset({"order_id", "channel", "region", "order_total", "customer_id"})

CANCELLATION_RATE_THRESHOLD = 0.15
REFUND_SPIKE_THRESHOLD = 5_000.0
CART_ABANDONMENT_RATE_THRESHOLD = 0.50

# BigQuery schemas — required because write_method defaults to
# STORAGE_WRITE_API, which (unlike legacy streaming inserts) needs field
# types up front to build its write protocol; it can't infer them from an
# already-existing table. Field sets mirror ENVELOPE_REQUIRED/
# PAYLOAD_REQUIRED and AggregateOrderWindow/_alert's output shape exactly —
# keep in sync if those change. Matches examples/retail_orders/terraform/bigquery.tf.
ORDER_EVENTS_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "source_system", "type": "STRING", "mode": "REQUIRED"},
        {"name": "domain", "type": "STRING", "mode": "REQUIRED"},
        {"name": "public_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {
            "name": "payload", "type": "RECORD", "mode": "REQUIRED",
            "fields": [
                {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "channel", "type": "STRING", "mode": "REQUIRED"},
                {"name": "region", "type": "STRING", "mode": "REQUIRED"},
                {"name": "order_total", "type": "FLOAT", "mode": "REQUIRED"},
                {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            ],
        },
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

ORDER_SUMMARY_SCHEMA = {
    "fields": [
        {"name": "event_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "created_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_order_value", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "cancelled_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "refunded_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "refunded_amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "cart_added_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "cart_removed_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "cart_abandoned_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "computed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "channel", "type": "STRING", "mode": "REQUIRED"},
        {"name": "region", "type": "STRING", "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# enriched.customer_360 — one row per customer_id per 5-minute window,
# written by CUSTOMER_360_DOMAIN. last_channel/last_region are best-effort:
# a CombineFn's accumulators can merge in any order across a distributed
# combine, so "last" means "an arbitrary event observed in this window,"
# not a true chronologically-last one.
CUSTOMER_360_SCHEMA = {
    "fields": [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "cart_added_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "cart_removed_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "cart_abandoned_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "created_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_spend", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "cancelled_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "refunded_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "refunded_amount", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "last_channel", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_region", "type": "STRING", "mode": "NULLABLE"},
        {"name": "computed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# Shared across every domain passed to cli_main() — this example only has
# one ("orders"), so this reflects _alert()'s shape specifically. A
# multi-domain deployment sharing one alerts table would need a schema
# covering every domain's alert context, or a JSON `context` column instead.
ALERTS_SCHEMA = {
    "fields": [
        {"name": "alert_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "alert_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "domain", "type": "STRING", "mode": "REQUIRED"},
        {"name": "severity", "type": "STRING", "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "metric_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "metric_value", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "threshold", "type": "FLOAT", "mode": "REQUIRED"},
        {
            "name": "context", "type": "RECORD", "mode": "NULLABLE",
            "fields": [
                {"name": "channel", "type": "STRING", "mode": "NULLABLE"},
                {"name": "region", "type": "STRING", "mode": "NULLABLE"},
                {"name": "event_count", "type": "INTEGER", "mode": "NULLABLE"},
            ],
        },
        {"name": "triggered_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


def order_key(event: dict) -> tuple:
    payload = event.get("payload") or {}
    return (
        payload.get("channel", "unknown"),
        payload.get("region", "unknown"),
    )


class AggregateOrderWindow(beam.CombineFn):
    """Key: (channel, region) — reattached to the output row by the
    framework (see DomainSpec.key_field_names on ORDERS_DOMAIN below), not
    by this class. A CombineFn's methods never see the grouping key, so
    window/key fields can't be attached here even if we wanted to.

    Deliberately keyed by (channel, region) only, not event_type: keying by
    event_type too makes every cross-type rate — cancellation rate, cart
    abandonment rate — structurally either 0% or 100%, since a single
    window/key would then only ever contain one event type. Grouping every
    event type together per (channel, region) is what makes those rates
    meaningful.
    """

    def create_accumulator(self):
        return {
            "event_count": 0,
            "created_count": 0,
            "total_order_value": 0.0,
            "cancelled_count": 0,
            "refunded_count": 0,
            "refunded_amount": 0.0,
            "cart_added_count": 0,
            "cart_removed_count": 0,
            "cart_abandoned_count": 0,
        }

    def add_input(self, accumulator, event):
        event_type = event.get("event_type")
        order_total = (event.get("payload") or {}).get("order_total") or 0
        accumulator["event_count"] += 1
        if event_type == "order.created":
            accumulator["created_count"] += 1
            accumulator["total_order_value"] += order_total
        elif event_type == "order.cancelled":
            accumulator["cancelled_count"] += 1
        elif event_type == "order.refunded":
            accumulator["refunded_count"] += 1
            accumulator["refunded_amount"] += order_total
        elif event_type == "cart.item_added":
            accumulator["cart_added_count"] += 1
        elif event_type == "cart.item_removed":
            accumulator["cart_removed_count"] += 1
        elif event_type == "cart.abandoned":
            accumulator["cart_abandoned_count"] += 1
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            for key in merged:
                merged[key] += acc[key]
        return merged

    def extract_output(self, accumulator):
        return {**accumulator, "computed_at": datetime.now(timezone.utc).isoformat()}


def customer_key(event: dict) -> tuple:
    payload = event.get("payload") or {}
    return (payload.get("customer_id", "unknown"),)


class AggregateCustomer360(beam.CombineFn):
    """Key: (customer_id,) — reattached by the framework, same as
    AggregateOrderWindow. See CUSTOMER_360_DOMAIN below and the module
    docstring for why this is a second CombineFn over the same event stream
    ORDERS_DOMAIN already aggregates by (channel, region)."""

    def create_accumulator(self):
        return {
            "event_count": 0,
            "cart_added_count": 0,
            "cart_removed_count": 0,
            "cart_abandoned_count": 0,
            "created_count": 0,
            "total_spend": 0.0,
            "cancelled_count": 0,
            "refunded_count": 0,
            "refunded_amount": 0.0,
            "last_channel": None,
            "last_region": None,
        }

    def add_input(self, accumulator, event):
        event_type = event.get("event_type")
        payload = event.get("payload") or {}
        order_total = payload.get("order_total") or 0
        accumulator["event_count"] += 1
        accumulator["last_channel"] = payload.get("channel")
        accumulator["last_region"] = payload.get("region")
        if event_type == "cart.item_added":
            accumulator["cart_added_count"] += 1
        elif event_type == "cart.item_removed":
            accumulator["cart_removed_count"] += 1
        elif event_type == "cart.abandoned":
            accumulator["cart_abandoned_count"] += 1
        elif event_type == "order.created":
            accumulator["created_count"] += 1
            accumulator["total_spend"] += order_total
        elif event_type == "order.cancelled":
            accumulator["cancelled_count"] += 1
        elif event_type == "order.refunded":
            accumulator["refunded_count"] += 1
            accumulator["refunded_amount"] += order_total
            accumulator["total_spend"] -= order_total
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            for key in merged:
                if key in ("last_channel", "last_region"):
                    merged[key] = acc[key] or merged[key]
                else:
                    merged[key] += acc[key]
        return merged

    def extract_output(self, accumulator):
        return {**accumulator, "computed_at": datetime.now(timezone.utc).isoformat()}


def _alert(alert_type: str, severity: str, agg: dict[str, Any],
           metric_name: str, metric_value: float, threshold: float) -> dict[str, Any]:
    return {
        "alert_id": str(uuid.uuid4()),
        "alert_type": alert_type,
        "domain": "orders",
        "severity": severity,
        "window_start": agg.get("window_start"),
        "window_end": agg.get("window_end"),
        "metric_name": metric_name,
        "metric_value": metric_value,
        "threshold": threshold,
        "context": {
            "channel": agg.get("channel"),
            "region": agg.get("region"),
            "event_count": agg.get("event_count"),
        },
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }


def evaluate_order_alerts(agg: dict[str, Any]) -> list[dict[str, Any]]:
    alerts = []
    created = agg.get("created_count") or 0
    cancelled = agg.get("cancelled_count") or 0
    refunded = agg.get("refunded_amount") or 0.0
    cart_added = agg.get("cart_added_count") or 0
    cart_abandoned = agg.get("cart_abandoned_count") or 0

    if created > 0:
        rate = cancelled / created
        if rate >= CANCELLATION_RATE_THRESHOLD:
            alerts.append(_alert(
                "high_cancellation_rate", "high" if rate >= 0.30 else "medium",
                agg, "cancellation_rate", round(rate, 4), CANCELLATION_RATE_THRESHOLD,
            ))

    if refunded >= REFUND_SPIKE_THRESHOLD:
        alerts.append(_alert(
            "refund_spike", "high", agg, "refunded_amount", refunded, REFUND_SPIKE_THRESHOLD,
        ))

    if cart_added > 0:
        rate = cart_abandoned / cart_added
        if rate >= CART_ABANDONMENT_RATE_THRESHOLD:
            alerts.append(_alert(
                "high_cart_abandonment_rate", "high" if rate >= 0.75 else "medium",
                agg, "cart_abandonment_rate", round(rate, 4), CART_ABANDONMENT_RATE_THRESHOLD,
            ))

    return alerts


ORDERS_DOMAIN = DomainSpec(
    name="orders",
    topic="order-events",
    raw_table="raw.order_events",
    raw_table_schema=ORDER_EVENTS_SCHEMA,
    envelope_required=ENVELOPE_REQUIRED,
    payload_required=PAYLOAD_REQUIRED,
    dlq_table="raw.order_events_dlq",  # malformed/invalid events land here, not silently dropped
    enriched_table="enriched.order_summary_5min",
    enriched_table_schema=ORDER_SUMMARY_SCHEMA,
    key_fn=order_key,
    aggregate_fn=AggregateOrderWindow,
    key_field_names=("channel", "region"),
    alert_evaluator=evaluate_order_alerts,
)

# Second view over the same "order-events" topic — see the module
# docstring. raw_table is omitted (ORDERS_DOMAIN already persists every raw
# event; writing it twice would be pure duplication) and
# enforce_domain_match=False (the events' own "domain" field is "orders",
# not "customer_360" — see DomainSpec.enforce_domain_match's docstring).
# No dlq_table either: ORDERS_DOMAIN already captures DLQ for this same
# event stream, so a second copy of every validation failure would just be
# noise.
CUSTOMER_360_DOMAIN = DomainSpec(
    name="customer_360",
    topic="order-events",
    envelope_required=ENVELOPE_REQUIRED,
    payload_required=PAYLOAD_REQUIRED,
    enforce_domain_match=False,
    enriched_table="enriched.customer_360",
    enriched_table_schema=CUSTOMER_360_SCHEMA,
    key_fn=customer_key,
    aggregate_fn=AggregateCustomer360,
    key_field_names=("customer_id",),
)


def _build_incident_notifier():
    """
    Opts into ServiceNow incident creation on pipeline crash, only if the
    required env vars are set — so this example still runs without
    ServiceNow configured at all (notifier stays None, crash-hook is a
    no-op). See streaming_pipeline_framework.servicenow.ServiceNowClient
    and health.run_with_incident_on_failure for what this wires up.
    """
    import os

    if not os.environ.get("SERVICENOW_INSTANCE_URL"):
        return None

    from streaming_pipeline_framework.servicenow import ServiceNowClient

    return ServiceNowClient.from_env()


if __name__ == "__main__":
    cli_main(
        [ORDERS_DOMAIN, CUSTOMER_360_DOMAIN],
        alerts_table="raw.alerts",
        alerts_table_schema=ALERTS_SCHEMA,
        description="Retail order events pipeline (streaming-pipeline-framework example)",
        incident_notifier=_build_incident_notifier(),
    )
