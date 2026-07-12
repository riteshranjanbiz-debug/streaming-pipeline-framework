"""
Example: e-commerce order events on the streaming-pipeline-framework.

Proves the framework is domain-agnostic — this has nothing to do with the
insurance use case the framework was extracted from. Swap in your own
DomainSpec, aggregator, and alert rules the same way.

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
PAYLOAD_REQUIRED = frozenset({"order_id", "channel", "region", "order_total"})

CANCELLATION_RATE_THRESHOLD = 0.15
REFUND_SPIKE_THRESHOLD = 5_000.0


def order_key(event: dict) -> tuple:
    payload = event.get("payload") or {}
    return (
        event.get("event_type", "unknown"),
        payload.get("channel", "unknown"),
        payload.get("region", "unknown"),
    )


class AggregateOrderWindow(beam.DoFn):
    """Key: (event_type, channel, region). Output: one row per window/key."""

    def process(self, element, window=beam.DoFn.WindowParam):
        key, events = element
        event_type, channel, region = key
        events = list(events)

        total_value = sum((e.get("payload") or {}).get("order_total") or 0 for e in events)
        cancelled = sum(1 for e in events if e.get("event_type") == "order.cancelled")
        refunded = sum(
            (e.get("payload") or {}).get("order_total") or 0
            for e in events
            if e.get("event_type") == "order.refunded"
        )

        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "event_type": event_type,
            "channel": channel,
            "region": region,
            "event_count": len(events),
            "total_order_value": total_value,
            "cancelled_count": cancelled,
            "refunded_amount": refunded,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }


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
            "event_type": agg.get("event_type"),
            "channel": agg.get("channel"),
            "region": agg.get("region"),
            "event_count": agg.get("event_count"),
        },
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }


def evaluate_order_alerts(agg: dict[str, Any]) -> list[dict[str, Any]]:
    alerts = []
    event_count = agg.get("event_count") or 0
    cancelled = agg.get("cancelled_count") or 0
    refunded = agg.get("refunded_amount") or 0.0

    if event_count > 0:
        rate = cancelled / event_count
        if rate >= CANCELLATION_RATE_THRESHOLD:
            alerts.append(_alert(
                "high_cancellation_rate", "high" if rate >= 0.30 else "medium",
                agg, "cancellation_rate", round(rate, 4), CANCELLATION_RATE_THRESHOLD,
            ))

    if refunded >= REFUND_SPIKE_THRESHOLD:
        alerts.append(_alert(
            "refund_spike", "high", agg, "refunded_amount", refunded, REFUND_SPIKE_THRESHOLD,
        ))

    return alerts


ORDERS_DOMAIN = DomainSpec(
    name="orders",
    topic="order-events",
    raw_table="raw.order_events",
    envelope_required=ENVELOPE_REQUIRED,
    payload_required=PAYLOAD_REQUIRED,
    enriched_table="enriched.order_summary_5min",
    key_fn=order_key,
    aggregate_fn=AggregateOrderWindow,
    alert_evaluator=evaluate_order_alerts,
)


if __name__ == "__main__":
    cli_main(
        [ORDERS_DOMAIN],
        alerts_table="raw.alerts",
        description="Retail order events pipeline (streaming-pipeline-framework example)",
    )
