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


class AggregateOrderWindow(beam.CombineFn):
    """Key: (event_type, channel, region) — reattached to the output row by
    the framework (see DomainSpec.key_field_names on ORDERS_DOMAIN below), not
    by this class. A CombineFn's methods never see the grouping key, so
    window/key fields can't be attached here even if we wanted to."""

    def create_accumulator(self):
        return {
            "event_count": 0,
            "total_order_value": 0.0,
            "cancelled_count": 0,
            "refunded_amount": 0.0,
        }

    def add_input(self, accumulator, event):
        order_total = (event.get("payload") or {}).get("order_total") or 0
        accumulator["event_count"] += 1
        accumulator["total_order_value"] += order_total
        if event.get("event_type") == "order.cancelled":
            accumulator["cancelled_count"] += 1
        if event.get("event_type") == "order.refunded":
            accumulator["refunded_amount"] += order_total
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["event_count"] += acc["event_count"]
            merged["total_order_value"] += acc["total_order_value"]
            merged["cancelled_count"] += acc["cancelled_count"]
            merged["refunded_amount"] += acc["refunded_amount"]
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
    dlq_table="raw.order_events_dlq",  # malformed/invalid events land here, not silently dropped
    enriched_table="enriched.order_summary_5min",
    key_fn=order_key,
    aggregate_fn=AggregateOrderWindow,
    key_field_names=("event_type", "channel", "region"),
    alert_evaluator=evaluate_order_alerts,
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
        [ORDERS_DOMAIN],
        alerts_table="raw.alerts",
        description="Retail order events pipeline (streaming-pipeline-framework example)",
        incident_notifier=_build_incident_notifier(),
    )
