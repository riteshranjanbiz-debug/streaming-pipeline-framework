"""
Example: QuoteFlow insurance quote-application funnel on the
streaming-pipeline-framework, with cart-style abandonment inferred from
inactivity — reusing exactly the pattern built for examples/retail_orders/
(see InactivityDetector in framework.py), applied to a real insurance
domain instead of e-commerce.

Journey: pre-quote:initiated -> pre-quote:person-add/-update ->
pre-quote:address-add/-update/-delete -> pre-quote:confirmed ->
quote:quoted -> quote:recalculate (optional, repeatable) ->
quote:payment-initiated -> post-quote:bind.

`pre-quote:confirmed` and `quote:payment-initiated` are placeholder event
names — the real system's names for "personal/address info locked" and
"customer moved to the payment page" weren't available when this example
was written. Update ENVELOPE_REQUIRED/PAYLOAD_REQUIRED usage and the
reducer in QUOTE_INACTIVITY_DETECTOR if your actual event names differ;
nothing else in this file depends on the exact strings.

No event for "application abandoned" exists in the real system, same as
retail_orders' cart.abandoned — it's inferred. QUOTE_INACTIVITY_DETECTOR
watches each quoteId; 20 minutes of silence on an unbound quote
synthesizes a quote:abandoned event carrying a classified scenario
(the "furthest stage reached" — see _quote_state_reducer), and — as a
best-effort side effect, never blocking the synthetic event itself — fires
one Day-0 transactional email via SFMC (see sfmc.py). Scope boundary: this
pipeline fires exactly one triggered send per abandonment. Any Day 1/2/3/
14/30 follow-up cadence, consent enforcement, and A/B testing are SFMC's
job (Journey Builder / Marketing Cloud), not this pipeline's — see the
capability discussion this example came out of. Re-engagement is handled
"for free": if a customer returns and progresses further before
abandoning again, the reducer's state naturally reflects the new, higher
scenario, and the next 20-minute timeout fires a fresh, correctly
reclassified event.

Two DomainSpecs consume the same "quote-events" topic, same as
retail_orders' ORDERS_DOMAIN/CUSTOMER_360_DOMAIN split: QUOTES_DOMAIN (the
operational per-product-line funnel view) and APPLICANT_360_DOMAIN (a
per-customer view keyed by mdmId). See DomainSpec.raw_table and
.enforce_domain_match's docstrings in framework.py for why
APPLICANT_360_DOMAIN sets raw_table=None and enforce_domain_match=False.

Run locally (DirectRunner, needs real Pub/Sub + BigQuery):
  python -m examples.insurance_quotes.pipeline --project <gcp-project> --runner DirectRunner

Deploy to Dataflow:
  python -m examples.insurance_quotes.pipeline \\
    --project <gcp-project> --region us-central1 --runner DataflowRunner \\
    --temp-location gs://<bucket>/tmp \\
    --service-account-email <dataflow-sa>@<gcp-project>.iam.gserviceaccount.com
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from streaming_pipeline_framework import DomainSpec, InactivityDetector, beam
from streaming_pipeline_framework.cli import main as cli_main

logger = logging.getLogger(__name__)

PRODUCT_TYPES = frozenset({"property", "auto", "bundled"})

ENVELOPE_REQUIRED = frozenset(
    {"event_id", "event_type", "source_system", "domain", "public_id", "timestamp"}
)
PAYLOAD_REQUIRED = frozenset({"quote_id", "session_id", "trace_id", "mdm_id", "product_type"})

# 20 minutes of real inactivity. Overridable for a short-lived test run —
# see examples/retail_orders/pipeline.py's CART_ABANDONMENT_TIMEOUT_SECS
# for why (processing-time timers only fire against real wall-clock time).
QUOTE_ABANDONMENT_TIMEOUT_SECS = int(os.environ.get("QUOTE_ABANDONMENT_TIMEOUT_SECS", 1200))

SCENARIO_3_ABANDONMENT_RATE_THRESHOLD = 0.30  # of those who reached payment, fraction who didn't bind

# BigQuery schemas — required because write_method defaults to
# STORAGE_WRITE_API, which needs field types up front; it can't infer them
# from an already-existing table. `payload` is a wide, mostly-NULLABLE
# RECORD because different event types carry different optional fields
# (person details, address, premium/coverage, abandonment scenario) — the
# 5 PAYLOAD_REQUIRED fields are the only ones every event guarantees.
QUOTE_EVENTS_SCHEMA = {
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
                {"name": "quote_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "trace_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "mdm_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "product_type", "type": "STRING", "mode": "REQUIRED"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "email", "type": "STRING", "mode": "NULLABLE"},
                {"name": "address_line", "type": "STRING", "mode": "NULLABLE"},
                {"name": "city", "type": "STRING", "mode": "NULLABLE"},
                {"name": "state", "type": "STRING", "mode": "NULLABLE"},
                {"name": "zip", "type": "STRING", "mode": "NULLABLE"},
                {"name": "premium", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "coverage_summary", "type": "STRING", "mode": "NULLABLE"},
                {"name": "scenario", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# enriched.quote_funnel_5min — one row per product_type per 5-minute
# window: funnel counts + abandonment-by-scenario, written by QUOTES_DOMAIN.
QUOTE_FUNNEL_SCHEMA = {
    "fields": [
        {"name": "event_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "initiated_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "quoted_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "recalculate_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "bound_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "abandoned_scenario_1_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "abandoned_scenario_2_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "abandoned_scenario_3_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "computed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "product_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# enriched.applicant_360 — one row per mdmId per 5-minute window, written
# by APPLICANT_360_DOMAIN. Same "windowed, not lifetime" caveat as
# retail_orders' CUSTOMER_360_SCHEMA — see that file's module docstring.
APPLICANT_360_SCHEMA = {
    "fields": [
        {"name": "mdm_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "quote_attempts", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "quoted_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "bound_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "abandoned_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "last_product_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "computed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

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
                {"name": "product_type", "type": "STRING", "mode": "NULLABLE"},
                {"name": "bound_count", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "abandoned_scenario_3_count", "type": "INTEGER", "mode": "NULLABLE"},
            ],
        },
        {"name": "triggered_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


def product_key(event: dict) -> tuple:
    payload = event.get("payload") or {}
    return (payload.get("product_type", "unknown"),)


class AggregateQuoteFunnel(beam.CombineFn):
    """Key: (product_type,) — reattached by the framework. Funnel +
    abandonment-by-scenario counts per product line per window."""

    def create_accumulator(self):
        return {
            "event_count": 0,
            "initiated_count": 0,
            "quoted_count": 0,
            "recalculate_count": 0,
            "bound_count": 0,
            "abandoned_scenario_1_count": 0,
            "abandoned_scenario_2_count": 0,
            "abandoned_scenario_3_count": 0,
        }

    def add_input(self, accumulator, event):
        event_type = event.get("event_type")
        payload = event.get("payload") or {}
        accumulator["event_count"] += 1
        if event_type == "pre-quote:initiated":
            accumulator["initiated_count"] += 1
        elif event_type == "quote:quoted":
            accumulator["quoted_count"] += 1
        elif event_type == "quote:recalculate":
            accumulator["recalculate_count"] += 1
        elif event_type == "post-quote:bind":
            accumulator["bound_count"] += 1
        elif event_type == "quote:abandoned":
            scenario = payload.get("scenario")
            key = f"abandoned_{scenario}_count"
            if key in accumulator:
                accumulator[key] += 1
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            for key in merged:
                merged[key] += acc[key]
        return merged

    def extract_output(self, accumulator):
        return {**accumulator, "computed_at": datetime.now(timezone.utc).isoformat()}


def applicant_key(event: dict) -> tuple:
    payload = event.get("payload") or {}
    return (payload.get("mdm_id", "unknown"),)


class AggregateApplicant360(beam.CombineFn):
    """Key: (mdm_id,) — reattached by the framework. Per-applicant activity
    within the current window; see the module docstring's "windowed, not
    lifetime" caveat."""

    def create_accumulator(self):
        return {
            "event_count": 0,
            "quote_attempts": 0,
            "quoted_count": 0,
            "bound_count": 0,
            "abandoned_count": 0,
            "last_product_type": None,
        }

    def add_input(self, accumulator, event):
        event_type = event.get("event_type")
        payload = event.get("payload") or {}
        accumulator["event_count"] += 1
        accumulator["last_product_type"] = payload.get("product_type") or accumulator["last_product_type"]
        if event_type == "pre-quote:initiated":
            accumulator["quote_attempts"] += 1
        elif event_type == "quote:quoted":
            accumulator["quoted_count"] += 1
        elif event_type == "post-quote:bind":
            accumulator["bound_count"] += 1
        elif event_type == "quote:abandoned":
            accumulator["abandoned_count"] += 1
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            for key in merged:
                if key == "last_product_type":
                    merged[key] = acc[key] or merged[key]
                else:
                    merged[key] += acc[key]
        return merged

    def extract_output(self, accumulator):
        return {**accumulator, "computed_at": datetime.now(timezone.utc).isoformat()}


# ── Abandonment inference + SFMC trigger ────────────────────────────────────

_STAGE_RANK = {"pre_quote": 0, "quoted": 1, "payment": 2}
_STAGE_TO_SCENARIO = {"pre_quote": "scenario_1", "quoted": "scenario_2", "payment": "scenario_3"}

# A marketer builds these Send Definitions in SFMC Content Builder; this
# pipeline only ever references them by key. See sfmc.py's module docstring
# for the scope boundary (one Day-0 send, not a multi-day cadence).
SFMC_SEND_DEFINITION_KEYS = {
    "scenario_1": "quote-abandon-scenario-1-day0",
    "scenario_2": "quote-abandon-scenario-2-day0",
    "scenario_3": "quote-abandon-scenario-3-day0",
}


def _quote_state_reducer(state: dict | None, event: dict) -> dict:
    """Tracks the furthest funnel stage reached + bound status per quoteId.
    Must return a new dict, not mutate `state` in place (InactivityDetector
    docstring). Stage advancement is monotonic — a customer can't "un-reach"
    a stage — which is exactly what makes re-engagement/path-switching
    (Feature 8 in the capability doc) fall out for free: if they return and
    progress further, the next abandonment is classified at the new,
    higher stage without any extra logic."""
    state = dict(state) if state else {
        "stage": "pre_quote", "bound": False, "mdm_id": None, "product_type": None, "premium": None,
    }
    payload = event.get("payload") or {}
    state["mdm_id"] = payload.get("mdm_id") or state["mdm_id"]
    state["product_type"] = payload.get("product_type") or state["product_type"]

    event_type = event.get("event_type")
    new_stage = None
    if event_type in ("quote:quoted", "quote:recalculate"):
        state["premium"] = payload.get("premium") or state["premium"]
        new_stage = "quoted"
    elif event_type == "quote:payment-initiated":
        new_stage = "payment"
    elif event_type == "post-quote:bind":
        state["bound"] = True

    if new_stage and _STAGE_RANK[new_stage] > _STAGE_RANK[state["stage"]]:
        state["stage"] = new_stage

    return state


def _quote_is_pending(state: dict | None) -> bool:
    return bool(state) and not state.get("bound", False)


def _build_sfmc_client():
    """Opts into SFMC triggering only if the required env vars are set —
    this example still runs without SFMC configured at all (client stays
    None, trigger is a no-op). See servicenow.py's analogous pattern in
    examples/retail_orders/pipeline.py's _build_incident_notifier."""
    if not os.environ.get("SFMC_SUBDOMAIN"):
        return None

    from streaming_pipeline_framework.sfmc import SFMCClient

    return SFMCClient.from_env()


_SFMC_CLIENT = _build_sfmc_client()


def _trigger_sfmc_abandonment_email(scenario: str, state: dict, event: dict) -> None:
    """Best-effort side effect — never raises, so a failed or unconfigured
    SFMC integration can never prevent the synthetic quote:abandoned event
    itself from being emitted and written. Runs inside a Beam timer
    callback (see QUOTE_INACTIVITY_DETECTOR / _InactivityWatcher), so this
    executes on Dataflow workers, not the submitting process — the
    _SFMC_CLIENT instance (built once locally, where env vars are set) gets
    shipped to workers via save_main_session, the same way DomainSpec's
    other callables already do."""
    if _SFMC_CLIENT is None:
        return
    send_key = SFMC_SEND_DEFINITION_KEYS.get(scenario)
    if not send_key:
        return
    try:
        _SFMC_CLIENT.send_transactional_email(
            send_definition_key=send_key,
            contact_key=state.get("mdm_id") or "unknown",
            attributes={
                "quoteId": event["payload"]["quote_id"],
                "productType": state.get("product_type"),
                "premium": state.get("premium"),
            },
        )
    except Exception:
        logger.exception(
            "Failed to trigger SFMC Day-0 email for mdm_id=%s scenario=%s",
            state.get("mdm_id"), scenario,
        )


def _quote_abandoned_event(key: Any, state: dict) -> dict:
    quote_id = key[0] if isinstance(key, tuple) else key
    scenario = _STAGE_TO_SCENARIO[state["stage"]]
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "quote:abandoned",
        "source_system": "inactivity-detector",
        "domain": "quotes",
        "public_id": f"PUB-{uuid.uuid4().hex[:8]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "quote_id": quote_id,
            "session_id": "unknown",  # not tracked by the reducer's state
            "trace_id": str(uuid.uuid4()),
            "mdm_id": state.get("mdm_id") or "unknown",
            "product_type": state.get("product_type") or "unknown",
            "premium": state.get("premium"),
            "scenario": scenario,
        },
    }
    _trigger_sfmc_abandonment_email(scenario, state, event)
    return event


QUOTE_INACTIVITY_DETECTOR = InactivityDetector(
    key_fn=lambda e: (e.get("payload") or {}).get("quote_id", "unknown"),
    timeout_secs=QUOTE_ABANDONMENT_TIMEOUT_SECS,
    reducer_fn=_quote_state_reducer,
    should_fire_fn=_quote_is_pending,
    timeout_event_fn=_quote_abandoned_event,
    initial_state_fn=lambda: {
        "stage": "pre_quote", "bound": False, "mdm_id": None, "product_type": None, "premium": None,
    },
)


# ── Alerts ───────────────────────────────────────────────────────────────

def _alert(alert_type: str, severity: str, agg: dict[str, Any],
           metric_name: str, metric_value: float, threshold: float) -> dict[str, Any]:
    return {
        "alert_id": str(uuid.uuid4()),
        "alert_type": alert_type,
        "domain": "quotes",
        "severity": severity,
        "window_start": agg.get("window_start"),
        "window_end": agg.get("window_end"),
        "metric_name": metric_name,
        "metric_value": metric_value,
        "threshold": threshold,
        "context": {
            "product_type": agg.get("product_type"),
            "bound_count": agg.get("bound_count"),
            "abandoned_scenario_3_count": agg.get("abandoned_scenario_3_count"),
        },
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    }


def evaluate_quote_alerts(agg: dict[str, Any]) -> list[dict[str, Any]]:
    """High near-miss abandonment: of applicants who reached payment
    (bound + abandoned at scenario 3), what fraction didn't bind. This is
    the highest-value drop-off — closest to a sale — so it's worth its own
    alert distinct from the overall funnel counts."""
    alerts = []
    bound = agg.get("bound_count") or 0
    abandoned_3 = agg.get("abandoned_scenario_3_count") or 0
    near_miss_total = bound + abandoned_3

    if near_miss_total > 0:
        rate = abandoned_3 / near_miss_total
        if rate >= SCENARIO_3_ABANDONMENT_RATE_THRESHOLD:
            alerts.append(_alert(
                "high_near_miss_abandonment", "high" if rate >= 0.5 else "medium",
                agg, "scenario_3_abandonment_rate", round(rate, 4), SCENARIO_3_ABANDONMENT_RATE_THRESHOLD,
            ))

    return alerts


# ── Domains ──────────────────────────────────────────────────────────────

QUOTES_DOMAIN = DomainSpec(
    name="quotes",
    topic="quote-events",
    raw_table="raw.quote_events",
    raw_table_schema=QUOTE_EVENTS_SCHEMA,
    envelope_required=ENVELOPE_REQUIRED,
    payload_required=PAYLOAD_REQUIRED,
    inactivity_detector=QUOTE_INACTIVITY_DETECTOR,
    dlq_table="raw.quote_events_dlq",  # malformed/invalid events land here, not silently dropped
    enriched_table="enriched.quote_funnel_5min",
    enriched_table_schema=QUOTE_FUNNEL_SCHEMA,
    key_fn=product_key,
    aggregate_fn=AggregateQuoteFunnel,
    key_field_names=("product_type",),
    alert_evaluator=evaluate_quote_alerts,
)

# Second view over the same "quote-events" topic — see the module
# docstring. raw_table is omitted (QUOTES_DOMAIN already persists every raw
# event) and enforce_domain_match=False (the events' own "domain" field is
# "quotes", not "applicant_360").
APPLICANT_360_DOMAIN = DomainSpec(
    name="applicant_360",
    topic="quote-events",
    envelope_required=ENVELOPE_REQUIRED,
    payload_required=PAYLOAD_REQUIRED,
    enforce_domain_match=False,
    inactivity_detector=QUOTE_INACTIVITY_DETECTOR,
    enriched_table="enriched.applicant_360",
    enriched_table_schema=APPLICANT_360_SCHEMA,
    key_fn=applicant_key,
    aggregate_fn=AggregateApplicant360,
    key_field_names=("mdm_id",),
)


def _build_incident_notifier():
    """Opts into ServiceNow incident creation on pipeline crash, only if
    the required env vars are set. See examples/retail_orders/pipeline.py
    for the identical pattern."""
    if not os.environ.get("SERVICENOW_INSTANCE_URL"):
        return None

    from streaming_pipeline_framework.servicenow import ServiceNowClient

    return ServiceNowClient.from_env()


if __name__ == "__main__":
    cli_main(
        [QUOTES_DOMAIN, APPLICANT_360_DOMAIN],
        alerts_table="raw.quote_alerts",
        alerts_table_schema=ALERTS_SCHEMA,
        description="QuoteFlow quote abandonment pipeline (streaming-pipeline-framework example)",
        incident_notifier=_build_incident_notifier(),
    )
