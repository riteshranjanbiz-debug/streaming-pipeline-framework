"""
Simulates N concurrent insurance applicants, each a persistent mdmId moving
through the EDSP quote funnel over time — not independent random events:

  NEW -> pre-quote:initiated -> PRE_QUOTE (person-add, address-add, confirmed)
  PRE_QUOTE -> quote:quoted -> QUOTED
  QUOTED -> quote:recalculate (optional, repeatable) -> quote:payment-initiated -> PAYMENT
  PAYMENT -> post-quote:bind -> BOUND
  (any of PRE_QUOTE / QUOTED / PAYMENT) -> stop acting -> STALLED

Deliberately does NOT publish a "quote:abandoned" event — no real EDSP
integration ever fires one. An applicant who "abandons" just stops acting
(STALLED, no event) for a while; the pipeline's QUOTE_INACTIVITY_DETECTOR
is what infers abandonment from that silence and classifies which scenario
(see pipeline.py's module docstring).

Every event a given applicant publishes during one attempt carries the
same quoteId/mdmId/productType, so enriched.applicant_360 (keyed by
mdmId) and enriched.quote_funnel_5min (keyed by productType) both
aggregate a believable, causally-connected stream.

Needs google-cloud-pubsub, already pulled in by the `gcp` extra
(`pip install streaming-pipeline-framework[gcp]`) that running the
pipeline itself requires.

Usage:
  python -m examples.edsp_quotes.simulate_traffic \\
    --project <gcp-project> --applicants 1000 --duration 60

  # To actually observe an inferred quote:abandoned + the SFMC trigger,
  # run the pipeline with a short QUOTE_ABANDONMENT_TIMEOUT_SECS (see
  # pipeline.py) and pass a matching --abandon-quiet-secs here, comfortably
  # longer than the pipeline's timeout:
  #   QUOTE_ABANDONMENT_TIMEOUT_SECS=20 python -m examples.edsp_quotes.pipeline ...
  #   python -m examples.edsp_quotes.simulate_traffic --abandon-quiet-secs 30 ...
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

PRODUCT_TYPES = ["property", "auto", "bundled"]

PRE_QUOTE_STEPS = ["pre-quote:person-add", "pre-quote:address-add", "pre-quote:confirmed"]

# Per-tick stall (abandon) probabilities at each stage.
P_STALL_PRE_QUOTE_STEP = 0.08     # checked before each of the 3 pre-quote steps
P_STALL_AFTER_QUOTED = 0.15       # checked after seeing the quote/coverage
P_RECALCULATE = 0.30              # chance to tweak coverage instead of moving to payment
P_STALL_AT_PAYMENT = 0.20         # checked at the payment page


@dataclass
class Applicant:
    mdm_id: str
    state: str = "NEW"  # NEW | PRE_QUOTE | AWAITING_QUOTE | QUOTED | PAYMENT | STALLED | BOUND
    quote_id: str = ""
    session_id: str = ""
    product_type: str = ""
    premium: float = 0.0
    pre_quote_step: int = 0
    next_action_at: float = field(default_factory=lambda: time.monotonic() + random.uniform(0, 2))


def _envelope(event_type: str, applicant: Applicant, extra_payload: dict | None = None) -> dict:
    payload = {
        "quote_id": applicant.quote_id,
        "session_id": applicant.session_id,
        "trace_id": str(uuid.uuid4()),
        "mdm_id": applicant.mdm_id,
        "product_type": applicant.product_type,
    }
    payload.update(extra_payload or {})
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source_system": "edsp-simulator",
        "domain": "quotes",
        "public_id": f"PUB-{uuid.uuid4().hex[:8]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def _stall(applicant: Applicant, abandon_quiet_secs: float) -> None:
    applicant.state = "STALLED"
    applicant.next_action_at = time.monotonic() + abandon_quiet_secs


def step(applicant: Applicant, min_interval: float, max_interval: float, abandon_quiet_secs: float) -> dict | None:
    """Advances one applicant by one action and returns the event to
    publish, or None if this tick doesn't produce one (going quiet, or
    coming back from being quiet)."""
    event = None

    if applicant.state == "NEW":
        applicant.quote_id = f"QT-{uuid.uuid4().hex[:10]}"
        applicant.session_id = f"SESS-{uuid.uuid4().hex[:10]}"
        applicant.product_type = random.choice(PRODUCT_TYPES)
        applicant.pre_quote_step = 0
        applicant.state = "PRE_QUOTE"
        event = _envelope("pre-quote:initiated", applicant)

    elif applicant.state == "PRE_QUOTE":
        if random.random() < P_STALL_PRE_QUOTE_STEP:
            _stall(applicant, abandon_quiet_secs)
            return None
        step_name = PRE_QUOTE_STEPS[applicant.pre_quote_step]
        extra = {}
        if step_name == "pre-quote:person-add":
            extra = {"name": f"Applicant {applicant.mdm_id}", "email": f"{applicant.mdm_id.lower()}@example.com"}
        elif step_name == "pre-quote:address-add":
            extra = {"address_line": "123 Main St", "city": "Springfield", "state": "IL", "zip": "62704"}
        event = _envelope(step_name, applicant, extra)
        applicant.pre_quote_step += 1
        if applicant.pre_quote_step >= len(PRE_QUOTE_STEPS):
            applicant.state = "AWAITING_QUOTE"

    elif applicant.state == "AWAITING_QUOTE":
        applicant.premium = round(random.uniform(50, 400), 2)
        event = _envelope("quote:quoted", applicant, {
            "premium": applicant.premium,
            "coverage_summary": f"{applicant.product_type} standard coverage",
        })
        applicant.state = "QUOTED"

    elif applicant.state == "QUOTED":
        if random.random() < P_STALL_AFTER_QUOTED:
            _stall(applicant, abandon_quiet_secs)
            return None
        if random.random() < P_RECALCULATE:
            applicant.premium = round(applicant.premium * random.uniform(0.85, 1.15), 2)
            event = _envelope("quote:recalculate", applicant, {
                "premium": applicant.premium,
                "coverage_summary": f"{applicant.product_type} adjusted coverage",
            })
        else:
            event = _envelope("quote:payment-initiated", applicant, {"premium": applicant.premium})
            applicant.state = "PAYMENT"

    elif applicant.state == "PAYMENT":
        if random.random() < P_STALL_AT_PAYMENT:
            _stall(applicant, abandon_quiet_secs)
            return None
        event = _envelope("post-quote:bind", applicant, {"premium": applicant.premium})
        applicant.state = "BOUND"

    elif applicant.state == "STALLED":
        # Quiet period is over; start a brand-new application (fresh
        # quoteId, same mdmId) on the next tick.
        applicant.state = "NEW"

    elif applicant.state == "BOUND":
        # Simulate them eventually shopping for another product.
        applicant.state = "NEW"

    applicant.next_action_at = time.monotonic() + random.uniform(min_interval, max_interval)
    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--topic", default="quote-events")
    parser.add_argument("--applicants", type=int, default=1000, help="concurrent applicants")
    parser.add_argument("--duration", type=int, default=60, help="seconds")
    parser.add_argument("--min-interval", type=float, default=1.0, help="min seconds between one applicant's actions")
    parser.add_argument("--max-interval", type=float, default=3.0, help="max seconds between one applicant's actions")
    parser.add_argument(
        "--abandon-quiet-secs", type=float, default=1500,
        help="how long a stalled applicant stays silent before starting a new application "
             "(default 1500s = 25min, comfortably longer than the pipeline's real 20min timeout; "
             "pass something shorter than a shortened QUOTE_ABANDONMENT_TIMEOUT_SECS to actually see it fire in a test run)",
    )
    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    applicants = [Applicant(mdm_id=f"MDM-{i:05d}") for i in range(args.applicants)]

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

    print(f"Simulating {args.applicants} concurrent applicants against {topic_path} for {args.duration}s...")
    start = time.monotonic()
    end = start + args.duration
    last_report = start

    while time.monotonic() < end:
        now = time.monotonic()
        for applicant in applicants:
            if applicant.next_action_at > now:
                continue
            event = step(applicant, args.min_interval, args.max_interval, args.abandon_quiet_secs)
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
