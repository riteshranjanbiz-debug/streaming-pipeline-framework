# streaming-pipeline-framework

A generic, domain-agnostic Apache Beam engine for the common shape of a
near-real-time ingestion pipeline:

```
Pub/Sub → parse → validate → enrich → write raw (BigQuery)
                                │
                                └─► tumbling window → aggregate
                                        → write enriched (BigQuery)
                                        → alert rules → write alerts (BigQuery)
```

Nothing in the framework knows about any particular domain. You supply one or
more `DomainSpec`s — each describing a topic, a raw table, required fields,
and optionally a windowed aggregation + alert evaluator — and
`build_streaming_pipeline` wires the DAG for all of them. Swap out the
`DomainSpec`s and you have a different pipeline: insurance events, retail
orders, IoT telemetry, whatever your event stream looks like.

This was extracted from a real insurance data pipeline
([SalesServiceHub](https://github.com/riteshranjanbiz-debug/SalesServiceHub))
where the same ingest→validate→enrich→window→alert shape was hand-duplicated
three times (once per domain). The framework is that shape, generalized;
`examples/retail_orders/` proves it works for a domain that has nothing to do
with insurance.

## Install

```bash
pip install -e .              # core: no dependencies, DoFns are unit-testable
pip install -e ".[gcp]"       # + apache-beam[gcp], needed to actually run a pipeline
pip install -e ".[servicenow]" # + requests, needed for ServiceNowClient
pip install -e ".[dev]"       # + pytest, requests, responses (for the test suite)
```

The core package has **zero required dependencies** — `framework.py` falls
back to a lightweight shim when `apache_beam` isn't installed, so you can
unit-test your `DoFn`s (by calling `.process()` directly) without pulling in
the full Beam/Dataflow stack. Install the `gcp` extra when you actually want
to run a pipeline.

## The `DomainSpec` contract

```python
from streaming_pipeline_framework import DomainSpec

orders = DomainSpec(
    name="orders",                              # step-name prefix + alert domain tag
    topic="order-events",                       # Pub/Sub topic (short name)
    raw_table="raw.order_events",                # "dataset.table"

    envelope_required=frozenset({"event_id", "event_type", "domain", "timestamp"}),
    payload_required=frozenset({"order_id", "channel", "region", "order_total"}),

    # Windowed aggregation — omit all three together to skip it entirely
    enriched_table="enriched.order_summary_5min",
    key_fn=lambda e: (e["event_type"], e["payload"]["channel"]),
    aggregate_fn=MyAggregateWindowDoFn,          # a class, not an instance

    # Alerting — optional, requires aggregation to also be set
    alert_evaluator=my_alert_rules,              # windowed-agg dict -> list[alert dict]
)
```

`DomainSpec.__post_init__` validates the combination at construction time
(partial aggregation config, or an alert evaluator without aggregation, both
raise `ValueError` immediately rather than failing confusingly at pipeline
build time).

## Quickstart

```python
from streaming_pipeline_framework.cli import main

main([orders], alerts_table="raw.alerts", description="Order events pipeline")
```

```bash
python my_pipeline.py --project <gcp-project> --runner DirectRunner
python my_pipeline.py --project <gcp-project> --runner DataflowRunner \
  --region us-central1 --temp-location gs://<bucket>/tmp \
  --service-account-email <sa>@<gcp-project>.iam.gserviceaccount.com
```

`cli.main()` handles the standard `--project/--region/--runner/--temp-location
/--service-account-email` argparse boilerplate and calls
`build_streaming_pipeline` for you. You can also call
`build_streaming_pipeline` directly if you want a different CLI shape.

**`DirectRunner` + `STORAGE_WRITE_API` (the default `write_method`) don't mix.**
`STORAGE_WRITE_API` is a cross-language (Java-backed) transform, and Beam's
streaming `DirectRunner` refuses to run cross-language pipelines at all — you'll
see `RuntimeError: Streaming Python direct runner does not support
cross-language pipelines`. For local testing, pass `write_method="STREAMING_INSERTS"`
to `main()`/`build_streaming_pipeline()` instead; switch back to the default
once you're running on `DataflowRunner`, which handles cross-language
transforms itself.

## Example: retail order events

`examples/retail_orders/pipeline.py` is a complete, runnable second domain —
order events with a 5-minute windowed aggregation (order value, cancellation
count, refund total) and two alert rules (cancellation-rate spike, refund
spike). It shares zero code with any insurance concept; it only depends on
the framework's generic engine. Read it alongside `framework.py`'s
`DomainSpec` docstring as the reference for plugging in your own domain.

Run it (needs real Pub/Sub + BigQuery, or read the file to see the wiring
without running anything):

```bash
python -m examples.retail_orders.pipeline --project <gcp-project> --runner DirectRunner
```

For `DataflowRunner`, `examples/retail_orders/terraform/` provisions everything
the example needs on GCP — the Pub/Sub topic, the `raw`/`enriched` BigQuery
datasets and tables (including a schema for `raw.order_events_dlq`), a Dataflow
worker service account with the right IAM roles, and the GCS temp bucket:

```bash
cd examples/retail_orders/terraform
terraform init
terraform apply -var="project_id=<gcp-project>"
terraform output run_command   # ready-to-run DataflowRunner invocation
```

When you're done, tear it back down (a `Makefile` wraps the commands above,
including this one — `make destroy PROJECT_ID=<gcp-project>`):

```bash
terraform destroy -var="project_id=<gcp-project>"
```

This deletes everything the module created but leaves the GCP project itself
intact. Note it does **not** stop a Dataflow job you started separately —
cancel that first (`gcloud dataflow jobs cancel <job-id>` or the Dataflow
console), since that's the actual compute cost driver, not the supporting
infrastructure.

## Incident creation on pipeline failure

Two independent, opt-in triggers, both in `health.py`, both accepting any
notifier with a `.create_incident(short_description, ...)` method — the
built-in `ServiceNowClient`, or your own:

**1. Pipeline crash.** Pass `incident_notifier=` to `cli.main()`:

```python
from streaming_pipeline_framework.cli import main
from streaming_pipeline_framework.servicenow import ServiceNowClient

main(
    [orders],
    alerts_table="raw.alerts",
    incident_notifier=ServiceNowClient.from_env(),  # reads SERVICENOW_* env vars
)
```

Any uncaught exception during pipeline execution — a `DirectRunner` crash, or
a Dataflow job that reaches `FAILED` while the submitting process is blocked
in `wait_until_finish()` — creates an incident with the error and full
traceback, then re-raises (the process still exits non-zero as before; a
failure to create the incident itself is logged, never masks the original
error). `ServiceNowClient.from_env()` reads `SERVICENOW_INSTANCE_URL`,
`SERVICENOW_CLIENT_ID`, `SERVICENOW_CLIENT_SECRET` — an OAuth
client-credentials app registered on the ServiceNow side, credentials never
hardcoded. Requires `pip install ".[servicenow]"`.

**2. DLQ volume.** A crash alone won't catch a pipeline that's alive and
running but silently dropping a chunk of its traffic (bad upstream data, a
schema drift). Set `dlq_table` on a `DomainSpec` so invalid/malformed events
are persisted instead of discarded, then run `check_dlq_thresholds`
periodically (Cloud Scheduler + Cloud Function/Run, or plain cron — this is
deliberately *not* part of the streaming pipeline itself):

```python
from google.cloud import bigquery
from streaming_pipeline_framework.health import check_dlq_thresholds

check_dlq_thresholds(
    bigquery.Client(project="my-project"), "my-project", [orders],
    window_minutes=15, threshold=10, notifier=ServiceNowClient.from_env(),
)
```

See `examples/retail_orders/check_health.py` for a complete, runnable
version of this.

## What the framework does *not* do

- **Aggregation math and alert thresholds are yours.** `AggregateOrderWindow`
  and `evaluate_order_alerts` in the example are business logic — the
  framework only wires them into the pipeline DAG at the right point.
- **No infrastructure provisioning.** Bring your own Terraform/topics/tables;
  the framework assumes the Pub/Sub topic and BigQuery tables already exist.
- **No serving layer.** Pair this with whatever API/dashboard framework you
  like on the BigQuery output side — SalesServiceHub uses FastAPI + a static
  dashboard, but that's a separate concern from this package.
- **No incident lifecycle management.** `create_incident` only creates —
  resolution, assignment routing, dedup/throttling of repeated alerts, and
  escalation policy all stay on the ServiceNow side.

## Testing

```bash
pip install -e ".[dev]"
pytest
```

- `tests/test_framework.py` — every generic `DoFn` (`ParseMessage`,
  `ValidateEvent`, `EnrichEvent`, `StripInternalFields`, `DetectAlerts`) and
  `DomainSpec`'s validation, using a synthetic "widget" domain — deliberately
  not insurance or retail, to keep the tests honest about what's actually
  generic.
- `tests/test_servicenow.py` — the OAuth token flow (fetch, cache, refresh
  on expiry) and incident creation, fully mocked via `responses`; no real
  ServiceNow instance is contacted.
- `tests/test_health.py` — the crash-incident hook (including that a
  notifier failure never masks the original pipeline error) and the DLQ
  threshold check, against a fake BigQuery client.
