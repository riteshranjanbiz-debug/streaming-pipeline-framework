# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A generic, domain-agnostic Apache Beam engine for the common shape of a near-real-time
ingestion pipeline:

```
Pub/Sub → parse → validate → enrich → write raw (BigQuery)
                                │
                                └─► tumbling window → aggregate
                                        → write enriched (BigQuery)
                                        → alert rules → write alerts (BigQuery)
```

Nothing in `framework.py` knows about any particular domain. Callers supply one or more
`DomainSpec` instances (topic, raw table, required fields, optional windowed aggregation +
alert evaluator), and `build_streaming_pipeline` wires the DAG for all of them.
`examples/retail_orders/` is the reference implementation of a second domain, proving the
framework doesn't secretly assume insurance-specific shape.

## Commands

```bash
pip install -e ".[dev]"        # pytest + requests + responses — enough to run the full test suite
pip install -e ".[gcp]"        # + apache-beam[gcp] — needed to actually run a pipeline
pip install -e ".[servicenow]" # + requests only — needed for ServiceNowClient

pytest                          # run everything
pytest tests/test_framework.py -v          # one file
pytest tests/test_framework.py::test_name -v  # one test
```

There is no lint/typecheck command configured in this repo (no ruff/mypy config wired into
CI) — `.mypy_cache` directories exist locally but aren't part of the checked-in tooling.

### CI has two parallel jobs — keep both green

`.github/workflows/ci.yml` runs the test suite **twice**: once with only `[dev]` installed
(`apache_beam` absent) and once with `[dev,gcp]` installed (`apache_beam` present). Each job
asserts `streaming_pipeline_framework.framework.BEAM_AVAILABLE` is `False`/`True`
respectively. Any change to `framework.py` must keep working in both states — see the Beam
shim note below. The beam-installed job needs `pip install "setuptools<81"` first since
apache-beam's legacy `setup.py` still depends on `pkg_resources`.

## Architecture

### The apache-beam-optional shim (`framework.py`)

`framework.py` opens with a `try: import apache_beam ... except ImportError:` block that
defines minimal stand-ins (`_DoFn`, `_TaggedOutput`, a fake `beam` namespace) when Beam isn't
installed, and sets `BEAM_AVAILABLE` accordingly. This is why the core package has **zero
required dependencies**: every `DoFn` subclass (`ParseMessage`, `ValidateEvent`,
`EnrichEvent`, `StripInternalFields`, `DetectAlerts`) can be unit-tested by calling
`.process()` directly without pulling in Beam at all. Only `build_streaming_pipeline` (which
actually constructs a `beam.Pipeline`) requires the real library. Keep this split in mind: if
you add a new DoFn, it must still import cleanly and be directly callable with Beam absent.

### `DomainSpec` is the entire extension point

A `DomainSpec` (dataclass, frozen) is one event stream: one Pub/Sub topic, one raw table,
optionally one windowed aggregation + one alert evaluator + one DLQ table + one dedup key.
`__post_init__` enforces two invariants at construction time rather than failing confusingly
mid-pipeline-build:
- `enriched_table`, `key_fn`, `aggregate_fn` must be all-set or all-omitted (partial
  aggregation config is a `ValueError`).
- `alert_evaluator` requires aggregation to also be configured.

`aggregate_fn` is a zero-arg **factory** (pass the class, not an instance) — the framework
calls it once per DAG wiring (`spec.aggregate_fn()` in `build_streaming_pipeline`).

### Pipeline wiring order (`build_streaming_pipeline`)

Per domain, in order: `Read` → `Parse` (bad JSON → `dlq` tag) → optional `Dedup`
(`DeduplicatePerKey`, keyed by `dedup_key_fn`, windowed by `dedup_window_secs`, applied
*before* validation so duplicates don't waste validate/enrich work) → `Validate` (missing
fields or domain mismatch → `dlq` tag) → `Enrich` (stamps `ingested_at` +
`_pipeline_version`) → `Strip` (drops underscore-prefixed internal fields) → `WriteRaw`. If
`enriched_table`/`key_fn`/`aggregate_fn` are set, the enriched (pre-strip) stream also
fans out into `Window` → `KV` → `Group` → `Agg` → `WriteAgg`, and if `alert_evaluator` is
also set, the aggregate stream fans out again into `Alerts` → `WriteAlerts`.

All DLQ-tagged outputs (from both `Parse` and `Validate`) flatten into a single
`spec.dlq_table` if one is configured — every DLQ row gets a consistent `_error` +
`ingested_at` shape via the internal `_dlq()` helper (see `framework.py`), which is what lets
`health.check_dlq_thresholds` query any domain's DLQ table uniformly.

Writes default to `write_method="STORAGE_WRITE_API"` (not legacy streaming inserts) — higher
throughput and, combined with `dedup_key_fn`, the practical way to get exactly-once rows out
of an at-least-once source like Pub/Sub.

### Health / incident creation (`health.py`, `servicenow.py`)

Two independent, opt-in triggers, decoupled from any specific notifier via the
`IncidentNotifier` protocol (anything with `.create_incident(short_description, ...)`):

1. **`run_with_incident_on_failure`** — wraps pipeline execution in `cli.main()`. Any
   uncaught exception creates an incident, then always re-raises (a notifier failure is
   logged but never masks the original pipeline error).
2. **`check_dlq_thresholds`** — a *separate*, periodic check (meant to run from Cloud
   Scheduler + Cloud Function/Run, or cron — deliberately not part of the streaming pipeline
   itself) that queries DLQ row volume per domain over a recent window. Catches pipelines
   that are alive but silently dropping traffic, which a crash-only trigger would miss.

`health.py` never imports `servicenow.py` — it only depends on the `IncidentNotifier`
protocol shape, so the core+health path stays dependency-free. `servicenow.py` is the only
module that requires `requests`, and is only imported when explicitly used — it is
deliberately **not** re-exported from `__init__.py` (see the comment there) to keep the
package's base import dependency-free. `ServiceNowClient` does OAuth client-credentials auth
against ServiceNow's REST Table API, caching the token and refreshing it 30s before expiry.

### `cli.py`

Thin argparse wrapper (`--project/--region/--runner/--temp-location
/--service-account-email`) that a downstream project's `main.py` calls with its own
`DomainSpec` list — see `examples/retail_orders/pipeline.py` for the full pattern. Wires
`build_streaming_pipeline` inside `run_with_incident_on_failure`.

`main()` also runs `_check_dataflow_worker_packaging` right after arg parsing: with
`--runner DataflowRunner`, `save_main_session=True` (set unconditionally) stages only
`__main__`'s state, never the `streaming_pipeline_framework` package itself — without
`--extra_package`/`--setup_file`/`--sdk_location`/`--sdk_container_image`, every worker
bundle fails with `ModuleNotFoundError` and the job sits at `RUNNING` with zero throughput
and no obvious error (confirmed live: took several debugging rounds to trace). This check
fails fast at submission instead. The fix in practice: `pip wheel . -w dist/ --no-deps`,
then pass `--extra_package dist/streaming_pipeline_framework-*.whl` — see either example's
"Deploy to Dataflow" docstring.

Separately, `STORAGE_WRITE_API` (the default write method) is a cross-language transform —
even submitting to `DataflowRunner` from a laptop requires a real local JRE to run Beam's
expansion service before staging. macOS ships a `java` stub that only prompts you to install
one; `brew install openjdk` (and putting it on `PATH`) is one way to get a real one.

## Testing conventions

- `tests/test_framework.py` uses a synthetic "widget" domain (not insurance or retail)
  deliberately, to keep tests honest about what's actually generic in the framework vs.
  domain-specific.
- `tests/test_servicenow.py` mocks all HTTP via `responses` — no real ServiceNow instance is
  ever contacted.
- `tests/test_health.py` uses a fake BigQuery client (anything with a `.query(sql).result()`
  shape) — `google-cloud-bigquery` is never a real dependency of this repo's tests.
- Beam-dependent behavior (actual pipeline execution, `DeduplicatePerKey`, windowing) is only
  exercised in the `test-with-beam` CI job; DoFn logic itself is tested via direct
  `.process()` calls, which works in both CI jobs.
