"""
streaming-pipeline-framework — a generic, domain-agnostic engine for the
common shape of a near-real-time ingestion pipeline:

    Pub/Sub → parse → validate → enrich → write raw (BigQuery)
                                    │
                                    └─► tumbling window → aggregate
                                            → write enriched (BigQuery)
                                            → alert rules → write alerts (BigQuery)

Nothing in this module knows about any particular domain (insurance, retail,
logistics, ...). A caller supplies one or more `DomainSpec` instances — each
describing a topic, a BigQuery raw table, required fields, and optionally a
windowed aggregation + alert evaluator — and `build_streaming_pipeline` wires
the DAG for all of them.

Works with or without apache-beam installed: the DoFns can be unit-tested by
calling `.process()` directly (see tests/test_framework.py), which is useful
in CI environments that don't want the full Beam/Dataflow dependency.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

try:
    import apache_beam as beam
    from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
    from apache_beam.transforms.deduplicate import DeduplicatePerKey
    from apache_beam.transforms.window import FixedWindows
    from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, AfterWatermark
    from apache_beam.metrics import Metrics
    from apache_beam.utils.timestamp import Timestamp

    _TaggedOutput = beam.pvalue.TaggedOutput
    BEAM_AVAILABLE = True
except ImportError:
    BEAM_AVAILABLE = False

    class _TaggedOutput:
        def __init__(self, tag: str, value: Any):
            self.tag = tag
            self.value = value

    class _PValue:
        TaggedOutput = _TaggedOutput

    class _DoFn:
        WindowParam = None

        def process(self, element: Any, *args: Any, **kwargs: Any):  # type: ignore[empty-body]
            ...

    class _CombineFn:
        def create_accumulator(self, *args: Any, **kwargs: Any) -> Any: ...
        def add_input(self, accumulator: Any, input: Any, *args: Any, **kwargs: Any) -> Any: ...
        def merge_accumulators(self, accumulators: Iterable[Any], *args: Any, **kwargs: Any) -> Any: ...
        def extract_output(self, accumulator: Any, *args: Any, **kwargs: Any) -> Any: ...

    class _FakeMetric:
        def inc(self, n: int = 1) -> None: ...
        def dec(self, n: int = 1) -> None: ...
        def update(self, value: Any) -> None: ...

    class Metrics:  # type: ignore[no-redef]
        @staticmethod
        def counter(namespace: str, name: str) -> "_FakeMetric":
            return _FakeMetric()

        @staticmethod
        def distribution(namespace: str, name: str) -> "_FakeMetric":
            return _FakeMetric()

    class _Beam:
        DoFn = _DoFn
        CombineFn = _CombineFn
        pvalue = _PValue()

    beam = _Beam()  # type: ignore[assignment]
    BigQueryDisposition = None  # type: ignore[assignment]
    WriteToBigQuery = None  # type: ignore[assignment]
    PipelineOptions = None  # type: ignore[assignment]
    StandardOptions = None  # type: ignore[assignment]
    FixedWindows = None  # type: ignore[assignment]
    DeduplicatePerKey = None  # type: ignore[assignment]
    AccumulationMode = None  # type: ignore[assignment]
    AfterProcessingTime = None  # type: ignore[assignment]
    AfterWatermark = None  # type: ignore[assignment]
    Timestamp = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_SECS = 300  # 5 minutes


# ═══════════════════════════════════════════════════════════════════════════════
# Domain contract
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class DomainSpec:
    """
    Everything the framework needs to know to wire one domain's slice of the
    pipeline. A "domain" is one event stream — one Pub/Sub topic, one raw
    table, optionally one windowed aggregate + alert rule.

    Required:
      name               short identifier, used in step names and as the
                         alert `domain` tag (e.g. "orders", "shipments")
      topic              Pub/Sub topic name (short form, not the full path —
                         the project is supplied separately at build time)
      raw_table          "dataset.table" the parsed/validated/enriched event
                         is written to, one row per event

    Validation (both optional — omit to skip validation entirely):
      envelope_required   field names every event's top level must have
      payload_required    field names required inside `event["payload"]`

    Windowed aggregation (all three required together, or all omitted):
      enriched_table      "dataset.table" for the windowed aggregate rows
      key_fn               dict -> grouping key (any hashable), e.g.
                           `lambda e: e["payload"]["region"]`
      aggregate_fn         zero-arg factory returning a fresh `beam.CombineFn`
                           instance, e.g. `MyAggregateWindow` (pass the class
                           itself, not an instance). Implements
                           create_accumulator/add_input/merge_accumulators/
                           extract_output — partial combining happens before
                           the shuffle, so hot keys don't bottleneck a single
                           worker the way GroupByKey+ParDo would.
      key_field_names       optional tuple of names to zip onto the tuple
                           `key_fn` returns, reattached to each aggregate
                           output row after combining, e.g.
                           `("event_type", "channel")` for a `key_fn`
                           returning `(e["event_type"], e["payload"]["channel"])`.
                           A `CombineFn`'s methods never see the grouping key
                           (Beam API constraint — they must also work for
                           non-keyed combines), so `extract_output` cannot
                           attach key fields itself; the framework does it
                           generically instead. Omit if the output row
                           doesn't need the key fields.

    Windowing behavior (optional, only meaningful alongside aggregation;
    defaults reproduce today's behavior exactly — a caller that never sets
    these sees no change):
      allowed_lateness_secs  how long past the watermark a late event can
                            still update its window (default 0, i.e. Beam's
                            own default — late events are dropped). Set this
                            if upstream redelivery/clock skew means events
                            can genuinely arrive after their window closes.
      early_firing_secs      if set, the window emits speculative results
                            every N seconds of processing time before the
                            watermark closes it — trades a bit of accuracy
                            for lower alerting latency. Omit for a single
                            firing at the watermark (today's behavior).
      accumulation_mode      "discarding" (default) or "accumulating".
                            Only matters when allowed_lateness_secs or
                            early_firing_secs cause more than one firing per
                            window: "discarding" emits only the new data
                            since the last firing, "accumulating" emits the
                            running total each time.

    Alerting (optional, only meaningful alongside aggregation):
      alert_evaluator      windowed-aggregate dict -> list[alert dict]

    Dead-letter persistence (optional):
      dlq_table             "dataset.table" — if set, events that fail
                            parsing or validation are written here (instead
                            of silently discarded) with an `_error` field.
                            Needed if you want to monitor DLQ volume as a
                            pipeline-health signal — see `health.py`.
                            Always written via BigQuery's legacy streaming
                            inserts, regardless of `write_method` — DLQ rows
                            come from several different failure points with
                            different field sets, which doesn't fit the
                            Storage Write API's strict, single, exact-schema
                            requirement. No `dlq_table_schema` field exists
                            for this reason; the table just needs to already
                            exist with a schema wide enough to cover
                            whatever a given domain's failure paths produce.

    BigQuery schemas (each a dict shaped `{"fields": [{"name", "type",
    "mode", "fields": [...]}, ...]}` — see any `apache_beam.io.gcp.bigquery`
    example, or `examples/retail_orders/pipeline.py` for a worked one).
    Required when `write_method="STORAGE_WRITE_API"` (the framework's
    default) for any table that's actually written to — the Storage Write
    API needs to know field types up front to build its write protocol; it
    cannot infer them from an already-existing BigQuery table the way
    legacy streaming inserts effectively can. `build_streaming_pipeline`
    raises a clear `ValueError` at build time if one's missing, rather than
    letting Beam fail deep inside pipeline construction. Not needed at all
    if using `write_method="STREAMING_INSERTS"`.
      raw_table_schema        schema for `raw_table`
      enriched_table_schema   schema for `enriched_table` (only meaningful
                             alongside aggregation)

    Deduplication (optional — omit to skip; recommended if exactly-once
    downstream rows matter, since Pub/Sub is at-least-once and *will*
    redeliver):
      dedup_key_fn          dict -> hashable identity key, e.g.
                            `lambda e: e["event_id"]`. Applied right after
                            parsing, before validation, so duplicates don't
                            waste validate/enrich work either.
      dedup_window_secs      how long a key is remembered (default 600 =
                            10 min). Bounded on purpose — unbounded dedup
                            state isn't practical; pick a window comfortably
                            longer than your realistic redelivery latency.
    """

    name: str
    topic: str
    raw_table: str
    envelope_required: frozenset[str] = field(default_factory=frozenset)
    payload_required: frozenset[str] = field(default_factory=frozenset)
    enriched_table: Optional[str] = None
    key_fn: Optional[Callable[[dict], Any]] = None
    aggregate_fn: Optional[Callable[[], Any]] = None
    key_field_names: Optional[tuple[str, ...]] = None
    allowed_lateness_secs: int = 0
    early_firing_secs: Optional[int] = None
    accumulation_mode: str = "discarding"
    alert_evaluator: Optional[Callable[[dict], list[dict]]] = None
    dlq_table: Optional[str] = None
    dedup_key_fn: Optional[Callable[[dict], Any]] = None
    dedup_window_secs: int = 600
    raw_table_schema: Optional[dict] = None
    enriched_table_schema: Optional[dict] = None

    def __post_init__(self):
        has_agg = (self.enriched_table, self.key_fn, self.aggregate_fn)
        if any(has_agg) and not all(has_agg):
            raise ValueError(
                f"DomainSpec {self.name!r}: enriched_table, key_fn, and "
                "aggregate_fn must all be set together, or all omitted"
            )
        if self.alert_evaluator and not all(has_agg):
            raise ValueError(
                f"DomainSpec {self.name!r}: alert_evaluator requires "
                "windowed aggregation (enriched_table/key_fn/aggregate_fn) "
                "to also be set"
            )
        if self.key_field_names is not None and not all(has_agg):
            raise ValueError(
                f"DomainSpec {self.name!r}: key_field_names requires "
                "windowed aggregation (enriched_table/key_fn/aggregate_fn) "
                "to also be set"
            )
        if self.accumulation_mode not in ("discarding", "accumulating"):
            raise ValueError(
                f"DomainSpec {self.name!r}: accumulation_mode must be "
                f"'discarding' or 'accumulating', got {self.accumulation_mode!r}"
            )
        has_windowing_config = (
            self.allowed_lateness_secs != 0
            or self.early_firing_secs is not None
            or self.accumulation_mode != "discarding"
        )
        if has_windowing_config and not all(has_agg):
            raise ValueError(
                f"DomainSpec {self.name!r}: allowed_lateness_secs/"
                "early_firing_secs/accumulation_mode requires windowed "
                "aggregation (enriched_table/key_fn/aggregate_fn) to also "
                "be set"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Generic transforms
# ═══════════════════════════════════════════════════════════════════════════════

def _ns(domain: str) -> str:
    """Beam Metrics namespace convention for this framework."""
    return f"streaming_pipeline_framework.{domain}"


def _dlq(domain: str, error: str, event: Optional[dict] = None, **extra: Any) -> dict[str, Any]:
    """Builds a DLQ row with a consistent shape — every DLQ row (from any
    stage) has `_error` and `ingested_at`, so a single dlq_table can be
    queried uniformly (see health.check_dlq_thresholds). `event` is spread
    in as a plain dict (not **kwargs) so an event field literally named
    `error` can never collide with this function's own `error` parameter.
    Also increments a `dlq_writes` counter — the single source of truth for
    that metric, since every DLQ row in the pipeline is built here."""
    Metrics.counter(_ns(domain), "dlq_writes").inc()
    return {
        **(event or {}),
        **extra,
        "_error": error,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


class ParseMessage(beam.DoFn):
    """Deserialize Pub/Sub bytes → dict. Malformed JSON goes to the 'dlq' tag."""

    def __init__(self, domain: str = "unknown"):
        self.domain = domain

    def process(self, message, *args, **kwargs):
        try:
            parsed = json.loads(message.data.decode("utf-8"))
        except Exception as e:
            Metrics.counter(_ns(self.domain), "parse_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(self.domain, str(e), raw=str(message.data[:500])))
            return
        Metrics.counter(_ns(self.domain), "parse_ok").inc()
        Metrics.distribution(_ns(self.domain), "message_bytes").update(len(message.data))
        yield parsed


class ValidateEvent(beam.DoFn):
    """
    Checks required top-level and payload fields for one domain. An event
    whose `domain` field doesn't match (when both are set) is treated as
    misrouted and also sent to DLQ — a cheap safety net for topic mixups.

    One instance is scoped to one domain; the framework creates one per
    `DomainSpec` rather than routing many domains through a single instance.
    """

    def __init__(
        self,
        domain: str,
        envelope_required: Iterable[str] = (),
        payload_required: Iterable[str] = (),
    ):
        self.domain = domain
        self.envelope_required = frozenset(envelope_required)
        self.payload_required = frozenset(payload_required)

    def process(self, event, *args, **kwargs):
        missing = self.envelope_required - event.keys()
        if missing:
            Metrics.counter(_ns(self.domain), "validate_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(self.domain, f"missing envelope fields: {missing}", event))
            return

        event_domain = event.get("domain")
        if event_domain is not None and event_domain != self.domain:
            Metrics.counter(_ns(self.domain), "validate_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(
                self.domain, f"domain mismatch: expected {self.domain!r}, got {event_domain!r}", event
            ))
            return

        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            Metrics.counter(_ns(self.domain), "validate_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(
                self.domain, f"payload is not an object (got {type(payload).__name__})", event
            ))
            return

        payload_missing = self.payload_required - payload.keys()
        if payload_missing:
            Metrics.counter(_ns(self.domain), "validate_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(self.domain, f"missing payload fields: {payload_missing}", event))
            return

        Metrics.counter(_ns(self.domain), "validate_ok").inc()
        yield event


class EnrichEvent(beam.DoFn):
    """Stamp pipeline metadata onto each event before writing to BigQuery."""

    def __init__(self, pipeline_version: str = "1.0", domain: str = "unknown"):
        self.pipeline_version = pipeline_version
        self.domain = domain

    def process(self, event, *args, **kwargs):
        try:
            enriched = {
                **event,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "_pipeline_version": self.pipeline_version,
            }
        except Exception as e:
            Metrics.counter(_ns(self.domain), "enrich_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(self.domain, str(e), event if isinstance(event, dict) else None))
            return
        Metrics.counter(_ns(self.domain), "enrich_ok").inc()
        yield enriched


class StripInternalFields(beam.DoFn):
    """Remove pipeline-internal underscore-prefixed fields before writing to BQ."""

    def process(self, event, *args, **kwargs):
        yield {k: v for k, v in event.items() if not k.startswith("_")}


class DetectAlerts(beam.DoFn):
    """Runs a windowed-aggregate dict through a domain's alert evaluator."""

    def __init__(self, evaluator: Callable[[dict], list[dict]], domain: str = "unknown"):
        self.evaluator = evaluator
        self.domain = domain

    def process(self, agg: dict, *args, **kwargs):
        try:
            alerts = list(self.evaluator(agg))
        except Exception as e:
            Metrics.counter(_ns(self.domain), "alert_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(self.domain, str(e), agg if isinstance(agg, dict) else None))
            return
        Metrics.counter(_ns(self.domain), "alert_ok").inc()
        Metrics.distribution(_ns(self.domain), "alerts_emitted").update(len(alerts))
        yield from alerts


# ═══════════════════════════════════════════════════════════════════════════════
# Windowed aggregation (CombineFn-based — partial combining before the shuffle)
# ═══════════════════════════════════════════════════════════════════════════════

def _zip_key(key: Any, key_field_names: Optional[tuple[str, ...]]) -> dict[str, Any]:
    """Zips a DomainSpec.key_fn result onto its declared field names, e.g.
    key=("a", "b"), key_field_names=("event_type", "channel") ->
    {"event_type": "a", "channel": "b"}. Returns {} if key_field_names is
    None (caller doesn't want the key fields reattached to the output row)."""
    if key_field_names is None:
        return {}
    key_tuple = key if isinstance(key, tuple) else (key,)
    return dict(zip(key_field_names, key_tuple))


class KeyEvent(beam.DoFn):
    """Keys an event by a domain's key_fn ahead of CombinePerKey. A key_fn
    failure (e.g. reaching into a missing payload field) goes to the 'dlq'
    tag instead of crashing the bundle."""

    def __init__(self, key_fn: Callable[[dict], Any], domain: str):
        self.key_fn = key_fn
        self.domain = domain

    def process(self, event, *args, **kwargs):
        try:
            key = self.key_fn(event)
        except Exception as e:
            Metrics.counter(_ns(self.domain), "key_failures").inc()
            yield beam.pvalue.TaggedOutput("dlq", _dlq(self.domain, str(e), event if isinstance(event, dict) else None))
            return
        Metrics.counter(_ns(self.domain), "key_ok").inc()
        yield (key, event)


class _DlqWrappingCombineFn(beam.CombineFn):
    """Wraps a domain's CombineFn so a bad element can't crash the whole
    aggregation bundle.

    add_input/merge_accumulators have no side-output channel mid-combine —
    an accumulator carries no per-event identity to attach to a DLQ row —
    so a failure there can only be logged and the offending input/merge
    dropped (the accumulator is returned unchanged, or for a failed merge,
    the first accumulator in the batch is kept as a best-effort fallback).

    extract_output failures instead produce a sentinel dict that
    _AttachWindowAndKey recognizes and routes to the 'dlq' tag — this is the
    one point in the chain where the aggregate result (not an individual
    event) can still be captured.
    """

    def __init__(self, inner: "beam.CombineFn", domain: str):
        self.inner = inner
        self.domain = domain

    def create_accumulator(self, *args, **kwargs):
        return self.inner.create_accumulator(*args, **kwargs)

    def add_input(self, accumulator, input, *args, **kwargs):
        try:
            result = self.inner.add_input(accumulator, input, *args, **kwargs)
        except Exception:
            logger.exception("%s: aggregate add_input failed, dropping element", self.domain)
            Metrics.counter(_ns(self.domain), "aggregate_failures").inc()
            return accumulator
        Metrics.counter(_ns(self.domain), "aggregate_ok").inc()
        return result

    def merge_accumulators(self, accumulators, *args, **kwargs):
        try:
            return self.inner.merge_accumulators(accumulators, *args, **kwargs)
        except Exception:
            logger.exception("%s: aggregate merge_accumulators failed", self.domain)
            Metrics.counter(_ns(self.domain), "aggregate_failures").inc()
            accumulators = list(accumulators)
            return accumulators[0] if accumulators else self.inner.create_accumulator()

    def extract_output(self, accumulator, *args, **kwargs):
        try:
            return self.inner.extract_output(accumulator, *args, **kwargs)
        except Exception as e:
            Metrics.counter(_ns(self.domain), "aggregate_failures").inc()
            return {"__agg_error__": True, "_error": str(e)}


class _AttachWindowAndKey(beam.DoFn):
    """Reattaches key and window fields to a CombinePerKey output row — a
    CombineFn's extract_output has no access to either (Beam API
    constraint: CombineFn methods must also work for non-keyed, non-windowed
    combines), so the framework does it here instead of every domain
    hand-rolling it. Routes _DlqWrappingCombineFn's error sentinel to the
    'dlq' tag."""

    def __init__(self, key_field_names: Optional[tuple[str, ...]], domain: str):
        self.key_field_names = key_field_names
        self.domain = domain

    def process(self, kv, window=beam.DoFn.WindowParam, *args, **kwargs):
        key, agg = kv
        if isinstance(agg, dict) and agg.get("__agg_error__"):
            yield beam.pvalue.TaggedOutput("dlq", _dlq(
                self.domain, agg.get("_error", "aggregate error"), None, **_zip_key(key, self.key_field_names)
            ))
            return
        Metrics.counter(_ns(self.domain), "aggregate_rows_ok").inc()
        yield {
            **agg,
            **_zip_key(key, self.key_field_names),
            # Timestamp.to_utc_datetime() returns a naive datetime (tzinfo=
            # None) despite the name — explicitly attach UTC so the
            # isoformat() string always carries an offset, consistent with
            # every other timestamp string this framework produces (e.g.
            # EnrichEvent's datetime.now(timezone.utc).isoformat()).
            # Without this, _coerce_timestamps_for_schema's
            # datetime.fromisoformat() round-trip produces a naive datetime
            # that Timestamp.from_utc_datetime() rejects outright.
            "window_start": window.start.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat(),
            "window_end": window.end.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat(),
        }


def _window_transform(spec: "DomainSpec", window_secs: int) -> "beam.WindowInto":
    """Builds the WindowInto transform for a domain's aggregation. With no
    lateness/early-firing config (the default), this is the exact
    zero-kwargs FixedWindows call the framework has always made — behavior
    is bit-for-bit unchanged for callers who don't opt in. Otherwise adds a
    trigger for early/speculative firing and the configured allowed
    lateness; Beam's default trigger already re-fires per late element once
    allowed_lateness > 0, so no separate late-firing-cadence knob is needed.
    """
    if spec.early_firing_secs is None and spec.allowed_lateness_secs == 0:
        return beam.WindowInto(FixedWindows(window_secs))

    trigger = None
    if spec.early_firing_secs is not None:
        trigger = AfterWatermark(early=AfterProcessingTime(spec.early_firing_secs))

    return beam.WindowInto(
        FixedWindows(window_secs),
        trigger=trigger,
        accumulation_mode=(
            AccumulationMode.DISCARDING
            if spec.accumulation_mode == "discarding"
            else AccumulationMode.ACCUMULATING
        ),
        allowed_lateness=spec.allowed_lateness_secs,
    )


def _coerce_timestamps_for_schema(row: dict, schema: dict) -> dict:
    """Every DoFn in this framework stamps timestamps as ISO 8601 strings
    (`datetime.isoformat()`) — the natural representation for JSON messages
    and for BigQuery's legacy streaming inserts, which JSON-encode rows
    directly. But the Storage Write API's schema-aware row conversion
    requires TIMESTAMP-typed fields to be actual
    `apache_beam.utils.timestamp.Timestamp` objects (it calls `.micros` on
    the value directly — see `apache_beam.typehints.schemas.MicrosInstant`)
    and, conversely, a `Timestamp` object isn't JSON-serializable, so it
    would break legacy streaming inserts (which DLQ writes always use).
    Rather than making every DoFn write-method-aware, this converts
    schema-declared TIMESTAMP string fields (recursing into RECORD fields)
    just-in-time, right before a STORAGE_WRITE_API write — see where it's
    called in build_streaming_pipeline."""
    out = dict(row)
    for field in schema.get("fields", ()):
        name = field.get("name")
        value = out.get(name)
        if value is None:
            continue
        field_type = field.get("type")
        if field_type == "TIMESTAMP" and isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            # This framework's convention is tz-aware ISO strings
            # everywhere, but Timestamp.from_utc_datetime() rejects naive
            # ones outright rather than assuming a timezone — treat a
            # naive string as UTC (the only timezone anything in this
            # framework ever produces) instead of crashing the bundle.
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            out[name] = Timestamp.from_utc_datetime(parsed)
        elif field_type in ("RECORD", "STRUCT") and isinstance(value, dict):
            out[name] = _coerce_timestamps_for_schema(value, field)
    return out


def _write_bigquery(pcoll, label: str, table: str, schema: Optional[dict], write_method: str, write_cfg: dict):
    """WriteToBigQuery wrapper that inserts the STORAGE_WRITE_API timestamp
    coercion (see _coerce_timestamps_for_schema) only when it's actually
    needed — STREAMING_INSERTS rows pass through unchanged, and so does any
    write without a schema (DLQ writes, which are schema-less by design)."""
    if write_method == "STORAGE_WRITE_API" and schema:
        pcoll = pcoll | f"{label}_CoerceTimestamps" >> beam.Map(
            lambda row: _coerce_timestamps_for_schema(row, schema)
        )
    return pcoll | label >> WriteToBigQuery(table, schema=schema, **write_cfg)


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline builder
# ═══════════════════════════════════════════════════════════════════════════════

def build_streaming_pipeline(
    project: str,
    domains: list[DomainSpec],
    alerts_table: Optional[str],
    options: "PipelineOptions",
    window_secs: int = DEFAULT_WINDOW_SECS,
    pipeline_version: str = "1.0",
    write_method: str = "STORAGE_WRITE_API",
    triggering_frequency_secs: int = 5,
    use_storage_api_auto_sharding: bool = True,
    alerts_table_schema: Optional[dict] = None,
) -> None:
    """
    Wires one Beam pipeline covering every domain in `domains`. Each domain
    gets its own read → parse → (dedup) → validate → enrich → write-raw
    branch, plus (if configured) a windowed aggregate → write-enriched →
    alert branch.

    `alerts_table` is required if any domain sets an `alert_evaluator`.
    `alerts_table_schema` likewise, but only when `write_method` is
    `STORAGE_WRITE_API` — see `DomainSpec`'s docstring for the schema shape
    and why it's needed. (`raw_table_schema`/`enriched_table_schema` are
    per-domain, set directly on the `DomainSpec`.)

    `write_method` defaults to the BigQuery Storage Write API rather than
    legacy streaming inserts: higher throughput, cheaper, and — combined
    with `DomainSpec.dedup_key_fn` — the practical way to get exactly-once
    rows out of an at-least-once source like Pub/Sub. `use_at_least_once`
    is left at Beam's default (False), i.e. exactly-once write semantics.
    `triggering_frequency_secs` controls how often batches commit; lower is
    lower-latency, higher is fewer/cheaper API calls. Pass
    `write_method="STREAMING_INSERTS"` to opt back into the legacy sink
    (which doesn't need any of the `*_table_schema` fields).

    DLQ tables are always written via legacy streaming inserts regardless of
    `write_method` — see `DomainSpec.dlq_table`'s docstring for why.

    A `runner="DirectRunner"` streaming pipeline (Pub/Sub source +
    `STORAGE_WRITE_API`) cannot run locally at all: `STORAGE_WRITE_API` is a
    cross-language (Java-backed) transform, and Beam's streaming
    `DirectRunner` categorically refuses to run cross-language pipelines
    ("Streaming Python direct runner does not support cross-language
    pipelines"). Its own suggested fallback, `PrismRunner`, doesn't support
    `ReadFromPubSub` either (as of apache-beam 2.75). Local testing of the
    full pipeline therefore only works with `write_method="STREAMING_INSERTS"`
    — use that to smoke-test locally, then switch to `DataflowRunner` (which
    handles cross-language transforms itself) to actually exercise
    `STORAGE_WRITE_API`.
    """
    if any(d.alert_evaluator for d in domains) and not alerts_table:
        raise ValueError("alerts_table is required when any domain has an alert_evaluator")

    if write_method == "STORAGE_WRITE_API":
        for spec in domains:
            if not spec.raw_table_schema:
                raise ValueError(
                    f"DomainSpec {spec.name!r}: raw_table_schema is required "
                    "when write_method='STORAGE_WRITE_API' (see DomainSpec's "
                    "docstring) — pass write_method='STREAMING_INSERTS' if "
                    "you don't want to supply one"
                )
            if spec.enriched_table and not spec.enriched_table_schema:
                raise ValueError(
                    f"DomainSpec {spec.name!r}: enriched_table_schema is "
                    "required when write_method='STORAGE_WRITE_API' and "
                    "enriched_table is set"
                )
        if any(d.alert_evaluator for d in domains) and not alerts_table_schema:
            raise ValueError(
                "alerts_table_schema is required when write_method="
                "'STORAGE_WRITE_API' and any domain has an alert_evaluator"
            )

    write_cfg: dict[str, Any] = dict(
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=write_method,
    )
    if write_method == "STORAGE_WRITE_API":
        write_cfg["triggering_frequency"] = triggering_frequency_secs
        write_cfg["with_auto_sharding"] = use_storage_api_auto_sharding

    # DLQ rows come from several different failure points with different
    # field sets (see DomainSpec.dlq_table) — incompatible with Storage
    # Write API's single, strict, exact schema. Always use the more
    # tolerant legacy path for this one sink.
    dlq_write_cfg: dict[str, Any] = dict(
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method="STREAMING_INSERTS",
    )

    with beam.Pipeline(options=options) as p:
        for spec in domains:
            topic_path = f"projects/{project}/topics/{spec.topic}"

            raw = p | f"{spec.name}_Read" >> beam.io.ReadFromPubSub(
                topic=topic_path, with_attributes=True
            )
            parsed = raw | f"{spec.name}_Parse" >> beam.ParDo(
                ParseMessage(spec.name)
            ).with_outputs("dlq", main="ok")

            to_validate = parsed.ok
            if spec.dedup_key_fn:
                to_validate = (
                    parsed.ok
                    | f"{spec.name}_DedupKey" >> beam.Map(lambda e, kf=spec.dedup_key_fn: (kf(e), e))
                    | f"{spec.name}_Dedup" >> DeduplicatePerKey(
                        processing_time_duration=spec.dedup_window_secs
                    )
                    | f"{spec.name}_DedupUnkey" >> beam.Map(lambda kv: kv[1])
                )

            valid = to_validate | f"{spec.name}_Validate" >> beam.ParDo(
                ValidateEvent(spec.name, spec.envelope_required, spec.payload_required)
            ).with_outputs("dlq", main="ok")
            enriched = valid.ok | f"{spec.name}_Enrich" >> beam.ParDo(
                EnrichEvent(pipeline_version, spec.name)
            ).with_outputs("dlq", main="ok")
            clean = enriched.ok | f"{spec.name}_Strip" >> beam.ParDo(StripInternalFields())

            _write_bigquery(
                clean, f"{spec.name}_WriteRaw", f"{project}:{spec.raw_table}",
                spec.raw_table_schema, write_method, write_cfg,
            )

            keyed = None
            agg = None
            alerts = None
            if spec.enriched_table:
                keyed = (
                    enriched.ok
                    | f"{spec.name}_Window" >> _window_transform(spec, window_secs)
                    | f"{spec.name}_KV" >> beam.ParDo(
                        KeyEvent(spec.key_fn, spec.name)
                    ).with_outputs("dlq", main="ok")
                )
                combined = keyed.ok | f"{spec.name}_Combine" >> beam.CombinePerKey(
                    _DlqWrappingCombineFn(spec.aggregate_fn(), spec.name)
                )
                agg = combined | f"{spec.name}_AttachKeyWindow" >> beam.ParDo(
                    _AttachWindowAndKey(spec.key_field_names, spec.name)
                ).with_outputs("dlq", main="ok")
                _write_bigquery(
                    agg.ok, f"{spec.name}_WriteAgg", f"{project}:{spec.enriched_table}",
                    spec.enriched_table_schema, write_method, write_cfg,
                )

                if spec.alert_evaluator:
                    alerts = agg.ok | f"{spec.name}_Alerts" >> beam.ParDo(
                        DetectAlerts(spec.alert_evaluator, spec.name)
                    ).with_outputs("dlq", main="ok")
                    _write_bigquery(
                        alerts.ok, f"{spec.name}_WriteAlerts", f"{project}:{alerts_table}",
                        alerts_table_schema, write_method, write_cfg,
                    )

            if spec.dlq_table:
                dlq_branches = [parsed.dlq, valid.dlq, enriched.dlq]
                if keyed is not None:
                    dlq_branches.append(keyed.dlq)
                if agg is not None:
                    dlq_branches.append(agg.dlq)
                if alerts is not None:
                    dlq_branches.append(alerts.dlq)
                dlq = tuple(dlq_branches) | f"{spec.name}_DlqFlatten" >> beam.Flatten()
                _write_bigquery(
                    dlq, f"{spec.name}_WriteDlq", f"{project}:{spec.dlq_table}",
                    None, "STREAMING_INSERTS", dlq_write_cfg,
                )
