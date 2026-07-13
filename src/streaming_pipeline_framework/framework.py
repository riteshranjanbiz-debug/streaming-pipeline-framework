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
    from apache_beam.transforms.window import FixedWindows

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

    class _Beam:
        DoFn = _DoFn
        pvalue = _PValue()

    beam = _Beam()  # type: ignore[assignment]
    BigQueryDisposition = None  # type: ignore[assignment]
    WriteToBigQuery = None  # type: ignore[assignment]
    PipelineOptions = None  # type: ignore[assignment]
    StandardOptions = None  # type: ignore[assignment]
    FixedWindows = None  # type: ignore[assignment]

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
      aggregate_fn         zero-arg factory returning a fresh `beam.DoFn`
                           instance per bundle, e.g. `MyAggregateWindow`
                           (pass the class itself, not an instance)

    Alerting (optional, only meaningful alongside aggregation):
      alert_evaluator      windowed-aggregate dict -> list[alert dict]

    Dead-letter persistence (optional):
      dlq_table             "dataset.table" — if set, events that fail
                            parsing or validation are written here (instead
                            of silently discarded) with an `_error` field.
                            Needed if you want to monitor DLQ volume as a
                            pipeline-health signal — see `health.py`.
    """

    name: str
    topic: str
    raw_table: str
    envelope_required: frozenset[str] = field(default_factory=frozenset)
    payload_required: frozenset[str] = field(default_factory=frozenset)
    enriched_table: Optional[str] = None
    key_fn: Optional[Callable[[dict], Any]] = None
    aggregate_fn: Optional[Callable[[], Any]] = None
    alert_evaluator: Optional[Callable[[dict], list[dict]]] = None
    dlq_table: Optional[str] = None

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


# ═══════════════════════════════════════════════════════════════════════════════
# Generic transforms
# ═══════════════════════════════════════════════════════════════════════════════

def _dlq(error: str, event: Optional[dict] = None, **extra: Any) -> dict[str, Any]:
    """Builds a DLQ row with a consistent shape — every DLQ row (from any
    stage) has `_error` and `ingested_at`, so a single dlq_table can be
    queried uniformly (see health.check_dlq_thresholds). `event` is spread
    in as a plain dict (not **kwargs) so an event field literally named
    `error` can never collide with this function's own `error` parameter."""
    return {
        **(event or {}),
        **extra,
        "_error": error,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


class ParseMessage(beam.DoFn):
    """Deserialize Pub/Sub bytes → dict. Malformed JSON goes to the 'dlq' tag."""

    def process(self, message, *args, **kwargs):
        try:
            yield json.loads(message.data.decode("utf-8"))
        except Exception as e:
            yield beam.pvalue.TaggedOutput("dlq", _dlq(str(e), raw=str(message.data[:500])))


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
            yield beam.pvalue.TaggedOutput("dlq", _dlq(f"missing envelope fields: {missing}", event))
            return

        event_domain = event.get("domain")
        if event_domain is not None and event_domain != self.domain:
            yield beam.pvalue.TaggedOutput("dlq", _dlq(
                f"domain mismatch: expected {self.domain!r}, got {event_domain!r}", event
            ))
            return

        payload = event.get("payload") or {}
        payload_missing = self.payload_required - payload.keys()
        if payload_missing:
            yield beam.pvalue.TaggedOutput("dlq", _dlq(f"missing payload fields: {payload_missing}", event))
            return

        yield event


class EnrichEvent(beam.DoFn):
    """Stamp pipeline metadata onto each event before writing to BigQuery."""

    def __init__(self, pipeline_version: str = "1.0"):
        self.pipeline_version = pipeline_version

    def process(self, event, *args, **kwargs):
        yield {
            **event,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "_pipeline_version": self.pipeline_version,
        }


class StripInternalFields(beam.DoFn):
    """Remove pipeline-internal underscore-prefixed fields before writing to BQ."""

    def process(self, event, *args, **kwargs):
        yield {k: v for k, v in event.items() if not k.startswith("_")}


class DetectAlerts(beam.DoFn):
    """Runs a windowed-aggregate dict through a domain's alert evaluator."""

    def __init__(self, evaluator: Callable[[dict], list[dict]]):
        self.evaluator = evaluator

    def process(self, agg: dict, *args, **kwargs):
        yield from self.evaluator(agg)


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
) -> None:
    """
    Wires one Beam pipeline covering every domain in `domains`. Each domain
    gets its own read → parse → validate → enrich → write-raw branch, plus
    (if configured) a windowed aggregate → write-enriched → alert branch.

    `alerts_table` is required if any domain sets an `alert_evaluator`.
    """
    if any(d.alert_evaluator for d in domains) and not alerts_table:
        raise ValueError("alerts_table is required when any domain has an alert_evaluator")

    write_cfg: dict[str, Any] = dict(
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
    )

    with beam.Pipeline(options=options) as p:
        for spec in domains:
            topic_path = f"projects/{project}/topics/{spec.topic}"

            raw = p | f"{spec.name}_Read" >> beam.io.ReadFromPubSub(
                topic=topic_path, with_attributes=True
            )
            parsed = raw | f"{spec.name}_Parse" >> beam.ParDo(
                ParseMessage()
            ).with_outputs("dlq", main="ok")
            valid = parsed.ok | f"{spec.name}_Validate" >> beam.ParDo(
                ValidateEvent(spec.name, spec.envelope_required, spec.payload_required)
            ).with_outputs("dlq", main="ok")
            enriched = valid.ok | f"{spec.name}_Enrich" >> beam.ParDo(
                EnrichEvent(pipeline_version)
            )
            clean = enriched | f"{spec.name}_Strip" >> beam.ParDo(StripInternalFields())

            clean | f"{spec.name}_WriteRaw" >> WriteToBigQuery(
                f"{project}:{spec.raw_table}", **write_cfg
            )

            if spec.dlq_table:
                dlq = (
                    (parsed.dlq, valid.dlq)
                    | f"{spec.name}_DlqFlatten" >> beam.Flatten()
                )
                dlq | f"{spec.name}_WriteDlq" >> WriteToBigQuery(
                    f"{project}:{spec.dlq_table}", **write_cfg
                )

            if spec.enriched_table:
                agg = (
                    enriched
                    | f"{spec.name}_Window" >> beam.WindowInto(FixedWindows(window_secs))
                    | f"{spec.name}_KV" >> beam.Map(lambda e, kf=spec.key_fn: (kf(e), e))
                    | f"{spec.name}_Group" >> beam.GroupByKey()
                    | f"{spec.name}_Agg" >> beam.ParDo(spec.aggregate_fn())
                )
                agg | f"{spec.name}_WriteAgg" >> WriteToBigQuery(
                    f"{project}:{spec.enriched_table}", **write_cfg
                )

                if spec.alert_evaluator:
                    (
                        agg
                        | f"{spec.name}_Alerts" >> beam.ParDo(DetectAlerts(spec.alert_evaluator))
                        | f"{spec.name}_WriteAlerts" >> WriteToBigQuery(
                            f"{project}:{alerts_table}", **write_cfg
                        )
                    )
