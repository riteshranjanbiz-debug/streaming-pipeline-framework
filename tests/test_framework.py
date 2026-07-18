"""
Unit tests for the generic engine. Deliberately uses a synthetic "widget"
domain, not insurance or retail — the point is these transforms don't know
or care what domain they're validating/enriching.

DoFns are tested by calling .process() directly, no Beam runtime required.
"""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from streaming_pipeline_framework.framework import (
    DomainSpec,
    ParseMessage,
    ValidateEvent,
    EnrichEvent,
    StripInternalFields,
    DetectAlerts,
    KeyEvent,
    _AttachWindowAndKey,
    _DlqWrappingCombineFn,
    _TaggedOutput,
    beam,
    Metrics,
    build_streaming_pipeline,
)


class FakeMessage:
    def __init__(self, data: dict):
        self.data = json.dumps(data).encode("utf-8")


def collect(dofn, *args, **kwargs):
    return list(dofn.process(*args, **kwargs))


def _widget_event(**overrides) -> dict:
    base = {
        "event_id": "evt-1",
        "event_type": "widget.created",
        "domain": "widgets",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "payload": {"widget_id": "W-1", "color": "red"},
    }
    base.update(overrides)
    return base


ENVELOPE_REQUIRED = frozenset({"event_id", "event_type", "domain", "timestamp"})
PAYLOAD_REQUIRED = frozenset({"widget_id", "color"})


# ── ParseMessage ───────────────────────────────────────────────────────────────

class TestParseMessage:
    def test_valid_json_passes_through(self):
        result = collect(ParseMessage(), FakeMessage(_widget_event()))
        assert len(result) == 1
        assert result[0]["event_id"] == "evt-1"

    def test_invalid_json_goes_to_dlq(self):
        class BadMsg:
            data = b"not json {"

        results = list(ParseMessage().process(BadMsg()))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert len(tagged) == 1
        assert tagged[0].tag == "dlq"


# ── ValidateEvent ──────────────────────────────────────────────────────────────

class TestValidateEvent:
    def _validator(self):
        return ValidateEvent("widgets", ENVELOPE_REQUIRED, PAYLOAD_REQUIRED)

    def test_valid_event_passes(self):
        result = collect(self._validator(), _widget_event())
        assert len(result) == 1

    def test_missing_envelope_field_goes_to_dlq(self):
        bad = _widget_event()
        del bad["event_id"]
        results = list(self._validator().process(bad))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert tagged[0].tag == "dlq"
        assert "missing envelope fields" in tagged[0].value["_error"]

    def test_domain_mismatch_goes_to_dlq(self):
        bad = _widget_event(domain="not-widgets")
        results = list(self._validator().process(bad))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert tagged[0].tag == "dlq"
        assert "domain mismatch" in tagged[0].value["_error"]

    def test_missing_payload_field_goes_to_dlq(self):
        bad = _widget_event()
        del bad["payload"]["color"]
        results = list(self._validator().process(bad))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert tagged[0].tag == "dlq"
        assert "missing payload fields" in tagged[0].value["_error"]

    def test_no_domain_field_required_when_spec_omits_it(self):
        validator = ValidateEvent("widgets", frozenset({"event_id"}), frozenset())
        result = collect(validator, {"event_id": "x"})
        assert len(result) == 1

    def test_non_dict_payload_goes_to_dlq(self):
        bad = _widget_event(payload=["not", "a", "dict"])
        results = list(self._validator().process(bad))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert len(tagged) == 1
        assert tagged[0].tag == "dlq"
        assert "payload is not an object" in tagged[0].value["_error"]


# ── EnrichEvent ────────────────────────────────────────────────────────────────

class TestEnrichEvent:
    def test_adds_ingested_at_and_version(self):
        result = collect(EnrichEvent(pipeline_version="2.3"), _widget_event())
        assert "ingested_at" in result[0]
        assert result[0]["_pipeline_version"] == "2.3"

    def test_default_version(self):
        result = collect(EnrichEvent(), _widget_event())
        assert result[0]["_pipeline_version"] == "1.0"

    def test_preserves_original_fields(self):
        result = collect(EnrichEvent(), _widget_event())
        assert result[0]["payload"]["widget_id"] == "W-1"

    def test_failure_goes_to_dlq(self):
        results = list(EnrichEvent().process("not-a-dict"))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert len(tagged) == 1
        assert tagged[0].tag == "dlq"


# ── StripInternalFields ────────────────────────────────────────────────────────

class TestStripInternalFields:
    def test_removes_underscore_fields(self):
        enriched = {**_widget_event(), "_pipeline_version": "1.0", "_error": "x"}
        result = collect(StripInternalFields(), enriched)
        assert "_pipeline_version" not in result[0]
        assert "_error" not in result[0]

    def test_keeps_public_fields(self):
        enriched = {**_widget_event(), "_pipeline_version": "1.0", "ingested_at": "ts"}
        result = collect(StripInternalFields(), enriched)
        assert result[0]["event_id"] == "evt-1"
        assert result[0]["ingested_at"] == "ts"


# ── DetectAlerts ───────────────────────────────────────────────────────────────

class TestDetectAlerts:
    def test_yields_evaluator_output(self):
        def evaluator(agg):
            return [{"alert_type": "test_alert", "value": agg["event_count"]}]

        result = collect(DetectAlerts(evaluator), {"event_count": 42})
        assert result == [{"alert_type": "test_alert", "value": 42}]

    def test_empty_when_evaluator_returns_nothing(self):
        result = collect(DetectAlerts(lambda agg: []), {"event_count": 0})
        assert result == []

    def test_evaluator_failure_goes_to_dlq(self):
        def bad_evaluator(agg):
            raise KeyError("missing_field")

        results = list(DetectAlerts(bad_evaluator).process({"event_count": 0}))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert len(tagged) == 1
        assert tagged[0].tag == "dlq"


# ── KeyEvent ───────────────────────────────────────────────────────────────────

class TestKeyEvent:
    def test_keys_event(self):
        event = _widget_event()
        result = collect(KeyEvent(lambda e: e["event_type"], "widgets"), event)
        assert result == [("widget.created", event)]

    def test_key_fn_failure_goes_to_dlq(self):
        def bad_key_fn(e):
            raise KeyError("missing")

        results = list(KeyEvent(bad_key_fn, "widgets").process(_widget_event()))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert len(tagged) == 1
        assert tagged[0].tag == "dlq"


# ── CombineFn aggregation plumbing ──────────────────────────────────────────────

class WidgetCountCombineFn(beam.CombineFn):
    """Minimal domain CombineFn, used to test the aggregation plumbing —
    not the framework's own code, mirrors what a real DomainSpec.aggregate_fn
    would look like."""

    def create_accumulator(self):
        return 0

    def add_input(self, accumulator, event):
        return accumulator + 1

    def merge_accumulators(self, accumulators):
        return sum(accumulators)

    def extract_output(self, accumulator):
        return {"event_count": accumulator}


class TestWidgetCountCombineFn:
    def test_create_add_merge_extract(self):
        combiner = WidgetCountCombineFn()
        acc1 = combiner.add_input(combiner.add_input(combiner.create_accumulator(), _widget_event()), _widget_event())
        acc2 = combiner.add_input(combiner.create_accumulator(), _widget_event())
        merged = combiner.merge_accumulators([acc1, acc2])
        assert combiner.extract_output(merged) == {"event_count": 3}


class TestDlqWrappingCombineFn:
    def test_add_input_failure_is_dropped(self):
        class RaisingAddInput(beam.CombineFn):
            def create_accumulator(self):
                return 0

            def add_input(self, accumulator, event):
                raise ValueError("boom")

            def merge_accumulators(self, accumulators):
                return sum(accumulators)

            def extract_output(self, accumulator):
                return {"event_count": accumulator}

        wrapped = _DlqWrappingCombineFn(RaisingAddInput(), "widgets")
        acc = wrapped.create_accumulator()
        result = wrapped.add_input(acc, _widget_event())
        assert result == acc

    def test_extract_output_failure_returns_sentinel(self):
        class RaisingExtractOutput(beam.CombineFn):
            def create_accumulator(self):
                return 0

            def add_input(self, accumulator, event):
                return accumulator + 1

            def merge_accumulators(self, accumulators):
                return sum(accumulators)

            def extract_output(self, accumulator):
                raise ValueError("boom")

        wrapped = _DlqWrappingCombineFn(RaisingExtractOutput(), "widgets")
        result = wrapped.extract_output(1)
        assert result["__agg_error__"] is True
        assert "boom" in result["_error"]


class TestAttachWindowAndKey:
    def _fake_window(self):
        start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
        return SimpleNamespace(
            start=SimpleNamespace(to_utc_datetime=lambda: start),
            end=SimpleNamespace(to_utc_datetime=lambda: end),
        )

    def test_happy_path_merges_key_and_window(self):
        dofn = _AttachWindowAndKey(key_field_names=("event_type", "color"), domain="widgets")
        kv = (("widget.created", "red"), {"event_count": 3})
        result = list(dofn.process(kv, window=self._fake_window()))
        assert len(result) == 1
        row = result[0]
        assert row["event_count"] == 3
        assert row["event_type"] == "widget.created"
        assert row["color"] == "red"
        assert "window_start" in row and "window_end" in row

    def test_no_key_field_names_omits_key_fields(self):
        dofn = _AttachWindowAndKey(key_field_names=None, domain="widgets")
        kv = ("widget.created", {"event_count": 1})
        result = list(dofn.process(kv, window=self._fake_window()))
        assert "event_type" not in result[0]

    def test_sentinel_error_routes_to_dlq(self):
        dofn = _AttachWindowAndKey(key_field_names=None, domain="widgets")
        kv = ("widget.created", {"__agg_error__": True, "_error": "boom"})
        results = list(dofn.process(kv, window=self._fake_window()))
        tagged = [r for r in results if isinstance(r, _TaggedOutput)]
        assert len(tagged) == 1
        assert tagged[0].tag == "dlq"
        assert tagged[0].value["_error"] == "boom"


# ── DomainSpec validation ──────────────────────────────────────────────────────

class TestDomainSpec:
    def test_minimal_spec_is_valid(self):
        spec = DomainSpec(name="widgets", topic="widget-events", raw_table="raw.widgets")
        assert spec.enriched_table is None

    def test_full_aggregation_spec_is_valid(self):
        spec = DomainSpec(
            name="widgets", topic="widget-events", raw_table="raw.widgets",
            enriched_table="enriched.widgets_5min",
            key_fn=lambda e: e["event_type"],
            aggregate_fn=lambda: object(),
        )
        assert spec.enriched_table == "enriched.widgets_5min"

    def test_partial_aggregation_config_raises(self):
        with pytest.raises(ValueError, match="must all be set together"):
            DomainSpec(
                name="widgets", topic="widget-events", raw_table="raw.widgets",
                enriched_table="enriched.widgets_5min",
                # key_fn and aggregate_fn missing
            )

    def test_alert_evaluator_without_aggregation_raises(self):
        with pytest.raises(ValueError, match="requires windowed aggregation"):
            DomainSpec(
                name="widgets", topic="widget-events", raw_table="raw.widgets",
                alert_evaluator=lambda agg: [],
            )

    def test_key_field_names_without_aggregation_raises(self):
        with pytest.raises(ValueError, match="requires windowed aggregation"):
            DomainSpec(
                name="widgets", topic="widget-events", raw_table="raw.widgets",
                key_field_names=("event_type",),
            )

    def test_key_field_names_with_aggregation_is_valid(self):
        spec = DomainSpec(
            name="widgets", topic="widget-events", raw_table="raw.widgets",
            enriched_table="enriched.widgets_5min",
            key_fn=lambda e: e["event_type"],
            aggregate_fn=lambda: object(),
            key_field_names=("event_type",),
        )
        assert spec.key_field_names == ("event_type",)

    def test_default_windowing_fields(self):
        spec = DomainSpec(name="widgets", topic="widget-events", raw_table="raw.widgets")
        assert spec.allowed_lateness_secs == 0
        assert spec.early_firing_secs is None
        assert spec.accumulation_mode == "discarding"

    def test_invalid_accumulation_mode_raises(self):
        with pytest.raises(ValueError, match="accumulation_mode must be"):
            DomainSpec(
                name="widgets", topic="widget-events", raw_table="raw.widgets",
                enriched_table="enriched.widgets_5min",
                key_fn=lambda e: e["event_type"],
                aggregate_fn=lambda: object(),
                accumulation_mode="bogus",
            )

    def test_lateness_config_without_aggregation_raises(self):
        with pytest.raises(ValueError, match="requires windowed aggregation"):
            DomainSpec(
                name="widgets", topic="widget-events", raw_table="raw.widgets",
                allowed_lateness_secs=60,
            )

    def test_early_firing_config_without_aggregation_raises(self):
        with pytest.raises(ValueError, match="requires windowed aggregation"):
            DomainSpec(
                name="widgets", topic="widget-events", raw_table="raw.widgets",
                early_firing_secs=30,
            )

    def test_windowing_config_with_aggregation_is_valid(self):
        spec = DomainSpec(
            name="widgets", topic="widget-events", raw_table="raw.widgets",
            enriched_table="enriched.widgets_5min",
            key_fn=lambda e: e["event_type"],
            aggregate_fn=lambda: object(),
            allowed_lateness_secs=120,
            early_firing_secs=30,
            accumulation_mode="accumulating",
        )
        assert spec.allowed_lateness_secs == 120
        assert spec.early_firing_secs == 30
        assert spec.accumulation_mode == "accumulating"


# ── Metrics shim ─────────────────────────────────────────────────────────────

class TestMetricsShim:
    def test_counter_and_distribution_noop(self):
        Metrics.counter("ns", "name").inc()
        Metrics.counter("ns", "name").dec()
        Metrics.distribution("ns", "name").update(5)


# ── build_streaming_pipeline schema validation ──────────────────────────────
# These raise before build_streaming_pipeline ever touches beam.Pipeline, so
# they're safe to call directly (unlike the rest of the function, which
# isn't unit-tested — see tests/test_dedup_integration.py and
# tests/test_aggregate_integration.py, which mirror its composition instead
# of calling it, precisely to avoid needing a real running pipeline).

class TestBuildStreamingPipelineSchemaValidation:
    def test_raw_table_schema_required_for_storage_write_api(self):
        spec = DomainSpec(name="widgets", topic="widget-events", raw_table="raw.widgets")
        with pytest.raises(ValueError, match="raw_table_schema is required"):
            build_streaming_pipeline("test-project", [spec], None, options=None)

    def test_enriched_table_schema_required_for_storage_write_api(self):
        spec = DomainSpec(
            name="widgets", topic="widget-events", raw_table="raw.widgets",
            raw_table_schema={"fields": []},
            enriched_table="enriched.widgets_5min",
            key_fn=lambda e: e["event_type"],
            aggregate_fn=lambda: object(),
        )
        with pytest.raises(ValueError, match="enriched_table_schema is required"):
            build_streaming_pipeline("test-project", [spec], None, options=None)

    def test_alerts_table_schema_required_for_storage_write_api(self):
        spec = DomainSpec(
            name="widgets", topic="widget-events", raw_table="raw.widgets",
            raw_table_schema={"fields": []},
            enriched_table="enriched.widgets_5min",
            enriched_table_schema={"fields": []},
            key_fn=lambda e: e["event_type"],
            aggregate_fn=lambda: object(),
            alert_evaluator=lambda agg: [],
        )
        with pytest.raises(ValueError, match="alerts_table_schema is required"):
            build_streaming_pipeline("test-project", [spec], "raw.alerts", options=None)
