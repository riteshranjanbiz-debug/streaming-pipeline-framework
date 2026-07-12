"""
Unit tests for the generic engine. Deliberately uses a synthetic "widget"
domain, not insurance or retail — the point is these transforms don't know
or care what domain they're validating/enriching.

DoFns are tested by calling .process() directly, no Beam runtime required.
"""

import json

import pytest

from streaming_pipeline_framework.framework import (
    DomainSpec,
    ParseMessage,
    ValidateEvent,
    EnrichEvent,
    StripInternalFields,
    DetectAlerts,
    _TaggedOutput,
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
