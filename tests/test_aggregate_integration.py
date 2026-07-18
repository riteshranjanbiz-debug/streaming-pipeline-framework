"""
Integration test for the CombineFn-based aggregation composition used in
build_streaming_pipeline (KeyEvent -> CombinePerKey -> _AttachWindowAndKey),
run for real against DirectRunner. Requires apache-beam; skipped entirely if
it isn't installed (the "no apache-beam" CI job never sees this file execute).

This exists because CombinePerKey is a PTransform, not a DoFn — the
partial-combine-before-shuffle behavior can't be verified by calling
.process() directly like the rest of tests/test_framework.py, so it needs an
actual (small, local) pipeline run.
"""

import time
from types import SimpleNamespace

import pytest

pytest.importorskip("apache_beam.testing.util")

import apache_beam as beam  # noqa: E402
from apache_beam.options.pipeline_options import PipelineOptions  # noqa: E402
from apache_beam.testing.test_stream import TestStream  # noqa: E402
from apache_beam.testing.util import assert_that, equal_to  # noqa: E402
from apache_beam.transforms.window import TimestampedValue  # noqa: E402

from streaming_pipeline_framework.framework import (  # noqa: E402
    KeyEvent,
    _AttachWindowAndKey,
    _DlqWrappingCombineFn,
    _window_transform,
)


def _with_now(events):
    """beam.Create doesn't assign real event timestamps, so FixedWindows
    would otherwise window elements near the epoch's negative extreme and
    window.start/end.to_utc_datetime() would overflow. Stamp everything
    with 'now' so windowing behaves like it does against a real Pub/Sub
    source."""
    now = time.time()
    return [TimestampedValue(e, now) for e in events]


class CountByColorCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return 0

    def add_input(self, accumulator, event):
        return accumulator + 1

    def merge_accumulators(self, accumulators):
        return sum(accumulators)

    def extract_output(self, accumulator):
        return {"event_count": accumulator}


@beam.ptransform_fn
def _aggregate_by(
    pcoll, key_fn, key_field_names, domain="widgets", window_secs=60,
    allowed_lateness_secs=0, early_firing_secs=None, accumulation_mode="discarding",
):
    """Mirrors build_streaming_pipeline's aggregate composition exactly,
    including flattening every stage's 'dlq' tagged output into one, the way
    build_streaming_pipeline does for spec.dlq_table. Uses _window_transform
    itself (not a hand-rolled WindowInto) so this also exercises that
    helper, the same one build_streaming_pipeline calls."""
    window_spec = SimpleNamespace(
        allowed_lateness_secs=allowed_lateness_secs,
        early_firing_secs=early_firing_secs,
        accumulation_mode=accumulation_mode,
    )
    keyed = (
        pcoll
        | "Window" >> _window_transform(window_spec, window_secs)
        | "KV" >> beam.ParDo(KeyEvent(key_fn, domain)).with_outputs("dlq", main="ok")
    )
    combined = keyed.ok | "Combine" >> beam.CombinePerKey(
        _DlqWrappingCombineFn(CountByColorCombineFn(), domain)
    )
    agg = combined | "AttachKeyWindow" >> beam.ParDo(
        _AttachWindowAndKey(key_field_names, domain)
    ).with_outputs("dlq", main="ok")
    dlq = (keyed.dlq, agg.dlq) | "FlattenDlq" >> beam.Flatten()
    return SimpleNamespace(ok=agg.ok, dlq=dlq)


class TestAggregateComposition:
    def test_groups_and_counts_by_key(self):
        events = [
            {"color": "red", "v": 1},
            {"color": "red", "v": 2},
            {"color": "blue", "v": 3},
        ]
        with beam.Pipeline(runner="BundleBasedDirectRunner") as p:
            result = (
                p
                | beam.Create(_with_now(events))
                | _aggregate_by(lambda e: e["color"], key_field_names=("color",))
            )
            counts = result.ok | "ExtractCounts" >> beam.Map(
                lambda row: (row["color"], row["event_count"])
            )
            assert_that(counts, equal_to([("red", 2), ("blue", 1)]))

    def test_output_rows_carry_window_fields(self):
        events = [{"color": "red", "v": 1}]
        with beam.Pipeline(runner="BundleBasedDirectRunner") as p:
            result = (
                p
                | beam.Create(_with_now(events))
                | _aggregate_by(lambda e: e["color"], key_field_names=("color",))
            )
            has_window_fields = result.ok | "CheckWindowFields" >> beam.Map(
                lambda row: ("window_start" in row and "window_end" in row)
            )
            assert_that(has_window_fields, equal_to([True]))

    def test_key_fn_failure_routes_to_dlq(self):
        events = [{"color": "red"}, {"no_color": True}]
        with beam.Pipeline(runner="BundleBasedDirectRunner") as p:
            result = (
                p
                | beam.Create(_with_now(events))
                | _aggregate_by(lambda e: e["color"], key_field_names=("color",))
            )
            dlq_count = (
                result.dlq
                | "CountDlq" >> beam.combiners.Count.Globally().without_defaults()
            )
            assert_that(dlq_count, equal_to([1]))


class TestWindowingTriggers:
    """Exercises _window_transform's trigger/lateness configuration with
    apache_beam.testing.test_stream.TestStream — the standard Beam idiom for
    deterministically advancing watermark and processing time, since none of
    this is observable via .process()-direct calls on a single element."""

    WINDOW_SECS = 60

    def test_early_firing_emits_more_than_one_pane(self):
        test_stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([TimestampedValue({"color": "red"}, 1)])
            .advance_processing_time(15)
            .add_elements([TimestampedValue({"color": "red"}, 2)])
            .advance_watermark_to(self.WINDOW_SECS + 1)
            .advance_processing_time(15)
        )
        with beam.Pipeline(runner="BundleBasedDirectRunner", options=PipelineOptions(streaming=True)) as p:
            result = (
                p
                | test_stream
                | _aggregate_by(
                    lambda e: e["color"], key_field_names=("color",),
                    window_secs=self.WINDOW_SECS, early_firing_secs=10,
                )
            )
            # assert_that materializes the whole PCollection regardless of
            # windowing, so no manual Count/GroupByKey is needed here (that
            # would hit "GroupByKey cannot be applied to an unbounded
            # PCollection with global windowing and a default trigger" for a
            # TestStream-sourced PCollection). The exact pane count is
            # trigger-timing-dependent (an early trigger can re-fire with no
            # new data and emit an empty pane), so assert "more than one"
            # rather than an exact number.
            rows = result.ok | "ExtractCounts" >> beam.Map(lambda row: row["event_count"])

            def _more_than_one_pane(actual):
                values = list(actual)
                assert len(values) > 1, f"expected more than one pane, got {values}"

            assert_that(rows, _more_than_one_pane)

    def test_late_element_within_allowed_lateness_still_fires(self):
        test_stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([TimestampedValue({"color": "red"}, 1)])
            .advance_watermark_to(self.WINDOW_SECS + 1)  # closes window, on-time firing
            .add_elements([TimestampedValue({"color": "red"}, 2)])  # late, within lateness
            .advance_watermark_to(self.WINDOW_SECS + 30)  # re-fires due to late data
        )
        with beam.Pipeline(runner="BundleBasedDirectRunner", options=PipelineOptions(streaming=True)) as p:
            result = (
                p
                | test_stream
                | _aggregate_by(
                    lambda e: e["color"], key_field_names=("color",),
                    window_secs=self.WINDOW_SECS, allowed_lateness_secs=60,
                    accumulation_mode="accumulating",
                )
            )
            rows = result.ok | "ExtractCounts" >> beam.Map(lambda row: row["event_count"])

            def _reaches_two(actual):
                values = list(actual)
                assert max(values) == 2, f"expected max event_count 2, got {values}"

            assert_that(rows, _reaches_two)

    def test_late_element_past_allowed_lateness_is_dropped(self):
        test_stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([TimestampedValue({"color": "red"}, 1)])
            .advance_watermark_to(self.WINDOW_SECS + 1)  # on-time firing
            .advance_watermark_to(self.WINDOW_SECS + 61)  # window GC'd (lateness=60)
            .add_elements([TimestampedValue({"color": "red"}, 2)])  # too late
            .advance_watermark_to(self.WINDOW_SECS + 120)
        )
        with beam.Pipeline(runner="BundleBasedDirectRunner", options=PipelineOptions(streaming=True)) as p:
            result = (
                p
                | test_stream
                | _aggregate_by(
                    lambda e: e["color"], key_field_names=("color",),
                    window_secs=self.WINDOW_SECS, allowed_lateness_secs=60,
                    accumulation_mode="accumulating",
                )
            )
            rows = result.ok | "ExtractCounts" >> beam.Map(lambda row: row["event_count"])

            def _stays_at_one(actual):
                values = list(actual)
                assert max(values) == 1, (
                    f"expected max event_count 1 (late element dropped), got {values}"
                )

            assert_that(rows, _stays_at_one)
