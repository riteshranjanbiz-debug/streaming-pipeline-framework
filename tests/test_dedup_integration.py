"""
Integration test for the dedup composition used in build_streaming_pipeline
(Map-to-key -> DeduplicatePerKey -> Map-to-unkey), run for real against
DirectRunner. Requires apache-beam; skipped entirely if it isn't installed
(the "no apache-beam" CI job never sees this file execute).

This exists because DeduplicatePerKey is a PTransform, not a DoFn — it can't
be unit-tested by calling .process() directly like the rest of
tests/test_framework.py, so it needs an actual (small, local) pipeline run.
"""

import pytest

pytest.importorskip("apache_beam.testing.util")

import apache_beam as beam  # noqa: E402
from apache_beam.testing.util import assert_that, equal_to  # noqa: E402

from streaming_pipeline_framework.framework import DeduplicatePerKey  # noqa: E402


@beam.ptransform_fn
def _dedup_by(pcoll, key_fn, window_secs=60):
    """Mirrors build_streaming_pipeline's dedup composition exactly."""
    return (
        pcoll
        | "Key" >> beam.Map(lambda e: (key_fn(e), e))
        | "Dedup" >> DeduplicatePerKey(processing_time_duration=window_secs)
        | "Unkey" >> beam.Map(lambda kv: kv[1])
    )


class TestDedupComposition:
    def test_removes_exact_duplicates_by_key(self):
        events = [
            {"event_id": "a", "v": 1},
            {"event_id": "a", "v": 1},  # duplicate — same key, redelivered
            {"event_id": "b", "v": 2},
        ]
        with beam.Pipeline() as p:
            result = p | beam.Create(events) | _dedup_by(lambda e: e["event_id"])
            assert_that(
                result,
                equal_to([{"event_id": "a", "v": 1}, {"event_id": "b", "v": 2}]),
            )

    def test_keeps_all_when_keys_distinct(self):
        events = [{"event_id": str(i), "v": i} for i in range(5)]
        with beam.Pipeline() as p:
            result = p | beam.Create(events) | _dedup_by(lambda e: e["event_id"])
            assert_that(result, equal_to(events))

    def test_triplicate_collapses_to_one(self):
        events = [{"event_id": "x", "v": 1}] * 3
        with beam.Pipeline() as p:
            result = p | beam.Create(events) | _dedup_by(lambda e: e["event_id"])
            assert_that(result, equal_to([{"event_id": "x", "v": 1}]))
