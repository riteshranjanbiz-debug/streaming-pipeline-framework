"""Tests for health.run_with_incident_on_failure and health.check_dlq_thresholds."""

import pytest

from streaming_pipeline_framework.framework import DomainSpec
from streaming_pipeline_framework.health import check_dlq_thresholds, run_with_incident_on_failure


class FakeNotifier:
    def __init__(self, raise_on_create: bool = False):
        self.calls: list[dict] = []
        self.raise_on_create = raise_on_create

    def create_incident(self, short_description, description="", **kwargs):
        if self.raise_on_create:
            raise RuntimeError("ServiceNow is down")
        record = {"short_description": short_description, "description": description, **kwargs}
        self.calls.append(record)
        return {"sys_id": "fake", "number": "INC0000001"}


# ── run_with_incident_on_failure ────────────────────────────────────────────

class TestRunWithIncidentOnFailure:
    def test_success_does_not_create_incident(self):
        notifier = FakeNotifier()
        run_with_incident_on_failure(lambda: None, notifier, pipeline_name="p", project="proj")
        assert notifier.calls == []

    def test_failure_creates_incident_and_reraises(self):
        notifier = FakeNotifier()

        def boom():
            raise ValueError("pipeline exploded")

        with pytest.raises(ValueError, match="pipeline exploded"):
            run_with_incident_on_failure(boom, notifier, pipeline_name="orders", project="proj-1")

        assert len(notifier.calls) == 1
        call = notifier.calls[0]
        assert "orders" in call["short_description"]
        assert "pipeline exploded" in call["description"]
        assert "proj-1" in call["description"]
        assert call["urgency"] == "1"

    def test_no_notifier_still_reraises(self):
        def boom():
            raise ValueError("x")

        with pytest.raises(ValueError):
            run_with_incident_on_failure(boom, None, pipeline_name="p", project="proj")

    def test_notifier_failure_does_not_mask_original_exception(self):
        notifier = FakeNotifier(raise_on_create=True)

        def boom():
            raise ValueError("original error")

        with pytest.raises(ValueError, match="original error"):
            run_with_incident_on_failure(boom, notifier, pipeline_name="p", project="proj")


# ── check_dlq_thresholds ────────────────────────────────────────────────────

class FakeRow(dict):
    def __getitem__(self, key):
        return dict.__getitem__(self, key)


class FakeJob:
    def __init__(self, count: int):
        self._count = count

    def result(self):
        return [FakeRow(c=self._count)]


class FakeBqClient:
    def __init__(self, counts_by_table: dict[str, int]):
        self.counts_by_table = counts_by_table
        self.queries: list[str] = []

    def query(self, sql: str):
        self.queries.append(sql)
        for table, count in self.counts_by_table.items():
            if table in sql:
                return FakeJob(count)
        return FakeJob(0)


def _domain(name, dlq_table=None):
    return DomainSpec(name=name, topic=f"{name}-events", raw_table=f"raw.{name}", dlq_table=dlq_table)


class TestCheckDlqThresholds:
    def test_skips_domains_without_dlq_table(self):
        client = FakeBqClient({})
        results = check_dlq_thresholds(client, "proj", [_domain("a")], threshold=10)
        assert results == []
        assert client.queries == []

    def test_below_threshold_no_incident(self):
        client = FakeBqClient({"raw.a_dlq": 3})
        notifier = FakeNotifier()
        results = check_dlq_thresholds(
            client, "proj", [_domain("a", "raw.a_dlq")], threshold=10, notifier=notifier
        )
        assert len(results) == 1
        assert results[0].count == 3
        assert not results[0].exceeded
        assert notifier.calls == []

    def test_at_threshold_creates_incident(self):
        client = FakeBqClient({"raw.a_dlq": 10})
        notifier = FakeNotifier()
        results = check_dlq_thresholds(
            client, "proj", [_domain("a", "raw.a_dlq")], threshold=10, notifier=notifier
        )
        assert results[0].exceeded
        assert len(notifier.calls) == 1
        assert "a" in notifier.calls[0]["short_description"]

    def test_multiple_domains_only_exceeding_ones_alert(self):
        client = FakeBqClient({"raw.a_dlq": 20, "raw.b_dlq": 1})
        notifier = FakeNotifier()
        results = check_dlq_thresholds(
            client, "proj",
            [_domain("a", "raw.a_dlq"), _domain("b", "raw.b_dlq"), _domain("c")],
            threshold=10, notifier=notifier,
        )
        assert len(results) == 2  # domain "c" has no dlq_table, skipped
        assert len(notifier.calls) == 1
        assert "a" in notifier.calls[0]["short_description"]

    def test_no_notifier_still_returns_results(self):
        client = FakeBqClient({"raw.a_dlq": 20})
        results = check_dlq_thresholds(client, "proj", [_domain("a", "raw.a_dlq")], threshold=10)
        assert results[0].exceeded
