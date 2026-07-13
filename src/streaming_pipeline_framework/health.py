"""
Pipeline health -> incident creation. Two independent triggers:

  1. run_with_incident_on_failure — wraps pipeline execution; any uncaught
     exception (a DirectRunner crash, or wait_until_finish() raising because
     a Dataflow job reached FAILED) creates an incident immediately, then
     re-raises so the process still exits non-zero as before.

  2. check_dlq_thresholds — a *separate*, periodic check (run it from a
     Cloud Scheduler + Cloud Function/Cloud Run job, or a cron, or your CI)
     that queries each domain's dlq_table for row volume in a recent window.
     This catches pipelines that are alive and not crashing, but silently
     failing to process a chunk of their traffic (bad upstream data,
     a schema drift, etc.) — a hard crash alone would never surface that.

Both accept any object with a `.create_incident(short_description, ...)`
method — a `ServiceNowClient`, a test double, or your own notifier. Neither
imports `servicenow.py`, keeping this module dependency-free.
"""

from __future__ import annotations

import logging
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol

from .framework import DomainSpec

logger = logging.getLogger(__name__)


class IncidentNotifier(Protocol):
    def create_incident(
        self, short_description: str, description: str = "", **kwargs: Any
    ) -> dict[str, Any]: ...


# ═══════════════════════════════════════════════════════════════════════════════
# Trigger 1 — pipeline crash
# ═══════════════════════════════════════════════════════════════════════════════

def run_with_incident_on_failure(
    fn: Callable[[], None],
    notifier: Optional[IncidentNotifier],
    *,
    pipeline_name: str,
    project: str,
) -> None:
    """Runs `fn()`. On any exception: creates an incident (if `notifier` is
    given — pass None to disable), logs if incident creation itself fails
    (never masks the original error), then always re-raises."""
    try:
        fn()
    except Exception as exc:
        if notifier is not None:
            try:
                notifier.create_incident(
                    short_description=f"Pipeline failure: {pipeline_name}",
                    description=(
                        f"Project: {project}\n"
                        f"Pipeline: {pipeline_name}\n"
                        f"Error: {exc}\n\n"
                        f"{traceback.format_exc()}"
                    ),
                    urgency="1",
                    impact="1",
                    category="software",
                )
            except Exception:
                logger.exception(
                    "Failed to create incident for %s pipeline failure "
                    "(original error follows, incident creation failure is logged only)",
                    pipeline_name,
                )
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# Trigger 2 — DLQ volume
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class DlqCheckResult:
    domain: str
    count: int
    threshold: int
    window_minutes: int

    @property
    def exceeded(self) -> bool:
        return self.count >= self.threshold


def check_dlq_thresholds(
    bq_client: Any,
    project: str,
    domains: list[DomainSpec],
    window_minutes: int = 15,
    threshold: int = 10,
    notifier: Optional[IncidentNotifier] = None,
) -> list[DlqCheckResult]:
    """
    For every domain with a `dlq_table` set, counts DLQ rows (by
    `ingested_at`, which every DLQ row has — see framework._dlq) in the last
    `window_minutes` minutes. Domains without `dlq_table` are skipped.

    `bq_client` is any object with a BigQuery-client-shaped `.query(sql)`
    returning a job whose `.result()` yields rows supporting `row["c"]` —
    i.e. a real `google.cloud.bigquery.Client`, or a fake for testing. This
    module never imports google-cloud-bigquery itself, so it stays optional.

    If `notifier` is given, creates one incident per domain whose count
    reaches `threshold`. Always returns the full per-domain result list
    regardless of whether an incident was created, so callers can log/assert
    on it directly.
    """
    results: list[DlqCheckResult] = []

    for spec in domains:
        if not spec.dlq_table:
            continue

        sql = f"""
            SELECT COUNT(*) AS c
            FROM `{project}.{spec.dlq_table}`
            WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(window_minutes)} MINUTE)
        """
        rows = list(bq_client.query(sql).result())
        count = int(rows[0]["c"]) if rows else 0

        result = DlqCheckResult(spec.name, count, threshold, window_minutes)
        results.append(result)

        if result.exceeded and notifier is not None:
            notifier.create_incident(
                short_description=f"DLQ volume threshold exceeded: {spec.name}",
                description=(
                    f"{count} events landed in the '{spec.name}' domain's DLQ "
                    f"({project}.{spec.dlq_table}) in the last {window_minutes} "
                    f"minute(s), at or above the threshold of {threshold}."
                ),
                urgency="2",
                impact="2",
                category="software",
            )

    return results
