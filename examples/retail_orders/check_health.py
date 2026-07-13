"""
Periodic DLQ-volume health check for the retail_orders example. Run this on
a schedule (Cloud Scheduler + Cloud Run/Function, or plain cron) — it's a
separate process from the streaming pipeline itself, since a pipeline that's
alive and not crashing can still be silently losing a slice of its traffic.

  python -m examples.retail_orders.check_health --project <gcp-project>
"""

from __future__ import annotations

import argparse

from google.cloud import bigquery

from streaming_pipeline_framework.health import check_dlq_thresholds
from streaming_pipeline_framework.servicenow import ServiceNowClient

from .pipeline import ORDERS_DOMAIN


def main() -> None:
    parser = argparse.ArgumentParser(description="Check DLQ volume and alert ServiceNow")
    parser.add_argument("--project", required=True)
    parser.add_argument("--window-minutes", type=int, default=15)
    parser.add_argument("--threshold", type=int, default=10)
    args = parser.parse_args()

    bq_client = bigquery.Client(project=args.project)
    notifier = ServiceNowClient.from_env()

    results = check_dlq_thresholds(
        bq_client,
        args.project,
        [ORDERS_DOMAIN],
        window_minutes=args.window_minutes,
        threshold=args.threshold,
        notifier=notifier,
    )
    for r in results:
        status = "EXCEEDED -> incident created" if r.exceeded else "ok"
        print(f"{r.domain}: {r.count}/{r.threshold} in last {r.window_minutes}m — {status}")


if __name__ == "__main__":
    main()
