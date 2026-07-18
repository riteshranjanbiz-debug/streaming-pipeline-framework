"""Reusable CLI plumbing for a pipeline built on this framework.

A downstream project defines its own DomainSpecs and calls `main()` with
them — see examples/retail_orders/pipeline.py for a full example.
"""

from __future__ import annotations

import argparse
import logging

from .framework import DomainSpec, PipelineOptions, StandardOptions, build_streaming_pipeline
from .health import IncidentNotifier, run_with_incident_on_failure


def build_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--region", default="us-central1")
    parser.add_argument(
        "--runner", default="DirectRunner", choices=["DirectRunner", "DataflowRunner"]
    )
    parser.add_argument(
        "--temp-location", default=None, help="GCS path for Dataflow temp, e.g. gs://bucket/tmp"
    )
    parser.add_argument(
        "--service-account-email", default=None, help="Dataflow SA email (DataflowRunner only)"
    )
    return parser


def main(
    domains: list[DomainSpec],
    alerts_table: str | None = None,
    *,
    description: str = "Streaming pipeline",
    window_secs: int = 300,
    pipeline_version: str = "1.0",
    incident_notifier: IncidentNotifier | None = None,
    write_method: str = "STORAGE_WRITE_API",
    triggering_frequency_secs: int = 5,
) -> None:
    """
    Parse standard CLI args and run `build_streaming_pipeline`.

    `incident_notifier`: optional (e.g. a `ServiceNowClient`). If given, any
    uncaught exception during pipeline execution creates an incident before
    re-raising — see `health.run_with_incident_on_failure`. Omit to disable.

    `write_method`/`triggering_frequency_secs`: see
    `build_streaming_pipeline`'s docstring — defaults to the BigQuery
    Storage Write API (exactly-once), not legacy streaming inserts.
    """
    parser = build_arg_parser(description)
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(
        beam_args,
        project=args.project,
        region=args.region,
        runner=args.runner,
        streaming=True,
        save_main_session=True,
        temp_location=args.temp_location,
        service_account_email=args.service_account_email,
    )
    options.view_as(StandardOptions).streaming = True

    logging.basicConfig(level=logging.INFO)

    def _run() -> None:
        build_streaming_pipeline(
            args.project,
            domains,
            alerts_table,
            options,
            window_secs=window_secs,
            pipeline_version=pipeline_version,
            write_method=write_method,
            triggering_frequency_secs=triggering_frequency_secs,
        )

    run_with_incident_on_failure(
        _run, incident_notifier, pipeline_name=description, project=args.project
    )
