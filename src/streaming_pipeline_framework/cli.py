"""Reusable CLI plumbing for a pipeline built on this framework.

A downstream project defines its own DomainSpecs and calls `main()` with
them — see examples/retail_orders/pipeline.py for a full example.
"""

from __future__ import annotations

import argparse
import logging

from .framework import DomainSpec, PipelineOptions, StandardOptions, build_streaming_pipeline
from .health import IncidentNotifier, run_with_incident_on_failure

# DataflowRunner workers run in a fresh container with only apache-beam
# installed — save_main_session (set unconditionally in main()) stages
# __main__'s state, but never the streaming_pipeline_framework package
# itself, since it's a separate installed module, not part of __main__.
# Without one of these flags, every worker bundle fails with
# ModuleNotFoundError: the job sits at RUNNING with zero throughput and no
# obvious error unless you go digging in Cloud Logging — fail fast at
# submission time instead. See _check_dataflow_worker_packaging.
_DATAFLOW_WORKER_PACKAGING_FLAGS = (
    "--extra_package",
    "--setup_file",
    "--sdk_location",
    "--sdk_container_image",
)


def _check_dataflow_worker_packaging(runner: str, beam_args: list[str]) -> None:
    if runner != "DataflowRunner":
        return
    if any(
        arg == flag or arg.startswith(f"{flag}=")
        for arg in beam_args
        for flag in _DATAFLOW_WORKER_PACKAGING_FLAGS
    ):
        return
    raise SystemExit(
        "DataflowRunner needs streaming_pipeline_framework staged onto workers "
        "explicitly -- save_main_session does not do this (it only captures "
        "__main__'s state). Build a wheel of this package and pass it via "
        "--extra_package:\n\n"
        "  pip wheel . -w dist/ --no-deps\n"
        "  python -m your.pipeline ... --extra_package dist/streaming_pipeline_framework-*.whl\n\n"
        "Or pass --setup_file/--sdk_location/--sdk_container_image yourself if "
        "you already have another way to get the package onto workers."
    )


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
    alerts_table_schema: dict | None = None,
) -> None:
    """
    Parse standard CLI args and run `build_streaming_pipeline`.

    `incident_notifier`: optional (e.g. a `ServiceNowClient`). If given, any
    uncaught exception during pipeline execution creates an incident before
    re-raising — see `health.run_with_incident_on_failure`. Omit to disable.

    `write_method`/`triggering_frequency_secs`/`alerts_table_schema`: see
    `build_streaming_pipeline`'s docstring — defaults to the BigQuery
    Storage Write API (exactly-once), not legacy streaming inserts, which
    means `alerts_table_schema` and each domain's `raw_table_schema`/
    `enriched_table_schema` are required.
    """
    parser = build_arg_parser(description)
    args, beam_args = parser.parse_known_args()
    _check_dataflow_worker_packaging(args.runner, beam_args)

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
            alerts_table_schema=alerts_table_schema,
        )

    run_with_incident_on_failure(
        _run, incident_notifier, pipeline_name=description, project=args.project
    )
