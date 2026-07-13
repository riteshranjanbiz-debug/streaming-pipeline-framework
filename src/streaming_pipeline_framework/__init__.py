from .framework import (
    DomainSpec,
    ParseMessage,
    ValidateEvent,
    EnrichEvent,
    StripInternalFields,
    DetectAlerts,
    build_streaming_pipeline,
    beam,
)
from .health import (
    DlqCheckResult,
    IncidentNotifier,
    check_dlq_thresholds,
    run_with_incident_on_failure,
)

# Note: servicenow.ServiceNowClient is deliberately NOT re-exported here —
# it requires `requests` (the `servicenow` extra). Import it directly:
#   from streaming_pipeline_framework.servicenow import ServiceNowClient
# so importing this package's core stays dependency-free.

__all__ = [
    "DomainSpec",
    "ParseMessage",
    "ValidateEvent",
    "EnrichEvent",
    "StripInternalFields",
    "DetectAlerts",
    "build_streaming_pipeline",
    "beam",
    "DlqCheckResult",
    "IncidentNotifier",
    "check_dlq_thresholds",
    "run_with_incident_on_failure",
]
