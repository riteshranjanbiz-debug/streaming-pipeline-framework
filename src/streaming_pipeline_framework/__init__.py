from .framework import (
    DomainSpec,
    InactivityDetector,
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

# Note: servicenow.ServiceNowClient and sfmc.SFMCClient are deliberately NOT
# re-exported here — both require `requests` (the `servicenow`/`sfmc`
# extras). Import them directly:
#   from streaming_pipeline_framework.servicenow import ServiceNowClient
#   from streaming_pipeline_framework.sfmc import SFMCClient
# so importing this package's core stays dependency-free.

__all__ = [
    "DomainSpec",
    "InactivityDetector",
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
