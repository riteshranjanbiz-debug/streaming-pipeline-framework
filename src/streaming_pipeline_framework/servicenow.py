"""
ServiceNow incident creation, via OAuth client-credentials auth against the
REST Table API.

Optional module — requires `pip install streaming-pipeline-framework[servicenow]`
(pulls in `requests`). Not imported by `framework.py` or `cli.py`; only
imported if you actually use it, so pipelines that don't want ServiceNow
integration pay no cost.

ServiceNow-side setup (once, by a ServiceNow admin): System OAuth > Application
Registry > create an OAuth API endpoint for external clients, grant type
"Client Credentials". The resulting client_id/client_secret go into
SERVICENOW_CLIENT_ID / SERVICENOW_CLIENT_SECRET, never in code.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

import requests


class ServiceNowError(RuntimeError):
    """Raised when OAuth authentication or incident creation fails."""


class ServiceNowClient:
    def __init__(
        self,
        instance_url: str,
        client_id: str,
        client_secret: str,
        timeout: float = 10.0,
    ):
        self.instance_url = instance_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> "ServiceNowClient":
        """Reads SERVICENOW_INSTANCE_URL, SERVICENOW_CLIENT_ID, SERVICENOW_CLIENT_SECRET."""
        env = env if env is not None else os.environ
        required = ("SERVICENOW_INSTANCE_URL", "SERVICENOW_CLIENT_ID", "SERVICENOW_CLIENT_SECRET")
        missing = [k for k in required if not env.get(k)]
        if missing:
            raise ServiceNowError(f"Missing required env vars: {', '.join(missing)}")
        return cls(
            instance_url=env["SERVICENOW_INSTANCE_URL"],
            client_id=env["SERVICENOW_CLIENT_ID"],
            client_secret=env["SERVICENOW_CLIENT_SECRET"],
        )

    def _authenticate(self) -> str:
        """Fetches (and caches) an OAuth access token via the client_credentials grant."""
        if self._token and time.time() < self._token_expires_at:
            return self._token

        resp = requests.post(
            f"{self.instance_url}/oauth_token.do",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            raise ServiceNowError(f"OAuth token request failed: {resp.status_code} {resp.text}")

        body = resp.json()
        self._token = body["access_token"]
        # refresh 30s early to avoid racing expiry
        self._token_expires_at = time.time() + int(body.get("expires_in", 1800)) - 30
        return self._token

    def create_incident(
        self,
        short_description: str,
        description: str = "",
        urgency: str = "2",
        impact: str = "2",
        category: str = "software",
        **extra_fields: Any,
    ) -> dict[str, Any]:
        """
        Creates an incident via POST /api/now/table/incident.
        urgency/impact: ServiceNow's 1 (high) - 3 (low) scale, as strings.
        extra_fields: any other incident table fields (e.g. assignment_group).
        Returns the created incident record (includes sys_id, number).
        """
        token = self._authenticate()
        payload = {
            "short_description": short_description,
            "description": description,
            "urgency": urgency,
            "impact": impact,
            "category": category,
            **extra_fields,
        }
        resp = requests.post(
            f"{self.instance_url}/api/now/table/incident",
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=self.timeout,
        )
        if resp.status_code not in (200, 201):
            raise ServiceNowError(f"Incident creation failed: {resp.status_code} {resp.text}")

        return resp.json()["result"]
