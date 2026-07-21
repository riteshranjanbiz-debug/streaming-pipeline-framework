"""
Salesforce Marketing Cloud (SFMC) Transactional Messaging client, via OAuth2
client-credentials auth against SFMC's REST API.

Optional module — requires `pip install streaming-pipeline-framework[sfmc]`
(pulls in `requests`). Not imported by `framework.py` or `cli.py`; only
imported if you actually use it, so pipelines that don't want SFMC
integration pay no cost.

Uses the Transactional Messaging API (one immediate "send this email now"
call against a pre-built Send Definition) rather than Journey Builder's
Track Event API (which enrolls a contact in an ongoing, multi-step
journey). That's a deliberate scope boundary: this client's job is to fire
a single triggered send in response to something this pipeline detected
(e.g. inferred cart/application abandonment) — any multi-day cadence, exit
criteria, or re-entry logic belongs to a Journey Builder journey (or a
separate scheduled job), not to a Beam pipeline.

SFMC-side setup (once, by an SFMC admin): Setup > Apps > Installed Packages
> create a package with a Server-to-Server API integration component,
grant the "Email > Send" permission the Send Definition needs. The
resulting client_id/client_secret and your subdomain go into
SFMC_CLIENT_ID / SFMC_CLIENT_SECRET / SFMC_SUBDOMAIN, never in code. A
marketer separately builds the Send Definition (template + from-address +
send classification) in Content Builder / Email Studio; this client only
ever references it by its definition key.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

import requests


class SFMCError(RuntimeError):
    """Raised when OAuth authentication or a send fails."""


class SFMCClient:
    def __init__(
        self,
        subdomain: str,
        client_id: str,
        client_secret: str,
        timeout: float = 10.0,
    ):
        self.subdomain = subdomain
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    @property
    def _auth_base(self) -> str:
        return f"https://{self.subdomain}.auth.marketingcloudapis.com"

    @property
    def _rest_base(self) -> str:
        return f"https://{self.subdomain}.rest.marketingcloudapis.com"

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> "SFMCClient":
        """Reads SFMC_SUBDOMAIN, SFMC_CLIENT_ID, SFMC_CLIENT_SECRET."""
        env = env if env is not None else os.environ
        required = ("SFMC_SUBDOMAIN", "SFMC_CLIENT_ID", "SFMC_CLIENT_SECRET")
        missing = [k for k in required if not env.get(k)]
        if missing:
            raise SFMCError(f"Missing required env vars: {', '.join(missing)}")
        return cls(
            subdomain=env["SFMC_SUBDOMAIN"],
            client_id=env["SFMC_CLIENT_ID"],
            client_secret=env["SFMC_CLIENT_SECRET"],
        )

    def _authenticate(self) -> str:
        """Fetches (and caches) an OAuth access token via the client_credentials grant."""
        if self._token and time.time() < self._token_expires_at:
            return self._token

        resp = requests.post(
            f"{self._auth_base}/v2/token",
            json={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            raise SFMCError(f"OAuth token request failed: {resp.status_code} {resp.text}")

        body = resp.json()
        self._token = body["access_token"]
        # refresh 30s early to avoid racing expiry
        self._token_expires_at = time.time() + int(body.get("expires_in", 1200)) - 30
        return self._token

    def send_transactional_email(
        self,
        send_definition_key: str,
        contact_key: str,
        to_email: Optional[str] = None,
        attributes: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Triggers one send via POST
        /messaging/v1/email/messageDefinitionSends/{send_definition_key}/send.

        `send_definition_key`: the Send Definition's external key, built by a
        marketer in Content Builder — this client never defines content.
        `contact_key`: identifies the recipient in SFMC's subscriber data
        (use your customer's stable identifier, e.g. an mdmId) — SFMC
        resolves the actual email address from its own subscriber record
        unless `to_email` is also supplied.
        `attributes`: merge fields available to the template
        (AMPscript/Content Builder personalization strings) — e.g. quote
        premium, product type, a resume link.
        Returns the send response (includes SFMC's requestId).
        """
        token = self._authenticate()
        recipient: dict[str, Any] = {"contactKey": contact_key, "attributes": attributes or {}}
        if to_email:
            recipient["to"] = to_email

        resp = requests.post(
            f"{self._rest_base}/messaging/v1/email/messageDefinitionSends/{send_definition_key}/send",
            json={"definitionKey": send_definition_key, "recipient": recipient},
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
        )
        if resp.status_code not in (200, 202):
            raise SFMCError(f"Transactional send failed: {resp.status_code} {resp.text}")

        return resp.json()
