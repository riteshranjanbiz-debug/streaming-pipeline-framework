"""
Tests for the ServiceNow client, entirely mocked via `responses` — no real
ServiceNow instance needed or contacted.
"""

import pytest
import responses

from streaming_pipeline_framework.servicenow import ServiceNowClient, ServiceNowError

INSTANCE = "https://example.service-now.com"


def _client() -> ServiceNowClient:
    return ServiceNowClient(instance_url=INSTANCE, client_id="cid", client_secret="secret")


def _mock_token(expires_in=1800):
    responses.add(
        responses.POST,
        f"{INSTANCE}/oauth_token.do",
        json={"access_token": "tok-123", "expires_in": expires_in, "token_type": "Bearer"},
        status=200,
    )


class TestFromEnv:
    def test_reads_required_vars(self):
        env = {
            "SERVICENOW_INSTANCE_URL": INSTANCE,
            "SERVICENOW_CLIENT_ID": "cid",
            "SERVICENOW_CLIENT_SECRET": "secret",
        }
        client = ServiceNowClient.from_env(env)
        assert client.instance_url == INSTANCE
        assert client.client_id == "cid"

    def test_missing_vars_raises(self):
        with pytest.raises(ServiceNowError, match="Missing required env vars"):
            ServiceNowClient.from_env({"SERVICENOW_INSTANCE_URL": INSTANCE})

    def test_trailing_slash_stripped(self):
        client = ServiceNowClient(f"{INSTANCE}/", "cid", "secret")
        assert client.instance_url == INSTANCE


class TestAuthenticate:
    @responses.activate
    def test_fetches_and_caches_token(self):
        _mock_token()
        client = _client()
        token1 = client._authenticate()
        token2 = client._authenticate()  # should reuse cache, not call again
        assert token1 == "tok-123"
        assert token2 == "tok-123"
        assert len(responses.calls) == 1

    @responses.activate
    def test_refetches_after_expiry(self):
        _mock_token(expires_in=-100)  # already expired
        responses.add(
            responses.POST,
            f"{INSTANCE}/oauth_token.do",
            json={"access_token": "tok-456", "expires_in": 1800},
            status=200,
        )
        client = _client()
        first = client._authenticate()
        second = client._authenticate()
        assert first == "tok-123"
        assert second == "tok-456"
        assert len(responses.calls) == 2

    @responses.activate
    def test_auth_failure_raises(self):
        responses.add(
            responses.POST, f"{INSTANCE}/oauth_token.do", json={"error": "invalid_client"}, status=401
        )
        with pytest.raises(ServiceNowError, match="OAuth token request failed"):
            _client()._authenticate()


class TestCreateIncident:
    @responses.activate
    def test_creates_incident_and_returns_result(self):
        _mock_token()
        responses.add(
            responses.POST,
            f"{INSTANCE}/api/now/table/incident",
            json={"result": {"sys_id": "abc123", "number": "INC0001234"}},
            status=201,
        )
        client = _client()
        result = client.create_incident("Pipeline down", "details here", urgency="1")
        assert result["number"] == "INC0001234"

        # verify the actual request body and auth header
        incident_call = responses.calls[1]
        assert incident_call.request.headers["Authorization"] == "Bearer tok-123"
        import json as _json
        body = _json.loads(incident_call.request.body)
        assert body["short_description"] == "Pipeline down"
        assert body["urgency"] == "1"

    @responses.activate
    def test_extra_fields_passed_through(self):
        _mock_token()
        responses.add(
            responses.POST,
            f"{INSTANCE}/api/now/table/incident",
            json={"result": {"sys_id": "x"}},
            status=201,
        )
        client = _client()
        client.create_incident("x", assignment_group="data-platform")
        import json as _json
        body = _json.loads(responses.calls[1].request.body)
        assert body["assignment_group"] == "data-platform"

    @responses.activate
    def test_creation_failure_raises(self):
        _mock_token()
        responses.add(
            responses.POST,
            f"{INSTANCE}/api/now/table/incident",
            json={"error": {"message": "bad request"}},
            status=400,
        )
        with pytest.raises(ServiceNowError, match="Incident creation failed"):
            _client().create_incident("x")
