"""
Tests for the SFMC client, entirely mocked via `responses` — no real SFMC
instance needed or contacted.
"""

import pytest
import responses

from streaming_pipeline_framework.sfmc import SFMCClient, SFMCError

SUBDOMAIN = "mc-example"
AUTH_BASE = f"https://{SUBDOMAIN}.auth.marketingcloudapis.com"
REST_BASE = f"https://{SUBDOMAIN}.rest.marketingcloudapis.com"


def _client() -> SFMCClient:
    return SFMCClient(subdomain=SUBDOMAIN, client_id="cid", client_secret="secret")


def _mock_token(expires_in=1200):
    responses.add(
        responses.POST,
        f"{AUTH_BASE}/v2/token",
        json={"access_token": "tok-123", "expires_in": expires_in, "token_type": "Bearer"},
        status=200,
    )


class TestFromEnv:
    def test_reads_required_vars(self):
        env = {
            "SFMC_SUBDOMAIN": SUBDOMAIN,
            "SFMC_CLIENT_ID": "cid",
            "SFMC_CLIENT_SECRET": "secret",
        }
        client = SFMCClient.from_env(env)
        assert client.subdomain == SUBDOMAIN
        assert client.client_id == "cid"

    def test_missing_vars_raises(self):
        with pytest.raises(SFMCError, match="Missing required env vars"):
            SFMCClient.from_env({"SFMC_SUBDOMAIN": SUBDOMAIN})


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
            f"{AUTH_BASE}/v2/token",
            json={"access_token": "tok-456", "expires_in": 1200},
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
            responses.POST, f"{AUTH_BASE}/v2/token", json={"error": "invalid_client"}, status=401
        )
        with pytest.raises(SFMCError, match="OAuth token request failed"):
            _client()._authenticate()


class TestSendTransactionalEmail:
    @responses.activate
    def test_sends_and_returns_result(self):
        _mock_token()
        responses.add(
            responses.POST,
            f"{REST_BASE}/messaging/v1/email/messageDefinitionSends/quote-abandon-scenario-2/send",
            json={"requestId": "req-abc123", "responses": [{"messageKey": "msg-1"}]},
            status=202,
        )
        client = _client()
        result = client.send_transactional_email(
            "quote-abandon-scenario-2", contact_key="mdm-001", attributes={"premium": 120.5},
        )
        assert result["requestId"] == "req-abc123"

        send_call = responses.calls[1]
        assert send_call.request.headers["Authorization"] == "Bearer tok-123"
        import json as _json
        body = _json.loads(send_call.request.body)
        assert body["definitionKey"] == "quote-abandon-scenario-2"
        assert body["recipient"]["contactKey"] == "mdm-001"
        assert body["recipient"]["attributes"] == {"premium": 120.5}
        assert "to" not in body["recipient"]

    @responses.activate
    def test_includes_to_email_when_given(self):
        _mock_token()
        responses.add(
            responses.POST,
            f"{REST_BASE}/messaging/v1/email/messageDefinitionSends/scenario-1/send",
            json={"requestId": "req-1"},
            status=200,
        )
        client = _client()
        client.send_transactional_email("scenario-1", contact_key="mdm-002", to_email="a@example.com")
        import json as _json
        body = _json.loads(responses.calls[1].request.body)
        assert body["recipient"]["to"] == "a@example.com"

    @responses.activate
    def test_send_failure_raises(self):
        _mock_token()
        responses.add(
            responses.POST,
            f"{REST_BASE}/messaging/v1/email/messageDefinitionSends/scenario-1/send",
            json={"message": "definition not found"},
            status=404,
        )
        with pytest.raises(SFMCError, match="Transactional send failed"):
            _client().send_transactional_email("scenario-1", contact_key="mdm-003")
