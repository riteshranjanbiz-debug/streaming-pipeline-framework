"""
Unit tests for examples/insurance_quotes/pipeline.py's abandonment-inference
logic — the reducer, pending check, and scenario classification are the
one piece of real business logic in this example worth dedicated coverage
(the two CombineFns and DomainSpecs follow the same pattern already
covered by tests/test_framework.py and retail_orders' CI sanity check).
"""

from examples.insurance_quotes.pipeline import (
    PAYLOAD_REQUIRED,
    _quote_state_reducer,
    _quote_is_pending,
    _quote_abandoned_event,
    _trigger_sfmc_abandonment_email,
)


def _event(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "payload": {"mdm_id": "MDM-1", "product_type": "auto", **payload}}


class TestQuoteStateReducer:
    def test_initial_state_is_pre_quote(self):
        state = _quote_state_reducer(None, _event("pre-quote:initiated"))
        assert state["stage"] == "pre_quote"
        assert state["bound"] is False

    def test_person_and_address_events_stay_pre_quote(self):
        state = _quote_state_reducer(None, _event("pre-quote:initiated"))
        state = _quote_state_reducer(state, _event("pre-quote:person-add"))
        state = _quote_state_reducer(state, _event("pre-quote:address-add"))
        state = _quote_state_reducer(state, _event("pre-quote:confirmed"))
        assert state["stage"] == "pre_quote"

    def test_quoted_advances_to_quoted_stage(self):
        state = _quote_state_reducer(None, _event("quote:quoted", premium=120.5))
        assert state["stage"] == "quoted"
        assert state["premium"] == 120.5

    def test_recalculate_stays_at_quoted_stage_and_updates_premium(self):
        state = _quote_state_reducer(None, _event("quote:quoted", premium=100.0))
        state = _quote_state_reducer(state, _event("quote:recalculate", premium=90.0))
        assert state["stage"] == "quoted"
        assert state["premium"] == 90.0

    def test_payment_initiated_advances_to_payment_stage(self):
        state = _quote_state_reducer(None, _event("quote:quoted", premium=100.0))
        state = _quote_state_reducer(state, _event("quote:payment-initiated"))
        assert state["stage"] == "payment"

    def test_bind_sets_bound_without_downgrading_stage(self):
        state = _quote_state_reducer(None, _event("quote:quoted"))
        state = _quote_state_reducer(state, _event("quote:payment-initiated"))
        state = _quote_state_reducer(state, _event("post-quote:bind"))
        assert state["bound"] is True
        assert state["stage"] == "payment"

    def test_stage_never_downgrades_on_a_late_pre_quote_event(self):
        state = _quote_state_reducer(None, _event("quote:payment-initiated"))
        assert state["stage"] == "payment"
        state = _quote_state_reducer(state, _event("pre-quote:person-update"))
        assert state["stage"] == "payment"  # would be a bug if this regressed to pre_quote

    def test_does_not_mutate_input_state(self):
        state = _quote_state_reducer(None, _event("pre-quote:initiated"))
        _quote_state_reducer(state, _event("quote:quoted"))
        assert state["stage"] == "pre_quote"  # original dict untouched

    def test_carries_forward_mdm_id_and_product_type(self):
        state = _quote_state_reducer(None, _event("pre-quote:initiated", mdm_id="MDM-42", product_type="bundled"))
        state = _quote_state_reducer(state, {"event_type": "pre-quote:person-add", "payload": {}})
        assert state["mdm_id"] == "MDM-42"
        assert state["product_type"] == "bundled"


class TestQuoteIsPending:
    def test_none_state_is_not_pending(self):
        assert _quote_is_pending(None) is False

    def test_unbound_state_is_pending(self):
        state = _quote_state_reducer(None, _event("pre-quote:initiated"))
        assert _quote_is_pending(state) is True

    def test_bound_state_is_not_pending(self):
        state = _quote_state_reducer(None, _event("post-quote:bind"))
        assert _quote_is_pending(state) is False


class TestQuoteAbandonedEvent:
    def test_classifies_scenario_1_when_pre_quote(self):
        state = _quote_state_reducer(None, _event("pre-quote:initiated"))
        event = _quote_abandoned_event(("QT-1",), state)
        assert event["payload"]["scenario"] == "scenario_1"

    def test_classifies_scenario_2_when_quoted(self):
        state = _quote_state_reducer(None, _event("quote:quoted", premium=50.0))
        event = _quote_abandoned_event(("QT-1",), state)
        assert event["payload"]["scenario"] == "scenario_2"
        assert event["payload"]["premium"] == 50.0

    def test_classifies_scenario_3_when_payment_reached(self):
        state = _quote_state_reducer(None, _event("quote:quoted"))
        state = _quote_state_reducer(state, _event("quote:payment-initiated"))
        event = _quote_abandoned_event(("QT-1",), state)
        assert event["payload"]["scenario"] == "scenario_3"

    def test_reengagement_reclassifies_to_higher_scenario(self):
        # The Day-3-return-and-progress-further case from the capability
        # doc: same quote_id, first classified at scenario_2, customer
        # returns and advances to payment before abandoning again — the
        # next synthetic event should reflect scenario_3, not scenario_2.
        state = _quote_state_reducer(None, _event("quote:quoted"))
        first = _quote_abandoned_event(("QT-1",), state)
        assert first["payload"]["scenario"] == "scenario_2"

        state = _quote_state_reducer(state, _event("quote:payment-initiated"))
        second = _quote_abandoned_event(("QT-1",), state)
        assert second["payload"]["scenario"] == "scenario_3"

    def test_synthetic_event_satisfies_payload_required(self):
        state = _quote_state_reducer(None, _event("quote:quoted"))
        event = _quote_abandoned_event(("QT-1",), state)
        assert PAYLOAD_REQUIRED <= event["payload"].keys()

    def test_unknown_mdm_id_falls_back_to_placeholder(self):
        state = _quote_state_reducer(None, {"event_type": "pre-quote:initiated", "payload": {}})
        event = _quote_abandoned_event(("QT-1",), state)
        assert event["payload"]["mdm_id"] == "unknown"


class TestSfmcTriggerNoOp:
    def test_no_op_when_sfmc_not_configured(self):
        # _SFMC_CLIENT is None unless SFMC_SUBDOMAIN etc. are set in the
        # environment — which they aren't in this test run — so this must
        # be a safe no-op, not an error.
        state = _quote_state_reducer(None, _event("quote:quoted"))
        event = _quote_abandoned_event(("QT-1",), state)
        _trigger_sfmc_abandonment_email("scenario_2", state, event)  # should not raise
