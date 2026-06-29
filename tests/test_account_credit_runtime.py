import unittest
from unittest.mock import patch

from mapmover.account_credit import (
    RESEARCH_CREDIT_CHECK_PATH,
    RESEARCH_CREDIT_SETTLE_PATH,
    check_research_budget,
    settle_research_charge,
)


AUTH_CALLER = {
    "caller_kind": "authenticated",
    "auth_user_id": "user-123",
}


class AccountCreditRuntimeTests(unittest.TestCase):
    def test_local_or_disabled_runtime_skips_private_credit_service(self):
        with patch.dict(
            "os.environ",
            {
                "RESEARCH_CREDIT_SERVICE_ENABLED": "false",
                "COMMERCIAL_ACCESS_ENABLED": "false",
            },
            clear=False,
        ), patch("mapmover.account_credit.post_research_credit") as post_credit:
            decision = check_research_budget(AUTH_CALLER)
        self.assertTrue(decision.allowed)
        post_credit.assert_not_called()

    def test_locked_account_response_preserves_existing_route_contract(self):
        response = {
            "allowed": False,
            "balance_micro_usd": -250000,
            "floor_micro_usd": -1000000,
            "error_code": "research_top_up_required",
            "message": "Top up your account to continue using hosted Research.",
            "cta": "top_up",
            "cta_url": "/settings/account",
        }
        with patch.dict(
            "os.environ",
            {"RESEARCH_CREDIT_SERVICE_ENABLED": "true"},
            clear=False,
        ), patch(
            "mapmover.account_credit.post_research_credit",
            return_value=(200, response),
        ) as post_credit:
            decision = check_research_budget(AUTH_CALLER, model="test-model")

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.balance_micro_usd, -250000)
        self.assertEqual(decision.error_code, "research_top_up_required")
        post_credit.assert_called_once_with(
            RESEARCH_CREDIT_CHECK_PATH,
            {
                "caller_kind": "authenticated",
                "user_id": "user-123",
                "model": "test-model",
            },
        )

    def test_credit_service_failure_keeps_existing_fail_open_behavior(self):
        with patch.dict(
            "os.environ",
            {"RESEARCH_CREDIT_SERVICE_ENABLED": "true"},
            clear=False,
        ), patch(
            "mapmover.account_credit.post_research_credit",
            side_effect=TimeoutError("offline"),
        ):
            decision = check_research_budget(AUTH_CALLER)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.balance_micro_usd, 0)

    def test_settlement_forwards_identity_and_idempotency_context(self):
        response = {
            "success": True,
            "charged_micro_usd": 12500,
            "charged_cost_usd": 0.0125,
        }
        with patch.dict(
            "os.environ",
            {"RESEARCH_CREDIT_SERVICE_ENABLED": "true"},
            clear=False,
        ), patch(
            "mapmover.account_credit.post_research_credit",
            return_value=(200, response),
        ) as post_credit:
            result = settle_research_charge(
                request_id="request-1",
                caller_ctx=AUTH_CALLER,
                request_fingerprint="session-1",
                selected_model="test-model",
            )

        self.assertEqual(result, response)
        post_credit.assert_called_once_with(
            RESEARCH_CREDIT_SETTLE_PATH,
            {
                "caller_kind": "authenticated",
                "user_id": "user-123",
                "request_id": "request-1",
                "request_fingerprint": "session-1",
                "selected_model": "test-model",
            },
        )


if __name__ == "__main__":
    unittest.main()
