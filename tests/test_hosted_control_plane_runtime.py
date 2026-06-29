import unittest
from unittest.mock import patch

from mapmover.hosted_control_plane import (
    ACCOUNT_CONTEXT_PATH,
    ANONYMOUS_USAGE_PATH,
    RUNTIME_EVENTS_PATH,
    HostedControlPlaneUnavailable,
    emit_runtime_event,
    get_account_context,
    get_anonymous_usage_cost,
    get_saved_corpus,
)


class HostedControlPlaneRuntimeTests(unittest.TestCase):
    def test_account_context_uses_narrow_private_endpoint(self):
        response = {"plan_id": "member", "user_packs": ["currency"]}
        with patch(
            "mapmover.hosted_control_plane.post_control_plane",
            return_value=(200, response),
        ) as post:
            result = get_account_context("user-1")
        self.assertEqual(result, response)
        post.assert_called_once_with(ACCOUNT_CONTEXT_PATH, {"user_id": "user-1"})

    def test_anonymous_usage_returns_none_when_authority_is_unavailable(self):
        with patch(
            "mapmover.hosted_control_plane.post_control_plane",
            return_value=(503, {"error": "unavailable"}),
        ):
            result = get_anonymous_usage_cost("hash-1", "2026-06-27T00:00:00+00:00")
        self.assertIsNone(result)

    def test_anonymous_usage_uses_private_cost_view_endpoint(self):
        with patch(
            "mapmover.hosted_control_plane.post_control_plane",
            return_value=(200, {"cost_usd": "0.125"}),
        ) as post:
            result = get_anonymous_usage_cost("hash-1", "2026-06-27T00:00:00+00:00")
        self.assertEqual(result, "0.125")
        post.assert_called_once_with(
            ANONYMOUS_USAGE_PATH,
            {"ip_hash": "hash-1", "start_at": "2026-06-27T00:00:00+00:00"},
        )

    def test_runtime_event_protocol_does_not_expose_table_names(self):
        with patch(
            "mapmover.hosted_control_plane.post_control_plane",
            return_value=(200, {"ok": True}),
        ) as post:
            self.assertTrue(emit_runtime_event("llm_usage", {"request_id": "request-1"}))
        post.assert_called_once_with(
            RUNTIME_EVENTS_PATH,
            {
                "event_kind": "llm_usage",
                "payload": {"request_id": "request-1"},
            },
        )

    def test_saved_corpus_distinguishes_unavailable_service_from_missing_row(self):
        with patch(
            "mapmover.hosted_control_plane.post_control_plane",
            return_value=(503, {"error": "unavailable"}),
        ):
            with self.assertRaises(HostedControlPlaneUnavailable):
                get_saved_corpus("user-1", "corpus-1")

        with patch(
            "mapmover.hosted_control_plane.post_control_plane",
            return_value=(404, {"error": "not_found"}),
        ):
            self.assertIsNone(get_saved_corpus("user-1", "corpus-1"))


if __name__ == "__main__":
    unittest.main()
