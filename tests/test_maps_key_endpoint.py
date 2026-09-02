"""Rate limiting and usage tracking for the Google Maps key endpoint.

`/api/config/maps-key` hands a credential to the browser, which is unavoidable
for client-side Places. The limiter here is defense in depth against a scripted
fetch loop; the key itself is protected by the referrer/API restrictions and
quota caps set in Google Cloud, because a caller who already holds the key never
returns to this process.
"""

import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app as app_module
from mapmover import logging_analytics, security


class MapsKeyRateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        # app.py imported the limiter by value, so the endpoint keeps using the
        # object it bound at import time. Rebinding mapmover.security would not
        # reach it; patch the name the route actually closes over.
        limiter_patch = patch.object(
            app_module, "rate_limiter", security.SlidingWindowRateLimiter()
        )
        limiter_patch.start()
        self.addCleanup(limiter_patch.stop)
        self.client = TestClient(app_module.app)
        self.addCleanup(self.client.close)

    def test_first_call_returns_the_key(self) -> None:
        with patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "test-key-value"}):
            response = self.client.get("/api/config/maps-key")
        self.assertEqual(200, response.status_code)
        self.assertEqual("test-key-value", response.json()["key"])

    def test_second_call_inside_the_window_is_rate_limited(self) -> None:
        with patch.dict(os.environ, {"GOOGLE_MAPS_API_KEY": "test-key-value"}):
            first = self.client.get("/api/config/maps-key")
            second = self.client.get("/api/config/maps-key")

        self.assertEqual(200, first.status_code)
        self.assertEqual(429, second.status_code)
        self.assertIn("Retry-After", second.headers)
        self.assertGreaterEqual(int(second.headers["Retry-After"]), 1)
        # A throttled response must never leak the credential.
        self.assertNotIn("key", second.json())

    def test_window_and_limit_are_env_tunable(self) -> None:
        env = {
            "GOOGLE_MAPS_API_KEY": "test-key-value",
            "MAPS_KEY_RATE_LIMIT": "3",
            "MAPS_KEY_RATE_WINDOW_SECONDS": "5",
        }
        with patch.dict(os.environ, env):
            codes = [self.client.get("/api/config/maps-key").status_code for _ in range(4)]
        self.assertEqual([200, 200, 200, 429], codes)


class MapsKeyUsageTrackingTests(unittest.TestCase):
    def test_successful_key_fetch_reaches_the_control_plane(self) -> None:
        # Routine 2xx traffic is deliberately not mirrored; this path is an
        # explicit exception because each call bills a third party.
        self.assertTrue(
            logging_analytics._should_mirror_route_event_to_control_plane(
                "/api/config/maps-key", method="GET", status_code=200
            )
        )

    def test_ordinary_api_traffic_is_still_not_mirrored(self) -> None:
        self.assertFalse(
            logging_analytics._should_mirror_route_event_to_control_plane(
                "/api/ops/ticker", method="GET", status_code=200
            )
        )

    def test_rate_limited_key_fetch_is_recorded(self) -> None:
        self.assertTrue(
            logging_analytics._should_mirror_route_event_to_control_plane(
                "/api/config/maps-key", method="GET", status_code=429, rate_limited=True
            )
        )


if __name__ == "__main__":
    unittest.main()
