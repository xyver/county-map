import unittest

from mapmover.ops_route_runtime import load_or_create_ops_watch


class Cache:
    def __init__(self, watch):
        self.map_state = {"ops_watch": watch}


class OpsWatchRuntimeTests(unittest.TestCase):
    def test_empty_cached_watch_heals_to_current_allowed_feeds(self):
        cache = Cache(
            {
                "watch_id": "watch-1",
                "label": "Ops watch",
                "active_feeds": [],
            }
        )
        watch = load_or_create_ops_watch(
            cache=cache,
            session_id="session-1",
            body={"watch_id": "watch-1", "watch_context": {"label": "Ops watch"}},
            allowed_feeds=["earthquakes", "wildfires_us_nifc"],
        )
        self.assertEqual(
            watch["active_feeds"],
            ["earthquakes", "wildfires_us_nifc"],
        )

    def test_explicit_feed_selection_updates_existing_watch(self):
        cache = Cache(
            {
                "watch_id": "watch-1",
                "label": "Ops watch",
                "active_feeds": ["earthquakes"],
            }
        )
        watch = load_or_create_ops_watch(
            cache=cache,
            session_id="session-1",
            body={
                "watch_id": "watch-1",
                "watch_context": {
                    "label": "Ops watch",
                    "sources": ["tsunamis", "not_allowed"],
                },
            },
            allowed_feeds=["earthquakes", "tsunamis"],
        )
        self.assertEqual(watch["active_feeds"], ["tsunamis"])

    def test_cached_watch_prunes_feeds_no_longer_allowed(self):
        cache = Cache(
            {
                "watch_id": "watch-1",
                "label": "Ops watch",
                "active_feeds": ["earthquakes", "removed_feed"],
            }
        )
        watch = load_or_create_ops_watch(
            cache=cache,
            session_id="session-1",
            body={"watch_id": "watch-1"},
            allowed_feeds=["earthquakes", "tsunamis"],
        )
        self.assertEqual(watch["active_feeds"], ["earthquakes"])


if __name__ == "__main__":
    unittest.main()
