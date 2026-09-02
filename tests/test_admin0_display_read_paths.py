"""The app and site Admin0 read paths use the Display bank, not global.csv.

Exact Admin0 geometry is a 400 MB+ CSV that stays resident for the life of the
process once any caller touches it. Point containment needs that precision;
viewport shortlists, bounding boxes, breadcrumb names, and metadata rows do not,
and every one of them used to pull the exact bank into ordinary map and site
requests. These tests pin the split so it does not drift back.
"""

import json
import unittest
from unittest.mock import patch

import pandas as pd

from mapmover import foundation_helpers, geometry_handlers, preprocessor_geo
from mapmover.runtime import loc_id_resolution


EXACT_MUST_NOT_LOAD = AssertionError("this path must not read exact global.csv")


def _display_frame() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "loc_id": "FRA",
            "name": "France",
            "admin_level": 0,
            "parent_id": "WORLD",
            "bbox_min_lon": -5.0,
            "bbox_min_lat": 41.0,
            "bbox_max_lon": 9.0,
            "bbox_max_lat": 51.0,
            "geometry": json.dumps({
                "type": "Polygon",
                "coordinates": [[[-5.0, 41.0], [9.0, 41.0], [9.0, 51.0], [-5.0, 51.0], [-5.0, 41.0]]],
            }),
        },
        {
            "loc_id": "HKG",
            "name": "Hong Kong",
            "admin_level": 0,
            "parent_id": "WORLD",
            "bbox_min_lon": 113.8,
            "bbox_min_lat": 22.1,
            "bbox_max_lon": 114.5,
            "bbox_max_lat": 22.6,
            "geometry": json.dumps({
                "type": "Polygon",
                "coordinates": [[[113.8, 22.1], [114.5, 22.1], [114.5, 22.6], [113.8, 22.6], [113.8, 22.1]]],
            }),
        },
    ])


class Admin0DisplayReadPathTests(unittest.TestCase):
    def setUp(self) -> None:
        geometry_handlers._country_bounds_cache = None
        self.addCleanup(setattr, geometry_handlers, "_country_bounds_cache", None)

    def test_country_bounds_come_from_display_bbox_columns(self) -> None:
        with patch.object(
            geometry_handlers, "load_global_country_display_frame", return_value=_display_frame()
        ), patch.object(
            geometry_handlers, "load_global_countries_frame", side_effect=EXACT_MUST_NOT_LOAD
        ):
            bounds = geometry_handlers.load_country_bounds()

        self.assertEqual({"FRA", "HKG"}, set(bounds))
        pad = geometry_handlers.COUNTRY_BOUNDS_DISPLAY_PAD_DEG
        min_lon, min_lat, max_lon, max_lat = bounds["FRA"]
        # A shortlist must over-select, and Display outlines sit inside the
        # exact ones, so every edge is padded outward.
        self.assertAlmostEqual(-5.0 - pad, min_lon)
        self.assertAlmostEqual(41.0 - pad, min_lat)
        self.assertAlmostEqual(9.0 + pad, max_lon)
        self.assertAlmostEqual(51.0 + pad, max_lat)

    def test_country_bounds_stay_inside_valid_lon_lat_range(self) -> None:
        edge = pd.DataFrame([{
            "loc_id": "ATA",
            "name": "Antarctica",
            "bbox_min_lon": -180.0,
            "bbox_min_lat": -90.0,
            "bbox_max_lon": 180.0,
            "bbox_max_lat": -60.0,
        }])
        with patch.object(geometry_handlers, "load_global_country_display_frame", return_value=edge):
            bounds = geometry_handlers.load_country_bounds()

        self.assertEqual((-180.0, -90.0, 180.0, -60.0 + geometry_handlers.COUNTRY_BOUNDS_DISPLAY_PAD_DEG),
                         bounds["ATA"])

    def test_viewport_country_filter_reads_display_bank(self) -> None:
        with patch.object(
            preprocessor_geo, "load_global_country_display_frame", return_value=_display_frame()
        ):
            visible = preprocessor_geo.get_countries_in_viewport(
                {"west": 113.0, "south": 21.0, "east": 115.0, "north": 23.0},
                geometry_dir=None,
                logger=_SilentLogger(),
            )

        # Hong Kong is one of the territories the exact bank merges at runtime
        # with a null bbox, which dropped it from every viewport.
        self.assertEqual(["HKG"], visible)

    def test_country_name_resolution_reads_display_bank(self) -> None:
        with patch.object(
            loc_id_resolution, "load_global_country_display_frame", return_value=_display_frame()
        ):
            match = loc_id_resolution._resolve_country_name_from_global_geometry("Hong Kong")

        self.assertIsNotNone(match)
        self.assertEqual("HKG", match.get("loc_id"))


class Admin0CountryUniverseTests(unittest.TestCase):
    """The exact bank recognizes the universe the Geometry Catalog overlay shows.

    The overlay takes shapes from geometry/display/admin_0.parquet and facts
    from geometry/geometry_catalog.json. A territory published into that
    Display bank should be admitted by the exact bank's supplemental merge
    without a second edit to the coverage reference.
    """

    def test_display_universe_extends_the_reference_codes(self) -> None:
        display = pd.DataFrame([{"loc_id": "NEWLAND"}, {"loc_id": "FRA"}])
        with patch.object(
            foundation_helpers, "_reference_country_codes", return_value={"FRA"}
        ), patch.object(
            foundation_helpers, "load_global_country_display_frame", return_value=display
        ):
            universe = foundation_helpers._admin0_country_universe()

        self.assertIn("NEWLAND", universe)
        self.assertIn("FRA", universe)

    def test_missing_display_bank_falls_back_to_the_reference_codes(self) -> None:
        # The Display loader fails closed. Losing that read must not shrink
        # exact Admin0 containment.
        with patch.object(
            foundation_helpers, "_reference_country_codes", return_value={"FRA", "HKG"}
        ), patch.object(
            foundation_helpers, "load_global_country_display_frame", return_value=None
        ):
            universe = foundation_helpers._admin0_country_universe()

        self.assertEqual({"FRA", "HKG"}, universe)

    def test_display_read_failure_falls_back_to_the_reference_codes(self) -> None:
        with patch.object(
            foundation_helpers, "_reference_country_codes", return_value={"FRA"}
        ), patch.object(
            foundation_helpers,
            "load_global_country_display_frame",
            side_effect=RuntimeError("artifact unavailable"),
        ):
            universe = foundation_helpers._admin0_country_universe()

        self.assertEqual({"FRA"}, universe)


class _SilentLogger:
    def warning(self, *args, **kwargs) -> None:
        pass

    def debug(self, *args, **kwargs) -> None:
        pass


if __name__ == "__main__":
    unittest.main()
