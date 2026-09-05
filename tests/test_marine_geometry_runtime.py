import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import mapmover.runtime.marine_geometry as marine_runtime
from mapmover.runtime.geometry_catalog import _merge_release_profiles
from mapmover.runtime.marine_geometry import (
    EEZ_PATH,
    WATER_BODIES_PATH,
    has_marine_geometry,
    is_marine_loc_id,
    load_marine_geometry,
    marine_bank_for_loc_id,
    resolve_marine_geometry_source,
)
from mapmover.runtime.geography_reference import is_named_water_loc_id


class MarineGeometryRuntimeTests(unittest.TestCase):
    def test_release_profile_overlay_patches_only_named_units(self):
        base = {
            "catalog_fingerprint": "base-1",
            "country_profiles": [{"country_code": "USA", "release_version": "1"}],
            "domain_profiles": [{"release_unit_id": "OTHER", "release_version": "1"}],
        }
        overlay = {
            "profile": "geometry_release_profile_overlay",
            "composition_mode": "patch",
            "base_catalog_fingerprint": "base-1",
            "country_profiles": [{"country_code": "USA", "release_version": "2"}],
            "domain_profiles": [{"release_unit_id": "MARINE", "release_version": "1.0.1"}],
        }
        merged = _merge_release_profiles(base, overlay)
        self.assertEqual(merged["country_profiles"][0]["release_version"], "2")
        self.assertEqual(
            {item["release_unit_id"] for item in merged["domain_profiles"]},
            {"MARINE", "OTHER"},
        )

    def test_active_domain_requires_admitted_pointer_and_complete_exact_banks(self):
        with TemporaryDirectory() as tmp:
            data_root = Path(tmp)
            geometry_root = data_root / "geometry"
            releases = geometry_root / "domains/MARINE/releases/geometry"
            release = releases / "marine_geometry_1_0_0"
            exact = release / "exact"
            exact.mkdir(parents=True)
            (release / "version.json").write_text("{}", encoding="utf-8")
            for name in ("jurisdictions.parquet", "water_bodies.parquet", "named_water_areas.parquet"):
                (exact / name).write_bytes(b"test")
            predicate = release / "predicate"
            predicate.mkdir()
            (predicate / "bbox_index.parquet").write_bytes(b"test")
            pointer = releases / "current.json"
            pointer.write_text(json.dumps({
                "release_unit_id": "MARINE",
                "publication_status": "adopted_local_unpublished",
                "version_path": "geometry/domains/MARINE/releases/geometry/marine_geometry_1_0_0/version.json",
            }), encoding="utf-8")

            with patch.object(marine_runtime, "GEOMETRY_DIR", geometry_root), patch.object(
                marine_runtime, "MARINE_DOMAIN_RELEASES_DIR", releases,
            ), patch.object(marine_runtime, "MARINE_DOMAIN_POINTER", pointer):
                self.assertEqual(marine_runtime._active_domain_root(), release)
                self.assertEqual(
                    marine_runtime._active_domain_paths()["named_water_areas"],
                    exact / "named_water_areas.parquet",
                )

    def test_classification(self):
        self.assertTrue(is_marine_loc_id("EEZ-USA"))
        self.assertTrue(is_marine_loc_id("EEZ-MRGID-21801"))
        self.assertTrue(is_marine_loc_id("XSG"))
        self.assertTrue(is_marine_loc_id("XLS"))
        self.assertTrue(is_marine_loc_id("IHO1953-123"))
        self.assertTrue(is_marine_loc_id("USA-EEZ-MRGID-8456"))
        self.assertTrue(is_marine_loc_id("XOP-TS-MRGID-48975"))
        self.assertTrue(is_named_water_loc_id("IHO1953-123"))
        self.assertFalse(is_marine_loc_id("USA"))
        self.assertFalse(is_marine_loc_id("USA-CA-037"))
        self.assertFalse(is_marine_loc_id(""))

    def test_bank_routing(self):
        self.assertEqual(marine_bank_for_loc_id("EEZ-USA"), EEZ_PATH)
        self.assertEqual(marine_bank_for_loc_id("EEZ-ASM"), EEZ_PATH)
        active = marine_runtime._active_domain_paths()
        expected_water_bodies = active["water_bodies"] if active else WATER_BODIES_PATH
        self.assertEqual(marine_bank_for_loc_id("XOP"), expected_water_bodies)
        self.assertEqual(marine_bank_for_loc_id("XLM"), expected_water_bodies)
        self.assertIsNone(marine_bank_for_loc_id("USA"))

    def test_resolve_source(self):
        self.assertEqual(resolve_marine_geometry_source("EEZ-USA")["marine_kind"], "marine_eez")
        self.assertEqual(resolve_marine_geometry_source("XSG")["marine_kind"], "water_body")
        self.assertIsNone(resolve_marine_geometry_source("USA")["parquet_file"])

    @unittest.skipUnless(has_marine_geometry(), "marine geometry banks not present locally")
    def test_load_geometry_for_loc_ids(self):
        df = load_marine_geometry(["EEZ-USA", "XSG", "XLG", "XLS", "XLM", "XLH", "XLE", "XLO"])
        ids = set(df["loc_id"])
        self.assertIn("EEZ-USA", ids)
        self.assertIn("XSG", ids)
        self.assertTrue({"XLG", "XLS", "XLM", "XLH", "XLE", "XLO"}.issubset(ids))
        self.assertTrue((df["geometry"].astype(str).str.len() > 0).all())

    @unittest.skipUnless(has_marine_geometry(), "marine geometry banks not present locally")
    def test_eez_only_query_skips_water_body_bank(self):
        df = load_marine_geometry(["EEZ-USA"])
        self.assertEqual(set(df["loc_id"]), {"EEZ-USA"})


if __name__ == "__main__":
    unittest.main()
