import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import mapmover.runtime.marine_geometry as marine_runtime
from mapmover.runtime.reference_exchange import _geometry_catalog_domains
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
    def test_catalog_domain_projection_exposes_activation_without_component_bloat(self):
        catalog = {"domain_profiles": [{
            "release_unit_id": "MARINE",
            "release_unit_kind": "global_domain",
            "label": "Marine",
            "family_ids": ["water_body", "marine_jurisdiction"],
            "release_status": "published",
            "active_release": {
                "release_id": "marine_geometry_1_0_1",
                "release_version": "1.0.1",
                "publication_status": "published",
                "runtime_artifacts": {
                    "jurisdictions": {"path": "geometry/domains/MARINE/jurisdictions.parquet"},
                    "country_components": {
                        "USA": {"path": "geometry/domains/MARINE/country_components/USA.parquet"},
                        "CAN": {"path": "geometry/domains/MARINE/country_components/CAN.parquet"},
                    },
                    "point_shards": {
                        f"{index:02d}": {"path": f"geometry/domains/MARINE/point_shards/{index:02d}.parquet"}
                        for index in range(32)
                    },
                },
            },
        }]}

        domains = _geometry_catalog_domains(catalog)

        self.assertEqual(len(domains), 1)
        self.assertEqual(domains[0]["release_unit_id"], "MARINE")
        self.assertEqual(domains[0]["release_id"], "marine_geometry_1_0_1")
        self.assertEqual(domains[0]["country_component_count"], 2)
        self.assertEqual(domains[0]["point_shard_count"], 32)
        self.assertNotIn("country_components", domains[0]["runtime_artifacts"])
        self.assertNotIn("point_shards", domains[0]["runtime_artifacts"])

    def test_active_domain_uses_catalog_paths_without_runtime_pointer(self):
        with TemporaryDirectory() as tmp:
            data_root = Path(tmp)
            geometry_root = data_root / "geometry"
            release_rel = "geometry/domains/MARINE/releases/geometry/marine_geometry_1_0_1"
            runtime_artifacts = {
                "jurisdictions": {"path": f"{release_rel}/exact/jurisdictions.parquet"},
                "water_bodies": {"path": f"{release_rel}/exact/water_bodies.parquet"},
                "named_water_areas": {"path": f"{release_rel}/exact/named_water_areas.parquet"},
                "bbox_index": {"path": f"{release_rel}/predicate/bbox_index.parquet"},
                "point_shards": {
                    f"{index:02d}": {"path": f"{release_rel}/predicate/point_shards/{index:02d}.parquet"}
                    for index in range(32)
                },
                "country_components": {
                    "USA": {"path": f"{release_rel}/country_components/USA/marine_jurisdictions.parquet"},
                },
            }
            catalog = {"domain_profiles": [{
                "release_unit_id": "MARINE",
                "active_release": {
                    "publication_status": "published",
                    "runtime_artifacts": runtime_artifacts,
                },
            }]}
            with patch.object(marine_runtime, "GEOMETRY_DIR", geometry_root), patch.object(
                marine_runtime, "load_geometry_catalog", return_value=catalog,
            ):
                self.assertEqual(
                    marine_runtime._active_domain_paths()["named_water_areas"],
                    data_root / runtime_artifacts["named_water_areas"]["path"],
                )
                self.assertEqual(
                    marine_bank_for_loc_id("USA-EEZ-MRGID-8456"),
                    data_root / runtime_artifacts["country_components"]["USA"]["path"],
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
