"""Marine geometry resolution for the EEZ / water-body overlay families.

The admin geometry loader (geometry_loader.py) resolves country/admin loc_ids to
the GeoBoundaries banks. Water-body and EEZ ids are ordinary ``loc_id`` values
from sibling geometry families; this module only routes those ids to their
geometry banks:

  - EEZ-<ISO3> / EEZ-MRGID-<n>  -> geometry/marine/eez.parquet
  - X* water-body aggregate codes (XOP..) -> geometry/marine/water_bodies.parquet
  - IHO1953-<n> reviewed IHO-1953 named waters -> iho1953_sea_areas.parquet

The MRGID-<n> legacy named-water bank (Marine Regions / VLIZ IHO Sea Areas) was
removed on 2026-08-16. It was licence-unreviewed, so its routing was already
inert, and the reviewed IHO-1953 bank supersedes it with wider coverage.

This is the geometry counterpart to the shared grid helper's classification
(is_eez_loc_id / is_water_body_loc_id): given marine loc_ids, return their
polygons so a metrics source aggregated onto marine zones (e.g. ocean_sst) can
render. It is shared across the whole ocean family (SST, CoralTemp, DHW, etc.),
not specific to one pack.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from ..duckdb_helpers import (
    parquet_available,
    parquet_columns,
    path_to_uri,
    quote_ident,
    run_df,
    select_columns_from_parquet,
)
from ..paths import GEOMETRY_DIR
from .geography_reference import (
    is_marine_jurisdiction_loc_id,
    is_named_water_loc_id,
    is_water_body_loc_id,
)
from .geometry_catalog import load_geometry_catalog

MARINE_DIR = GEOMETRY_DIR / "marine"
EEZ_PATH = MARINE_DIR / "eez.parquet"
WATER_BODIES_PATH = MARINE_DIR / "water_bodies.parquet"
IHO1953_NAMED_WATER_PATH = MARINE_DIR / "iho1953_sea_areas.parquet"
GEOMETRY_CATALOG_PATH = GEOMETRY_DIR / "geometry_catalog.json"
_MARINE_COLUMNS = ["loc_id", "name", "geometry", "centroid_lon", "centroid_lat"]
_MARINE_POINT_COLUMNS = _MARINE_COLUMNS + [
    "geometry_wkb", "area_km2",
    "bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat",
]


def is_marine_loc_id(loc_id: str | None) -> bool:
    """True for either marine overlay family or canonical named water."""
    return is_marine_jurisdiction_loc_id(loc_id) or is_water_body_loc_id(loc_id) or is_named_water_loc_id(loc_id)


def _catalog_approves_geometry(path: Path) -> bool:
    """Do not expose candidate sea geometry until catalog review is explicit."""
    try:
        catalog = json.loads(GEOMETRY_CATALOG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    try:
        rel_path = path.relative_to(GEOMETRY_DIR).as_posix()
    except ValueError:
        rel_path = path.as_posix()
    for bank in catalog.get("geometry_banks") or []:
        if not isinstance(bank, dict):
            continue
        if str(bank.get("geometry_path") or "").replace("\\", "/") != rel_path:
            continue
        return (
            bank.get("license_review_status") == "approved"
            and bank.get("usable_for_derivation") is True
        )
    return False


def named_water_bank_approved(loc_id: str | None = None) -> bool:
    """True if the bank owning this named-water namespace is reviewed."""
    return _catalog_approves_geometry(IHO1953_NAMED_WATER_PATH)


def _catalog_artifact_path(record: Any) -> Optional[Path]:
    relative = str(record.get("path") or "").strip() if isinstance(record, dict) else ""
    if not relative:
        return None
    path = GEOMETRY_DIR.parent / relative
    try:
        path.resolve().relative_to(GEOMETRY_DIR.parent.resolve())
    except (OSError, ValueError):
        return None
    return path


def _active_domain_paths() -> Optional[dict[str, Any]]:
    """Resolve the active Marine banks only from the canonical runtime catalog."""
    catalog = load_geometry_catalog()
    profile = next((
        item for item in catalog.get("domain_profiles") or []
        if isinstance(item, dict)
        and str(item.get("release_unit_id") or "").strip().upper() == "MARINE"
    ), None)
    active = profile.get("active_release") if isinstance(profile, dict) else None
    if not isinstance(active, dict) or active.get("publication_status") not in {
        "approved_for_publication", "published",
    }:
        return None
    artifacts = active.get("runtime_artifacts")
    if not isinstance(artifacts, dict):
        return None
    paths = {
        key: _catalog_artifact_path(artifacts.get(key))
        for key in ("jurisdictions", "water_bodies", "named_water_areas", "bbox_index")
    }
    if any(path is None for path in paths.values()):
        return None
    country_components = {
        str(country).upper(): path
        for country, record in (artifacts.get("country_components") or {}).items()
        if (path := _catalog_artifact_path(record)) is not None
    }
    point_shards = {
        str(shard): path
        for shard, record in (artifacts.get("point_shards") or {}).items()
        if (path := _catalog_artifact_path(record)) is not None
    }
    if len(point_shards) != 32:
        return None
    return {**paths, "country_components": country_components, "point_shards": point_shards}


def marine_bank_for_loc_id(loc_id: str | None) -> Optional[Path]:
    """Return the marine geometry bank that owns this loc_id, or None."""
    value = str(loc_id or "").strip().upper()
    domain = _active_domain_paths()
    if is_marine_jurisdiction_loc_id(loc_id):
        if domain and not value.startswith("EEZ-"):
            prefix = value.split("-", 1)[0]
            if is_water_body_loc_id(prefix):
                return domain["jurisdictions"]
            return domain["country_components"].get(prefix)
        return EEZ_PATH
    if is_named_water_loc_id(loc_id):
        if domain:
            return domain["named_water_areas"]
        return IHO1953_NAMED_WATER_PATH if named_water_bank_approved(loc_id) else None
    if is_water_body_loc_id(loc_id):
        if domain:
            return domain["water_bodies"]
        return WATER_BODIES_PATH
    return None


def has_marine_geometry() -> bool:
    """True when at least one marine bank is readable (local or cloud)."""
    if _active_domain_paths():
        return True
    return (
        parquet_available(EEZ_PATH)
        or parquet_available(WATER_BODIES_PATH)
        or (named_water_bank_approved("IHO1953-0") and parquet_available(IHO1953_NAMED_WATER_PATH))
    )


def resolve_marine_geometry_source(loc_id: str | None) -> dict:
    """Mirror geometry_loader.resolve_country_geometry_source for marine loc_ids.

    Keys: `parquet_file` (path or None), `source_kind` (`marine_bank`/`missing`),
    `marine_kind` (`marine_eez`/`water_body`/`named_water`/None).
    """
    bank = marine_bank_for_loc_id(loc_id)
    if bank is None:
        return {"parquet_file": None, "source_kind": "missing", "marine_kind": None}
    accessible = parquet_available(bank)
    return {
        "parquet_file": bank if accessible else None,
        "source_kind": "marine_bank" if accessible else "missing",
        "marine_kind": (
            ("marine_eez" if str(loc_id or "").strip().upper().startswith("EEZ-") else "marine_jurisdiction")
            if is_marine_jurisdiction_loc_id(loc_id)
            else "named_water" if is_named_water_loc_id(loc_id)
            else "water_body"
        ),
    }


def _read_bank(path: Path, want: Optional[set], columns: Optional[list[str]] = None) -> pd.DataFrame:
    selected_columns = [column for column in (columns or _MARINE_COLUMNS) if column in _MARINE_POINT_COLUMNS]
    if "loc_id" not in selected_columns:
        selected_columns.insert(0, "loc_id")
    if not parquet_available(path):
        return pd.DataFrame(columns=selected_columns)
    try:
        if want:
            placeholders = ", ".join("?" for _ in want)
            selected_sql = ", ".join(quote_ident(column) for column in selected_columns)
            df = run_df(
                f"SELECT {selected_sql} FROM read_parquet(?) WHERE loc_id IN ({placeholders})",
                [path_to_uri(path), *sorted(want)],
            )
        else:
            df = select_columns_from_parquet(path, selected_columns)
    except Exception:
        df = None
    if df is None or df.empty:
        try:
            df = pd.read_parquet(path, columns=selected_columns)
        except Exception:
            return pd.DataFrame(columns=selected_columns)
    if want is not None and "loc_id" in df.columns:
        df = df[df["loc_id"].isin(want)]
    return df


def _read_bank_at_point(path: Path, lon: float, lat: float) -> pd.DataFrame:
    """Read only rows whose stored bounds can contain a point.

    Marine polygons are large enough that loading every geometry made a water
    click unnecessarily slow. The bounds predicate is only a cheap candidate
    filter; the resolver still performs exact Shapely containment afterward.
    """
    if not parquet_available(path):
        return pd.DataFrame(columns=_MARINE_POINT_COLUMNS)
    available = parquet_columns(path)
    selected = [column for column in _MARINE_POINT_COLUMNS if column in available]
    if "geometry" not in selected or "loc_id" not in selected:
        return pd.DataFrame(columns=selected)
    select_sql = ", ".join(quote_ident(column) for column in selected)
    if {"bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat"}.issubset(available):
        sql = (
            f"SELECT {select_sql} FROM read_parquet(?) "
            "WHERE bbox_min_lon <= ? AND bbox_max_lon >= ? "
            "AND bbox_min_lat <= ? AND bbox_max_lat >= ?"
        )
        return run_df(sql, [path_to_uri(path), lon, lon, lat, lat])
    return select_columns_from_parquet(path, selected)


def _read_bbox_index_at_point(path: Path, lon: float, lat: float) -> pd.DataFrame:
    columns = ["loc_id", "bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat"]
    if not parquet_available(path) or not set(columns).issubset(parquet_columns(path)):
        return pd.DataFrame(columns=columns)
    selected_sql = ", ".join(quote_ident(column) for column in columns)
    return run_df(
        f"SELECT {selected_sql} FROM read_parquet(?) "
        "WHERE bbox_min_lon <= ? AND bbox_max_lon >= ? "
        "AND bbox_min_lat <= ? AND bbox_max_lat >= ?",
        [path_to_uri(path), lon, lon, lat, lat],
    )


def _point_shard_id(loc_id: str) -> str:
    digest = hashlib.sha256(str(loc_id).encode("utf-8")).hexdigest()
    return f"{int(digest, 16) % 32:02d}"


def _read_point_shard(path: Path, want: set[str]) -> pd.DataFrame:
    columns = [
        "loc_id", "name", "geometry_wkb", "area_km2",
        "bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat",
    ]
    if not path or not want or not parquet_available(path):
        return pd.DataFrame(columns=columns)
    if not set(columns).issubset(parquet_columns(path)):
        return pd.DataFrame(columns=columns)
    placeholders = ", ".join("?" for _ in want)
    selected_sql = ", ".join(quote_ident(column) for column in columns)
    return run_df(
        f"SELECT {selected_sql} FROM read_parquet(?) WHERE loc_id IN ({placeholders})",
        [path_to_uri(path), *sorted(want)],
    )


def load_marine_geometry_at_point(lon: float, lat: float) -> pd.DataFrame:
    """Load bbox-filtered candidates from every approved marine point bank."""
    domain = _active_domain_paths()
    if domain:
        bbox_candidates = _read_bbox_index_at_point(domain["bbox_index"], float(lon), float(lat))
        jurisdiction_ids = (
            set(bbox_candidates["loc_id"].astype(str))
            if bbox_candidates is not None and not bbox_candidates.empty else set()
        )
        by_shard: dict[str, set[str]] = {}
        for loc_id in jurisdiction_ids:
            by_shard.setdefault(_point_shard_id(loc_id), set()).add(loc_id)
        frames = [
            _read_point_shard(domain["point_shards"].get(shard), ids)
            for shard, ids in sorted(by_shard.items())
            if domain["point_shards"].get(shard) is not None
        ]
        frames.extend([
            _read_bank_at_point(domain["water_bodies"], float(lon), float(lat)),
            _read_bank_at_point(domain["named_water_areas"], float(lon), float(lat)),
        ])
    else:
        paths = [EEZ_PATH, WATER_BODIES_PATH]
        if named_water_bank_approved("IHO1953-0"):
            paths.append(IHO1953_NAMED_WATER_PATH)
        frames = [_read_bank_at_point(path, float(lon), float(lat)) for path in paths]
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=_MARINE_POINT_COLUMNS)
    return pd.concat(frames, ignore_index=True).reset_index(drop=True)


def load_marine_geometry(loc_ids: Optional[Iterable[str]] = None, *, columns: Optional[list[str]] = None) -> pd.DataFrame:
    """Load marine geometry rows for the given loc_ids (or all marine geometry).

    Returns columns [loc_id, name, geometry, centroid_lon, centroid_lat]. Only
    the bank(s) actually referenced by the requested loc_ids are read, so an
    EEZ-only query never touches the water-body bank and vice versa. Callers
    that only need availability metadata can omit the heavy geometry column.
    """
    want = {str(x).strip() for x in loc_ids} if loc_ids is not None else None
    need_eez = want is None or any(is_marine_jurisdiction_loc_id(x) for x in want)
    need_wb = want is None or any(is_water_body_loc_id(x) for x in want)
    need_named_water = want is None or any(is_named_water_loc_id(x) for x in want)

    domain = _active_domain_paths()
    frames = []
    if need_eez:
        jurisdiction_banks = {marine_bank_for_loc_id(value) for value in want} if want is not None else {
            domain["jurisdictions"] if domain else EEZ_PATH
        }
        frames.extend(_read_bank(path, want, columns=columns) for path in jurisdiction_banks if path is not None)
    if need_wb:
        frames.append(_read_bank(domain["water_bodies"] if domain else WATER_BODIES_PATH, want, columns=columns))
    if need_named_water and (domain or named_water_bank_approved("IHO1953-0")):
        frames.append(_read_bank(domain["named_water_areas"] if domain else IHO1953_NAMED_WATER_PATH, want, columns=columns))
    if not frames:
        return pd.DataFrame(columns=columns or _MARINE_COLUMNS)
    return pd.concat(frames, ignore_index=True).reset_index(drop=True)
