"""GTFS-RT JSON -> Parquet processor.

Reads GTFS-rt FeedMessage JSON files from `data/raw` and writes
polars DataFrames as Parquet files under `data/bronze` partitioned by
agency and feed type.

Requirements (user): pandas>=2.2.0, pyarrow>=17.0.0, polars>=1.0.0

    # If filename didn't include agency, try to infer from feed content
    if meta.get("agency") == "unknown":
        inferred = infer_agency_from_feed(feed)
        if inferred:
            meta["agency"] = inferred

    # Some JSON dumps use a flat 'trip_updates' list instead of entity wrappers
    if isinstance(feed, dict) and isinstance(feed.get("trip_updates"), list):
        rows = []
        for tu in feed.get("trip_updates", []):
            row = {
                "snapshot_filename": path.name,
                "snapshot_ts": meta["snapshot_ts"],
                "date_str": meta["date_str"],
                "agency": meta["agency"],
                "entity_id": None,
                "trip_id": tu.get("trip_id"),
                "route_id": tu.get("route_id"),
                "direction_id": tu.get("direction_id"),
                "start_time": tu.get("start_time"),
                "start_date": tu.get("start_date"),
                "vehicle_id": tu.get("vehicle_id"),
                "tu_timestamp": tu.get("timestamp"),
                "delay": tu.get("delay"),
            }
            rows.append(row)
        if not rows:
            return _empty_trip_updates_df()
        try:
            return pl.DataFrame(rows)
        except Exception:
            return pl.from_dicts(rows)
"""
import logging
import json
import argparse
import re
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)


def parse_metadata_from_filename(path: Path) -> Dict[str, Union[str, pd.Timestamp]]:
    """Parse agency, feed_type and snapshot timestamp from filename.

    Expected pattern: gtfs_rt_{feed_type}_{agency}_{YYYYMMDD_HHMMSS}.json

    Args:
        path: Path to the JSON file.

    Returns:
        dict with keys: agency, feed_type, snapshot_ts (pd.Timestamp), date_str

    Raises:
        ValueError: if filename doesn't match expected pattern.
    """
    name = path.name
    # try with agency first
    m = re.match(r"^gtfs_rt_(trip_updates|vehicle_positions)_(?P<agency>.+?)_(?P<ts>\d{8}_\d{6})\.json$", name)
    if m:
        feed_type = m.group(1)
        agency = m.group("agency")
        ts_str = m.group("ts")
    else:
        # fallback: allow filenames without agency (agency will be inferred later)
        m2 = re.match(r"^gtfs_rt_(trip_updates|vehicle_positions)_(?P<ts>\d{8}_\d{6})\.json$", name)
        if not m2:
            raise ValueError(f"Filename does not match expected pattern: {name}")
        feed_type = m2.group(1)
        agency = "unknown"
        ts_str = m2.group("ts")
    # ts_str like 20251114_214913
    try:
        snapshot_ts = pd.to_datetime(ts_str, format="%Y%m%d_%H%M%S")
    except Exception as exc:
        raise ValueError(f"Invalid timestamp in filename {name}: {exc}")
    date_str = snapshot_ts.strftime("%Y%m%d")
    return {"agency": agency, "feed_type": feed_type, "snapshot_ts": snapshot_ts, "date_str": date_str}


def infer_agency_from_feed(feed: dict) -> Optional[str]:
    """Infer agency string from FeedMessage content.

    Heuristics:
      - Check vehicle ids in entities (trip_update.vehicle.id, vehicle.vehicle.id, vehicle.id).
      - For ids like 'chitetsu_tram_5007' return 'chitetsu_tram'.
      - For ids like 'chitetsu_bus-5007' or 'chitetsu_bus.5007' normalize separators to underscore.

    Returns agency or None.
    """
    if not isinstance(feed, dict):
        return None
    # 1) legacy entity list
    entities = feed.get("entity", [])
    for ent in entities:
        # trip_update vehicle
        if "trip_update" in ent:
            tu = ent.get("trip_update", {})
            vehicle = tu.get("vehicle") or {}
            if isinstance(vehicle, dict):
                vid = vehicle.get("id")
                if vid:
                    v = str(vid)
                    # normalize separators
                    v = v.replace("-", "_").replace(".", "_")
                    parts = v.split("_")
                    if len(parts) >= 2:
                        return "_".join(parts[:2])
                    if parts:
                        return parts[0]
        # vehicle entity
        if "vehicle" in ent:
            veh = ent.get("vehicle", {})
            nested = veh.get("vehicle") if isinstance(veh, dict) else None
            vid = None
            if isinstance(nested, dict):
                vid = nested.get("id")
            elif isinstance(veh, dict):
                vid = veh.get("id")
            if vid:
                v = str(vid).replace("-", "_").replace(".", "_")
                parts = v.split("_")
                if len(parts) >= 2:
                    return "_".join(parts[:2])
                if parts:
                    return parts[0]
        # 2) direct trip_updates list (flat list of dicts)
        tus = feed.get("trip_updates")
        if isinstance(tus, list):
            for tu in tus:
                vid = tu.get("vehicle_id") or (tu.get("vehicle") and tu.get("vehicle").get("id"))
                if vid:
                    v = str(vid).replace("-", "_").replace(".", "_")
                    parts = v.split("_")
                    if len(parts) >= 2:
                        return "_".join(parts[:2])
                    if parts:
                        return parts[0]

        # 3) direct vehicle_positions list
        vps = feed.get("vehicle_positions")
        if isinstance(vps, list):
            for vp in vps:
                posveh = vp.get("vehicle") or vp
                vid = None
                if isinstance(posveh, dict):
                    vid = posveh.get("vehicle_id") or posveh.get("vehicle", {}).get("id") or posveh.get("id")
                if vid:
                    v = str(vid).replace("-", "_").replace(".", "_")
                    parts = v.split("_")
                    if len(parts) >= 2:
                        return "_".join(parts[:2])
                    if parts:
                        return parts[0]

        return None


def _empty_trip_updates_df() -> pl.DataFrame:
    """Return an empty trip_updates DataFrame with expected columns."""
    cols = {
        "snapshot_filename": [],
        "snapshot_ts": [],
        "date_str": [],
        "agency": [],
        "entity_id": [],
        "trip_id": [],
        "route_id": [],
        "direction_id": [],
        "start_time": [],
        "start_date": [],
        "vehicle_id": [],
        "tu_timestamp": [],
        "delay": [],
    }
    return pl.DataFrame(cols)


def _empty_vehicle_positions_df() -> pl.DataFrame:
    """Return an empty vehicle_positions DataFrame with expected columns."""
    cols = {
        "snapshot_filename": [],
        "snapshot_ts": [],
        "date_str": [],
        "agency": [],
        "entity_id": [],
        "vehicle_id": [],
        "trip_id": [],
        "route_id": [],
        "direction_id": [],
        "start_time": [],
        "start_date": [],
        "current_stop_sequence": [],
        "current_status": [],
        "vp_timestamp": [],
        "lat": [],
        "lon": [],
        "bearing": [],
        "speed": [],
    }
    return pl.DataFrame(cols)


def load_trip_updates_from_json(path: Path) -> pl.DataFrame:
    """Load a single GTFS-rt TripUpdate feed JSON into a polars.DataFrame.

    The function expects the JSON to be a dict-like FeedMessage with a
    top-level `entity` list. Only entities containing `trip_update` are
    converted.

    Args:
        path: Path to the JSON file.

    Returns:
        pl.DataFrame with columns described in the design.
        Returns an empty DataFrame (with expected schema) on error.
    """
    try:
        meta = parse_metadata_from_filename(path)
    except ValueError as exc:
        logger.warning("Skipping file with invalid name '%s': %s", path, exc)
        return _empty_trip_updates_df()

    try:
        with path.open("r", encoding="utf-8") as fh:
            feed = json.load(fh)
    except Exception as exc:
        logger.warning("Failed to read JSON %s: %s", path, exc)
        return _empty_trip_updates_df()
    # If filename didn't include agency, try to infer from feed content
    if meta.get("agency") == "unknown":
        inferred = infer_agency_from_feed(feed)
        if inferred:
            meta["agency"] = inferred

    # Some JSON dumps use a flat 'trip_updates' list instead of entity wrappers
    if isinstance(feed, dict) and isinstance(feed.get("trip_updates"), list):
        rows = []
        for tu in feed.get("trip_updates", []):
            # tu may be a dict with fields similar to entity.trip_update
            vehicle_id = None
            # flat items sometimes use 'vehicle_id' or nested 'vehicle': {'id': ..}
            if isinstance(tu, dict):
                vehicle_id = tu.get("vehicle_id") or (tu.get("vehicle") and (tu.get("vehicle").get("id") if isinstance(tu.get("vehicle"), dict) else None))
            row = {
                "snapshot_filename": path.name,
                "snapshot_ts": meta["snapshot_ts"],
                "date_str": meta["date_str"],
                "agency": meta["agency"],
                "entity_id": None,
                "trip_id": tu.get("trip_id") if isinstance(tu, dict) else None,
                "route_id": tu.get("route_id") if isinstance(tu, dict) else None,
                "direction_id": tu.get("direction_id") if isinstance(tu, dict) else None,
                "start_time": tu.get("start_time") if isinstance(tu, dict) else None,
                "start_date": tu.get("start_date") if isinstance(tu, dict) else None,
                "vehicle_id": vehicle_id,
                "tu_timestamp": tu.get("timestamp") if isinstance(tu, dict) else None,
                "delay": tu.get("delay") if isinstance(tu, dict) else None,
            }
            rows.append(row)
        if not rows:
            return _empty_trip_updates_df()
        try:
            return pl.DataFrame(rows)
        except Exception:
            return pl.from_dicts(rows)

    entities = feed.get("entity", [])
    rows: List[Dict] = []
    for ent in entities:
        if "trip_update" not in ent:
            continue
        tu = ent.get("trip_update", {})
        trip = tu.get("trip", {})
        vehicle = tu.get("vehicle", {}) or {}
        row = {
            "snapshot_filename": path.name,
            "snapshot_ts": meta["snapshot_ts"],
            "date_str": meta["date_str"],
            "agency": meta["agency"],
            "entity_id": ent.get("id"),
            "trip_id": trip.get("trip_id") if trip else None,
            "route_id": trip.get("route_id") if trip else None,
            "direction_id": trip.get("direction_id") if trip else None,
            "start_time": trip.get("start_time") if trip else None,
            "start_date": trip.get("start_date") if trip else None,
            "vehicle_id": vehicle.get("id") if vehicle else None,
            "tu_timestamp": tu.get("timestamp"),
            "delay": tu.get("delay"),
        }
        rows.append(row)

    if not rows:
        return _empty_trip_updates_df()

    try:
        df = pl.DataFrame(rows)
    except Exception:
        # fallback: construct via from_dicts
        df = pl.from_dicts(rows)
    return df


def load_vehicle_positions_from_json(path: Path) -> pl.DataFrame:
    """Load a single GTFS-rt VehiclePosition feed JSON into a polars.DataFrame.

    The function expects the JSON to be a dict-like FeedMessage with a
    top-level `entity` list. Only entities containing `vehicle` are
    converted.

    Note: if a `position` block is missing for an entity, this implementation
    keeps the row and sets `lat`, `lon`, `bearing`, `speed` to null. This
    preserves vehicle records that may later receive position updates.

    Args:
        path: Path to the JSON file.

    Returns:
        pl.DataFrame with columns described in the design.
        Returns an empty DataFrame (with expected schema) on error.
    """
    try:
        meta = parse_metadata_from_filename(path)
    except ValueError as exc:
        logger.warning("Skipping file with invalid name '%s': %s", path, exc)
        return _empty_vehicle_positions_df()

    try:
        with path.open("r", encoding="utf-8") as fh:
            feed = json.load(fh)
    except Exception as exc:
        logger.warning("Failed to read JSON %s: %s", path, exc)
        return _empty_vehicle_positions_df()
    # If filename didn't include agency, try to infer from feed content
    if meta.get("agency") == "unknown":
        inferred = infer_agency_from_feed(feed)
        if inferred:
            meta["agency"] = inferred

    # Some JSON dumps use a flat 'vehicle_positions' list instead of entity wrappers
    if isinstance(feed, dict) and isinstance(feed.get("vehicle_positions"), list):
        rows = []
        for v in feed.get("vehicle_positions", []):
            pos = v.get("position") or {}
            row = {
                "snapshot_filename": path.name,
                "snapshot_ts": meta["snapshot_ts"],
                "date_str": meta["date_str"],
                "agency": meta["agency"],
                "entity_id": None,
                "vehicle_id": v.get("vehicle_id") or (v.get("vehicle") and v.get("vehicle").get("id")),
                "trip_id": (v.get("trip") or {}).get("trip_id"),
                "route_id": (v.get("trip") or {}).get("route_id"),
                "direction_id": (v.get("trip") or {}).get("direction_id"),
                "start_time": (v.get("trip") or {}).get("start_time"),
                "start_date": (v.get("trip") or {}).get("start_date"),
                "current_stop_sequence": v.get("current_stop_sequence"),
                "current_status": v.get("current_status"),
                "vp_timestamp": v.get("timestamp"),
                "lat": pos.get("latitude"),
                "lon": pos.get("longitude"),
                "bearing": pos.get("bearing"),
                "speed": pos.get("speed"),
            }
            rows.append(row)
        if not rows:
            return _empty_vehicle_positions_df()
        try:
            return pl.DataFrame(rows)
        except Exception:
            return pl.from_dicts(rows)
    entities = feed.get("entity", [])
    rows: List[Dict] = []
    for ent in entities:
        if "vehicle" not in ent:
            continue
        vehicle = ent.get("vehicle", {})
        pos = vehicle.get("position") or {}
        row = {
            "snapshot_filename": path.name,
            "snapshot_ts": meta["snapshot_ts"],
            "date_str": meta["date_str"],
            "agency": meta["agency"],
            "entity_id": ent.get("id"),
            "vehicle_id": vehicle.get("vehicle", {}).get("id") if vehicle.get("vehicle") else vehicle.get("id") or vehicle.get("id"),
            # some encodings put vehicle id at vehicle.vehicle.id or vehicle.id
            "trip_id": (vehicle.get("trip") or {}).get("trip_id"),
            "route_id": (vehicle.get("trip") or {}).get("route_id"),
            "direction_id": (vehicle.get("trip") or {}).get("direction_id"),
            "start_time": (vehicle.get("trip") or {}).get("start_time"),
            "start_date": (vehicle.get("trip") or {}).get("start_date"),
            "current_stop_sequence": vehicle.get("current_stop_sequence"),
            "current_status": vehicle.get("current_status"),
            "vp_timestamp": vehicle.get("timestamp"),
            "lat": pos.get("latitude"),
            "lon": pos.get("longitude"),
            "bearing": pos.get("bearing"),
            "speed": pos.get("speed"),
        }
        rows.append(row)

    if not rows:
        return _empty_vehicle_positions_df()

    try:
        df = pl.DataFrame(rows)
    except Exception:
        df = pl.from_dicts(rows)
    return df


def load_all_snapshots(base_dir: Path, feed_type: Literal["trip_updates", "vehicle_positions"]) -> pl.DataFrame:
    """Load all snapshots of a given feed_type under base_dir into a single DataFrame.

    Args:
        base_dir: base directory to search (recursively).
        feed_type: one of 'trip_updates' or 'vehicle_positions'.

    Returns:
        Concatenated pl.DataFrame (vertical). If no files found, returns an
        empty DataFrame with expected schema for the feed_type.
    """
    pattern = f"gtfs_rt_{feed_type}_*.json"
    files = list(base_dir.rglob(pattern))
    if not files:
        logger.info("No files found for pattern %s under %s", pattern, base_dir)
        return _empty_trip_updates_df() if feed_type == "trip_updates" else _empty_vehicle_positions_df()

    metas: List[tuple[Path, pd.Timestamp]] = []
    for p in files:
        try:
            meta = parse_metadata_from_filename(p)
            metas.append((p, meta["snapshot_ts"]))
        except ValueError:
            logger.warning("Skipping file with invalid filename: %s", p)
            continue

    metas.sort(key=lambda x: x[1])
    dfs: List[pl.DataFrame] = []
    for p, _ in metas:
        try:
            if feed_type == "trip_updates":
                df = load_trip_updates_from_json(p)
            else:
                df = load_vehicle_positions_from_json(p)
            if df.is_empty():
                continue
            dfs.append(df)
        except Exception as exc:
            logger.exception("Failed to process %s: %s", p, exc)
            continue

    if not dfs:
        return _empty_trip_updates_df() if feed_type == "trip_updates" else _empty_vehicle_positions_df()

    try:
        combined = pl.concat(dfs, how="vertical")
    except Exception:
        # fallback: reduce concatenation
        combined = dfs[0]
        for d in dfs[1:]:
            combined = pl.concat([combined, d], how="vertical")
    return combined


def save_to_parquet_partitioned(df: pl.DataFrame, output_base_dir: Path, agency: str, feed_type: str, date_str: str) -> Path:
    """Save given DataFrame as Parquet under output_base_dir/agency/feed_type/date_str.parquet.

    Args:
        df: polars DataFrame to save.
        output_base_dir: base directory for outputs (e.g., ./data/bronze).
        agency: agency name used for partitioning.
        feed_type: 'trip_updates' or 'vehicle_positions'.
        date_str: YYYYMMDD string.

    Returns:
        Path to written Parquet file.
    """
    out_dir = output_base_dir / agency / feed_type
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date_str}.parquet"
    # Prefer polars write_parquet which uses pyarrow under the hood when available.
    try:
        df.write_parquet(out_path, compression="zstd")
    except Exception:
        # as a fallback, convert to pandas then to pyarrow
        try:
            df.to_pandas().to_parquet(out_path, compression="zstd", engine="pyarrow")
        except Exception as exc:
            logger.exception("Failed to write parquet to %s: %s", out_path, exc)
            raise
    return out_path


def to_pandas_if_needed(df: pl.DataFrame, as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return pandas.DataFrame if as_pandas True, otherwise return polars.DataFrame.

    Args:
        df: polars DataFrame
        as_pandas: whether to return pandas
    """
    if as_pandas:
        return df.to_pandas()
    return df


def _group_and_save(df: pl.DataFrame, output_dir: Path, feed_type: str, agency_filter: Optional[str], as_pandas: bool) -> None:
    """Group DataFrame by agency and date_str and save partitions.

    This helper expects a polars DataFrame `df` that contains `agency` and `date_str` columns.
    """
    if df.is_empty():
        logger.info("No records to save for feed_type=%s", feed_type)
        return

    # Optionally convert to pandas for consumers, but saving uses polars internally.
    pandas_mode = as_pandas
    if pandas_mode:
        df_for_inspect = df.to_pandas()
    else:
        df_for_inspect = df

    # Determine unique agencies
    if pandas_mode:
        agencies = sorted(df_for_inspect["agency"].dropna().unique())
    else:
        agencies = [a for a in df.select("agency").unique().to_series().to_list() if a is not None]

    for agency in agencies:
        if agency_filter and agency != agency_filter:
            continue
        if pandas_mode:
            sub = df_for_inspect[df_for_inspect["agency"] == agency]
            date_strs = sorted(sub["date_str"].dropna().unique())
        else:
            sub = df.filter(pl.col("agency") == agency)
            date_strs = [d for d in sub.select("date_str").unique().to_series().to_list() if d is not None]

        for date_str in date_strs:
            try:
                if pandas_mode:
                    part_pd = sub[sub["date_str"] == date_str]
                    part_pl = pl.from_pandas(part_pd)
                else:
                    part_pl = sub.filter(pl.col("date_str") == date_str)

                if part_pl.is_empty():
                    continue

                out_path = save_to_parquet_partitioned(part_pl, output_dir, agency, feed_type, date_str)
                logger.info("Saved %s rows to %s", part_pl.height, out_path)
            except Exception as exc:
                logger.exception("Failed saving partition agency=%s date=%s: %s", agency, date_str, exc)
                continue


def main(argv: Optional[List[str]] = None) -> None:
    """CLI entry point for processing GTFS-rt JSON snapshots.

    Args:
        argv: Optional list of arguments (for testing). If None, uses sys.argv.
    """
    parser = argparse.ArgumentParser(description="Process GTFS-rt JSON snapshots into Parquet partitions.")
    parser.add_argument("--input-dir", type=Path, default=Path("./data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("./data/bronze"))
    parser.add_argument("--feed-type", choices=["trip_updates", "vehicle_positions", "both"], default="both")
    parser.add_argument("--agency-filter", type=str, default="", help="Only process this agency if provided")
    parser.add_argument("--as-pandas", action="store_true", help="Convert DataFrame to pandas for downstream consumers (internally still writes parquet via polars)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    feed_types: List[str]
    if args.feed_type == "both":
        feed_types = ["trip_updates", "vehicle_positions"]
    else:
        feed_types = [args.feed_type]

    for ft in feed_types:
        try:
            logger.info("Loading feed_type=%s from %s", ft, args.input_dir)
            df = load_all_snapshots(args.input_dir, ft)  # pl.DataFrame
            logger.info("Loaded %s records for feed_type=%s", df.height, ft)

            # Optionally convert to pandas for downstream work. We'll keep a polars copy
            # for saving; if as_pandas is requested we still allow inspection in pandas.
            if args.as_pandas:
                _ = to_pandas_if_needed(df, True)

            _group_and_save(df, args.output_dir, ft, args.agency_filter or None, args.as_pandas)
        except Exception:
            logger.exception("Unhandled error while processing feed_type=%s", ft)
            continue


if __name__ == "__main__":
    main()
