"""Create one Gold Parquet with explicit HUC-based fallback values.

The original Gold stays unchanged. For each percentile window, retain the
direct value when present; otherwise restrict donors to the target's clipped
20x20 grid neighborhood, then prefer IDW and HUC-8, HUC-6, HUC-4 in order.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


WINDOWS = (7, 14, 28)
LEVELS = (8, 6, 4)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def add_filled_columns(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    summary: dict[str, dict] = {}
    for window in WINDOWS:
        direct_col = f"p{window}"
        filled_col = f"p{window}_filled"
        method_col = f"p{window}_fill_method"
        level_col = f"p{window}_fill_huc_level"

        direct = frame[direct_col]
        filled = direct.copy()
        method = pd.Series("observed", index=frame.index, dtype="object")
        level = pd.Series("direct", index=frame.index, dtype="object")
        missing = direct.isna()

        for huc_level in LEVELS:
            candidates = missing & filled.isna()
            if not candidates.any():
                break

            count_col = f"n_donors_huc{huc_level}_{window}"
            idw_col = f"idw_huc{huc_level}_{window}"
            mean_col = f"mean_huc{huc_level}_{window}"
            has_donors = frame[count_col].fillna(0).gt(0)

            # Prefer IDW; use the same-level mean only when IDW is unavailable.
            use_idw = candidates & has_donors & frame[idw_col].notna()
            filled.loc[use_idw] = frame.loc[use_idw, idw_col]
            method.loc[use_idw] = "idw"
            level.loc[use_idw] = str(huc_level)

            still_missing = candidates & filled.isna()
            use_mean = still_missing & has_donors & frame[mean_col].notna()
            filled.loc[use_mean] = frame.loc[use_mean, mean_col]
            method.loc[use_mean] = "mean"
            level.loc[use_mean] = str(huc_level)

        method.loc[filled.isna()] = "unfilled"
        level.loc[filled.isna()] = "unresolved"
        frame[filled_col] = filled
        frame[method_col] = method
        frame[level_col] = level

        summary[str(window)] = {
            "original_missing": int(direct.isna().sum()),
            "filled_total": int((direct.isna() & filled.notna()).sum()),
            "remaining_missing": int(filled.isna().sum()),
            "method_counts": {str(k): int(v) for k, v in method.value_counts().items()},
            "huc_level_counts_for_imputed": {
                str(k): int(v)
                for k, v in level.loc[direct.isna() & filled.notna()].value_counts().items()
            },
        }
    return frame, summary


def lattice_indices(grid: pd.DataFrame) -> dict[str, tuple[int, int]]:
    """Return each grid's row/column in the 0.5-degree USDM lattice."""
    north, west = float(grid.Lat.max()), float(grid.Lon.min())
    row_by_lat = {value: round((north - float(value)) / 0.5) for value in grid.Lat.unique()}
    col_by_lon = {value: round((float(value) - west) / 0.5) for value in grid.Lon.unique()}
    if any(abs((north - value) / 0.5 - row_by_lat[value]) > 1e-7 for value in row_by_lat):
        raise ValueError("USDM latitude coordinates are not aligned to 0.5-degree cells")
    if any(abs((value - west) / 0.5 - col_by_lon[value]) > 1e-7 for value in col_by_lon):
        raise ValueError("USDM longitude coordinates are not aligned to 0.5-degree cells")
    return {
        r.grid_id: (row_by_lat[r.Lat], col_by_lon[r.Lon])
        for r in grid.itertuples()
    }


def add_local_filled_columns(frame: pd.DataFrame, input_gold: Path,
                             silver_run: Path) -> tuple[pd.DataFrame, dict]:
    """Restrict each HUC donor pool to the clipped 20x20 grid neighborhood."""
    grid = pd.read_parquet(silver_run / "grid.parquet")
    station_grid = pd.read_parquet(silver_run / "station_grid.parquet")
    grid_cell = lattice_indices(grid)
    station_cell = {
        r.site_id: grid_cell[r.grid_id]
        for r in station_grid.itertuples()
        if isinstance(r.grid_id, str) and r.grid_id in grid_cell
    }

    stats = {}
    # Read each weekly lineage file alongside its matching grid-week file.
    lineage_root = input_gold / "lineage"
    weekly_root = input_gold / "grid_weekly"
    for weekly_path in sorted(weekly_root.rglob("*.parquet")):
        lineage_path = lineage_root / weekly_path.parent.name / weekly_path.name
        weekly = pd.read_parquet(weekly_path)
        lineage = pd.read_parquet(lineage_path)
        lineage_by_key = {
            (r.grid_id, int(r.window), int(r.level)): r
            for r in lineage.itertuples()
            if int(r.level) in LEVELS
        }
        for r in weekly.itertuples():
            target = grid_cell[r.grid_id]
            # Exactly 20 rows by 20 columns when in the interior; clip at edges.
            r0, c0 = target
            neighborhood = (range(max(0, r0 - 10), r0 + 10),
                            range(max(0, c0 - 10), c0 + 10))
            rmin, rmax = neighborhood[0].start, neighborhood[0].stop
            cmin, cmax = neighborhood[1].start, neighborhood[1].stop

            for window in WINDOWS:
                if pd.notna(getattr(r, f"p{window}")):
                    continue
                for level in LEVELS:
                    rec = lineage_by_key.get((r.grid_id, window, level))
                    if rec is None:
                        stats[(r.grid_id, r.week_date, window, level)] = (None, None, 0)
                        continue
                    selected = [i for i, site in enumerate(rec.site_ids)
                                if site in station_cell
                                and rmin <= station_cell[site][0] < rmax
                                and cmin <= station_cell[site][1] < cmax]
                    if not selected:
                        stats[(r.grid_id, r.week_date, window, level)] = (None, None, 0)
                        continue
                    values = np.asarray(rec.percentiles, dtype=float)[selected]
                    distances = np.asarray(rec.distances_degrees, dtype=float)[selected]
                    mean = float(values.mean())
                    zero = distances == 0
                    idw = float(values[zero].mean() if zero.any()
                                else np.average(values, weights=1 / distances))
                    stats[(r.grid_id, r.week_date, window, level)] = (mean, idw, len(values))

    summary = {}
    for window in WINDOWS:
        # Keep previous all-HUC result in the same file for comparison.
        frame = frame.rename(columns={
            f"p{window}_filled": f"p{window}_filled_all_huc",
            f"p{window}_fill_method": f"p{window}_fill_method_all_huc",
            f"p{window}_fill_huc_level": f"p{window}_fill_huc_level_all_huc",
        })
        local_values, methods, levels = [], [], []
        local_means = {level: [] for level in LEVELS}
        local_idws = {level: [] for level in LEVELS}
        local_counts = {level: [] for level in LEVELS}
        for r in frame.itertuples():
            direct = getattr(r, f"p{window}")
            value, method, chosen_level = direct, "observed", "direct"
            for level in LEVELS:
                mean, idw, count = stats.get((r.grid_id, r.week_date, window, level), (None, None, 0))
                local_means[level].append(mean)
                local_idws[level].append(idw)
                local_counts[level].append(count)
                if pd.isna(direct) and pd.isna(value) and count:
                    if pd.notna(idw):
                        value, method, chosen_level = idw, "idw", str(level)
                    elif pd.notna(mean):
                        value, method, chosen_level = mean, "mean", str(level)
            if pd.isna(value):
                method, chosen_level = "unfilled", "unresolved"
            local_values.append(value)
            methods.append(method)
            levels.append(chosen_level)
        for level in LEVELS:
            frame[f"mean_huc{level}_{window}_local20"] = local_means[level]
            frame[f"idw_huc{level}_{window}_local20"] = local_idws[level]
            frame[f"n_donors_huc{level}_{window}_local20"] = local_counts[level]
        frame[f"p{window}_filled"] = local_values
        frame[f"p{window}_fill_method"] = methods
        frame[f"p{window}_fill_huc_level"] = levels
        direct_missing = frame[f"p{window}"].isna()
        filled = frame[f"p{window}_filled"].notna()
        summary[str(window)] = {
            "original_missing": int(direct_missing.sum()),
            "filled_total": int((direct_missing & filled).sum()),
            "remaining_missing": int(frame[f"p{window}_filled"].isna().sum()),
            "method_counts": {str(k): int(v) for k, v in frame[f"p{window}_fill_method"].value_counts().items()},
            "huc_level_counts_for_imputed": {
                str(k): int(v) for k, v in frame.loc[direct_missing & filled,
                    f"p{window}_fill_huc_level"].value_counts().items()
            },
            "donor_count_total_after_spatial_filter": int(sum(
                int(frame[f"n_donors_huc{level}_{window}_local20"].sum()) for level in LEVELS
            )),
        }
    return frame, summary


def build(input_gold: Path, silver_run: Path, output_file: Path) -> dict:
    files = sorted((input_gold / "grid_weekly").rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No grid_weekly Parquet files under {input_gold}")

    frames = [pd.read_parquet(path) for path in files]
    gold = pd.concat(frames, ignore_index=True)
    gold = gold.sort_values(["week_date", "grid_id"], kind="stable").reset_index(drop=True)
    if gold.duplicated(["grid_id", "week_date"]).any():
        raise ValueError("Gold has duplicate (grid_id, week_date) rows")

    # Preserve the earlier unrestricted-HUC version for a transparent comparison.
    if not all(f"p{w}_filled" in gold for w in WINDOWS):
        gold, _ = add_filled_columns(gold)
    gold, fill_summary = add_local_filled_columns(gold, input_gold, silver_run)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    gold.to_parquet(output_file, index=False)

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_gold": str(input_gold.resolve()),
        "source_file_count": len(files),
        "source_sha256_by_relative_path": {
            str(path.relative_to(input_gold)): sha256(path) for path in files
        },
        "output_file": str(output_file.resolve()),
        "output_sha256": sha256(output_file),
        "row_count": len(gold),
        "unique_grid_week_rows": int(gold[["grid_id", "week_date"]].drop_duplicates().shape[0]),
        "fill_rule": "Keep direct pN; otherwise restrict HUC donors to a clipped 20x20 cell neighborhood (offsets -10..+9); then HUC-8, HUC-6, HUC-4; at each level prefer IDW, then mean.",
        "silver_run": str(silver_run.resolve()),
        "window_summary": fill_summary,
        "note": "Local donor filtering uses each station's mapped grid cell and the USDM 0.5-degree lattice. Previous unrestricted-HUC filled values are retained as pN_filled_all_huc.",
    }
    report_file = output_file.with_suffix(".report.json")
    report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, required=True, help="Gold run directory")
    parser.add_argument("--silver", type=Path, required=True, help="Matching Silver run directory")
    parser.add_argument("--out", type=Path, required=True, help="Output single Parquet file")
    args = parser.parse_args()
    report = build(args.gold, args.silver, args.out)
    print(json.dumps({k: v for k, v in report.items() if k != "source_sha256_by_relative_path"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
