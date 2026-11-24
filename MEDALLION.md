# Streamflow Bronze → Silver → Gold

Python implementation for retrospective 2020–2022 analysis. Baseline is fixed to
2020-01-01 through 2022-12-31, per station/window. This is not an exact recreation
of the paper's historical baseline and must not be used unchanged for forecast
validation within those years.

## Inputs and commands

Run from `/Users/macbook/QTDL/USGS`. Install optional dependencies with
`.venv/bin/python -m pip install -e '.[gold,test]'`.
The existing `config.example.toml` specifies the raw archive and exact date scope.
Do not expand its dates for this method.

Download the original Dryad `USDMDataAvg.csv` dated 2024-03-14. Its values are not
used as streamflow features; only the original grid ID and centre coordinates are
extracted. The extractor checks the published checksum by default.

```sh
.venv/bin/python grid_huc/prepare_paper_inputs.py grid --csv /absolute/path/USDMDataAvg.csv
# Prepare WBD only when the polygon snapshot is missing:
.venv/bin/python grid_huc/prepare_paper_inputs.py wbd
.venv/bin/python -m usgs_pipeline build-gold --config config.example.toml
```

The default grid is `bronze/data/grid_huc_inputs/grid.csv`; the prepared polygon file is
`bronze/data/grid_huc_inputs/wbd/WBDHU8.gpkg`. Override using `--grid` and `--wbd`.
A missing input stops the run before expensive raw validation.

Stages can also be run separately, passing the exact run path printed previously:

```sh
.venv/bin/python -m usgs_pipeline silver --config config.example.toml
.venv/bin/python -m usgs_pipeline weekly --silver-run silver/data/medallion/silver/RUN_ID
.venv/bin/python -m usgs_pipeline gold --weekly-run silver/data/medallion/weekly/RUN_ID
.venv/bin/python -m usgs_pipeline validate-gold --gold-run gold/data/medallion/gold/RUN_ID
```

Each stage creates a new run. The CLI shares the existing pipeline lock.
Upstream completion, method and file checksums are checked. There is no automatic
reuse of old runs; explicit upstream paths provide reuse without overwriting data.
Interrupted runs remain `incomplete` and cannot be consumed downstream.

## Data contracts

Bronze remains in the existing raw archive. The Silver Bronze manifest references
raw sidecars, which contain acquisition metadata and raw checksums; the raw audit
must pass. Normalized Parquet and catalog checksums are recorded separately.

Silver: `station_daily/region=XX/year=YYYY/*.parquet` retains daily QC, source IDs,
qualifiers, approval status, cfs unit and null missing values. Catalog metadata is
stored in `stations.parquet`. Grid, station-grid and grid-HUC tables are separate.
Ambiguous HUCs remain null and are also exported to `review_unresolved.parquet`.
Stations with invalid coordinates/HUC remain in the metadata and coverage reports.

Weekly: `station_weekly/year=YYYY/SITE.parquet` has one row per station/Tuesday,
`q7/q14/q28`, `p7/p14/p28`, valid-day counts, baseline counts and missing reasons.
`baseline_coverage.parquet` summarizes each station/window without silently
excluding short station records. No seasonal grouping or minimum baseline sample
threshold is applied. An empty baseline yields null percentiles.

Gold: `grid_weekly/year=YYYY/YYYYMMDD.parquet` includes every grid and Tuesday,
with unique `(grid_id, week_date)`, direct `p7/p14/p28`, `n_stations7/14/28`,
and `mean_huc{8|6|4}_{7|14|28}`, `idw_huc...`, `n_donors_huc...`, `reason_huc...`.
Alternatives are populated only if direct grid percentile is missing. No final
fallback priority is selected. `lineage` stores one row per grid/week/window/level
with aligned station IDs, source percentiles and distances; level 0 is direct.
Follow the weekly/Silver manifests to the daily records and raw source IDs.

`usgs_pipeline/fill_gold.py` creates a single Parquet retaining every Gold
column and adding `p{7|14|28}_filled`, `p{7|14|28}_fill_method`, and
`p{7|14|28}_fill_huc_level`. It preserves direct percentiles and fills missing
ones using only donor stations whose assigned grid cell lies in the target's
clipped 20x20 neighborhood on the 0.5-degree lattice (offsets -10 through +9).
It then tries HUC-8, HUC-6 and HUC-4, preferring IDW over mean at each level.
The earlier unrestricted-HUC result remains in `p{7|14|28}_filled_all_huc` for
comparison. The output has a JSON report beside it.

## Method and deliberate differences

Reference: heplersa/USDMdata commit
`5419a165a6366473e1021c8294076a03e23b2e11`.

- Windows are `[t-w,t-1]` at Tuesday t, requiring 4/7, 8/14 or 15/28 valid days.
  Windows starting before 2020-01-01 remain null, including otherwise usable
  partial windows. No daily imputation is performed before aggregation.
- Percentile uses the original MATLAB tie convention, not pandas average ranks:
  first position for 1–2 ties; ceil(m/2)th tied position for m≥3; rank/N×100.
- Stations use nearest grid by L1 decimal-degree difference; ties use original
  extracted grid order and are flagged. Grid HUC uses polygon containment of the
  centre. The WBD snapshot is current, not the historical paper vintage.
- Direct values average station percentiles. Donors are valid original station
  percentiles at the same Tuesday/window and matching station HUC prefix.
- The standard Gold HUC alternatives use all eligible donors independently.
  The companion filled Parquet also provides a separate 20x20 local-window
  variant, matching the donor search's spatial restriction in the MATLAB code.
- IDW uses Euclidean decimal degrees and exponent 1. Zero-distance donors take
  precedence and their mean is returned. No filled value becomes a donor.
- Station catalog HUC and current WBD may differ in vintage; this limitation is
  retained, and no station code is silently reassigned.

Gold reports automated validation, per-column coverage and contributor-count
histograms. Automated completion does not replace the independent manual USGS
comparison of at least five stations. `manual_comparison_completed` and
`acceptance_status` keep that distinction visible.

## Verification

`.venv/bin/python -m pytest -q` covers the existing ingestion/QC tests plus MATLAB
rank ties, early windows, leap dates, exact thresholds, station-grid ties, HUC
missingness, zero-distance IDW, stage checksums and full synthetic stage execution.


## Versioned contracts and acceptance gates

New manifests use `manifest_version: 2` and explicitly label the Medallion layer;
`weekly` belongs to Silver as a derived station table. Every Parquet output records
its row count and Arrow field names, types and nullability beside its checksum.
Existing manifests remain readable. No existing run is rewritten or migrated.

Silver records raw `.json.gz` checksums as direct sources in addition to catalog,
normalized Parquet, grid and WBD. Verification traverses upstream manifests and
checks their sources and outputs, rejecting cycles, incompatible methods and
incomplete runs. Legacy Silver manifests that only recorded sidecars do not gain
raw-payload coverage retroactively; create a new Silver run for this guarantee.
Raw acquisition files remain immutable; the checkpoint is operational mutable state.

Daily tables require exactly one row per station/calendar day, finite or null cfs,
and agreement between nulls and QC flags. Zero and negative valid measurements
remain available. Weekly consumes a complete daily calendar for 2020–2022,
including null rows for missing observations; a truncated input stops the run.
Silver's report includes daily row, valid, missing and QC counts, with `filled_values=0`.

Gold recomputes direct values and HUC alternatives from upstream weekly records
and compares all output columns, including missing values, counts and reason codes.
The existing lineage checks also verify source percentiles, membership, distances,
means and IDW. `validate-gold` exits 2 on a failed quality report; corrupt or missing
upstream artifacts cause an error and cannot produce an accepted report.

The output calendar is 156 Tuesdays across 2020–2022; expected Gold size is
`156 × number of input grid cells`. USDM's example covers only 130 weeks and its
3,259-cell count is not imposed here. Grid IDs and coordinates come from the
explicit grid input. For a future join, align common grid IDs, coordinates and
Tuesday dates rather than joining by row order. The historical Dryad grid and
new USDM raster may differ (including cell R25).

The independent five-station comparison remains a separate acceptance condition.
A successful synthetic test run is not evidence that the local production archive
has been fully downloaded, audited or independently compared.


## Physical data layout

Bronze uses `bronze/data/`, Silver uses `silver/data/`, and Gold uses
`gold/data/`. Configure new runs with those paths. Existing manifests are migrated
to these canonical paths; the legacy root `data/` directory has been removed.
