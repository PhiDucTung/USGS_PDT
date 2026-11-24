"""Silver station, grid alignment and weekly feature transformations."""
from collections import Counter
import json
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from bronze.scripts.core import Config, Store, atomic_json, job_path, now
from silver.scripts.preprocess import clean_day, metadata, require_complete, audit_raw, sha256
from grid_huc.build_grid_huc import read_grid, read_huc, map_grid
from usgs_pipeline.medallion_common import *  # noqa: F401,F403

def silver(cfg, grid_path, wbd_path):
    root = Path(cfg.output)
    silver_root = SILVER_ROOT
    for source in (grid_path,wbd_path):
        if not source.is_file(): raise FileNotFoundError(f'Required input missing: {source}')
    require_complete(cfg)
    if not audit_raw(cfg)['passed']:
        raise ValueError('Raw audit failed')
    grid = read_grid(grid_path, 'grid_id', 'lon', 'lat')
    wbd, crs = read_huc(wbd_path, 'WBDHU8', 'huc8')
    mapped = map_grid(grid, wbd)
    catalog = json.loads((root / 'catalog.json').read_text())
    sites = sorted({r['site_id'] for r in catalog if r['decision'] == 'included'})
    meta, issues = metadata(catalog, sites)
    stations = pd.DataFrame([{'site_id': s, 'lon': meta[s]['coordinates'][0] if meta[s]['coordinates'] else None,
        'lat': meta[s]['coordinates'][1] if meta[s]['coordinates'] else None,
        **{f'huc{k}': v for k,v in meta[s]['hucs'].items()}} for s in sites])
    store = Store(cfg)
    try:
        jobs = list(store.db.execute('SELECT * FROM jobs ORDER BY site,year'))
        with stage(silver_root, 'silver', [root/'catalog.json', grid_path, wbd_path]) as (run,m):
            m['config'] = asdict(cfg)
            provenance=[grid_path.with_suffix('.source.json'),wbd_path.parent/'wbd_source.json']
            m['sources'].extend(inputs([p for p in provenance if p.is_file()]))
            m['sources'].extend(inputs(sorted((root/'raw').rglob('*.json.gz'))))
            save(stations, run/'stations.parquet')
            save(grid, run/'grid.parquet')
            save(mapped, run/'grid_huc.parquet')
            save(station_alignment(stations, grid), run/'station_grid.parquet')
            save(mapped[mapped.huc8.isna()], run/'review_unresolved.parquet')
            counts = Counter(); daily_rows = 0; valid_rows = 0
            # Bronze remains immutable; catalog, raw sidecars and normalized files are referenced.
            bronze = inputs(sorted((root/'raw').rglob('*.meta.json')))
            m['sources'].extend(bronze)
            atomic_json(run/'bronze_manifest.json', {'raw_sidecars': bronze, 'raw_audit': json.loads((root/'reports/raw_audit.json').read_text())})
            for j in jobs:
                source = job_path(root,j)
                m['sources'].extend(inputs([source]))
                groups = {}
                for r in pq.ParquetFile(source).read().to_pylist():
                    groups.setdefault(r['date'], []).append(r)
                rows = [clean_day(j['site'], t.date().isoformat(), groups.get(t.date().isoformat(), []))
                        for t in pd.date_range(j['start'], j['end'])]
                df = pd.DataFrame(rows); df['unit'] = 'cfs'
                validate_daily(df, j['site'], j['start'], j['end'], check_qc=True)
                daily_rows += len(df); valid_rows += int(df.discharge_cfs.notna().sum())
                counts.update(flag for row in rows for flag in row['qc_flags'])
                save(df,run/'station_daily'/f"region={j['region']}"/f"year={j['year']}"/f"{j['site']}.parquet")
            atomic_json(run/'report.json', {'passed': True, 'daily_rows': daily_rows, 'valid_daily_rows': valid_rows, 'missing_daily_rows': daily_rows-valid_rows, 'filled_values': 0, 'stations':len(stations),'grid_cells':len(grid), 'unresolved_grid_huc':int(mapped.huc8.isna().sum()),'metadata_issues':issues,'qc_counts':dict(counts),'wbd_crs':crs})
        return run
    finally:
        store.close()

def validate_daily(df, site, start, end, check_qc=False):
    """One record per calendar day; missing values stay null and zero stays valid."""
    dates = pd.to_datetime(df.date, errors='raise')
    expected = pd.date_range(start, end)
    if dates.duplicated().any() or set(dates) != set(expected):
        raise ValueError(f'Incomplete or duplicate daily calendar: {site}')
    if 'site_id' in df and not df.site_id.eq(site).all():
        raise ValueError(f'Unexpected station in daily table: {site}')
    values = pd.to_numeric(df.discharge_cfs, errors='raise')
    if not np.isfinite(values.dropna()).all():
        raise ValueError(f'Non-finite discharge: {site}')
    if check_qc:
        flagged = df.qc_flags.map(len).gt(0)
        if not flagged.eq(values.isna()).all() or not df.unit.eq('cfs').all():
            raise ValueError(f'Daily QC/null or unit mismatch: {site}')


def paper_percentile(values):
    values = np.asarray(values, dtype=float)
    out = np.full(len(values), np.nan)
    valid = np.isfinite(values)
    ordered = np.sort(values[valid]); n = len(ordered)
    if n:
        q = values[valid]
        left = np.searchsorted(ordered,q,'left'); right = np.searchsorted(ordered,q,'right')
        multiplicity = right-left
        position = left + np.where(multiplicity < 3, 1, (multiplicity+1)//2)
        out[valid] = 100*position/n
    return out

def weekly_station(daily, site):
    start, end = METHOD['baseline_start'], METHOD['baseline_end']
    daily = daily.copy(); daily['date'] = pd.to_datetime(daily.date)
    if daily.date.duplicated().any(): raise ValueError('Duplicate station/day')
    series = daily.set_index('date').discharge_cfs.reindex(pd.date_range(start,end)).astype(float)
    dates = pd.date_range(start,end,freq='W-TUE')
    result = pd.DataFrame({'site_id':site,'week_date':dates})
    for w in WINDOWS:
        count = series.shift(1).rolling(w,min_periods=1).count().reindex(dates).fillna(0)
        q = series.shift(1).rolling(w,min_periods=METHOD['min_days'][str(w)]).mean().reindex(dates)
        q.loc[dates-pd.Timedelta(days=w) < pd.Timestamp(start)] = np.nan
        result[f'q{w}'] = q.to_numpy()
        result[f'p{w}'] = paper_percentile(q.to_numpy())
        result[f'n_days{w}'] = count.to_numpy(dtype=int)
        result[f'n_baseline{w}'] = int(q.notna().sum())
        result[f'reason{w}'] = np.where(dates-pd.Timedelta(days=w)<pd.Timestamp(start),'window_before_start',np.where(q.isna(),'insufficient_valid_days','ok'))
    return result

def weekly(silver_run):
    sm = verified(silver_run, expected_stage='silver')
    with stage(SILVER_ROOT,'weekly',[silver_run/'manifest.json']) as (run,m):
        m['silver_run'] = str(silver_run.resolve())
        stations = read(silver_run/'stations.parquet')
        files = {}
        for p in (silver_run/'station_daily').rglob('*.parquet'):
            files.setdefault(p.stem,[]).append(p)
        coverage=[]
        for site in stations.site_id:
            daily = pd.concat([read(p) for p in files[site]],ignore_index=True)
            validate_daily(daily, site, METHOD['baseline_start'], METHOD['baseline_end'])
            df = weekly_station(daily,site)
            coverage.append({'site_id':site,**{f'n_baseline{w}':int(df[f'n_baseline{w}'].iloc[0]) for w in WINDOWS}})
            for year,group in df.groupby(df.week_date.dt.year):
                save(group,run/'station_weekly'/f'year={year}'/f'{site}.parquet')
        save(pd.DataFrame(coverage),run/'baseline_coverage.parquet')
        atomic_json(run/'report.json',{'passed': True, 'stations':len(stations),'weeks_per_station':len(pd.date_range(METHOD['baseline_start'], METHOD['baseline_end'], freq='W-TUE')), 'baseline':METHOD,'silver_manifest_sha256':sha256(silver_run/'manifest.json')})
    return run

