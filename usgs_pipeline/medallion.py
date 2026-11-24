"""Versioned retrospective streamflow features; baseline and output 2020–2022."""
from __future__ import annotations
import argparse
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict
import json
from pathlib import Path
import time
import tomllib
import fcntl
from importlib.metadata import version
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from .core import Config, Store, atomic_json, job_path, now
from .preprocess import clean_day, metadata, require_complete, audit_raw, sha256
from grid_huc.build_grid_huc import read_grid, read_huc, map_grid

METHOD = {
    'reference_commit': '5419a165a6366473e1021c8294076a03e23b2e11',
    'reference_repository': 'https://github.com/heplersa/USDMdata',
    'baseline_start': '2020-01-01', 'baseline_end': '2022-12-31',
    'min_days': {'7': 4, '14': 8, '28': 15}, 'week_day': 'Tuesday',
    'window': '[t-window,t-1]; full calendar window required',
    'rank': 'MATLAB: first of 1/2 ties; ceil(m/2)th of >=3 ties, 1-based',
    'station_grid': 'minimum L1 decimal degrees; first original grid row breaks ties',
    'idw': '1/Euclidean decimal degrees; mean of zero-distance donors if any',
    'donors': 'all valid station percentiles in HUC, no 20-donor cap or spatial window',
    'purpose': 'retrospective analysis; not forecast validation or exact paper replication',
}
WINDOWS = (7, 14, 28)
REPO_ROOT = Path(__file__).resolve().parents[1]
SILVER_ROOT = REPO_ROOT / "silver" / "data"
GOLD_ROOT = REPO_ROOT / "gold" / "data"

def save(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def read(path):
    return pq.ParquetFile(path).read().to_pandas()

def inputs(paths):
    return [{'path': str(Path(p).resolve()), 'sha256': sha256(p)} for p in paths]

def verified(run, expected_stage=None, _active=None, _checked=None):
    """Verify the entire manifest dependency graph, including raw sources."""
    run = Path(run).resolve()
    active = set() if _active is None else _active
    checked = {} if _checked is None else _checked
    if run in active:
        raise ValueError(f'Cyclic manifest dependency: {run}')
    m = json.loads((run / 'manifest.json').read_text())
    if expected_stage is not None and m.get('stage') != expected_stage:
        raise ValueError(f'Expected {expected_stage} stage: {run}')
    if run in checked:
        return checked[run]
    if m['status'] != 'complete' or m['method'] != METHOD:
        raise ValueError('Upstream run incomplete or method incompatible')
    active.add(run)
    for item in m['sources']:
        if sha256(item['path']) != item['sha256']:
            raise ValueError(f"Source checksum mismatch: {item['path']}")
        source = Path(item['path'])
        if source.name == 'manifest.json':
            parent = json.loads(source.read_text())
            if 'method' in parent and 'stage' in parent:
                verified(source.parent, _active=active, _checked=checked)
    for item in m['outputs']:
        if sha256(run / item['path']) != item['sha256']:
            raise ValueError(f"Upstream checksum mismatch: {item['path']}")
    active.remove(run)
    checked[run] = m
    return m

@contextmanager
def stage(root, name, sources):
    run = root / 'medallion' / name / str(time.time_ns())
    run.mkdir(parents=True)
    code = list(Path(__file__).parent.glob('*.py')) + list(Path(__file__).parent.parent.joinpath('grid_huc').glob('*.py'))
    m = {'manifest_version': 2, 'layer': 'silver' if name == 'weekly' else name, 'status': 'running', 'stage': name, 'created_at': now(), 'method': METHOD,
         'sources': inputs(sources), 'code': inputs(code), 'outputs': []}
    m['versions']={name:version(name) for name in ['pandas','numpy','pyarrow','geopandas','shapely','pyogrio']}
    atomic_json(run / 'manifest.json', m)
    try:
        yield run, m
        m['outputs'] = [{'path': str(p.relative_to(run)), 'sha256': sha256(p)}
                        for p in sorted(run.rglob('*')) if p.is_file() and p.name != 'manifest.json']
        for item in m['outputs']:
            if item['path'].endswith('.parquet'):
                parquet = pq.ParquetFile(run / item['path'])
                item['rows'] = parquet.metadata.num_rows
                item['schema'] = [{'name': f.name, 'type': str(f.type), 'nullable': f.nullable}
                                  for f in parquet.schema_arrow]
        m.update(status='complete', completed_at=now())
    except BaseException as exc:
        m.update(status='incomplete', error=str(exc))
        raise
    finally:
        atomic_json(run / 'manifest.json', m)

def station_alignment(stations, grid):
    rows = []
    xy = grid[['Lon', 'Lat']].to_numpy(float)
    for s in stations.itertuples():
        if pd.isna(s.lon) or pd.isna(s.lat):
            rows.append((s.site_id, None, None, False))
        else:
            d = np.abs(xy - [s.lon, s.lat]).sum(axis=1)
            idx = np.flatnonzero(d == d.min())
            rows.append((s.site_id, grid.grid_id.iloc[idx[0]], float(d[idx[0]]), len(idx) > 1))
    return pd.DataFrame(rows, columns=['site_id', 'grid_id', 'distance_degrees_l1', 'tie'])

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

def aggregate_week(grid, mapped, stations, alignment, observations, run_id):
    merged = observations.merge(stations,on='site_id',validate='one_to_one').merge(alignment[['site_id','grid_id']],on='site_id',validate='one_to_one')
    output, lineage = [], []
    def pack(frame, w):
        return (frame.site_id.tolist(), frame[f'p{w}'].to_numpy(float),
                frame.lon.to_numpy(float), frame.lat.to_numpy(float))
    direct_pools = {(w,code):pack(g,w) for w in WINDOWS
                    for code,g in merged[merged[f'p{w}'].notna()].groupby('grid_id')}
    hucs = mapped.set_index('grid_id').to_dict('index')
    pools = {(w,level,code):pack(g,w) for w in WINDOWS for level in (8,6,4)
             for code,g in merged[merged[f'p{w}'].notna() & merged.lon.notna() & merged.lat.notna()].groupby(f'huc{level}')}
    day = observations.week_date.iloc[0]
    for cell in grid.itertuples():
        row = {'grid_id':cell.grid_id,'week_date':day,'run_id':run_id}
        for w in WINDOWS:
            obs = direct_pools.get((w,cell.grid_id))
            nobs = len(obs[0]) if obs is not None else 0
            row[f'p{w}'] = float(obs[1].mean()) if nobs else None
            row[f'n_stations{w}'] = nobs
            row[f'reason{w}'] = 'direct' if nobs else 'no_valid_station'
            if nobs:
                lineage.append({'grid_id':cell.grid_id,'week_date':day,'window':w,'level':0,'site_ids':obs[0],'percentiles':obs[1].tolist(),'distances_degrees':[]})
            for level in (8,6,4):
                key = f'huc{level}_{w}'
                code = hucs[cell.grid_id][f'huc{level}']
                pool = pools.get((w,level,code)) if pd.notna(code) and not nobs else None
                row[f'mean_{key}'] = row[f'idw_{key}'] = None
                row[f'n_donors_{key}'] = 0
                row[f'reason_{key}'] = 'direct_available' if nobs else 'missing_huc' if pd.isna(code) else 'no_valid_donors'
                if pool is not None:
                    distances = np.hypot(pool[2]-cell.Lon,pool[3]-cell.Lat)
                    values = pool[1]
                    zero = distances == 0
                    row[f'mean_{key}'] = float(values.mean())
                    row[f'idw_{key}'] = float(values[zero].mean() if zero.any() else np.average(values,weights=1/distances))
                    row[f'n_donors_{key}'] = len(pool[0])
                    row[f'reason_{key}'] = 'ok'
                    lineage.append({'grid_id':cell.grid_id,'week_date':day,'window':w,'level':level,'site_ids':pool[0],'percentiles':values.tolist(),'distances_degrees':distances.tolist()})
        output.append(row)
    return pd.DataFrame(output), pd.DataFrame(lineage,columns=['grid_id','week_date','window','level','site_ids','percentiles','distances_degrees'])

def gold(weekly_run):
    wm = verified(weekly_run, expected_stage='weekly')
    sr = Path(wm['silver_run']); verified(sr, expected_stage='silver')
    grid,mapped,stations,alignment = [read(sr/f'{name}.parquet') for name in ['grid','grid_huc','stations','station_grid']]
    with stage(GOLD_ROOT,'gold',[weekly_run/'manifest.json',sr/'manifest.json']) as (run,m):
        m.update(silver_run=str(sr.resolve()),weekly_run=str(weekly_run.resolve()))
        atomic_json(run/'manifest.json',m)
        for year in (2020,2021,2022):
            allweeks = pd.concat([read(p) for p in sorted((weekly_run/'station_weekly'/f'year={year}').glob('*.parquet'))],ignore_index=True)
            for day,week in allweeks.groupby('week_date',sort=True):
                df,trace = aggregate_week(grid,mapped,stations,alignment,week,run.name)
                filename = f'{day:%Y%m%d}.parquet'
                save(df,run/'grid_weekly'/f'year={year}'/filename)
                save(trace,run/'lineage'/f'year={year}'/filename)
            print(f'Gold {year} complete',flush=True)
        report = validate_gold(run, check_manifest=False)
        atomic_json(run/'report.json',report)
        if not report['passed']: raise ValueError(report)
    return run

def validate_gold(run, check_manifest=True):
    if check_manifest: verified(run, expected_stage='gold')
    m = json.loads((run/'manifest.json').read_text())
    sr = Path(m['silver_run']); grid = read(sr/'grid.parquet')
    station_meta=read(sr/'stations.parquet').set_index('site_id')
    aligned=read(sr/'station_grid.parquet').set_index('site_id')
    grid_meta=read(sr/'grid_huc.parquet').set_index('grid_id')
    grid_xy=grid.set_index('grid_id')
    weekly_run=Path(m['weekly_run'])
    cached_year=None
    station_records=station_meta.to_dict('index')
    aligned_records=aligned.grid_id.to_dict()
    grid_records=grid_meta.to_dict('index')
    xy_records=grid_xy.to_dict('index')
    expected_dates = set(pd.date_range(METHOD['baseline_start'],METHOD['baseline_end'],freq='W-TUE'))
    dates=set(); rows=0; coverage=Counter(); problems=[]; donor_histograms={}
    for p in sorted((run/'grid_weekly').rglob('*.parquet')):
        df=read(p)
        if df.empty:
            problems.append(f'Empty Gold partition: {p}'); continue
        day=pd.Timestamp(df.week_date.iloc[0])
        if cached_year != day.year:
            cached_year=day.year
            weekly_sources=pd.concat([read(f) for f in sorted((weekly_run/'station_weekly'/f'year={day.year}').glob('*.parquet'))],ignore_index=True)
            if weekly_sources.duplicated(['site_id','week_date']).any():
                raise ValueError('Duplicate upstream station/week')
            weekly_sources=weekly_sources.set_index(['week_date','site_id'])
        if day in dates or set(df.week_date)!= {day} or set(df.grid_id)!=set(grid.grid_id) or df.grid_id.duplicated().any(): problems.append(str(p))
        dates.add(day); rows+=len(df)
        for c in df.columns:
            if c.startswith(('n_donors_','n_stations')):
                hist=donor_histograms.setdefault(c,Counter())
                hist.update(int(n) for n in df[c])
            if c in ['p7','p14','p28'] or c.startswith(('mean_','idw_')):
                v=df[c].dropna().astype(float)
                if not np.isfinite(v).all() or not v.between(-1e-10,100+1e-10).all(): problems.append(c)
                coverage[c]+=len(v)
        observations = weekly_sources.xs(day).reset_index().assign(week_date=day)
        expected, _ = aggregate_week(grid, grid_meta.reset_index(), station_meta.reset_index(),
                                     aligned.reset_index(), observations, run.name)
        try:
            pd.testing.assert_frame_equal(
                df.sort_values('grid_id').reset_index(drop=True),
                expected.sort_values('grid_id').reset_index(drop=True),
                check_dtype=False, check_like=True, check_exact=False, rtol=1e-10, atol=1e-10)
        except AssertionError:
            problems.append(f'Gold differs from upstream aggregation: {day.date()}')
        source_records=weekly_sources.xs(day).to_dict('index')
        trace=read(run/'lineage'/p.parent.name/p.name)
        trace_counts=Counter((r.grid_id,r.window,r.level) for r in trace.itertuples())
        lookup=df.set_index('grid_id').to_dict('index')
        for r in trace.itertuples():
            c=f'n_stations{r.window}' if r.level==0 else f'n_donors_huc{r.level}_{r.window}'
            if lookup[r.grid_id][c]!=len(r.site_ids) or len(r.site_ids)!=len(set(r.site_ids)) or len(r.site_ids)!=len(r.percentiles): problems.append('lineage count')
            if pd.Timestamp(r.week_date)!=day: problems.append('lineage date')
            if any(site not in station_records or site not in source_records for site in r.site_ids):
                problems.append('unknown donor'); continue
            source_values=np.array([source_records[site][f'p{r.window}'] for site in r.site_ids],dtype=float)
            if len(source_values)!=len(r.percentiles) or not np.isfinite(source_values).all() or not np.allclose(source_values,r.percentiles):
                problems.append('donor differs from station percentile')
            if r.level == 0:
                if any(aligned_records[site] != r.grid_id for site in r.site_ids): problems.append('wrong direct grid')
            else:
                code=grid_records[r.grid_id][f'huc{r.level}']
                station_codes=[station_records[site][f'huc{r.level}'] for site in r.site_ids]
                if pd.isna(code) or any(pd.isna(c) or c!=code for c in station_codes): problems.append('wrong donor HUC')
                if pd.notna(lookup[r.grid_id][f'p{r.window}']): problems.append('filled direct value')
                distances=np.hypot(np.array([station_records[site]['lon'] for site in r.site_ids])-xy_records[r.grid_id]['Lon'],np.array([station_records[site]['lat'] for site in r.site_ids])-xy_records[r.grid_id]['Lat'])
                if len(distances)!=len(r.distances_degrees) or not np.allclose(distances,r.distances_degrees): problems.append('donor distance mismatch')
            vals=np.asarray(r.percentiles,dtype=float)
            if not np.isfinite(vals).all() or ((vals<0)|(vals>100)).any(): problems.append('invalid donor percentile')
            value_col=f'p{r.window}' if r.level==0 else f'mean_huc{r.level}_{r.window}'
            if not np.isclose(lookup[r.grid_id][value_col],vals.mean()): problems.append('mean mismatch')
            if r.level:
                d=np.asarray(r.distances_degrees,dtype=float)
                if len(d)!=len(vals) or not np.isfinite(d).all() or (d<0).any(): problems.append('invalid distance')
                else:
                    expected_idw=vals[d==0].mean() if (d==0).any() else np.average(vals,weights=1/d)
                    if not np.isclose(lookup[r.grid_id][f'idw_huc{r.level}_{r.window}'],expected_idw): problems.append('IDW mismatch')
        for r in df.itertuples():
            for w in WINDOWS:
                for level in (0,8,6,4):
                    c=f'n_stations{w}' if level==0 else f'n_donors_huc{level}_{w}'
                    expected=int(getattr(r,c))
                    # Positive counts require exactly one trace record.
                    if expected and trace_counts[(r.grid_id,w,level)]!=1: problems.append('missing lineage')
    if dates!=expected_dates: problems.append('incomplete Tuesday calendar')
    silver_manifest = json.loads((sr/'manifest.json').read_text())
    source_root = Path(silver_manifest.get('config', {}).get('output', REPO_ROOT/'bronze/data'))
    manual_path=source_root/'reports/manual_comparison.json'
    manual_ok=manual_path.is_file() and bool(json.loads(manual_path.read_text()).get('passed'))
    return {'passed':not problems,'issues':problems[:100],'rows':rows,'weeks':len(dates),'grid_cells':len(grid),'coverage':dict(coverage),'donor_count_histograms':donor_histograms,'manual_comparison_completed':manual_ok,'acceptance_status':'complete' if not problems and manual_ok else 'automated_checks_only' if not problems else 'failed','purpose':METHOD['purpose']}

def main(argv):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['silver','weekly','gold','validate-gold','build-gold'])
    parser.add_argument('--config',default='config.example.toml')
    parser.add_argument('--grid',default='bronze/data/grid_huc_inputs/grid.csv')
    parser.add_argument('--wbd',default='bronze/data/grid_huc_inputs/wbd/WBDHU8.gpkg')
    parser.add_argument('--silver-run',type=Path)
    parser.add_argument('--weekly-run',type=Path)
    parser.add_argument('--gold-run',type=Path)
    args=parser.parse_args(argv)
    cfg=Config(**tomllib.loads(Path(args.config).read_text())).check()
    if (cfg.start,cfg.end)!=('2020-01-01','2022-12-31'): parser.error('This method requires exact 2020–2022 scope')
    root=Path(cfg.output);root.mkdir(parents=True,exist_ok=True)
    with (root/'.pipeline.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.command in ('silver','build-gold'):
            args.silver_run=silver(cfg,Path(args.grid),Path(args.wbd)); print(args.silver_run)
        if args.command in ('weekly','build-gold'):
            if not args.silver_run: parser.error('--silver-run required')
            args.weekly_run=weekly(args.silver_run); print(args.weekly_run)
        if args.command in ('gold','build-gold'):
            if not args.weekly_run: parser.error('--weekly-run required')
            print(gold(args.weekly_run))
        if args.command=='validate-gold':
            if not args.gold_run: parser.error('--gold-run required')
            report=validate_gold(args.gold_run);print(json.dumps(report,indent=2));return 0 if report['passed'] else 2
    return 0
