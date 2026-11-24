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
from bronze.scripts.core import Config, Store, atomic_json, job_path, now
from silver.scripts.preprocess import sha256
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
    code = [p for folder in (REPO_ROOT / 'usgs_pipeline', REPO_ROOT / 'bronze' / 'scripts', REPO_ROOT / 'silver' / 'scripts', REPO_ROOT / 'gold' / 'scripts', REPO_ROOT / 'grid_huc') for p in folder.glob('*.py')]
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

