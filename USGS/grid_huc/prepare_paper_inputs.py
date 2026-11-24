#!/usr/bin/env python3
"""Prepare original paper grid CSV or a cached USGS HUC8 service snapshot."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import geopandas as gpd
import httpx
import pandas as pd
from shapely import make_valid
from shapely.geometry import MultiPolygon
from shapely.validation import explain_validity

from build_grid_huc import read_grid

SERVICE = 'https://hydrowfs.nationalmap.gov/arcgis/rest/services/wbd/MapServer/4'
WHERE = "huc8 >= '01000000' AND huc8 < '19000000'"
PAPER_SHA256 = 'ae7da84c03dbd0573b32b80f823503bd4fad7ce294e5f5b496f4ddace155910f'


def get_json(client, url, params):
    for attempt in range(3):
        try:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if 'error' in data:
                raise ValueError(data['error'])
            return data
        except (httpx.HTTPError, ValueError):
            if attempt == 2:
                raise
            time.sleep(2)


def fetch_wbd(folder, repair_invalid=False):
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / 'WBDHU8.gpkg'
    if target.exists():
        raise ValueError(f'Already exists: {target}')
    with httpx.Client(timeout=45, follow_redirects=True, headers={'Connection': 'close'}) as client:
        metadata = get_json(client, SERVICE, {'f': 'json'})
        ids = sorted(get_json(client, SERVICE + '/query', {
            'where': WHERE, 'returnIdsOnly': 'true', 'f': 'json'})['objectIds'])
        if not ids:
            raise ValueError('Empty HUC8 selection')
        def fetch_batch(offset):
            batch = ids[offset:offset + 10]
            cache = folder / f'wbd10_{offset:05d}.geojson'
            if cache.exists():
                data = json.loads(cache.read_text())
            else:
                data = get_json(client, SERVICE + '/query', {
                    'objectIds': ','.join(map(str, batch)),
                    'outFields': 'objectid,huc8,name',
                    'returnGeometry': 'true', 'outSR': 4326, 'f': 'geojson'})
            if data.get('exceededTransferLimit'):
                raise ValueError('Service truncated geometry batch')
            actual = sorted(f['properties']['objectid'] for f in data['features'])
            if actual != batch:
                raise ValueError('Service/cache returned different object IDs')
            cache.write_text(json.dumps(data))
            return data['features']
        # Write each batch immediately: full-resolution boundaries are large.
        partial = folder / 'WBDHU8.partial.gpkg'
        count = 0
        repairs = []
        offsets = list(range(0, len(ids), 10))
        with ThreadPoolExecutor(max_workers=4) as executor:
            for start in range(0, len(offsets), 4):
                for batch_features in executor.map(fetch_batch, offsets[start:start + 4]):
                    wbd = gpd.GeoDataFrame.from_features(batch_features, crs=4326)
                    for index in wbd.index[~wbd.is_valid]:
                        old = wbd.geometry[index]
                        if not repair_invalid:
                            raise ValueError(f'Invalid HUC {wbd.huc8[index]}: {explain_validity(old)}. Use --repair-invalid to explicitly record make_valid repairs.')
                        fixed = make_valid(old)
                        if fixed.geom_type not in {'Polygon', 'MultiPolygon'}:
                            raise ValueError(f'Repair of {wbd.huc8[index]} produced {fixed.geom_type}; manual review needed')
                        before, after = gpd.GeoSeries([old, fixed], crs=4326).to_crs(5070).area
                        delta = abs(after - before) / max(abs(before), 1)
                        if delta > 0.000001:
                            raise ValueError(f'Repair area change too large for {wbd.huc8[index]}: {delta}')
                        repairs.append({'huc8': wbd.huc8[index], 'reason': explain_validity(old), 'method': 'shapely.make_valid', 'relative_area_change_epsg5070': delta})
                        wbd.loc[index, 'geometry'] = fixed
                    if wbd.geometry.is_empty.any() or wbd.geometry.isna().any():
                        raise ValueError('Source has empty geometry')
                    wbd.geometry = wbd.geometry.map(lambda geom: MultiPolygon([geom]) if geom.geom_type == 'Polygon' else geom)
                    wbd.to_file(partial, layer='WBDHU8', driver='GPKG', mode='w' if count == 0 else 'a')
                    count += len(wbd)
                    print(f'WBD: {count}/{len(ids)} polygons', flush=True)
        partial.replace(target)
        report = {'downloaded_at': datetime.now(timezone.utc).isoformat(),
                  'source': SERVICE, 'where': WHERE, 'polygon_count': count,
                  'source_metadata': metadata,
                  'repairs': repairs,
                  'note': 'USGS web service snapshot, regions 01–18, full returned geometry; no requested simplification. Not the historical WBD vintage used by the paper.'}
        with target.open('rb') as handle:
            report['sha256'] = hashlib.file_digest(handle, 'sha256').hexdigest()
        (folder / 'wbd_source.json').write_text(json.dumps(report, indent=2))


def extract_grid(source, target, expected_hash=None):
    if target.exists():
        raise ValueError(f'Already exists: {target}')
    # Read every row: one week alone need not contain the complete spatial domain.
    unique = pd.DataFrame(columns=['grid', 'lon', 'lat'])
    rows = 0
    for chunk in pd.read_csv(source, usecols=['grid', 'lon', 'lat'],
                             dtype={'grid': 'string'}, chunksize=100000):
        if chunk.isna().any().any():
            raise ValueError('Missing grid ID or coordinates in original CSV')
        unique = pd.concat([unique, chunk]).drop_duplicates()
        rows += len(chunk)
    if not rows:
        raise ValueError('Empty paper CSV')
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix('.partial.csv')
    unique.rename(columns={'grid': 'grid_id'}).to_csv(temp, index=False)
    checked = read_grid(temp, 'grid_id', 'lon', 'lat')
    with source.open('rb') as handle:
        source_hash = hashlib.file_digest(handle, 'sha256').hexdigest()
    if expected_hash and source_hash != expected_hash:
        raise ValueError('CSV checksum does not match the selected Dryad version')
    temp.replace(target)
    report = {'source': str(source.resolve()), 'source_rows': rows,
              'unique_grid_cells': len(checked),
              'source_sha256': source_hash,
              'grid_sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
              'method': 'Unique original grid/lon/lat triples across all rows; no generated IDs or coordinates'}
    target.with_suffix('.source.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    w = commands.add_parser('wbd')
    w.add_argument('--out', type=Path, default=Path('bronze/data/grid_huc_inputs/wbd'))
    w.add_argument('--repair-invalid', action='store_true')
    g = commands.add_parser('grid')
    g.add_argument('--csv', type=Path, required=True)
    g.add_argument('--out', type=Path, default=Path('bronze/data/grid_huc_inputs/grid.csv'))
    g.add_argument('--expected-sha256', default=PAPER_SHA256)
    args = parser.parse_args()
    if args.command == 'wbd':
        fetch_wbd(args.out, args.repair_invalid)
    else:
        extract_grid(args.csv, args.out, args.expected_sha256)
