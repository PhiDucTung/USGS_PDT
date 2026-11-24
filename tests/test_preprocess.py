import json
import math
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from usgs_pipeline.core import API, Config, Store, crawl, validate
from usgs_pipeline.preprocess import audit_raw, clean_day, haversine, metadata, preprocess
from usgs_pipeline.workflow import run_all


def prepared(root, records, hucs=None, coordinates=None, end='2020-02-29'):
    cfg = Config(output=str(root), start='2020-02-28', end=end, requests_per_second=100000)
    sites = sorted(records)
    catalog = [{'site_id': site, 'region': '02', 'decision': 'included',
                'huc': (hucs or {}).get(site, '020700080001'),
                'coordinates': (coordinates or {}).get(site, [-77 + i * .01, 39])}
               for i, site in enumerate(sites)]
    (root / 'catalog.json').write_text(json.dumps(catalog))
    def handler(req):
        site = req.url.params['monitoring_location_id']
        features = []
        for index, (day, value, unit, series) in enumerate(records[site]):
            features.append({'id': f'{site}-{index}', 'properties': {
                'monitoring_location_id': site, 'time': day, 'value': value,
                'parameter_code': '00060', 'statistic_id': '00003',
                'unit_of_measure': unit, 'time_series_id': series,
                'approval_status': 'Approved', 'qualifier': 'e'}})
        return httpx.Response(200, json={'features': features, 'links': []})
    client = API(cfg, httpx.MockTransport(handler), sleep=lambda _: None)
    try:
        crawl(cfg, client)
    finally:
        client.close()
    return cfg


def read_rows(run, folder):
    return [row for path in sorted((run / folder).rglob('*.parquet')) for row in pq.ParquetFile(path).read().to_pylist()]


def test_gate_rejects_pilot(tmp_path):
    cfg = Config(output=str(tmp_path))
    (tmp_path / 'catalog.json').write_text(json.dumps([{'site_id': 'USGS-00000001', 'region': '02', 'decision': 'included'}]))
    with pytest.raises(ValueError, match='blocked'):
        preprocess(cfg)
    assert not (tmp_path / 'preprocessed').exists()


def test_gate_rejects_corrupt_parquet(tmp_path):
    cfg = prepared(tmp_path, {'USGS-00000001': []})
    next((tmp_path / 'normalized').rglob('*.parquet')).write_bytes(b'broken')
    with pytest.raises(ValueError, match='blocked'):
        preprocess(cfg)


def test_exact_job_scope_not_only_count(tmp_path):
    cfg = prepared(tmp_path, {'USGS-00000001': []})
    store = Store(cfg)
    store.db.execute("UPDATE jobs SET region='03'")
    store.db.commit()
    store.close()
    assert not validate(cfg)['all_jobs_successful']


def test_leap_daily_donors_and_repeatability(tmp_path):
    target, near, far = ['USGS-00000001', 'USGS-00000002', 'USGS-00000003']
    cfg = prepared(tmp_path, {
        target: [],
        near: [('2020-02-28', '0', 'ft^3/s', 'a'), ('2020-02-29', 'Ice', 'ft^3/s', 'a')],
        far: [('2020-02-28', '-2', 'ft^3/s', 'a'), ('2020-02-29', '-3', 'ft^3/s', 'a')],
    })
    hashes = {str(p): p.read_bytes() for p in (tmp_path / 'normalized').rglob('*.parquet')}
    run = preprocess(cfg, 1)
    daily = read_rows(run, 'daily')
    assert len(daily) == 6
    assert len([r for r in daily if r['date'] == '2020-02-29']) == 3
    assert next(r for r in daily if r['site_id'] == near and r['date'] == '2020-02-28')['discharge_cfs'] == 0
    donors = [r for r in read_rows(run, 'candidates') if r['target_site_id'] == target]
    assert len(donors) == 6
    assert {r['donor_site_id'] for r in donors if r['date'] == '2020-02-28'} == {near}
    assert {r['donor_site_id'] for r in donors if r['date'] == '2020-02-29'} == {far}
    assert all(r['target_site_id'] != r['donor_site_id'] for r in donors)
    summaries = [r for r in read_rows(run, 'candidate_summary') if r['target_site_id'] == target and r['date'] == '2020-02-28']
    assert all(r['eligible_count'] == 2 and r['retained_count'] == 1 for r in summaries)
    assert all(Path(p).read_bytes() == content for p, content in hashes.items())
    repeat = preprocess(cfg, 1)
    assert read_rows(repeat, 'daily') == daily
    assert read_rows(repeat, 'candidates') == read_rows(run, 'candidates')
    assert json.loads((run / 'report.json').read_text())['filled_values'] == 0


def test_limit_20_huc_membership_and_ties(tmp_path):
    target = 'USGS-00000000'
    records = {target: []}
    records.update({f'USGS-{i:08d}': [('2020-02-28', str(i), 'cfs', 'a')] for i in range(1, 24)})
    outside = 'USGS-00000023'
    cfg = prepared(tmp_path, records, hucs={outside: '999900000001'},
                   coordinates={s: [-77, 39] for s in records}, end='2020-02-28')
    run = preprocess(cfg)
    donors = read_rows(run, 'candidates')
    for level in (4, 6, 8):
        subset = [r for r in donors if r['huc_level'] == level]
        assert len(subset) == 20
        assert [r['donor_site_id'] for r in subset] == [f'USGS-{i:08d}' for i in range(1, 21)]
        assert all(r['distance_km'] == 0 for r in subset)
    assert all(r['eligible_count'] == 22 for r in read_rows(run, 'candidate_summary'))


def test_conflict_unit_and_metadata_gaps(tmp_path):
    target, donor = 'USGS-00000001', 'USGS-00000002'
    cfg = prepared(tmp_path, {target: [('2020-02-28', '1', 'cfs', 'a'), ('2020-02-28', '2', 'cfs', 'b')],
        donor: [('2020-02-28', '3', 'm3/s', 'a')]}, hucs={target: None},
        coordinates={donor: None}, end='2020-02-28')
    run = preprocess(cfg)
    daily = read_rows(run, 'daily')
    assert all(r['discharge_cfs'] is None for r in daily)
    assert 'conflict' in next(r for r in daily if r['site_id'] == target)['qc_flags']
    assert next(r for r in daily if r['site_id'] == donor)['qc_flags'] == ['unexpected_unit']
    assert read_rows(run, 'candidates') == []
    assert {r['reason'] for r in read_rows(run, 'candidate_summary')} == {'missing_huc', 'missing_coordinates'}


def test_haversine_and_inconsistent_metadata():
    assert haversine([0, 0], [0, 0]) == 0
    assert math.isclose(haversine([0, 0], [1, 0]), 111.19508, rel_tol=1e-6)
    rows = [{'site_id': 's', 'decision': 'included', 'coordinates': [0, 0], 'huc': h} for h in ['02070001', '02080002']]
    meta, issues = metadata(rows, {'s'})
    assert meta['s']['hucs'] == {4: None, 6: None, 8: None}
    assert len(issues) == 1


def test_raw_corruption_blocks_preprocessing(tmp_path):
    cfg = prepared(tmp_path, {'USGS-00000001': []})
    assert audit_raw(cfg)['passed']
    next((tmp_path / 'raw').rglob('*.json.gz')).write_bytes(b'corrupt')
    assert not audit_raw(cfg)['passed']
    with pytest.raises(ValueError, match='Raw archive'):
        preprocess(cfg)


def test_full_workflow_missing_key_blocks_network(tmp_path, monkeypatch):
    monkeypatch.delenv('USGS_API_KEY', raising=False)
    cfg = Config(output=str(tmp_path))
    (tmp_path / 'catalog.json').write_text(json.dumps([{'site_id': 'USGS-00000001', 'region': '02', 'decision': 'included'}]))
    with pytest.raises(ValueError, match='USGS_API_KEY'):
        run_all(cfg)
    assert json.loads((tmp_path / 'reports/workflow_status.json').read_text())['status'] == 'blocked'
    assert not (tmp_path / 'preprocessed').exists()


def test_full_workflow_already_downloaded_no_key_needed(tmp_path, monkeypatch):
    monkeypatch.delenv('USGS_API_KEY', raising=False)
    cfg = prepared(tmp_path, {'USGS-00000001': []})
    run = run_all(cfg)
    assert (tmp_path / 'csv/USGS-00000001.csv').exists()
    state = json.loads((tmp_path / 'reports/workflow_status.json').read_text())
    assert state['status'] == 'complete'
    assert state['manual_comparison_completed'] is False
    assert Path(state['preprocessing_output']) == run


def test_full_workflow_crawls_only_missing_job(tmp_path, monkeypatch):
    import usgs_pipeline.workflow as workflow
    first, missing = 'USGS-00000001', 'USGS-00000002'
    cfg = prepared(tmp_path, {first: [], missing: []}, end='2020-02-28')
    store = Store(cfg)
    store.db.execute('DELETE FROM jobs WHERE site=?', (missing,))
    store.db.commit()
    store.close()
    calls = []
    def handler(req):
        calls.append(req.url.params['monitoring_location_id'])
        return httpx.Response(200, json={'features': [], 'links': []})
    monkeypatch.setenv('USGS_API_KEY', 'fixture-only-not-a-real-key')
    monkeypatch.setattr(workflow, 'API', lambda c: API(c, httpx.MockTransport(handler), sleep=lambda _: None))
    result = run_all(cfg)
    assert calls == [missing]
    assert validate(cfg)['all_jobs_successful']
    assert len(read_rows(result, 'daily')) == 2
    assert json.loads((tmp_path / 'reports/workflow_status.json').read_text())['status'] == 'complete'
