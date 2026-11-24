import gzip
import json
from pathlib import Path

import httpx
import pytest
import pyarrow.parquet as pq

from usgs_pipeline.core import API, APIError, AuthError, Config, Store, crawl, discover, export, normalize, validate


def feature(oid="1", day="2020-02-29", value="0", unit="ft3/s", series="a"):
    return {"id": oid, "properties": {"monitoring_location_id": "USGS-01646500", "time": day,
        "value": value, "unit_of_measure": unit, "time_series_id": series,
        "parameter_code": "00060", "statistic_id": "00003", "approval_status": "Approved", "qualifier": "e"}}


def job():
    return dict(site="USGS-01646500", year=2020, region="02", start="2020-01-01", end="2020-12-31")


def config(tmp_path, **kw):
    return Config(output=str(tmp_path), requests_per_second=100000, **kw)


def api(cfg, handler):
    return API(cfg, transport=httpx.MockTransport(handler), sleep=lambda _: None)


def catalog(tmp_path):
    (tmp_path / "catalog.json").write_text(json.dumps([{"site_id": "USGS-01646500", "region": "02", "decision": "included"}]))


def test_normalize():
    rows = normalize([feature(), feature(), feature("2", value="12", series="b"),
                      feature("3", day="2020-03-01", value="Ice"),
                      feature("4", day="2020-03-02", unit="m3/s", value="3")], job())
    assert len(rows) == 4
    assert rows[0]["discharge_cfs"] == 0
    assert rows[0]["site_number"] == "01646500"
    assert rows[0]["qualifier"] == "e"
    assert rows[0]["conflict"] and rows[1]["conflict"]
    assert rows[2]["invalid_numeric"] and rows[2]["discharge_cfs"] is None
    assert rows[3]["unexpected_unit"] and rows[3]["discharge_cfs"] is None


@pytest.mark.parametrize("field,value", [("parameter_code", "00065"), ("time", "2019-12-31"), ("monitoring_location_id", "USGS-wrong")])
def test_scope_enforcement(field, value):
    f = feature()
    f["properties"][field] = value
    with pytest.raises((APIError, ValueError)):
        normalize([f], job())


def test_pages_retries_and_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("USGS_API_KEY", "secret")
    calls = []
    def handler(req):
        calls.append(req)
        assert req.headers["X-Api-Key"] == "secret"
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "2"})
        if len(calls) == 2:
            return httpx.Response(503)
        if "offset" not in req.url.params:
            assert req.url.params["parameter_code"] == "00060"
            return httpx.Response(200, json={"features": [feature()], "links": [{"rel": "next", "href":
                "https://api.waterdata.usgs.gov/ogcapi/v1/collections/daily/items?offset=1&api_key=secret"}]})
        return httpx.Response(200, json={"features": [feature("2")], "links": []})
    client = api(config(tmp_path), handler)
    sleeps = []
    client.sleep = sleeps.append
    pages = list(client.pages("daily", {"parameter_code": "00060"}, tmp_path / "raw"))
    assert len(pages) == 2 and len(calls) == 4
    assert any(s >= 2 for s in sleeps)
    for path in tmp_path.rglob("*"):
        if path.is_file():
            content = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
            assert b"secret" not in content
    client.close()


def test_transport_retry_and_auth(tmp_path):
    count = 0
    def handler(req):
        nonlocal count
        count += 1
        if count == 1:
            raise httpx.ConnectError("offline", request=req)
        return httpx.Response(200, json={"features": []})
    client = api(config(tmp_path), handler)
    client.get("https://api.waterdata.usgs.gov/ogcapi/v1/collections/daily/items")
    assert count == 2
    client.close()
    client = api(config(tmp_path), lambda req: httpx.Response(401))
    with pytest.raises(AuthError):
        client.get("https://api.waterdata.usgs.gov/x")
    assert client.stop.is_set()
    client.close()


def test_retry_exhaustion(tmp_path):
    calls = []
    client = api(config(tmp_path, retries=2), lambda r: (calls.append(r) or httpx.Response(500)))
    with pytest.raises(APIError):
        client.get("https://api.waterdata.usgs.gov/x")
    assert len(calls) == 3
    client.close()


def test_pagination_security(tmp_path):
    client = api(config(tmp_path), lambda r: httpx.Response(200, json={"features": [feature()],
        "links": [{"rel": "next", "href": "https://evil.example/steal"}]}))
    with pytest.raises(APIError, match="origin"):
        list(client.pages("daily", {}, tmp_path / "raw"))
    client.close()


def test_resume_leap_and_export(tmp_path):
    cfg = config(tmp_path)
    catalog(tmp_path)
    calls = []
    def handler(req):
        calls.append(req)
        assert req.url.params["statistic_id"] == "00003"
        interval = req.url.params["datetime"]
        return httpx.Response(200, json={"features": [feature()] if interval.startswith("2020") else [], "links": []})
    client = api(cfg, handler)
    crawl(cfg, client)
    assert len(calls) == 3
    crawl(cfg, client)
    assert len(calls) == 3
    report = validate(cfg)
    assert report["all_jobs_successful"]
    assert report["stations"][0]["expected_days"] == 1096
    assert report["stations"][0]["observed_days"] == 1
    assert report["stations"][0]["years"][0]["expected_days"] == 366
    assert report["stations"][0]["years"][0]["missing_ranges"] == [["2020-01-01", "2020-02-28"], ["2020-03-01", "2020-12-31"]]
    assert len(export(cfg)) == 1
    assert "01646500" in (tmp_path / "csv/USGS-01646500.csv").read_text()
    store = Store(cfg)
    store.update("USGS-01646500", 2020, "running")
    store.close()
    crawl(cfg, client)
    assert len(calls) == 4
    path = next((tmp_path / "normalized").rglob("*2020*")) / "USGS-01646500.parquet"
    assert pq.ParquetFile(path).metadata.num_rows == 1
    client.close()


def test_failure_then_resume(tmp_path):
    cfg = config(tmp_path, start="2020-01-01", end="2020-12-31", retries=0)
    catalog(tmp_path)
    count = 0
    def handler(req):
        nonlocal count
        count += 1
        if count == 1:
            return httpx.Response(500)
        return httpx.Response(200, json={"features": [feature()], "links": []})
    client = api(cfg, handler)
    crawl(cfg, client)
    assert validate(cfg)["unresolved_jobs"]
    crawl(cfg, client)
    assert validate(cfg)["all_jobs_successful"]
    client.close()


def test_scope_lock(tmp_path):
    Store(config(tmp_path)).close()
    with pytest.raises(ValueError, match="another"):
        Store(config(tmp_path, start="2021-01-01"))


def test_discovery_historical_missing_huc(tmp_path):
    def handler(req):
        if "time-series-metadata" in req.url.path:
            features = [{"properties": {"id": str(i), "monitoring_location_id": f"USGS-{i:08d}",
                "begin": begin, "end": end, "unit_of_measure": "ft3/s"}} for i, begin, end in
                [(1, "1990-01-01", "2021-01-01"), (2, "1990-01-01", "2022-01-01"),
                 (3, "1990-01-01", "2022-01-01"), (4, "1990-01-01", "2019-01-01")]]
        else:
            features = [{"properties": {"id": f"USGS-{i:08d}", "hydrologic_unit_code": huc}, "geometry": None}
                        for i, huc in [(1, "02070010"), (2, None), (3, "19010000"), (4, "02070010")]]
        return httpx.Response(200, json={"features": features, "links": []})
    cfg = config(tmp_path)
    client = api(cfg, handler)
    rows = discover(cfg, client)
    assert [r["decision"] for r in rows] == ["included", "missing_or_invalid_huc", "outside_regions", "outside_period"]
    client.close()


def test_incomplete_pagination_not_committed(tmp_path):
    cfg = config(tmp_path, start="2020-01-01", end="2020-12-31", retries=0)
    catalog(tmp_path)
    count = 0
    def handler(req):
        nonlocal count
        count += 1
        if count == 1:
            return httpx.Response(200, json={"features": [feature()], "links": [{"rel": "next", "href":
                "https://api.waterdata.usgs.gov/ogcapi/v1/collections/daily/items?offset=1"}]})
        return httpx.Response(500)
    client = api(cfg, handler)
    crawl(cfg, client)
    assert not list(tmp_path.rglob("*.parquet"))
    assert validate(cfg)["status_counts"]["error"] == 1
    client.close()


def test_pilot_not_full_success(tmp_path):
    cfg = config(tmp_path)
    catalog(tmp_path)
    assert validate(cfg)["unplanned_jobs"] == 3
    assert not validate(cfg)["all_jobs_successful"]


def test_auth_stops_new_jobs(tmp_path):
    cfg = config(tmp_path, workers=1)
    catalog(tmp_path)
    calls = []
    client = api(cfg, lambda req: (calls.append(req) or httpx.Response(403)))
    with pytest.raises(AuthError):
        crawl(cfg, client)
    assert len(calls) == 1
    report = validate(cfg)
    assert report["status_counts"]["error"] == 1
    assert report["status_counts"]["pending"] == 2
    client.close()


def test_compare_five_sites(tmp_path):
    from usgs_pipeline.core import SCHEMA, compare, job_path
    import pyarrow as pa
    cfg = config(tmp_path, start="2020-01-01", end="2020-12-31")
    store = Store(cfg)
    refs = ["site_id,date,discharge_cfs"]
    for i in range(5):
        j = {**job(), "site": f"USGS-{i:08d}"}
        f = feature(str(i))
        f["properties"]["monitoring_location_id"] = j["site"]
        rows = normalize([f], j)
        path = job_path(tmp_path, j)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)
        store.db.execute("INSERT INTO jobs(site,year,region,start,end,status,rows) VALUES (?,?,?,?,?,'complete',1)",
                         (j["site"], j["year"], j["region"], j["start"], j["end"]))
        refs.append(f"{j['site']},2020-02-29,0")
    store.db.commit()
    store.close()
    reference = tmp_path / "reference.csv"
    reference.write_text("\n".join(refs))
    assert compare(cfg, reference)["passed"]
    reference.write_text("\n".join(refs).replace(",0", ",1"))
    assert not compare(cfg, reference)["passed"]


def test_unknown_site_rejected(tmp_path):
    cfg = config(tmp_path)
    catalog(tmp_path)
    client = api(cfg, lambda r: httpx.Response(200, json={"features": [], "links": []}))
    with pytest.raises(ValueError, match="absent or ineligible"):
        crawl(cfg, client, site_ids=["USGS-00000000"])
    client.close()


def test_round_robin_pilot_and_history():
    from usgs_pipeline.core import choose_sites
    rows = [{"site_id": f"USGS-{i:08d}", "region": f"{i:02d}", "decision": "included", "end": "2030-01-01"}
            for i in range(1, 19)]
    rows.append({"site_id": "USGS-99999999", "region": "02", "decision": "included", "end": "2021-01-01"})
    chosen = choose_sites(rows, 20)
    assert len(chosen) == 19 and len(set(chosen)) == 19
    assert "USGS-99999999" in chosen


def test_discovery_without_matching_series(tmp_path):
    cfg = config(tmp_path)
    client = api(cfg, lambda req: httpx.Response(200, json={"features": [], "links": []}))
    try:
        assert discover(cfg, client) == []
        assert json.loads((tmp_path / "catalog.json").read_text()) == []
        manifest = json.loads((tmp_path / "discovery_manifest.json").read_text())
        assert manifest["included_sites"] == 0
        assert manifest["series"] == 0
    finally:
        client.close()


def test_rate_limit_cooldown_survives_restart(tmp_path):
    import time
    cfg = config(tmp_path, retries=1)
    calls = []
    def handler(req):
        calls.append(req)
        if len(calls) == 1:
            return httpx.Response(429, headers={'Retry-After': '120', 'X-RateLimit-Limit': '1000', 'X-RateLimit-Remaining': '0'})
        return httpx.Response(200, json={'features': []})
    first = api(cfg, handler)
    first.get('https://api.waterdata.usgs.gov/ogcapi/v1/collections/daily/items')
    first.close()
    saved = json.loads((tmp_path / 'reports/rate_limit.json').read_text())
    assert saved['retry_not_before'] > time.time() + 100
    assert saved['rate_limit'] == '1000'
    waits = []
    second = API(cfg, httpx.MockTransport(lambda req: httpx.Response(200, json={'features': []})), sleep=waits.append)
    second.get('https://api.waterdata.usgs.gov/ogcapi/v1/collections/daily/items')
    second.close()
    assert waits[0] > 100
