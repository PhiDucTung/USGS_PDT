from __future__ import annotations
import csv
import gzip
import hashlib
import json
import math
import os
import random
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, urlencode

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

BASE = "https://api.waterdata.usgs.gov/ogcapi/v1/collections/"

def now():
    return datetime.now(timezone.utc).isoformat()

def atomic_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def safe_url(url):
    p = urlsplit(str(url))
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode([
        (k, v) for k, v in parse_qsl(p.query) if k.lower() not in {"api_key", "apikey", "token"}
    ]), ""))

@dataclass
class Config:
    start: str = "2020-01-01"
    end: str = "2022-12-31"
    regions: tuple = tuple(f"{i:02d}" for i in range(1, 19))
    output: str = "bronze/data"
    workers: int = 2
    requests_per_second: float = 0.25
    page_size: int = 1000
    retries: int = 6

    def check(self):
        if date.fromisoformat(self.start) > date.fromisoformat(self.end):
            raise ValueError("start must not exceed end")
        if self.workers < 1 or self.requests_per_second <= 0 or self.page_size < 1 or self.retries < 0:
            raise ValueError("Invalid concurrency, rate, page size or retries")
        if not self.regions or not set(self.regions) <= {f"{i:02d}" for i in range(1, 19)}:
            raise ValueError("regions must be strings 01 through 18")
        return self

class AuthError(RuntimeError):
    pass
class APIError(RuntimeError):
    pass

class API:
    def __init__(self, config, transport=None, sleep=time.sleep):
        self.config = config
        key = os.environ.get("USGS_API_KEY")
        self.client = httpx.Client(timeout=90, transport=transport,
                                   headers={"X-Api-Key": key} if key else {})
        self.sleep = sleep
        self.lock = threading.Lock()
        self.cooldown_path = Path(config.output) / 'reports' / 'rate_limit.json'
        self.retry_not_before = 0.0
        if self.cooldown_path.exists():
            saved = json.loads(self.cooldown_path.read_text())
            self.retry_not_before = float(saved.get('retry_not_before', 0))
            if not math.isfinite(self.retry_not_before):
                raise ValueError('Invalid stored rate-limit deadline')
        remaining = max(0, self.retry_not_before - time.time())
        self.next_request = time.monotonic() + remaining
        if remaining:
            print(f'USGS cooldown restored: waiting {math.ceil(remaining)}s before requests', file=sys.stderr, flush=True)
        self.stop = threading.Event()

    def close(self):
        self.client.close()

    def throttle(self):
        with self.lock:
            delay = max(0, self.next_request - time.monotonic())
            self.sleep(delay)
            self.next_request = time.monotonic() + 1 / self.config.requests_per_second

    def get(self, url, params=None):
        url = safe_url(url)
        if urlsplit(url).scheme != "https" or urlsplit(url).netloc != "api.waterdata.usgs.gov":
            raise APIError("Refusing pagination outside USGS API origin")
        for attempt in range(self.config.retries + 1):
            if self.stop.is_set():
                raise AuthError("Run stopped after authentication failure")
            self.throttle()
            try:
                r = self.client.get(url, params=params)
            except httpx.TransportError:
                if attempt == self.config.retries:
                    raise APIError("Transport failure after retries") from None
                self.sleep(min(60, 2 ** attempt) + random.random())
                continue
            if r.status_code in (401, 403):
                self.stop.set()
                raise AuthError(f"Authentication failure: HTTP {r.status_code}")
            if r.status_code == 429 or r.status_code >= 500:
                if attempt == self.config.retries:
                    raise APIError(f"HTTP {r.status_code} after retries")
                delay = min(60, 2 ** attempt) + random.random()
                header = r.headers.get("Retry-After")
                if header:
                    try:
                        delay = max(delay, float(header))
                    except ValueError:
                        try:
                            delay = max(delay, (parsedate_to_datetime(header) - datetime.now(timezone.utc)).total_seconds())
                        except (ValueError, TypeError):
                            pass
                print(f"USGS HTTP {r.status_code}: retry {attempt + 1}/{self.config.retries} in {math.ceil(delay)}s", file=sys.stderr, flush=True)
                with self.lock:
                    self.next_request = max(self.next_request, time.monotonic() + delay)
                    if r.status_code == 429:
                        self.retry_not_before = max(self.retry_not_before, time.time() + delay)
                        atomic_json(self.cooldown_path, {
                            'updated_at': now(), 'last_http_status': 429,
                            'retry_not_before': self.retry_not_before,
                            'resume_at_utc': datetime.fromtimestamp(self.retry_not_before, timezone.utc).isoformat(),
                            'rate_limit': r.headers.get('X-RateLimit-Limit'),
                            'rate_remaining': r.headers.get('X-RateLimit-Remaining'),
                        })
                self.sleep(delay)
                continue
            if r.status_code >= 400:
                raise APIError(f"Query failure: HTTP {r.status_code} at {safe_url(r.url)}")
            try:
                return r.json(), safe_url(r.url)
            except ValueError:
                raise APIError("USGS returned invalid JSON") from None
        raise APIError("Retry exhaustion")

    def pages(self, collection, params, archive):
        url = BASE + collection + "/items"
        query = {"f": "json", "limit": self.config.page_size, **params}
        seen = set()
        index = 0
        while url:
            body, request_url = self.get(url, query)
            if request_url in seen:
                raise APIError("Repeated pagination URL")
            seen.add(request_url)
            if not isinstance(body.get("features"), list):
                raise APIError("Missing features array")
            archive.mkdir(parents=True, exist_ok=True)
            body = json.loads(json.dumps(body))
            for link in body.get("links", []):
                if "href" in link:
                    link["href"] = safe_url(link["href"])
            payload = json.dumps(body, ensure_ascii=False).encode()
            path = archive / f"page-{index:06d}.json.gz"
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(gzip.compress(payload, mtime=0))
            tmp.replace(path)
            atomic_json(path.with_suffix(".meta.json"), {
                "url": request_url, "retrieved_at": now(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "features": len(body["features"]),
            })
            yield body["features"]
            next_links = [x["href"] for x in body.get("links", []) if x.get("rel") == "next"]
            if not body["features"] and next_links:
                raise APIError("Empty page with next link; completeness is ambiguous")
            url = urljoin(request_url, next_links[0]) if next_links else None
            query = None
            index += 1

class Store:
    def __init__(self, config):
        self.config = config.check()
        self.root = Path(config.output)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / "checkpoint.sqlite")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS jobs (
            site TEXT, year INTEGER, region TEXT, start TEXT, end TEXT,
            status TEXT DEFAULT 'pending', error TEXT, rows INTEGER DEFAULT 0,
            elapsed REAL, pages INTEGER DEFAULT 0, bytes INTEGER DEFAULT 0,
            updated TEXT, PRIMARY KEY(site,year));
        """)
        scope = json.dumps([config.start, config.end, sorted(config.regions)])
        old = self.db.execute("SELECT value FROM settings WHERE key='scope'").fetchone()
        if old and old[0] != scope:
            raise ValueError("Output directory belongs to another date/region scope; choose a new output")
        self.db.execute("INSERT OR IGNORE INTO settings VALUES ('scope',?)", (scope,))
        self.db.commit()

    def update(self, site, year, status, **values):
        values.update(status=status, updated=now())
        self.db.execute("UPDATE jobs SET " + ",".join(f"{k}=?" for k in values) +
                        " WHERE site=? AND year=?", [*values.values(), site, year])
        self.db.commit()

    def close(self):
        self.db.close()

def discover(config, api):
    store = Store(config)
    root = store.root
    run = root / "raw" / "discovery" / str(time.time_ns())
    candidates = []
    for features in api.pages("time-series-metadata", {
        "parameter_code": "00060", "statistic_id": "00003",
        "computation_period_identifier": "Daily",
    }, run / "series"):
        for f in features:
            p = f["properties"]
            if not str(p.get("monitoring_location_id", "")).startswith("USGS-"):
                continue
            candidates.append(p)
        print(f"Discovery: {len(candidates)} USGS series", flush=True)
    sites = {}
    wanted = sorted({p["monitoring_location_id"] for p in candidates
        if (not p.get("begin") or p["begin"][:10] <= config.end)
        and (not p.get("end") or p["end"][:10] >= config.start)})
    for offset in range(0, len(wanted), 100):
        batch = wanted[offset:offset + 100]
        if any(not s.removeprefix("USGS-").isdigit() for s in batch):
            raise APIError("Unexpected station identifier in metadata")
        expression = "id IN (" + ",".join("'" + s + "'" for s in batch) + ")"
        for features in api.pages("monitoring-locations", {
            "filter": expression, "filter-lang": "cql2-text"
        }, run / "locations" / str(offset)):
            for f in features:
                p = f["properties"]
                sites[p["id"]] = (p, f.get("geometry"))
        if offset % 1000 == 0:
            print(f"Locations: {min(offset + 100, len(wanted))}/{len(wanted)}", flush=True)
    catalog = []
    for p in candidates:
        site = p["monitoring_location_id"]
        loc, geometry = sites.get(site, ({}, None))
        huc = str(loc.get("hydrologic_unit_code") or "")
        region = huc[:2]
        begin, end = (p.get("begin") or "")[:10], (p.get("end") or "")[:10]
        reason = "included"
        if begin and begin > config.end or end and end < config.start:
            reason = "outside_period"
        elif not huc or len(huc) < 2 or not huc.isdigit():
            reason = "missing_or_invalid_huc"
        elif region not in config.regions:
            reason = "outside_regions"
        row = {"site_id": site, "site_number": site.removeprefix("USGS-"),
            "name": loc.get("monitoring_location_name"), "huc": huc, "region": region,
            "coordinates": geometry.get("coordinates") if geometry else None,
            "time_series_id": p.get("id"), "unit": p.get("unit_of_measure"),
            "begin": begin or None, "end": end or None, "decision": reason}
        catalog.append(row)
    atomic_json(root / "catalog.json", catalog)
    atomic_json(root / "discovery_manifest.json", {"created_at": now(), "config": asdict(config),
                "archive": str(run), "series": len(catalog),
                "included_sites": len({r["site_id"] for r in catalog if r["decision"] == "included"}),
                "decisions": {d: sum(r["decision"] == d for r in catalog) for d in sorted({r["decision"] for r in catalog})},
                "region_sites": {region: len({r["site_id"] for r in catalog
                    if r["region"] == region and r["decision"] == "included"}) for region in config.regions}})
    store.close()
    return catalog

SCHEMA = pa.schema([
    ("observation_id", pa.string()), ("site_id", pa.string()), ("site_number", pa.string()),
    ("date", pa.string()), ("discharge_cfs", pa.float64()), ("raw_value", pa.string()),
    ("unit", pa.string()), ("parameter_code", pa.string()), ("statistic_id", pa.string()),
    ("time_series_id", pa.string()), ("approval_status", pa.string()), ("qualifier", pa.string()),
    ("last_modified", pa.string()), ("region", pa.string()), ("year", pa.int32()),
    ("invalid_numeric", pa.bool_()), ("unexpected_unit", pa.bool_()), ("conflict", pa.bool_()),
])

def text_value(x):
    return x if isinstance(x, str) or x is None else json.dumps(x, ensure_ascii=False)

def normalize(features, job):
    rows = {}
    for f in features:
        p = f["properties"]
        day = str(p.get("time", ""))[:10]
        date.fromisoformat(day)
        if p.get("monitoring_location_id") != job["site"] or p.get("parameter_code") != "00060" or p.get("statistic_id") != "00003":
            raise APIError("Response violates station/parameter/statistic filter")
        if not job["start"] <= day <= job["end"]:
            raise APIError("Response contains an out-of-range date")
        raw = p.get("value")
        try:
            value = float(raw)
            if not math.isfinite(value):
                value = None
        except (TypeError, ValueError):
            value = None
        unit = p.get("unit_of_measure")
        unexpected = unit not in {"ft3/s", "ft^3/s", "ft³/s", "cfs"}
        oid = str(f.get("id") or "")
        if not oid:
            raise APIError("Observation has no ID")
        row = dict(observation_id=oid, site_id=job["site"], site_number=job["site"].removeprefix("USGS-"),
                   date=day, discharge_cfs=None if unexpected else value, raw_value=text_value(raw),
                   unit=unit, parameter_code="00060", statistic_id="00003",
                   time_series_id=p.get("time_series_id") or p.get("timeseries_id"),
                   approval_status=text_value(p.get("approval_status", p.get("approvals_status"))),
                   qualifier=text_value(p.get("qualifier")), last_modified=p.get("last_modified"),
                   region=job["region"], year=job["year"], invalid_numeric=value is None,
                   unexpected_unit=unexpected, conflict=False)
        if oid in rows and rows[oid] != row:
            raise APIError("Observation ID has inconsistent values across pages")
        rows[oid] = row
    groups = {}
    for row in rows.values():
        groups.setdefault(row["date"], []).append(row)
    for group in groups.values():
        values = {(x["raw_value"], x["unit"]) for x in group}
        if len(values) > 1:
            for row in group:
                row["conflict"] = True
    return sorted(rows.values(), key=lambda x: (x["date"], x["observation_id"]))

def job_path(root, job):
    return root / "normalized" / f"region={job['region']}" / f"year={job['year']}" / f"{job['site']}.parquet"

def download_job(config, api, job):
    started = time.monotonic()
    archive = Path(config.output) / "raw" / "daily" / job["site"] / str(job["year"]) / str(time.time_ns())
    features, pages = [], 0
    for batch in api.pages("daily", {"monitoring_location_id": job["site"],
        "parameter_code": "00060", "statistic_id": "00003",
        "datetime": f"{job['start']}/{job['end']}"}, archive):
        pages += 1
        features.extend(batch)
    rows = normalize(features, job)
    path = job_path(Path(config.output), job)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp, compression="zstd")
    tmp.replace(path)
    return {"rows": len(rows), "pages": pages, "bytes": sum(p.stat().st_size for p in archive.glob("*.gz")),
            "elapsed": time.monotonic() - started}

def choose_sites(catalog, limit=None):
    included = {r["site_id"]: r for r in catalog if r["decision"] == "included"}
    groups = {}
    for site, row in sorted(included.items()):
        groups.setdefault(row["region"], []).append(site)
    result = []
    for region in sorted(groups):
        result.append(groups[region].pop(0))
    if limit is not None:
        historical = sorted({r["site_id"] for r in catalog if r["decision"] == "included"
            and r.get("end") and r["end"] < str(date.today().year) + "-01-01"})
        for site in historical:
            if site not in result:
                result.append(site)
                break
    while any(groups.values()):
        for region in sorted(groups):
            if groups[region]:
                site = groups[region].pop(0)
                if site not in result:
                    result.append(site)
    return result if limit is None else result[:limit]

def crawl(config, api, limit=None, site_ids=None):
    store = Store(config)
    try:
        catalog = json.loads((store.root / "catalog.json").read_text())
        selected = choose_sites(catalog, limit)
        if site_ids:
            unknown = set(site_ids) - set(selected)
            if unknown:
                raise ValueError("Requested sites are absent or ineligible: " + ", ".join(sorted(unknown)))
            selected = [s for s in selected if s in site_ids]
        mapping = {r["site_id"]: r["region"] for r in catalog if r["decision"] == "included"}
        for site in selected:
            for year in range(int(config.start[:4]), int(config.end[:4]) + 1):
                start, end = max(config.start, f"{year}-01-01"), min(config.end, f"{year}-12-31")
                store.db.execute("INSERT OR IGNORE INTO jobs(site,year,region,start,end) VALUES (?,?,?,?,?)",
                                 (site, year, mapping[site], start, end))
        store.db.commit()
        jobs = [dict(r) for r in store.db.execute("SELECT * FROM jobs ORDER BY region,site,year")
                if r["site"] in selected and r["status"] not in {"complete", "empty"}]
        auth_failure = None
        with ThreadPoolExecutor(max_workers=config.workers) as pool:
            pending = {}
            iterator = iter(jobs)
            def submit():
                job = next(iterator, None)
                if job is None or api.stop.is_set():
                    return False
                store.update(job["site"], job["year"], "running", error=None)
                pending[pool.submit(download_job, config, api, job)] = job
                return True
            for _ in range(config.workers):
                submit()
            while pending:
                future = next(as_completed(pending))
                job = pending.pop(future)
                try:
                    metrics = future.result()
                    status = "complete" if metrics["rows"] else "empty"
                    store.update(job["site"], job["year"], status, error=None, **metrics)
                    print(f"{job['site']} {job['year']}: {status} ({metrics['rows']} rows)", flush=True)
                except AuthError as exc:
                    auth_failure = exc
                    store.update(job["site"], job["year"], "error", error=str(exc))
                except Exception as exc:
                    message = str(exc) if isinstance(exc, APIError) else type(exc).__name__
                    store.update(job["site"], job["year"], "error", error=message)
                if not api.stop.is_set():
                    submit()
        atomic_json(store.root / "crawl_manifest.json", {"updated_at": now(), "config": asdict(config),
            "selected_sites": selected, "snapshot_note": "Historical values as supplied by USGS at retrieval time"})
        if auth_failure:
            raise auth_failure
    finally:
        store.close()

def validate(config):
    store = Store(config)
    station_stats, errors, region_stats = {}, [], {}
    try:
        jobs = [dict(r) for r in store.db.execute("SELECT * FROM jobs ORDER BY site,year")]
        for job in jobs:
            site = job["site"]
            stat = station_stats.setdefault(site, {"site_id": site, "region": job["region"],
                "observed_days": 0, "numeric_days": 0, "expected_days": 0, "conflict_rows": 0,
                "invalid_numeric_rows": 0, "unexpected_unit_rows": 0, "years": []})
            expected = (date.fromisoformat(job["end"]) - date.fromisoformat(job["start"])).days + 1
            stat["expected_days"] += expected
            ys = {"year": job["year"], "status": job["status"], "expected_days": expected,
                  "observed_days": 0, "missing_ranges": [], "rows": 0}
            if job["status"] not in {"complete", "empty"}:
                errors.append({"site_id": site, "year": job["year"], "status": job["status"], "error": job["error"]})
            else:
                try:
                    rows = pq.ParquetFile(job_path(store.root, job)).read().to_pylist()
                    ids = [r["observation_id"] for r in rows]
                    if len(set(ids)) != len(ids) or len(rows) != job["rows"]:
                        raise ValueError("Duplicate IDs or checkpoint row count mismatch")
                    if (job["status"] == "empty") != (len(rows) == 0):
                        raise ValueError("Job status inconsistent with row count")
                    for row in rows:
                        date.fromisoformat(row["date"])
                        if row["site_id"] != site or not job["start"] <= row["date"] <= job["end"] or row["parameter_code"] != "00060" or row["statistic_id"] != "00003":
                            raise ValueError("Row violates scope")
                    days = {r["date"] for r in rows}
                    numeric = {r["date"] for r in rows if r["discharge_cfs"] is not None}
                    ys.update(rows=len(rows), observed_days=len(days), numeric_days=len(numeric))
                    stat["observed_days"] += len(days)
                    stat["numeric_days"] += len(numeric)
                    for flag in ("conflict", "invalid_numeric", "unexpected_unit"):
                        stat[flag + "_rows"] += sum(r[flag] for r in rows)
                    start_ord, end_ord = date.fromisoformat(job["start"]).toordinal(), date.fromisoformat(job["end"]).toordinal()
                    gap = None
                    for ordinal in range(start_ord, end_ord + 2):
                        missing = ordinal <= end_ord and date.fromordinal(ordinal).isoformat() not in days
                        if missing and gap is None:
                            gap = ordinal
                        elif not missing and gap is not None:
                            ys["missing_ranges"].append([date.fromordinal(gap).isoformat(), date.fromordinal(ordinal - 1).isoformat()])
                            gap = None
                except (OSError, ValueError, pa.ArrowException) as exc:
                    errors.append({"site_id": site, "year": job["year"], "error": str(exc)})
            stat["years"].append(ys)
        year_stats = {}
        for stat in station_stats.values():
            for ys in stat["years"]:
                group = year_stats.setdefault(str(ys["year"]), {"sites": 0, "rows": 0, "observed_days": 0, "expected_days": 0})
                group["sites"] += 1
                for field in ("rows", "observed_days", "expected_days"):
                    group[field] += ys[field]
            stat["missing_days"] = stat["expected_days"] - stat["observed_days"]
            stat["coverage"] = stat["observed_days"] / stat["expected_days"]
            region = region_stats.setdefault(stat["region"], {"sites": 0, "observed_days": 0, "expected_days": 0})
            region["sites"] += 1
            region["observed_days"] += stat["observed_days"]
            region["expected_days"] += stat["expected_days"]
        catalog = json.loads((store.root / "catalog.json").read_text()) if (store.root / "catalog.json").exists() else []
        eligible = {r["site_id"] for r in catalog if r["decision"] == "included"}
        expected = {(site, year) for site in eligible
                    for year in range(int(config.start[:4]), int(config.end[:4]) + 1)}
        actual = {(j["site"], j["year"]) for j in jobs}
        planned = len(expected)
        regions = {r["site_id"]: r["region"] for r in catalog if r["decision"] == "included"}
        for job in jobs:
            key = (job["site"], job["year"])
            if (key not in expected or job["region"] != regions.get(job["site"])
                    or job["start"] != max(config.start, f"{job['year']}-01-01")
                    or job["end"] != min(config.end, f"{job['year']}-12-31")):
                errors.append({"site_id": job["site"], "year": job["year"], "error": "Job differs from catalog/date/region scope"})
        report = {"created_at": now(), "scope": asdict(config), "stations": list(station_stats.values()),
            "regions": region_stats, "years": year_stats, "unresolved_jobs": errors, "eligible_sites": len(eligible),
            "pilot_characteristics": {
                "regions": sorted(region_stats),
                "full_coverage_sites": [s["site_id"] for s in station_stats.values() if s["coverage"] == 1],
                "gapped_sites": [s["site_id"] for s in station_stats.values() if 0 < s["coverage"] < 1],
                "historically_ended_series_sites": sorted({r["site_id"] for r in catalog
                    if r["site_id"] in station_stats and r.get("end") and r["end"] < str(date.today().year) + "-01-01"}),
            },
            "unplanned_jobs": len(expected - actual), "unexpected_jobs": len(actual - expected), "total_jobs": len(jobs),
            "all_jobs_successful": bool(expected) and not errors and expected == actual,
            "status_counts": {s: sum(j["status"] == s for j in jobs) for s in ("pending", "running", "complete", "empty", "error")},
            "metrics": {"elapsed_worker_seconds": sum(j["elapsed"] or 0 for j in jobs),
                        "pages": sum(j["pages"] for j in jobs), "raw_bytes": sum(j["bytes"] for j in jobs)},
            "note": "Missing days are not filled. Expected days cover requested calendar scope, not station operating lifetime."}
        successful = [j for j in jobs if j["status"] in {"complete", "empty"}]
        remaining = max(0, planned - len(successful))
        if successful:
            average_elapsed = sum(j["elapsed"] or 0 for j in successful) / len(successful)
            average_pages = sum(j["pages"] for j in successful) / len(successful)
            report["estimate"] = {
                "remaining_jobs": remaining,
                "remaining_raw_bytes": round(remaining * sum(j["bytes"] for j in successful) / len(successful)),
                "ideal_remaining_wall_seconds": round(max(remaining * average_elapsed / config.workers,
                    remaining * average_pages / config.requests_per_second)),
                "note": "Pilot extrapolation only; excludes future rate-limit waits, retries and changes in station density.",
            }
        atomic_json(store.root / "reports" / "quality.json", report)
        atomic_json(store.root / "reports" / "unresolved_jobs.json", errors)
        return report
    finally:
        store.close()

def export(config):
    store = Store(config)
    try:
        folder = store.root / "csv"
        folder.mkdir(exist_ok=True)
        sites = [r[0] for r in store.db.execute("SELECT DISTINCT site FROM jobs ORDER BY site")]
        files = []
        for site in sites:
            jobs = [dict(r) for r in store.db.execute("SELECT * FROM jobs WHERE site=? AND status IN ('complete','empty') ORDER BY year", (site,))]
            path = folder / f"{site}.csv"
            with path.with_suffix(".tmp").open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=SCHEMA.names)
                writer.writeheader()
                for job in jobs:
                    writer.writerows(pq.ParquetFile(job_path(store.root, job)).read().to_pylist())
            path.with_suffix(".tmp").replace(path)
            files.append(str(path))
        atomic_json(store.root / "export_manifest.json", {"created_at": now(), "csv_files": files,
            "warning": "Read quality.json for incomplete jobs; site IDs must be imported as text."})
        return files
    finally:
        store.close()

def compare(config, reference):
    """Compare user-downloaded CSV: site_id,date,discharge_cfs (minimum 5 sites)."""
    with open(reference, newline="", encoding="utf-8-sig") as f:
        refs = list(csv.DictReader(f))
    if not refs or not {"site_id", "date", "discharge_cfs"} <= set(refs[0]):
        raise ValueError("Reference needs site_id,date,discharge_cfs columns")
    sites = {r["site_id"] for r in refs}
    if len(sites) < 5:
        raise ValueError("Manual cross-check requires at least five stations")
    store = Store(config)
    mismatches = []
    try:
        for site in sites:
            observations = {}
            for job in store.db.execute("SELECT * FROM jobs WHERE site=? AND status IN ('complete','empty')", (site,)):
                for r in pq.ParquetFile(job_path(store.root, job)).read().to_pylist():
                    observations.setdefault(r["date"], set()).add(r["discharge_cfs"])
            for ref in (r for r in refs if r["site_id"] == site):
                expected = float(ref["discharge_cfs"])
                actual = observations.get(ref["date"], set())
                if not any(v is not None and math.isclose(v, expected, rel_tol=1e-9, abs_tol=1e-9) for v in actual):
                    mismatches.append({**ref, "actual": list(actual)})
        result = {"checked_at": now(), "reference": str(reference),
                  "reference_sha256": hashlib.sha256(Path(reference).read_bytes()).hexdigest(),
                  "sites": len(sites), "rows": len(refs), "mismatches": mismatches, "passed": not mismatches}
        atomic_json(store.root / "reports" / "manual_comparison.json", result)
        return result
    finally:
        store.close()