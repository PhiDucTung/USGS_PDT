"""Compatibility API and CLI for the tier-specific Medallion modules."""
from __future__ import annotations
import argparse
import fcntl
import json
from pathlib import Path
import tomllib
from bronze.scripts.core import Config
from usgs_pipeline.medallion_common import *  # noqa: F401,F403
from silver.scripts.medallion import silver, validate_daily, paper_percentile, weekly_station, weekly
from gold.scripts.medallion import aggregate_week, gold, validate_gold

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
