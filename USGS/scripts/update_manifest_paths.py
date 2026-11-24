#!/usr/bin/env python3
"""Rewrite legacy data/ paths in JSON lineage and refresh recorded hashes."""
import hashlib, json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OLD_PREFIXES=[str(ROOT/'data')+'/', str(ROOT.parent/'data')+'/']
PREFIXES={
 'raw/':'bronze/data/raw/', 'normalized/':'bronze/data/normalized/', 'csv/':'bronze/data/csv/',
 'grid_huc_inputs/':'bronze/data/grid_huc_inputs/', 'reference/':'bronze/data/reference/',
 'reports/':'bronze/data/reports/', 'logs/':'bronze/data/logs/',
 'catalog.json':'bronze/data/catalog.json','checkpoint.sqlite':'bronze/data/checkpoint.sqlite',
 'crawl_manifest.json':'bronze/data/crawl_manifest.json','discovery_manifest.json':'bronze/data/discovery_manifest.json',
 'export_manifest.json':'bronze/data/export_manifest.json',
 'preprocessed/':'silver/data/preprocessed/','medallion/silver/':'silver/data/medallion/silver/',
 'medallion/weekly/':'silver/data/medallion/weekly/','medallion/gold/':'gold/data/medallion/gold/',
 'medallion/gold_filled/':'gold/data/medallion/gold_filled/','medallion/gold_local_20x20/':'gold/data/medallion/gold_local_20x20/' }
def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def conv(x):
 if isinstance(x,str) and any(x.startswith(prefix) for prefix in OLD_PREFIXES):
  prefix=next(prefix for prefix in OLD_PREFIXES if x.startswith(prefix))
  tail=x[len(prefix):]
  for old,new in PREFIXES.items():
   if tail.startswith(old):return str(ROOT/new)+tail[len(old):]
  raise ValueError('Unmapped legacy path: '+x)
 if isinstance(x,list):return [conv(v) for v in x]
 if isinstance(x,dict):return {k:conv(v) for k,v in x.items()}
 return x
for p in list(ROOT.glob('bronze/data/**/*.json'))+list(ROOT.glob('silver/data/**/*.json'))+list(ROOT.glob('gold/data/**/*.json')):
 try:o=json.loads(p.read_text())
 except (UnicodeDecodeError,json.JSONDecodeError):continue
 new=conv(o)
 if new!=o:p.write_text(json.dumps(new,ensure_ascii=False,indent=2)+'\n')
# Manifests have relative output paths, which remain stable after the physical move.
# Refresh all recorded source/code/output hashes after path and implementation changes.
for p in list(ROOT.glob('bronze/data/**/*.json'))+list(ROOT.glob('silver/data/**/*.json'))+list(ROOT.glob('gold/data/**/*.json')):
 try:m=json.loads(p.read_text())
 except Exception:continue
 if not isinstance(m,dict) or 'outputs' not in m or 'sources' not in m:continue
 for group in ('sources','code','outputs'):
  for item in m.get(group,[]):
   path=Path(item.get('path',''))
   if group=='outputs':path=p.parent/path
   if path.is_file():item['sha256']=sha(path)
 p.write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n')
print('Updated JSON references and manifest checksums.')
