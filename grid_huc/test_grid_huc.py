import tempfile
from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from grid_huc.build_grid_huc import map_grid, read_grid, read_huc

w = gpd.GeoDataFrame({'huc8':['01010001','01010002']}, geometry=[box(0,0,1,1),box(1,0,2,1)], crs=4326)
g = pd.DataFrame({'grid_id':['in','edge','outside','single_edge'], 'Lon':[.5,1,3,0], 'Lat':[.5,.5,.5,.5]})
r = map_grid(g,w)
assert list(r.match_status)==['inside','boundary_ambiguous','unmatched','boundary_unique']
assert r.huc8[0]=='01010001' and r.huc4[0]=='0101'
assert pd.isna(r.huc8[1]) and r.candidate_huc8[1]=='01010001|01010002'
assert map_grid(g.iloc[[0]].reset_index(drop=True),w.to_crs(3857)).huc8[0]=='01010001'
w2=gpd.GeoDataFrame({'huc8':['01010001','01010002']}, geometry=[box(0,0,1,1),box(.2,.2,.8,.8)], crs=4326)
assert map_grid(g.iloc[[0]].reset_index(drop=True),w2).match_status[0]=='overlap_ambiguous'
with tempfile.TemporaryDirectory() as d:
    p=Path(d)/'grid.csv'
    pd.concat([g,g]).to_csv(p,index=False)
    assert len(read_grid(p,'grid_id','Lon','Lat'))==4
    bad=pd.concat([g,pd.DataFrame({'grid_id':['in'],'Lon':[.7],'Lat':[.7]})])
    bad.to_csv(p,index=False)
    try: read_grid(p,'grid_id','Lon','Lat')
    except ValueError: pass
    else: raise AssertionError('Conflicting ID was accepted')
    f=Path(d)/'wbd.gpkg'
    w.to_file(f,layer='WBDHU8',driver='GPKG')
    loaded,_=read_huc(f,'WBDHU8','HUC8')
    assert loaded.huc8[0]=='01010001'
print('PASS: spatial mapping, CRS, HUC strings, duplicate and conflict checks')
