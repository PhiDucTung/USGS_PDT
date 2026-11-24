import json
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from usgs_pipeline.medallion import (paper_percentile, weekly_station, station_alignment,
    aggregate_week, stage, save, read, weekly, gold, validate_gold, verified, METHOD)


def test_matlab_ranks():
    assert np.allclose(paper_percentile([1,1,2,2,2,3,3,3,3]),[100/9]*2+[400/9]*3+[700/9]*4)
    assert np.isnan(paper_percentile([np.nan])[0])
    assert paper_percentile([0])[0]==100


def test_windows_and_thresholds():
    daily=pd.DataFrame({'date':pd.date_range('2020-01-01','2022-12-31'),'discharge_cfs':1.})
    result=weekly_station(daily,'S').set_index('week_date')
    assert np.isnan(result.loc['2020-01-07','q7'])
    assert result.loc['2020-01-14','q7']==1
    assert np.isnan(result.loc['2020-01-14','q14'])
    assert result.loc['2020-01-21','q14']==1
    assert np.isnan(result.loc['2020-01-28','q28'])
    assert result.loc['2020-02-04','q28']==1
    daily.loc[daily.date.between('2020-02-25','2020-02-27'),'discharge_cfs']=np.nan
    daily.loc[daily.date.eq('2020-03-03'),'discharge_cfs']=999
    r=weekly_station(daily,'S').set_index('week_date')
    assert r.loc['2020-03-03','q7']==1 and r.loc['2020-03-03','n_days7']==4
    daily.loc[daily.date.eq('2020-02-28'),'discharge_cfs']=np.nan
    assert np.isnan(weekly_station(daily,'S').set_index('week_date').loc['2020-03-03','q7'])


def fixture():
    grid=pd.DataFrame({'grid_id':['A','B','C'],'Lon':[0.,1.,2.],'Lat':[0.,0.,0.]})
    stations=pd.DataFrame({'site_id':['S1','S2'],'lon':[0.,.5],'lat':[0.,0.], 'huc8':['01010001']*2,'huc6':['010100']*2,'huc4':['0101']*2})
    mapped=grid.assign(huc8=['01010001','01010001',None],huc6=['010100','010100',None],huc4=['0101','0101',None])
    alignment=station_alignment(stations,grid)
    return grid,stations,mapped,alignment


def test_spatial_and_donors():
    g,s,h,a=fixture()
    assert a.grid_id.tolist()==['A','A'] and a.tie.tolist()==[False,True]
    obs=pd.DataFrame({'site_id':['S1','S2'],'week_date':pd.Timestamp('2020-02-04'),**{f'p{w}':[20.,80.] for w in (7,14,28)}})
    out,tr=aggregate_week(g,h,s,a,obs,'test');out=out.set_index('grid_id')
    assert out.loc['A','p7']==50 and out.loc['A','n_stations7']==2
    assert pd.isna(out.loc['A','mean_huc8_7'])
    assert out.loc['B','mean_huc8_7']==50 and out.loc['B','idw_huc8_7']==60
    assert out.loc['C','reason_huc8_7']=='missing_huc'
    # Explicit zero-distance donor handling, without recursive use of A's grid percentile.
    s.loc[0,'lon']=1.
    out,_=aggregate_week(g,h,s,a,obs,'test')
    assert out.set_index('grid_id').loc['B','idw_huc8_7']==20


def test_stages_end_to_end_and_integrity(tmp_path):
    g,s,h,a=fixture()
    with stage(tmp_path,'silver',[]) as (sr,m):
        for name,df in [('grid',g),('stations',s),('grid_huc',h),('station_grid',a)]:save(df,sr/f'{name}.parquet')
        for site in s.site_id:
            for year in (2020,2021,2022):
                df=pd.DataFrame({'date':pd.date_range(f'{year}-01-01',f'{year}-12-31'),'discharge_cfs':np.arange(366 if year==2020 else 365,dtype=float)})
                save(df,sr/'station_daily'/f'year={year}'/f'{site}.parquet')
    wr=weekly(tmp_path,sr)
    gr=gold(tmp_path,wr)
    report=validate_gold(gr)
    assert report['passed'] and report['rows']==3*156 and report['weeks']==156
    file=next((gr/'grid_weekly').rglob('*.parquet'))
    # A dropped value/count must fail even before manifests are finalized.
    frame = read(file)
    frame.loc[0, 'reason7'] = 'incorrect_reason'
    save(frame, file)
    assert not validate_gold(gr, check_manifest=False)['passed']
    file.write_bytes(b'corrupt')
    with pytest.raises(ValueError,match='checksum'):verified(gr)


def test_failed_stage_not_complete(tmp_path):
    with pytest.raises(RuntimeError):
        with stage(tmp_path,'silver',[]) as (run,m):raise RuntimeError('interrupted')
    assert json.loads((run/'manifest.json').read_text())['status']=='incomplete'


def test_silver_real_ingestion_fixture(tmp_path):
    from test_preprocess import prepared
    from usgs_pipeline.medallion import silver, read
    import geopandas as gpd
    from shapely.geometry import box
    cfg=prepared(tmp_path,{'USGS-00000001':[('2020-02-28','0','cfs','a')]})
    pd.DataFrame({'grid_id':['A'],'lon':[-77.],'lat':[39.]}).to_csv(tmp_path/'grid.csv',index=False)
    gpd.GeoDataFrame({'huc8':['02070008']},geometry=[box(-78,38,-76,40)],crs=4326).to_file(tmp_path/'wbd.gpkg',layer='WBDHU8')
    run=silver(cfg,tmp_path/'grid.csv',tmp_path/'wbd.gpkg')
    assert verified(run)['stage']=='silver'
    daily=read(next((run/'station_daily').rglob('*.parquet')))
    assert daily.discharge_cfs.iloc[0]==0 and pd.isna(daily.discharge_cfs.iloc[1])
    assert read(run/'grid_huc.parquet').huc8.iloc[0]=='02070008'
    report = json.loads((run/'report.json').read_text())
    assert report['passed'] and report['daily_rows'] == 2 and report['valid_daily_rows'] == 1
    output = next(o for o in verified(run)['outputs'] if o['path'].startswith('station_daily/'))
    assert output['rows'] == 2 and any(f['name'] == 'qc_flags' for f in output['schema'])
    next((tmp_path/'raw').rglob('*.json.gz')).write_bytes(b'changed raw')
    with pytest.raises(ValueError, match='Source checksum'):
        verified(run)


def test_donor_pool_not_capped_and_windows_independent():
    g,s,h,a=fixture()
    count=25
    s=pd.DataFrame({'site_id':[f'S{i:03}' for i in range(count)],'lon':np.zeros(count),'lat':np.zeros(count),'huc8':['01010001']*count,'huc6':['010100']*count,'huc4':['0101']*count})
    a=station_alignment(s,g)
    obs=pd.DataFrame({'site_id':s.site_id,'week_date':pd.Timestamp('2020-02-04'),'p7':np.arange(count,dtype=float),'p14':[np.nan]*count,'p28':np.arange(count,dtype=float)})
    out,trace=aggregate_week(g,h,s,a,obs,'test')
    row=out.set_index('grid_id').loc['B']
    assert row.n_donors_huc8_7==25 and row.mean_huc8_7==12
    assert row.n_donors_huc8_14==0 and pd.isna(row.mean_huc8_14)
    assert set(trace.window)=={7,28}


def test_downstream_verification_checks_transitive_sources(tmp_path):
    source = tmp_path/'raw.json.gz'
    source.write_bytes(b'original')
    with stage(tmp_path, 'silver', [source]) as (sr, _):
        pass
    with stage(tmp_path, 'weekly', [sr/'manifest.json']) as (wr, _):
        pass
    with stage(tmp_path, 'gold', [wr/'manifest.json']) as (gr, _):
        pass
    assert verified(gr, expected_stage='gold')['manifest_version'] == 2
    with pytest.raises(ValueError, match='Expected silver'):
        verified(wr, expected_stage='silver')
    source.write_bytes(b'changed')
    with pytest.raises(ValueError, match='Source checksum'):
        verified(gr)


def test_daily_contract_calendar_and_qc():
    from usgs_pipeline.medallion import validate_daily
    df = pd.DataFrame({'site_id':['S']*3, 'date':pd.date_range('2020-02-28', '2020-03-01'),
                       'discharge_cfs':[0., -2., np.nan], 'qc_flags':[[], [], ['missing_day']],
                       'unit':['cfs']*3})
    validate_daily(df, 'S', '2020-02-28', '2020-03-01', check_qc=True)
    with pytest.raises(ValueError, match='calendar'):
        validate_daily(df.iloc[:2], 'S', '2020-02-28', '2020-03-01')
    with pytest.raises(ValueError, match='calendar'):
        validate_daily(pd.concat([df, df.iloc[:1]]), 'S', '2020-02-28', '2020-03-01')
    df.loc[0, 'discharge_cfs'] = np.inf
    with pytest.raises(ValueError, match='Non-finite'):
        validate_daily(df, 'S', '2020-02-28', '2020-03-01')
    df.loc[0, 'discharge_cfs'] = np.nan
    with pytest.raises(ValueError, match='QC/null'):
        validate_daily(df, 'S', '2020-02-28', '2020-03-01', check_qc=True)
