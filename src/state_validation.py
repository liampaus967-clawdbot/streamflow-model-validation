#!/usr/bin/env python3
"""
State-by-state validation: HPP Model vs USGS Observed vs NWM
"""

import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm

DB_CONFIG = {
    'host': 'river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com',
    'database': 'river_router',
    'user': 'river_router',
    'password': 'Pacific1ride'
}

TEST_DATE = '2024-07-15'

def get_state(lon, lat):
    if -107 < lon < -93 and 25 < lat < 37:
        return 'TX'
    elif -125 < lon < -114 and 32 < lat < 42:
        return 'CA'
    elif -85 < lon < -75 and 33 < lat < 37:
        return 'NC'
    return 'Other'

def fetch_usgs_batch(site_ids, date):
    """Fetch USGS data for batch of sites."""
    base_url = "https://waterservices.usgs.gov/nwis/dv/"
    results = {}
    
    batch_size = 100
    site_list = list(site_ids)
    
    for i in range(0, len(site_list), batch_size):
        batch = site_list[i:i+batch_size]
        sites_str = ','.join(batch)
        
        params = {
            'format': 'json',
            'sites': sites_str,
            'startDT': date,
            'endDT': date,
            'parameterCd': '00060',
            'siteStatus': 'all'
        }
        
        try:
            resp = requests.get(base_url, params=params, timeout=60)
            if resp.ok:
                data = resp.json()
                ts = data.get('value', {}).get('timeSeries', [])
                for series in ts:
                    site_code = series['sourceInfo']['siteCode'][0]['value']
                    values = series.get('values', [{}])[0].get('value', [])
                    if values:
                        flow = float(values[0]['value'])
                        if flow >= 0:
                            results[site_code] = flow
        except Exception as e:
            pass
    
    return results

def fetch_nwm_for_sites(site_ids):
    """Fetch NWM data for USGS sites via spatial lookup."""
    import psycopg2
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    CMS_TO_CFS = 35.3147
    results = {}
    
    # Get COMID and NWM flow for each USGS gauge
    for site_id in site_ids:
        # Lookup gauge location and find nearest NWM reach
        cur.execute("""
            SELECT g.site_no, n.streamflow_cms * %s as flow_cfs
            FROM usgs_gauges g
            JOIN LATERAL (
                SELECT nv.streamflow_cms
                FROM nwm_velocity nv
                JOIN river_edges re ON re.comid = nv.comid
                WHERE ST_DWithin(g.geom, re.geom, 0.01)
                ORDER BY ST_Distance(g.geom, re.geom)
                LIMIT 1
            ) n ON true
            WHERE g.site_no = %s
        """, (CMS_TO_CFS, site_id))
        
        row = cur.fetchone()
        if row:
            results[row[0]] = row[1]
    
    cur.close()
    conn.close()
    return results

def compute_metrics(observed, predicted):
    """Compute validation metrics."""
    obs = np.array(observed)
    pred = np.array(predicted)
    
    valid = (obs > 0) & (pred > 0)
    obs_valid = obs[valid]
    pred_valid = pred[valid]
    
    if len(obs_valid) < 5:
        return None
    
    rmse = np.sqrt(np.mean((pred_valid - obs_valid)**2))
    pbias = 100 * np.sum(pred_valid - obs_valid) / np.sum(obs_valid)
    nse = 1 - np.sum((obs_valid - pred_valid)**2) / np.sum((obs_valid - np.mean(obs_valid))**2)
    corr = np.corrcoef(obs_valid, pred_valid)[0, 1]
    
    log_obs = np.log10(obs_valid)
    log_pred = np.log10(pred_valid)
    log_nse = 1 - np.sum((log_obs - log_pred)**2) / np.sum((log_obs - np.mean(log_obs))**2)
    
    return {
        'n': len(obs_valid),
        'rmse': round(rmse, 1),
        'pbias': round(pbias, 1),
        'nse': round(nse, 3),
        'r2': round(corr**2, 3),
        'log_nse': round(log_nse, 3)
    }

def main():
    print("="*70)
    print("HPP vs NWM VALIDATION REPORT - BY STATE")
    print(f"Test Date: {TEST_DATE}")
    print("="*70)
    
    # Load pour points
    with open('data/pour_points.geojson') as f:
        pour_points = json.load(f)
    
    # Build site info
    sites = []
    for f in pour_points['features']:
        p = f['properties']
        if p.get('site_id'):
            coords = f['geometry']['coordinates']
            sites.append({
                'uuid': str(p['UUID']),
                'site_id': p['site_id'],
                'state': get_state(coords[0], coords[1]),
                'lon': coords[0],
                'lat': coords[1]
            })
    
    sites_df = pd.DataFrame(sites)
    print(f"\nUSGS gauge sites by state:")
    print(sites_df['state'].value_counts())
    
    # Load HPP predictions
    print("\n[1/3] Loading HPP predictions...")
    hpp_df = pd.read_parquet('data/model_predictions.parquet')
    hpp_df['time'] = pd.to_datetime(hpp_df['time'])
    target_date = pd.to_datetime(TEST_DATE)
    hpp_daily = hpp_df[hpp_df['time'] == target_date].set_index('UUID')
    print(f"  HPP predictions for {TEST_DATE}: {len(hpp_daily)}")
    
    # Fetch USGS data
    print("\n[2/3] Fetching USGS observed data...")
    all_site_ids = sites_df['site_id'].tolist()
    usgs_data = {}
    
    for state in ['TX', 'CA', 'NC']:
        state_sites = sites_df[sites_df['state'] == state]['site_id'].tolist()
        print(f"  Fetching {state}: {len(state_sites)} sites...")
        state_usgs = fetch_usgs_batch(state_sites, TEST_DATE)
        usgs_data.update(state_usgs)
        print(f"    Retrieved: {len(state_usgs)}")
    
    print(f"  Total USGS: {len(usgs_data)}")
    
    # Build comparison dataset
    print("\n[3/3] Building comparison dataset...")
    rows = []
    for _, site in sites_df.iterrows():
        uuid = site['uuid']
        site_id = site['site_id']
        state = site['state']
        
        if uuid not in hpp_daily.index:
            continue
        
        hpp_flow = hpp_daily.loc[uuid, 'ft3_s_q50']
        usgs_flow = usgs_data.get(site_id)
        
        if usgs_flow is not None:
            rows.append({
                'uuid': uuid,
                'site_id': site_id,
                'state': state,
                'hpp_cfs': hpp_flow,
                'usgs_cfs': usgs_flow
            })
    
    df = pd.DataFrame(rows)
    print(f"\nComparison sites: {len(df)}")
    print(df.groupby('state').size())
    
    # Now get NWM data via spatial join (simpler approach using our river_edges)
    print("\nFetching NWM data...")
    import psycopg2
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    CMS_TO_CFS = 35.3147
    nwm_data = {}
    
    for site_id in tqdm(df['site_id'].unique(), desc="NWM lookup"):
        cur.execute("""
            SELECT n.streamflow_cms * %s as flow_cfs
            FROM usgs_gauges g
            JOIN nwm_velocity n ON n.comid = (
                SELECT re.comid FROM river_edges re
                WHERE ST_DWithin(g.geom, re.geom, 0.01)
                ORDER BY ST_Distance(g.geom, re.geom)
                LIMIT 1
            )
            WHERE g.site_no = %s
        """, (CMS_TO_CFS, site_id))
        row = cur.fetchone()
        if row and row[0]:
            nwm_data[site_id] = row[0]
    
    cur.close()
    conn.close()
    
    df['nwm_cfs'] = df['site_id'].map(nwm_data)
    print(f"NWM data retrieved: {df['nwm_cfs'].notna().sum()}")
    
    # Save full comparison
    df.to_csv('results/state_comparison.csv', index=False)
    
    # Generate report
    print("\n" + "="*70)
    print("VALIDATION RESULTS")
    print("="*70)
    
    # Overall metrics
    print("\n" + "-"*50)
    print("OVERALL METRICS")
    print("-"*50)
    
    hpp_usgs = df.dropna(subset=['hpp_cfs', 'usgs_cfs'])
    nwm_usgs = df.dropna(subset=['nwm_cfs', 'usgs_cfs'])
    
    hpp_metrics = compute_metrics(hpp_usgs['usgs_cfs'], hpp_usgs['hpp_cfs'])
    nwm_metrics = compute_metrics(nwm_usgs['usgs_cfs'], nwm_usgs['nwm_cfs'])
    
    print(f"\nHPP vs USGS (n={hpp_metrics['n']}):")
    print(f"  NSE: {hpp_metrics['nse']}, R²: {hpp_metrics['r2']}, PBIAS: {hpp_metrics['pbias']}%, Log-NSE: {hpp_metrics['log_nse']}")
    
    if nwm_metrics:
        print(f"\nNWM vs USGS (n={nwm_metrics['n']}):")
        print(f"  NSE: {nwm_metrics['nse']}, R²: {nwm_metrics['r2']}, PBIAS: {nwm_metrics['pbias']}%, Log-NSE: {nwm_metrics['log_nse']}")
    
    # By state
    print("\n" + "-"*50)
    print("METRICS BY STATE")
    print("-"*50)
    
    state_results = []
    for state in ['TX', 'CA', 'NC']:
        state_df = df[df['state'] == state]
        
        hpp_state = state_df.dropna(subset=['hpp_cfs', 'usgs_cfs'])
        nwm_state = state_df.dropna(subset=['nwm_cfs', 'usgs_cfs'])
        
        hpp_m = compute_metrics(hpp_state['usgs_cfs'], hpp_state['hpp_cfs']) if len(hpp_state) >= 5 else None
        nwm_m = compute_metrics(nwm_state['usgs_cfs'], nwm_state['nwm_cfs']) if len(nwm_state) >= 5 else None
        
        print(f"\n{state}:")
        if hpp_m:
            print(f"  HPP vs USGS (n={hpp_m['n']}): NSE={hpp_m['nse']}, R²={hpp_m['r2']}, PBIAS={hpp_m['pbias']}%, Log-NSE={hpp_m['log_nse']}")
            state_results.append({'state': state, 'model': 'HPP', **hpp_m})
        if nwm_m:
            print(f"  NWM vs USGS (n={nwm_m['n']}): NSE={nwm_m['nse']}, R²={nwm_m['r2']}, PBIAS={nwm_m['pbias']}%, Log-NSE={nwm_m['log_nse']}")
            state_results.append({'state': state, 'model': 'NWM', **nwm_m})
    
    # Save state results
    pd.DataFrame(state_results).to_csv('results/state_metrics.csv', index=False)
    
    print("\n" + "="*70)
    print("Reports saved to results/")
    print("="*70)

if __name__ == '__main__':
    main()
