#!/usr/bin/env python3
"""
Three-way validation: HPP Model vs USGS Observed vs NWM
For sites that have USGS gauges with COMID matches.
"""

import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import pyarrow.parquet as pq

# Config
DB_CONFIG = {
    'host': 'river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com',
    'database': 'river_router',
    'user': 'river_router',
    'password': 'Pacific1ride'
}

TEST_DATE = '2024-07-15'

def load_hpp_predictions(parquet_path, uuids, date):
    """Load HPP model predictions for specific UUIDs and date."""
    df = pd.read_parquet(parquet_path)
    df['time'] = pd.to_datetime(df['time'])
    
    # Filter to target date and UUIDs
    target_date = pd.to_datetime(date)
    mask = (df['time'] == target_date) & (df['UUID'].isin(uuids))
    
    result = df[mask].set_index('UUID')
    return result[['ft3_s_q50', 'ft3_s_q25', 'ft3_s_q75']]

def fetch_usgs_data(site_ids, date):
    """Fetch USGS streamflow data for given sites and date."""
    # USGS Water Services API
    base_url = "https://waterservices.usgs.gov/nwis/dv/"
    
    results = {}
    
    # Batch sites (USGS allows up to 100 sites per request)
    batch_size = 100
    site_list = list(site_ids)
    
    for i in tqdm(range(0, len(site_list), batch_size), desc="Fetching USGS"):
        batch = site_list[i:i+batch_size]
        sites_str = ','.join(batch)
        
        params = {
            'format': 'json',
            'sites': sites_str,
            'startDT': date,
            'endDT': date,
            'parameterCd': '00060',  # Discharge, cubic feet per second
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
                        if flow >= 0:  # Valid reading
                            results[site_code] = flow
        except Exception as e:
            print(f"Error fetching batch {i}: {e}")
    
    return results

def fetch_nwm_data(comids, date):
    """Fetch NWM data from our database for given COMIDs."""
    import psycopg2
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    results = {}
    comid_list = list(comids)
    
    # Get NWM streamflow (cms -> cfs conversion: 1 cms = 35.3147 cfs)
    CMS_TO_CFS = 35.3147
    
    # First try nwm_velocity table
    cur.execute("""
        SELECT comid, streamflow_cms * %s as flow_cfs
        FROM nwm_velocity 
        WHERE comid = ANY(%s) AND streamflow_cms IS NOT NULL
    """, (CMS_TO_CFS, comid_list))
    
    for row in cur.fetchall():
        results[row[0]] = row[1]
    
    print(f"  Found {len(results)} in nwm_velocity")
    
    # Also try river_edges for any missing
    missing = [c for c in comid_list if c not in results]
    if missing:
        cur.execute("""
            SELECT comid, flow_cfs 
            FROM river_edges 
            WHERE comid = ANY(%s) AND flow_cfs IS NOT NULL
        """, (missing,))
        
        for row in cur.fetchall():
            if row[0] not in results:
                results[row[0]] = row[1]
        
        print(f"  Found {len(results)} total after river_edges")
    
    cur.close()
    conn.close()
    
    return results

def compute_metrics(observed, predicted):
    """Compute validation metrics."""
    obs = np.array(observed)
    pred = np.array(predicted)
    
    # Filter out zeros/negatives for log metrics
    valid = (obs > 0) & (pred > 0)
    obs_valid = obs[valid]
    pred_valid = pred[valid]
    
    if len(obs_valid) < 10:
        return None
    
    # RMSE
    rmse = np.sqrt(np.mean((pred_valid - obs_valid)**2))
    
    # Percent Bias
    pbias = 100 * np.sum(pred_valid - obs_valid) / np.sum(obs_valid)
    
    # Nash-Sutcliffe Efficiency
    nse = 1 - np.sum((obs_valid - pred_valid)**2) / np.sum((obs_valid - np.mean(obs_valid))**2)
    
    # Correlation
    corr = np.corrcoef(obs_valid, pred_valid)[0, 1]
    
    # Log metrics (better for flow data)
    log_obs = np.log10(obs_valid)
    log_pred = np.log10(pred_valid)
    log_rmse = np.sqrt(np.mean((log_pred - log_obs)**2))
    log_nse = 1 - np.sum((log_obs - log_pred)**2) / np.sum((log_obs - np.mean(log_obs))**2)
    
    return {
        'n': len(obs_valid),
        'rmse': rmse,
        'pbias': pbias,
        'nse': nse,
        'r': corr,
        'r2': corr**2,
        'log_rmse': log_rmse,
        'log_nse': log_nse
    }

def main():
    print("="*60)
    print("THREE-WAY VALIDATION: HPP vs USGS vs NWM")
    print(f"Test Date: {TEST_DATE}")
    print("="*60)
    
    # Load crosswalk (UUID -> COMID)
    with open('data/uuid_comid_crosswalk.json') as f:
        crosswalk = json.load(f)
    print(f"\nLoaded crosswalk: {len(crosswalk)} sites with COMID matches")
    
    # Load pour points for site_id info
    with open('data/pour_points.geojson') as f:
        pour_points = json.load(f)
    
    # Build UUID -> site_id mapping for USGS sites
    uuid_to_site = {}
    for f in pour_points['features']:
        p = f['properties']
        if p.get('site_id'):
            uuid_to_site[p['UUID']] = p['site_id']
    
    # Get UUIDs that have both USGS site_id AND COMID match
    valid_uuids = [u for u in crosswalk.keys() if u in uuid_to_site]
    print(f"Sites with both USGS ID and COMID: {len(valid_uuids)}")
    
    # 1. Load HPP predictions
    print("\n[1/3] Loading HPP model predictions...")
    hpp_data = load_hpp_predictions('data/model_predictions.parquet', valid_uuids, TEST_DATE)
    print(f"  HPP predictions loaded: {len(hpp_data)}")
    
    # 2. Fetch USGS observed data
    print("\n[2/3] Fetching USGS observed data...")
    site_ids = [uuid_to_site[u] for u in valid_uuids if u in hpp_data.index]
    usgs_data = fetch_usgs_data(site_ids, TEST_DATE)
    print(f"  USGS data retrieved: {len(usgs_data)} sites")
    
    # 3. Fetch NWM data
    print("\n[3/3] Fetching NWM data...")
    comids = [crosswalk[u]['comid'] for u in valid_uuids if u in hpp_data.index]
    nwm_data = fetch_nwm_data(comids, TEST_DATE)
    print(f"  NWM data retrieved: {len(nwm_data)} sites")
    
    # Build comparison dataframe
    print("\n" + "="*60)
    print("BUILDING COMPARISON DATASET")
    print("="*60)
    
    rows = []
    for uuid in valid_uuids:
        if uuid not in hpp_data.index:
            continue
        
        site_id = uuid_to_site[uuid]
        comid = crosswalk[uuid]['comid']
        
        hpp_flow = hpp_data.loc[uuid, 'ft3_s_q50']
        usgs_flow = usgs_data.get(site_id)
        nwm_flow = nwm_data.get(comid)
        
        if usgs_flow is not None:
            rows.append({
                'uuid': uuid,
                'site_id': site_id,
                'comid': comid,
                'hpp_cfs': hpp_flow,
                'usgs_cfs': usgs_flow,
                'nwm_cfs': nwm_flow
            })
    
    df = pd.DataFrame(rows)
    print(f"\nComparison dataset: {len(df)} sites")
    print(f"  With all 3 sources: {df.dropna().shape[0]}")
    
    # Save comparison data
    df.to_csv('results/three_way_comparison.csv', index=False)
    print(f"\nSaved to: results/three_way_comparison.csv")
    
    # Compute metrics
    print("\n" + "="*60)
    print("VALIDATION METRICS")
    print("="*60)
    
    # HPP vs USGS
    hpp_usgs = df.dropna(subset=['hpp_cfs', 'usgs_cfs'])
    if len(hpp_usgs) >= 10:
        metrics = compute_metrics(hpp_usgs['usgs_cfs'].values, hpp_usgs['hpp_cfs'].values)
        print(f"\n📊 HPP vs USGS Observed (n={metrics['n']}):")
        print(f"   NSE:    {metrics['nse']:.3f}")
        print(f"   R²:     {metrics['r2']:.3f}")
        print(f"   PBIAS:  {metrics['pbias']:.1f}%")
        print(f"   RMSE:   {metrics['rmse']:.1f} CFS")
        print(f"   Log-NSE: {metrics['log_nse']:.3f}")
    
    # NWM vs USGS
    nwm_usgs = df.dropna(subset=['nwm_cfs', 'usgs_cfs'])
    if len(nwm_usgs) >= 10:
        metrics = compute_metrics(nwm_usgs['usgs_cfs'].values, nwm_usgs['nwm_cfs'].values)
        print(f"\n📊 NWM vs USGS Observed (n={metrics['n']}):")
        print(f"   NSE:    {metrics['nse']:.3f}")
        print(f"   R²:     {metrics['r2']:.3f}")
        print(f"   PBIAS:  {metrics['pbias']:.1f}%")
        print(f"   RMSE:   {metrics['rmse']:.1f} CFS")
        print(f"   Log-NSE: {metrics['log_nse']:.3f}")
    
    # HPP vs NWM (where both exist)
    hpp_nwm = df.dropna(subset=['hpp_cfs', 'nwm_cfs'])
    if len(hpp_nwm) >= 10:
        metrics = compute_metrics(hpp_nwm['nwm_cfs'].values, hpp_nwm['hpp_cfs'].values)
        print(f"\n📊 HPP vs NWM (n={metrics['n']}):")
        print(f"   NSE:    {metrics['nse']:.3f}")
        print(f"   R²:     {metrics['r2']:.3f}")
        print(f"   PBIAS:  {metrics['pbias']:.1f}%")
        print(f"   RMSE:   {metrics['rmse']:.1f} CFS")
    
    print("\n" + "="*60)
    print("DONE")
    print("="*60)

if __name__ == '__main__':
    main()
