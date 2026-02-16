#!/usr/bin/env python3
"""
State-by-state validation: HPP Model vs USGS Observed vs NWM
VERSION 2 - With full date verification and audit trail

Author: Sandy (Clawdbot CTO)
Date: February 16, 2026
"""

import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm
import os
import sys

# =============================================================================
# CONFIGURATION - SINGLE SOURCE OF TRUTH FOR TEST DATE
# =============================================================================
TEST_DATE = '2024-07-15'
TEST_DATE_DT = pd.to_datetime(TEST_DATE)

# NWM file path derived from TEST_DATE
NWM_FILE = f"data/nwm/nwm_{TEST_DATE.replace('-', '')}_12z.parquet"

DB_CONFIG = {
    'host': 'river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com',
    'database': 'river_router',
    'user': 'river_router',
    'password': 'Pacific1ride'
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_state(lon, lat):
    """Classify coordinates into TX, CA, or NC."""
    if -107 < lon < -93 and 25 < lat < 37:
        return 'TX'
    elif -125 < lon < -114 and 32 < lat < 42:
        return 'CA'
    elif -85 < lon < -75 and 33 < lat < 37:
        return 'NC'
    return 'Other'


def verify_hpp_date(hpp_df, target_date):
    """Verify HPP data contains the target date."""
    hpp_df['time'] = pd.to_datetime(hpp_df['time'])
    has_date = (hpp_df['time'] == target_date).any()
    count = (hpp_df['time'] == target_date).sum()
    
    if not has_date:
        raise ValueError(f"HPP data does not contain {target_date}")
    
    return count


def fetch_usgs_batch(site_ids, date):
    """
    Fetch USGS daily values for specified sites and date.
    
    Args:
        site_ids: List of USGS site numbers
        date: Date string in YYYY-MM-DD format
        
    Returns:
        Dict mapping site_id -> flow in CFS
    """
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
            'startDT': date,  # Explicitly set to TEST_DATE
            'endDT': date,    # Same day - single day request
            'parameterCd': '00060',  # Discharge in CFS
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
                        # Verify the date in response matches request
                        value_date = values[0].get('dateTime', '')[:10]
                        if value_date == date:
                            flow = float(values[0]['value'])
                            if flow >= 0:  # Valid reading (negative = missing)
                                results[site_code] = flow
        except Exception as e:
            print(f"  Warning: USGS batch {i} failed: {e}")
    
    return results


def get_usgs_to_comid_mapping():
    """
    Get USGS gauge to COMID mapping via spatial join.
    
    Uses PostGIS to find the nearest NHD+ reach for each USGS gauge.
    """
    import psycopg2
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT g.site_no, re.comid, ST_Distance(g.geom, re.geom) as dist
        FROM usgs_gauges g
        CROSS JOIN LATERAL (
            SELECT comid, geom
            FROM river_edges
            WHERE ST_DWithin(g.geom, geom, 0.01)  -- ~1km search radius
            ORDER BY ST_Distance(g.geom, geom)
            LIMIT 1
        ) re
    """)
    
    mapping = {}
    for row in cur.fetchall():
        mapping[row[0]] = row[1]
    
    cur.close()
    conn.close()
    
    return mapping


def compute_metrics(observed, predicted):
    """
    Compute standard hydrological validation metrics.
    
    Args:
        observed: Array of observed values
        predicted: Array of predicted values
        
    Returns:
        Dict with n, rmse, pbias, nse, r2, log_nse
    """
    obs = np.array(observed)
    pred = np.array(predicted)
    
    # Filter to positive values (required for log transform)
    valid = (obs > 0) & (pred > 0)
    obs_valid = obs[valid]
    pred_valid = pred[valid]
    
    if len(obs_valid) < 5:
        return None
    
    # RMSE
    rmse = np.sqrt(np.mean((pred_valid - obs_valid)**2))
    
    # Percent Bias
    pbias = 100 * np.sum(pred_valid - obs_valid) / np.sum(obs_valid)
    
    # Nash-Sutcliffe Efficiency
    nse = 1 - np.sum((obs_valid - pred_valid)**2) / np.sum((obs_valid - np.mean(obs_valid))**2)
    
    # Correlation coefficient
    corr = np.corrcoef(obs_valid, pred_valid)[0, 1]
    
    # Log-transformed NSE
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


# =============================================================================
# MAIN VALIDATION
# =============================================================================

def main():
    print("="*70)
    print("HPP vs NWM VALIDATION - VERSION 2 (with date verification)")
    print("="*70)
    print(f"\n*** TEST DATE: {TEST_DATE} ***\n")
    
    # -------------------------------------------------------------------------
    # STEP 0: Verify all data files exist and dates match
    # -------------------------------------------------------------------------
    print("[0/5] VERIFYING DATA SOURCES AND DATES")
    print("-"*50)
    
    # Check HPP file
    if not os.path.exists('data/model_predictions.parquet'):
        sys.exit("ERROR: HPP data file not found")
    
    # Check NWM file
    if not os.path.exists(NWM_FILE):
        sys.exit(f"ERROR: NWM file not found: {NWM_FILE}")
    
    # Check pour_points
    if not os.path.exists('data/pour_points.geojson'):
        sys.exit("ERROR: pour_points.geojson not found")
    
    print(f"  ✓ HPP file: data/model_predictions.parquet")
    print(f"  ✓ NWM file: {NWM_FILE}")
    print(f"  ✓ Site metadata: data/pour_points.geojson")
    
    # -------------------------------------------------------------------------
    # STEP 1: Load and verify HPP data
    # -------------------------------------------------------------------------
    print("\n[1/5] LOADING HPP PREDICTIONS")
    print("-"*50)
    
    hpp_df = pd.read_parquet('data/model_predictions.parquet')
    hpp_count = verify_hpp_date(hpp_df, TEST_DATE_DT)
    print(f"  HPP date range: {hpp_df['time'].min()} to {hpp_df['time'].max()}")
    print(f"  ✓ HPP records for {TEST_DATE}: {hpp_count}")
    
    hpp_daily = hpp_df[hpp_df['time'] == TEST_DATE_DT].set_index('UUID')
    
    # -------------------------------------------------------------------------
    # STEP 2: Load site metadata
    # -------------------------------------------------------------------------
    print("\n[2/5] LOADING SITE METADATA")
    print("-"*50)
    
    with open('data/pour_points.geojson') as f:
        pour_points = json.load(f)
    
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
    print(f"  Total USGS gauge sites: {len(sites_df)}")
    print(f"  By state:")
    for state, count in sites_df['state'].value_counts().items():
        print(f"    {state}: {count}")
    
    # -------------------------------------------------------------------------
    # STEP 3: Fetch USGS observed data
    # -------------------------------------------------------------------------
    print(f"\n[3/5] FETCHING USGS OBSERVED DATA FOR {TEST_DATE}")
    print("-"*50)
    
    usgs_data = {}
    for state in ['TX', 'CA', 'NC']:
        state_sites = sites_df[sites_df['state'] == state]['site_id'].tolist()
        print(f"  {state}: requesting {len(state_sites)} sites...", end=' ')
        state_usgs = fetch_usgs_batch(state_sites, TEST_DATE)
        usgs_data.update(state_usgs)
        print(f"received {len(state_usgs)}")
    
    print(f"  ✓ Total USGS observations: {len(usgs_data)}")
    
    # -------------------------------------------------------------------------
    # STEP 4: Load NWM data
    # -------------------------------------------------------------------------
    print(f"\n[4/5] LOADING NWM DATA")
    print("-"*50)
    print(f"  File: {NWM_FILE}")
    print(f"  Source: gs://national-water-model/nwm.{TEST_DATE.replace('-','')}/")
    print(f"         analysis_assim/nwm.t12z.analysis_assim.channel_rt.tm00.conus.nc")
    
    nwm_df = pd.read_parquet(NWM_FILE)
    CMS_TO_CFS = 35.3147
    nwm_df['flow_cfs'] = nwm_df['streamflow_cms'] * CMS_TO_CFS
    nwm_lookup = dict(zip(nwm_df['comid'], nwm_df['flow_cfs']))
    print(f"  ✓ NWM reaches loaded: {len(nwm_lookup)}")
    
    # Get USGS -> COMID mapping
    print("\n  Building USGS → COMID spatial mapping...")
    usgs_to_comid = get_usgs_to_comid_mapping()
    print(f"  ✓ Mapped {len(usgs_to_comid)} USGS sites to COMIDs")
    
    # -------------------------------------------------------------------------
    # STEP 5: Build comparison dataset
    # -------------------------------------------------------------------------
    print(f"\n[5/5] BUILDING COMPARISON DATASET")
    print("-"*50)
    
    rows = []
    for _, site in sites_df.iterrows():
        uuid = site['uuid']
        site_id = site['site_id']
        state = site['state']
        
        if uuid not in hpp_daily.index:
            continue
        
        hpp_flow = hpp_daily.loc[uuid, 'ft3_s_q50']
        usgs_flow = usgs_data.get(site_id)
        comid = usgs_to_comid.get(site_id)
        nwm_flow = nwm_lookup.get(comid) if comid else None
        
        if usgs_flow is not None:
            rows.append({
                'uuid': uuid,
                'site_id': site_id,
                'comid': comid,
                'state': state,
                'date': TEST_DATE,  # Explicitly record date in output
                'hpp_cfs': hpp_flow,
                'usgs_cfs': usgs_flow,
                'nwm_cfs': nwm_flow
            })
    
    df = pd.DataFrame(rows)
    
    print(f"  Sites with HPP + USGS: {len(df)}")
    print(f"  Sites with all 3 sources: {df['nwm_cfs'].notna().sum()}")
    print(f"  By state:")
    for state, count in df.groupby('state').size().items():
        print(f"    {state}: {count}")
    
    # Save with date column for verification
    output_file = 'results/state_comparison_v2.csv'
    df.to_csv(output_file, index=False)
    print(f"\n  ✓ Saved to: {output_file}")
    
    # -------------------------------------------------------------------------
    # RESULTS
    # -------------------------------------------------------------------------
    print("\n" + "="*70)
    print(f"VALIDATION RESULTS FOR {TEST_DATE}")
    print("="*70)
    
    # Overall
    print("\n--- OVERALL ---")
    hpp_usgs = df.dropna(subset=['hpp_cfs', 'usgs_cfs'])
    nwm_usgs = df.dropna(subset=['nwm_cfs', 'usgs_cfs'])
    
    hpp_m = compute_metrics(hpp_usgs['usgs_cfs'], hpp_usgs['hpp_cfs'])
    nwm_m = compute_metrics(nwm_usgs['usgs_cfs'], nwm_usgs['nwm_cfs'])
    
    print(f"HPP vs USGS (n={hpp_m['n']}): NSE={hpp_m['nse']}, R²={hpp_m['r2']}, PBIAS={hpp_m['pbias']}%, Log-NSE={hpp_m['log_nse']}")
    print(f"NWM vs USGS (n={nwm_m['n']}): NSE={nwm_m['nse']}, R²={nwm_m['r2']}, PBIAS={nwm_m['pbias']}%, Log-NSE={nwm_m['log_nse']}")
    
    # By state
    state_results = []
    for state in ['TX', 'CA', 'NC']:
        print(f"\n--- {state} ---")
        state_df = df[df['state'] == state]
        
        hpp_state = state_df.dropna(subset=['hpp_cfs', 'usgs_cfs'])
        nwm_state = state_df.dropna(subset=['nwm_cfs', 'usgs_cfs'])
        
        hpp_sm = compute_metrics(hpp_state['usgs_cfs'], hpp_state['hpp_cfs'])
        nwm_sm = compute_metrics(nwm_state['usgs_cfs'], nwm_state['nwm_cfs'])
        
        if hpp_sm:
            print(f"HPP vs USGS (n={hpp_sm['n']}): NSE={hpp_sm['nse']}, R²={hpp_sm['r2']}, PBIAS={hpp_sm['pbias']}%, Log-NSE={hpp_sm['log_nse']}")
            state_results.append({'state': state, 'model': 'HPP', 'date': TEST_DATE, **hpp_sm})
        if nwm_sm:
            print(f"NWM vs USGS (n={nwm_sm['n']}): NSE={nwm_sm['nse']}, R²={nwm_sm['r2']}, PBIAS={nwm_sm['pbias']}%, Log-NSE={nwm_sm['log_nse']}")
            state_results.append({'state': state, 'model': 'NWM', 'date': TEST_DATE, **nwm_sm})
    
    # Save metrics
    metrics_file = 'results/state_metrics_v2.csv'
    pd.DataFrame(state_results).to_csv(metrics_file, index=False)
    print(f"\n✓ Metrics saved to: {metrics_file}")
    
    print("\n" + "="*70)
    print("AUDIT TRAIL")
    print("="*70)
    print(f"  Test Date: {TEST_DATE}")
    print(f"  HPP Source: data/model_predictions.parquet (filtered to {TEST_DATE})")
    print(f"  USGS Source: USGS Water Services API (startDT={TEST_DATE}, endDT={TEST_DATE})")
    print(f"  NWM Source: {NWM_FILE}")
    print(f"  NWM Origin: gs://national-water-model/nwm.{TEST_DATE.replace('-','')}/analysis_assim/")
    print("="*70)

if __name__ == '__main__':
    main()
