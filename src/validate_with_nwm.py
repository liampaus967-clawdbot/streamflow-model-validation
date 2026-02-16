"""
Three-way validation: Model vs USGS vs NWM
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, Optional
import psycopg2
from datetime import datetime
import argparse

from fetch_usgs import fetch_usgs_daily
from map_usgs_to_comid_local import map_usgs_to_comid_local
from validate import load_pour_points, calculate_metrics


# Database connection for NWM data
DB_CONFIG = {
    "host": "river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com",
    "database": "river_router",
    "user": "river_router",
    "password": "Pacific1ride"
}


def fetch_nwm_from_db(comids: list) -> pd.DataFrame:
    """
    Fetch NWM velocity/streamflow from our database.
    
    Note: This fetches the CURRENT NWM data (real-time).
    For historical comparison, you'd need the NWM archive.
    """
    if not comids:
        return pd.DataFrame()
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Query NWM data
    query = """
        SELECT 
            comid,
            velocity_ms,
            streamflow_cms,
            updated_at
        FROM nwm_velocity
        WHERE comid = ANY(%s)
    """
    
    df = pd.read_sql(query, conn, params=(comids,))
    conn.close()
    
    # Convert to CFS (1 cms = 35.3147 cfs)
    if len(df) > 0:
        df["nwm_cfs"] = df["streamflow_cms"] * 35.3147
    
    return df


def run_three_way_validation(
    parquet_path: str,
    geojson_path: str,
    target_date: str = "2024-07-15",
    output_dir: str = "results",
    sample_size: Optional[int] = None
) -> Dict:
    """
    Run three-way comparison: Model vs USGS vs NWM
    """
    print(f"\n{'='*70}")
    print(f"THREE-WAY VALIDATION: Model vs USGS vs NWM")
    print(f"Target Date: {target_date}")
    print(f"{'='*70}\n")
    
    # Load pour points
    print("Loading pour points metadata...")
    pour_points = load_pour_points(geojson_path)
    print(f"  Total sites: {len(pour_points)}")
    
    # Filter to sites with USGS IDs
    usgs_sites = pour_points[pour_points["site_id"].notna()].copy()
    usgs_sites["site_id"] = usgs_sites["site_id"].astype(str).str.zfill(8)
    print(f"  Sites with USGS IDs: {len(usgs_sites)}")
    
    # Sample if requested
    if sample_size and sample_size < len(usgs_sites):
        usgs_sites = usgs_sites.sample(n=sample_size, random_state=42)
        print(f"  Sampled to: {len(usgs_sites)} sites")
    
    # Step 1: Map USGS sites to NHD COMIDs
    print(f"\nStep 1: Mapping USGS sites to NHD COMIDs via local database...")
    site_list = usgs_sites["site_id"].tolist()
    comid_mapping = map_usgs_to_comid_local(site_list, max_distance_m=1000)
    print(f"  Successfully mapped: {len(comid_mapping)} sites")
    
    # Add COMIDs to dataframe
    usgs_sites["comid"] = usgs_sites["site_id"].map(comid_mapping)
    sites_with_comid = usgs_sites[usgs_sites["comid"].notna()]
    print(f"  Sites with valid COMIDs: {len(sites_with_comid)}")
    
    # Step 2: Fetch USGS gauge data
    print(f"\nStep 2: Fetching USGS gauge data for {target_date}...")
    usgs_data = fetch_usgs_daily(
        site_list,
        target_date,
        target_date,
        chunk_size=50,
        delay=0.3
    )
    print(f"  Retrieved data for: {usgs_data['site_id'].nunique()} sites")
    
    # Merge USGS data
    merged = usgs_sites.merge(
        usgs_data[["site_id", "discharge_cfs"]],
        on="site_id",
        how="left"
    )
    merged = merged.rename(columns={
        "discharge_cfs": "usgs_cfs",
        "model_flow_july15": "model_cfs"
    })
    
    # Step 3: Fetch NWM data for sites with COMIDs
    print(f"\nStep 3: Fetching NWM data from database...")
    comids_to_fetch = merged[merged["comid"].notna()]["comid"].astype(int).tolist()
    nwm_data = fetch_nwm_from_db(comids_to_fetch)
    
    if len(nwm_data) > 0:
        print(f"  Retrieved NWM data for: {len(nwm_data)} COMIDs")
        print(f"  NWM data timestamp: {nwm_data['updated_at'].iloc[0]}")
        
        # Merge NWM data
        merged = merged.merge(
            nwm_data[["comid", "nwm_cfs"]],
            on="comid",
            how="left"
        )
    else:
        print("  No NWM data available in database")
        merged["nwm_cfs"] = np.nan
    
    # Step 4: Calculate metrics
    print(f"\nStep 4: Calculating validation metrics...")
    
    # Model vs USGS (sites with both)
    model_vs_usgs = merged[
        merged["model_cfs"].notna() & 
        merged["usgs_cfs"].notna()
    ]
    
    # Model vs NWM (sites with both)
    model_vs_nwm = merged[
        merged["model_cfs"].notna() & 
        merged["nwm_cfs"].notna()
    ]
    
    # USGS vs NWM (sites with both) - baseline
    usgs_vs_nwm = merged[
        merged["usgs_cfs"].notna() & 
        merged["nwm_cfs"].notna()
    ]
    
    results = {
        "date": target_date,
        "sample_counts": {
            "total_sites": len(usgs_sites),
            "sites_with_comid": len(sites_with_comid),
            "model_vs_usgs": len(model_vs_usgs),
            "model_vs_nwm": len(model_vs_nwm),
            "usgs_vs_nwm": len(usgs_vs_nwm),
        },
        "metrics": {}
    }
    
    # Calculate metrics for each comparison
    if len(model_vs_usgs) >= 10:
        results["metrics"]["model_vs_usgs"] = calculate_metrics(
            model_vs_usgs["usgs_cfs"].values,
            model_vs_usgs["model_cfs"].values
        )
    
    if len(model_vs_nwm) >= 10:
        results["metrics"]["model_vs_nwm"] = calculate_metrics(
            model_vs_nwm["nwm_cfs"].values,
            model_vs_nwm["model_cfs"].values
        )
    
    if len(usgs_vs_nwm) >= 10:
        results["metrics"]["usgs_vs_nwm"] = calculate_metrics(
            usgs_vs_nwm["usgs_cfs"].values,
            usgs_vs_nwm["nwm_cfs"].values
        )
    
    # Print results
    print(f"\n{'='*70}")
    print("VALIDATION RESULTS")
    print(f"{'='*70}")
    
    print(f"\nSample Sizes:")
    for key, val in results["sample_counts"].items():
        print(f"  {key}: {val}")
    
    for comparison, metrics in results["metrics"].items():
        print(f"\n{comparison.upper().replace('_', ' ')}:")
        print(f"  n = {metrics['n']}")
        print(f"  Correlation (r): {metrics['r']}")
        print(f"  R²: {metrics['r2']}")
        print(f"  NSE: {metrics['nse']}")
        print(f"  KGE: {metrics['kge']}")
        print(f"  RMSE: {metrics['rmse_cfs']} CFS")
        print(f"  Percent Bias: {metrics['pbias_pct']}%")
    
    print(f"\n{'='*70}\n")
    
    # Save results
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    merged.to_csv(output_path / f"three_way_comparison_{target_date}.csv", index=False)
    
    with open(output_path / f"three_way_metrics_{target_date}.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"Results saved to {output_path}/")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Three-way validation")
    parser.add_argument("--date", default="2024-07-15")
    parser.add_argument("--parquet", default="data/model_predictions.parquet")
    parser.add_argument("--geojson", default="data/pour_points.geojson")
    parser.add_argument("--output", default="results")
    parser.add_argument("--sample", type=int, default=None, help="Sample size for faster testing")
    
    args = parser.parse_args()
    
    run_three_way_validation(
        args.parquet,
        args.geojson,
        args.date,
        args.output,
        args.sample
    )


if __name__ == "__main__":
    main()
