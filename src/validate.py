"""
Main validation script comparing model predictions against USGS gauges and NWM.
"""
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import json
from pathlib import Path
from typing import Tuple, Dict, Optional
from datetime import datetime
import argparse

from fetch_usgs import fetch_usgs_single_day, fetch_usgs_daily
from fetch_nwm import map_usgs_to_nwm_comid


def load_model_predictions(
    parquet_path: str,
    target_date: str,
    uuid_filter: Optional[list] = None
) -> pd.DataFrame:
    """
    Load model predictions for a specific date.
    
    Args:
        parquet_path: Path to parquet file
        target_date: Date to extract (YYYY-MM-DD)
        uuid_filter: Optional list of UUIDs to filter
    
    Returns:
        DataFrame with model predictions
    """
    # Use pyarrow for efficient filtering on large parquet
    pf = pq.ParquetFile(parquet_path)
    
    # Read all row groups but filter to target date
    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    
    all_chunks = []
    for i in range(pf.metadata.num_row_groups):
        table = pf.read_row_group(i)
        df = table.to_pandas()
        
        # Filter to target date
        df = df[df["time"] == target_dt]
        
        # Filter to specific UUIDs if provided
        if uuid_filter is not None:
            df = df[df["UUID"].isin(uuid_filter)]
        
        if len(df) > 0:
            all_chunks.append(df)
    
    if not all_chunks:
        return pd.DataFrame()
    
    return pd.concat(all_chunks, ignore_index=True)


def load_pour_points(geojson_path: str) -> pd.DataFrame:
    """
    Load pour points with site metadata.
    """
    with open(geojson_path) as f:
        data = json.load(f)
    
    records = []
    for feature in data["features"]:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        records.append({
            "UUID": props.get("UUID"),
            "site_id": props.get("site_id"),
            "comid": props.get("comid"),
            "lng": coords[0],
            "lat": coords[1],
            # Pre-computed metrics from model
            "model_flow_july15": props.get("flow"),
            "model_percentile": props.get("percentile"),
            "model_category": props.get("category"),
            "model_trend": props.get("trend"),
            "model_pct_normal": props.get("pct_of_normal"),
        })
    
    return pd.DataFrame(records)


def calculate_metrics(
    observed: np.ndarray,
    predicted: np.ndarray
) -> Dict[str, float]:
    """
    Calculate validation metrics.
    
    Args:
        observed: Observed values (e.g., USGS)
        predicted: Predicted values (e.g., model)
    
    Returns:
        Dictionary of metrics
    """
    # Remove any NaN pairs
    mask = ~(np.isnan(observed) | np.isnan(predicted))
    obs = observed[mask]
    pred = predicted[mask]
    
    if len(obs) < 2:
        return {"n": len(obs), "error": "Insufficient data"}
    
    # Basic stats
    n = len(obs)
    
    # Correlation
    r = np.corrcoef(obs, pred)[0, 1] if n > 1 else np.nan
    r2 = r ** 2 if not np.isnan(r) else np.nan
    
    # Error metrics
    errors = pred - obs
    mae = np.mean(np.abs(errors))
    rmse = np.sqrt(np.mean(errors ** 2))
    
    # Percent bias
    pbias = 100 * np.sum(errors) / np.sum(obs) if np.sum(obs) != 0 else np.nan
    
    # Nash-Sutcliffe Efficiency
    ss_res = np.sum(errors ** 2)
    ss_tot = np.sum((obs - np.mean(obs)) ** 2)
    nse = 1 - (ss_res / ss_tot) if ss_tot != 0 else np.nan
    
    # Kling-Gupta Efficiency
    r_kge = r if not np.isnan(r) else 0
    alpha = np.std(pred) / np.std(obs) if np.std(obs) != 0 else np.nan
    beta = np.mean(pred) / np.mean(obs) if np.mean(obs) != 0 else np.nan
    
    if not np.isnan(alpha) and not np.isnan(beta):
        kge = 1 - np.sqrt((r_kge - 1)**2 + (alpha - 1)**2 + (beta - 1)**2)
    else:
        kge = np.nan
    
    # Log-space metrics (useful for streamflow)
    log_obs = np.log10(obs[obs > 0])
    log_pred = np.log10(pred[pred > 0])
    if len(log_obs) > 1 and len(log_pred) > 1 and len(log_obs) == len(log_pred):
        log_rmse = np.sqrt(np.mean((log_pred - log_obs) ** 2))
    else:
        log_rmse = np.nan
    
    return {
        "n": n,
        "r": round(r, 4) if not np.isnan(r) else None,
        "r2": round(r2, 4) if not np.isnan(r2) else None,
        "mae_cfs": round(mae, 2),
        "rmse_cfs": round(rmse, 2),
        "log_rmse": round(log_rmse, 4) if not np.isnan(log_rmse) else None,
        "pbias_pct": round(pbias, 2) if not np.isnan(pbias) else None,
        "nse": round(nse, 4) if not np.isnan(nse) else None,
        "kge": round(kge, 4) if not np.isnan(kge) else None,
    }


def run_validation(
    parquet_path: str,
    geojson_path: str,
    target_date: str = "2024-07-15",
    output_dir: str = "results"
) -> Tuple[pd.DataFrame, Dict]:
    """
    Run full validation for a target date.
    """
    print(f"\n{'='*60}")
    print(f"STREAMFLOW MODEL VALIDATION - {target_date}")
    print(f"{'='*60}\n")
    
    # Load pour points (has USGS site IDs and pre-computed model results)
    print("Loading pour points metadata...")
    pour_points = load_pour_points(geojson_path)
    print(f"  Found {len(pour_points)} sites")
    
    # Filter to sites with USGS IDs
    usgs_sites = pour_points[pour_points["site_id"].notna()].copy()
    usgs_sites["site_id"] = usgs_sites["site_id"].astype(str).str.zfill(8)
    print(f"  {len(usgs_sites)} sites have USGS IDs")
    
    # Fetch USGS data for target date
    print(f"\nFetching USGS gauge data for {target_date}...")
    site_list = usgs_sites["site_id"].tolist()
    usgs_data = fetch_usgs_daily(
        site_list, 
        target_date, 
        target_date,
        chunk_size=50,
        delay=0.3
    )
    print(f"  Retrieved data for {usgs_data['site_id'].nunique()} sites")
    
    # Merge USGS data with pour points
    merged = usgs_sites.merge(
        usgs_data[["site_id", "discharge_cfs"]],
        on="site_id",
        how="left"
    )
    merged = merged.rename(columns={"discharge_cfs": "usgs_cfs"})
    
    # The pour points already have model flow for July 15
    # Rename for clarity
    merged = merged.rename(columns={"model_flow_july15": "model_cfs"})
    
    # Filter to sites with both model and USGS data
    valid = merged[
        merged["usgs_cfs"].notna() & 
        merged["model_cfs"].notna()
    ].copy()
    
    print(f"\nSites with both model and USGS data: {len(valid)}")
    
    if len(valid) == 0:
        print("ERROR: No matching data found!")
        return merged, {}
    
    # Calculate metrics
    print("\nCalculating validation metrics...")
    metrics = calculate_metrics(
        valid["usgs_cfs"].values,
        valid["model_cfs"].values
    )
    
    # Print results
    print(f"\n{'='*60}")
    print("VALIDATION RESULTS: Model vs USGS Gauges")
    print(f"{'='*60}")
    print(f"  Sample size (n):      {metrics['n']}")
    print(f"  Correlation (r):      {metrics['r']}")
    print(f"  R-squared:            {metrics['r2']}")
    print(f"  Nash-Sutcliffe (NSE): {metrics['nse']}")
    print(f"  Kling-Gupta (KGE):    {metrics['kge']}")
    print(f"  RMSE (CFS):           {metrics['rmse_cfs']}")
    print(f"  Log RMSE:             {metrics['log_rmse']}")
    print(f"  Percent Bias:         {metrics['pbias_pct']}%")
    print(f"{'='*60}\n")
    
    # Save results
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Save comparison data
    valid.to_csv(output_path / f"comparison_{target_date}.csv", index=False)
    
    # Save metrics
    with open(output_path / f"metrics_{target_date}.json", "w") as f:
        json.dump(metrics, f, indent=2)
    
    print(f"Results saved to {output_path}/")
    
    return valid, metrics


def main():
    parser = argparse.ArgumentParser(description="Validate streamflow model")
    parser.add_argument("--date", default="2024-07-15", help="Target date (YYYY-MM-DD)")
    parser.add_argument("--parquet", default="data/model_predictions.parquet")
    parser.add_argument("--geojson", default="data/pour_points.geojson")
    parser.add_argument("--output", default="results")
    
    args = parser.parse_args()
    
    run_validation(
        args.parquet,
        args.geojson,
        args.date,
        args.output
    )


if __name__ == "__main__":
    main()
