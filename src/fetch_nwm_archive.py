"""
Fetch NWM historical data from AWS Open Data archive.
s3://noaa-nwm-pds/

NWM products:
- analysis_assim: Analysis and assimilation (best hindcast)
- short_range: 0-18h forecast
- medium_range: 0-10 day forecast
- long_range: 0-30 day ensemble
"""
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# NLDI service for USGS -> COMID mapping
NLDI_BASE = "https://labs.waterdata.usgs.gov/api/nldi/linked-data"


def map_usgs_to_comid(usgs_site_ids: List[str], max_workers: int = 10) -> Dict[str, int]:
    """
    Map USGS site IDs to NHD COMIDs using the NLDI service.
    
    Args:
        usgs_site_ids: List of USGS site IDs (8 digits)
        max_workers: Number of parallel requests
    
    Returns:
        Dictionary mapping site_id -> COMID
    """
    mapping = {}
    
    def fetch_comid(site_id: str) -> tuple:
        try:
            url = f"{NLDI_BASE}/nwissite/USGS-{site_id}"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if "features" in data and len(data["features"]) > 0:
                    props = data["features"][0].get("properties", {})
                    comid = props.get("comid")
                    if comid:
                        return (site_id, int(comid))
            return (site_id, None)
        except Exception:
            return (site_id, None)
    
    # Parallel fetching
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_comid, sid): sid for sid in usgs_site_ids}
        
        for i, future in enumerate(as_completed(futures)):
            site_id, comid = future.result()
            if comid:
                mapping[site_id] = comid
            
            if (i + 1) % 100 == 0:
                print(f"  Mapped {i + 1}/{len(usgs_site_ids)} sites...")
    
    return mapping


def fetch_nwm_at_comids_owp(
    comids: List[int],
    date: str,
    variable: str = "streamflow"
) -> pd.DataFrame:
    """
    Fetch NWM data at COMIDs using OWP (Office of Water Prediction) API.
    
    Note: This may have limited historical data availability.
    """
    # OWP API endpoints
    # https://api.water.noaa.gov/nwps/v1/
    
    all_data = []
    
    # OWP provides gauge-referenced data, need to check availability
    print(f"OWP API lookup for {len(comids)} COMIDs...")
    
    return pd.DataFrame(all_data)


def fetch_nwm_retrospective(
    comids: List[int],
    date: str
) -> pd.DataFrame:
    """
    Fetch NWM retrospective data from HydroShare or similar.
    
    The NWM v2.1 retrospective simulation covers 1979-2020.
    For 2024 data, we need analysis_assim from the operational archive.
    """
    # HydroShare NWM endpoints
    # For production use, would need to set up proper data access
    
    all_data = []
    
    # Placeholder - would implement actual data fetch
    for comid in comids:
        all_data.append({
            "comid": comid,
            "date": date,
            "nwm_streamflow_cfs": None,  # Would be fetched
            "source": "nwm_analysis_assim"
        })
    
    return pd.DataFrame(all_data)


def estimate_nwm_from_usgs(
    usgs_data: pd.DataFrame,
    mapping: Dict[str, int]
) -> pd.DataFrame:
    """
    For sites where we have both USGS and COMID mapping,
    we can use the USGS as ground truth and compare to model.
    
    NWM typically agrees well with USGS at gauged locations since
    NWM assimilates gauge data in analysis_assim products.
    """
    # Add COMID to USGS data
    usgs_data = usgs_data.copy()
    usgs_data["comid"] = usgs_data["site_id"].map(mapping)
    
    return usgs_data


if __name__ == "__main__":
    # Test USGS -> COMID mapping
    test_sites = ["01646500", "02146409", "08066500", "11152650"]
    
    print("Mapping USGS sites to NHD COMIDs...")
    mapping = map_usgs_to_comid(test_sites)
    
    for site, comid in mapping.items():
        print(f"  {site} -> COMID {comid}")
