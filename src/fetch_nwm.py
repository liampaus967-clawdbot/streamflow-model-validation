"""
Fetch National Water Model (NWM) data from NOAA services.

NWM data sources:
1. NOAA NOMADS (real-time forecasts)
2. AWS Open Data (archive)
3. Google Cloud (archive)
4. OWP API (processed data)
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import json

# OWP NWM Retrospective API (processed data)
OWP_API_BASE = "https://nwm-api.us-east-1.prod.wfp.external.bsp.usgs.gov"

# NWPS Flood API (includes NWM data at gauges)
NWPS_API_BASE = "https://api.water.noaa.gov/nwps/v1"


def fetch_nwm_at_usgs_gauges(
    nws_ids: List[str],
    date: str,
) -> pd.DataFrame:
    """
    Fetch NWM analysis/forecast data at USGS gauge locations via NWPS API.
    
    Note: NWPS uses NWS Location IDs (5-char), not USGS site IDs.
    You may need to map USGS IDs to NWS IDs first.
    
    Args:
        nws_ids: List of NWS location IDs
        date: Target date (YYYY-MM-DD)
        
    Returns:
        DataFrame with NWM predictions
    """
    all_data = []
    
    for nws_id in nws_ids:
        try:
            url = f"{NWPS_API_BASE}/gauges/{nws_id}"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                # Extract streamflow data if available
                if "streamflow" in data:
                    all_data.append({
                        "nws_id": nws_id,
                        "date": date,
                        "nwm_discharge_cfs": data.get("streamflow", {}).get("value"),
                    })
        except Exception as e:
            print(f"Warning: Failed to fetch {nws_id}: {e}")
    
    return pd.DataFrame(all_data)


def fetch_nwm_by_comid(
    comids: List[int],
    date: str,
    product: str = "analysis_assim"
) -> pd.DataFrame:
    """
    Fetch NWM data by NHD COMID.
    
    Uses the HydroShare/CUAHSI NWM data service or similar.
    
    Args:
        comids: List of NHD COMIDs (reach identifiers)
        date: Target date
        product: NWM product type (analysis_assim, short_range, etc.)
    
    Returns:
        DataFrame with NWM predictions
    """
    # This would require setting up access to NWM archive data
    # Options:
    # 1. AWS S3: s3://noaa-nwm-pds/
    # 2. Google Cloud: gs://national-water-model/
    # 3. THREDDS: https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/
    
    # For now, return empty - you'd need to implement based on your data access
    print(f"NWM COMID fetch not yet implemented. Would fetch {len(comids)} reaches for {date}")
    return pd.DataFrame(columns=["comid", "date", "nwm_discharge_cfs"])


def fetch_nwm_from_hydroshare(
    comids: List[int],
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    Fetch NWM retrospective data from HydroShare.
    
    HydroShare hosts NWM v2.1 retrospective data (1979-2020).
    For 2024 data, you'd need the operational archive.
    """
    # HydroShare NWM API endpoint
    url = "https://hs-apps.hydroshare.org/apps/nwm-data-explorer/api/GetWaterML/"
    
    all_data = []
    
    for comid in comids[:10]:  # Limit for testing
        params = {
            "config": "analysis_assim",
            "comid": comid,
            "startDate": start_date,
            "endDate": end_date,
            "variable": "streamflow",
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code == 200:
                # Parse WaterML response
                # This is simplified - actual parsing would be more complex
                all_data.append({
                    "comid": comid,
                    "status": "fetched"
                })
        except Exception as e:
            print(f"Warning: Failed to fetch COMID {comid}: {e}")
    
    return pd.DataFrame(all_data)


def map_usgs_to_nwm_comid(usgs_site_ids: List[str]) -> Dict[str, int]:
    """
    Map USGS site IDs to NHD COMIDs using the NLDI service.
    
    The NLDI (Network Linked Data Index) provides this mapping.
    """
    mapping = {}
    
    nldi_base = "https://labs.waterdata.usgs.gov/api/nldi/linked-data/nwissite"
    
    for site_id in usgs_site_ids:
        try:
            url = f"{nldi_base}/USGS-{site_id}"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if "features" in data and len(data["features"]) > 0:
                    props = data["features"][0].get("properties", {})
                    comid = props.get("comid")
                    if comid:
                        mapping[site_id] = int(comid)
        except Exception as e:
            continue
    
    return mapping


if __name__ == "__main__":
    # Test USGS to COMID mapping
    test_sites = ["02146409", "11152650", "01646500"]
    
    print("Mapping USGS sites to NHD COMIDs...")
    mapping = map_usgs_to_nwm_comid(test_sites)
    for site, comid in mapping.items():
        print(f"  {site} -> COMID {comid}")
