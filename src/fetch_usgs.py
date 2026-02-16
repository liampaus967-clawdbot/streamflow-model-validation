"""
Fetch streamflow data from USGS Water Services API
https://waterservices.usgs.gov/
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
import time

USGS_BASE_URL = "https://waterservices.usgs.gov/nwis/dv/"

def fetch_usgs_daily(
    site_ids: List[str],
    start_date: str,
    end_date: str,
    parameter_code: str = "00060",  # Discharge in CFS
    chunk_size: int = 100,
    delay: float = 0.5
) -> pd.DataFrame:
    """
    Fetch daily streamflow values from USGS for multiple sites.
    
    Args:
        site_ids: List of USGS site IDs (8 digits)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        parameter_code: USGS parameter code (00060 = discharge CFS)
        chunk_size: Number of sites per request
        delay: Delay between requests (seconds)
    
    Returns:
        DataFrame with columns: site_id, date, discharge_cfs
    """
    all_data = []
    
    # Process in chunks to avoid URL length limits
    for i in range(0, len(site_ids), chunk_size):
        chunk = site_ids[i:i + chunk_size]
        sites_str = ",".join(chunk)
        
        params = {
            "format": "json",
            "sites": sites_str,
            "startDT": start_date,
            "endDT": end_date,
            "parameterCd": parameter_code,
            "siteStatus": "all",
        }
        
        try:
            response = requests.get(USGS_BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # Parse the nested JSON response
            if "value" in data and "timeSeries" in data["value"]:
                for ts in data["value"]["timeSeries"]:
                    site_id = ts["sourceInfo"]["siteCode"][0]["value"]
                    
                    if "values" in ts and len(ts["values"]) > 0:
                        for value_set in ts["values"]:
                            for val in value_set.get("value", []):
                                if val.get("value") is not None:
                                    try:
                                        all_data.append({
                                            "site_id": site_id,
                                            "date": val["dateTime"][:10],
                                            "discharge_cfs": float(val["value"]),
                                            "qualifier": val.get("qualifiers", [""])[0] if val.get("qualifiers") else ""
                                        })
                                    except (ValueError, TypeError):
                                        continue
        
        except requests.exceptions.RequestException as e:
            print(f"Warning: Failed to fetch chunk {i//chunk_size + 1}: {e}")
            continue
        
        if delay > 0 and i + chunk_size < len(site_ids):
            time.sleep(delay)
    
    if not all_data:
        return pd.DataFrame(columns=["site_id", "date", "discharge_cfs", "qualifier"])
    
    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    
    return df


def fetch_usgs_single_day(
    site_ids: List[str],
    date: str
) -> pd.DataFrame:
    """
    Convenience function to fetch data for a single day.
    """
    return fetch_usgs_daily(site_ids, date, date)


def get_site_info(site_ids: List[str]) -> pd.DataFrame:
    """
    Fetch site metadata from USGS.
    """
    url = "https://waterservices.usgs.gov/nwis/site/"
    
    all_info = []
    chunk_size = 100
    
    for i in range(0, len(site_ids), chunk_size):
        chunk = site_ids[i:i + chunk_size]
        
        params = {
            "format": "rdb",
            "sites": ",".join(chunk),
            "siteOutput": "expanded",
            "siteStatus": "all",
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse RDB format (tab-separated with comment lines)
            lines = response.text.strip().split("\n")
            data_lines = [l for l in lines if not l.startswith("#")]
            
            if len(data_lines) >= 2:
                headers = data_lines[0].split("\t")
                for line in data_lines[2:]:  # Skip header and format lines
                    values = line.split("\t")
                    if len(values) >= len(headers):
                        all_info.append(dict(zip(headers, values)))
        
        except requests.exceptions.RequestException as e:
            print(f"Warning: Failed to fetch site info: {e}")
    
    return pd.DataFrame(all_info)


if __name__ == "__main__":
    # Test with a few sites
    test_sites = ["02146409", "11152650", "01646500"]
    
    print("Fetching data for July 15, 2024...")
    df = fetch_usgs_single_day(test_sites, "2024-07-15")
    print(df)
