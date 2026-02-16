"""
Map USGS site IDs to NHD COMIDs using our local database.
Since NLDI is unavailable, we can:
1. Get USGS site coordinates from USGS API
2. Match to nearest river reach in our database
"""
import requests
import pandas as pd
import psycopg2
from typing import List, Dict, Tuple
import time

DB_CONFIG = {
    "host": "river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com",
    "database": "river_router",
    "user": "river_router",
    "password": "Pacific1ride"
}


def get_usgs_site_coords(site_ids: List[str]) -> Dict[str, Tuple[float, float]]:
    """
    Get coordinates for USGS sites from NWIS.
    """
    coords = {}
    
    # USGS site service
    base_url = "https://waterservices.usgs.gov/nwis/site/"
    
    # Process in chunks
    chunk_size = 100
    for i in range(0, len(site_ids), chunk_size):
        chunk = site_ids[i:i + chunk_size]
        
        params = {
            "format": "rdb",
            "sites": ",".join(chunk),
            "siteOutput": "basic",
            "siteStatus": "all",
        }
        
        try:
            resp = requests.get(base_url, params=params, timeout=60)
            if resp.status_code == 200:
                lines = resp.text.strip().split("\n")
                # Parse RDB format
                data_lines = [l for l in lines if not l.startswith("#")]
                if len(data_lines) >= 2:
                    headers = data_lines[0].split("\t")
                    
                    # Find column indices
                    try:
                        site_idx = headers.index("site_no")
                        lat_idx = headers.index("dec_lat_va")
                        lng_idx = headers.index("dec_long_va")
                    except ValueError:
                        continue
                    
                    for line in data_lines[2:]:  # Skip header and format lines
                        values = line.split("\t")
                        if len(values) > max(site_idx, lat_idx, lng_idx):
                            site = values[site_idx]
                            try:
                                lat = float(values[lat_idx])
                                lng = float(values[lng_idx])
                                coords[site] = (lng, lat)
                            except (ValueError, IndexError):
                                continue
        except Exception as e:
            print(f"Warning: Failed to fetch site coords: {e}")
            continue
        
        time.sleep(0.2)
    
    return coords


def match_coords_to_comid(
    coords: Dict[str, Tuple[float, float]],
    max_distance_m: float = 500
) -> Dict[str, int]:
    """
    Match coordinates to nearest COMID in our river database.
    """
    if not coords:
        return {}
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    mapping = {}
    
    for site_id, (lng, lat) in coords.items():
        try:
            # Find nearest river reach within max distance
            cur.execute("""
                SELECT 
                    comid,
                    gnis_name,
                    ST_Distance(
                        geom::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ) as dist_m
                FROM river_edges
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s
                )
                ORDER BY dist_m
                LIMIT 1
            """, (lng, lat, lng, lat, max_distance_m))
            
            row = cur.fetchone()
            if row:
                mapping[site_id] = row[0]  # COMID
        except Exception as e:
            continue
    
    conn.close()
    return mapping


def map_usgs_to_comid_local(
    site_ids: List[str],
    max_distance_m: float = 500
) -> Dict[str, int]:
    """
    Full pipeline: USGS site IDs -> coordinates -> COMIDs
    """
    print(f"Getting coordinates for {len(site_ids)} USGS sites...")
    coords = get_usgs_site_coords(site_ids)
    print(f"  Got coordinates for {len(coords)} sites")
    
    print(f"Matching to COMIDs (max distance: {max_distance_m}m)...")
    mapping = match_coords_to_comid(coords, max_distance_m)
    print(f"  Matched {len(mapping)} sites to COMIDs")
    
    return mapping


if __name__ == "__main__":
    # Test
    test_sites = ["01646500", "02146409", "08066500", "11152650"]
    
    mapping = map_usgs_to_comid_local(test_sites)
    
    for site, comid in mapping.items():
        print(f"  {site} -> COMID {comid}")
