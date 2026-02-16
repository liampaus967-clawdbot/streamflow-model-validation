#!/usr/bin/env python3
"""Build UUID -> COMID crosswalk by spatial join in batches."""

import json
import psycopg2
from tqdm import tqdm

DB_CONFIG = {
    'host': 'river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com',
    'database': 'river_router',
    'user': 'river_router',
    'password': 'Pacific1ride'
}

def main():
    # Load pour points
    with open('data/pour_points.geojson') as f:
        data = json.load(f)
    
    features = data['features']
    print(f"Total sites: {len(features)}")
    
    # Connect to DB
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Process in batches
    crosswalk = {}
    failed = []
    
    for f in tqdm(features, desc="Matching sites to COMIDs"):
        site_id = f['properties']['site_id']
        lon, lat = f['geometry']['coordinates']
        
        # Use simple bounding box filter first (faster), then distance
        cur.execute("""
            SELECT comid, gnis_name,
                   ST_Distance(
                       geom::geography, 
                       ST_SetSRID(ST_Point(%s, %s), 4326)::geography
                   ) as dist_m
            FROM river_edges
            WHERE geom && ST_Expand(ST_SetSRID(ST_Point(%s, %s), 4326), 0.01)
            ORDER BY geom <-> ST_SetSRID(ST_Point(%s, %s), 4326)
            LIMIT 1
        """, (lon, lat, lon, lat, lon, lat))
        
        row = cur.fetchone()
        if row and row[2] < 500:  # Within 500m
            crosswalk[site_id] = {
                'comid': row[0],
                'river_name': row[1],
                'dist_m': round(row[2], 1)
            }
        else:
            failed.append(site_id)
    
    cur.close()
    conn.close()
    
    # Save crosswalk
    with open('data/uuid_comid_crosswalk.json', 'w') as f:
        json.dump(crosswalk, f, indent=2)
    
    print(f"\nMatched: {len(crosswalk)} / {len(features)}")
    print(f"Failed (>500m from stream): {len(failed)}")
    print(f"Saved to: data/uuid_comid_crosswalk.json")

if __name__ == '__main__':
    main()
