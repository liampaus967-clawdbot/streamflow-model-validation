# Streamflow Model Validation

Validation framework comparing the **HPP neural network model** against:
1. **USGS gauge data** (in-situ measurements) 
2. **NOAA National Water Model (NWM)** predictions

## Quick Start

```bash
# Setup
cd streamflow-model-validation
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install python-docx tqdm

# Download the HPP model predictions (1.3GB)
mkdir -p data
curl -o data/model_predictions.parquet \
  "https://storage.googleapis.com/onwater-test-bucket/derived-data/full_dataset_basins_filtered.parquet"

# Run the state-by-state validation (TX, CA, NC)
python3 src/state_validation.py

# Generate the DOCX report
python3 src/generate_report.py
```

## Data Sources

### Included in Repo
- `data/pour_points.geojson` — Site metadata with UUID → USGS site_id mapping
- `data/uuid_comid_crosswalk.json` — USGS gauge → NHD+ COMID spatial matches (1,348 sites)

### Download Required
- **HPP Model Predictions** (1.3GB parquet):
  ```
  https://storage.googleapis.com/onwater-test-bucket/derived-data/full_dataset_basins_filtered.parquet
  ```
  Save to: `data/model_predictions.parquet`

### Database Connection
The NWM comparison requires access to a PostgreSQL database with:
- `usgs_gauges` table with gauge locations
- `nwm_velocity` table with NWM streamflow by COMID
- `river_edges` table with NHD+ geometry

Update the `DB_CONFIG` in `src/state_validation.py` with your connection details.

## Test Date

Primary validation date: **July 15, 2024** (operational test case)

## Project Structure

```
streamflow-model-validation/
├── data/
│   ├── model_predictions.parquet  # HPP predictions (download required)
│   ├── pour_points.geojson        # Site metadata with USGS IDs
│   └── uuid_comid_crosswalk.json  # USGS → COMID mapping
├── src/
│   ├── build_crosswalk.py         # Spatial join: USGS gauge → COMID
│   ├── state_validation.py        # Main validation script
│   ├── three_way_validation.py    # 3-way comparison logic
│   └── generate_report.py         # DOCX report generator
├── results/
│   ├── state_comparison.csv       # Full comparison dataset
│   ├── state_metrics.csv          # Summary metrics by state
│   └── HPP_NWM_Validation_Report.docx
└── requirements.txt
```

## Validation Metrics

- **NSE** — Nash-Sutcliffe Efficiency (1 = perfect, <0 = worse than mean)
- **R²** — Coefficient of determination (correlation strength)
- **PBIAS** — Percent bias (negative = underestimate, positive = overestimate)
- **Log-NSE** — NSE on log-transformed values (better for low flows)
- **RMSE** — Root mean square error (CFS)

## Results Summary

| State | HPP NSE | NWM NSE | Better Model |
|-------|---------|---------|--------------|
| Texas | 0.255 | 0.113 | **HPP** |
| California | 0.124 | 0.469 | **NWM** |
| North Carolina | 0.617 | 0.524 | **HPP** |

See `results/HPP_NWM_Validation_Report.docx` for full analysis.
