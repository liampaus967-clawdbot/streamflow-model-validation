# Streamflow Model Validation

Validation framework comparing a neural network streamflow prediction model against:
1. **USGS gauge data** (in-situ measurements) 
2. **NOAA National Water Model (NWM)** predictions

## Data Sources

- **Model Predictions**: Neural network ensemble predictions (1991-2024) for ~4,054 watersheds
  - `ft3_s_q50`: Median prediction (CFS)
  - `ft3_s_q25/q75`: 25th/75th percentile bounds
  
- **USGS Gauges**: Real-time and historical streamflow via USGS Water Services API
  
- **NWM**: NOAA National Water Model hindcast/analysis data

## Test Date

Primary validation date: **July 15, 2024** (operational test case)

## Project Structure

```
streamflow-model-validation/
├── data/
│   ├── model_predictions.parquet  # NN model predictions
│   └── pour_points.geojson        # Site metadata with USGS IDs
├── src/
│   ├── fetch_usgs.py              # USGS data retrieval
│   ├── fetch_nwm.py               # NWM data retrieval  
│   ├── validate.py                # Comparison logic
│   └── utils.py                   # Shared utilities
├── tests/
│   └── test_july15_2024.py        # Main validation tests
├── results/
│   └── (generated outputs)
└── requirements.txt
```

## Quick Start

```bash
# Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run validation
python -m pytest tests/ -v

# Or run specific comparison
python src/validate.py --date 2024-07-15
```

## Metrics

- Nash-Sutcliffe Efficiency (NSE)
- Kling-Gupta Efficiency (KGE)
- Percent Bias (PBIAS)
- Root Mean Square Error (RMSE)
- Correlation coefficient (r²)
- Categorical accuracy (drought/pluvial classification)
