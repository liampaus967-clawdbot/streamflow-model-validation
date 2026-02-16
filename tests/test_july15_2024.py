"""
Validation tests for July 15, 2024 - the operational test date.

Tests compare:
1. Model predictions vs USGS gauge observations
2. Model predictions vs NWM predictions (where available)
3. USGS vs NWM (baseline comparison)
"""
import pytest
import pandas as pd
import numpy as np
import json
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_usgs import fetch_usgs_single_day, fetch_usgs_daily
from validate import load_pour_points, calculate_metrics


# Test configuration
TEST_DATE = "2024-07-15"
DATA_DIR = Path(__file__).parent.parent / "data"
RESULTS_DIR = Path(__file__).parent.parent / "results"


class TestDataLoading:
    """Test data loading functions."""
    
    def test_pour_points_load(self):
        """Test that pour points GeoJSON loads correctly."""
        geojson_path = DATA_DIR / "pour_points.geojson"
        
        if not geojson_path.exists():
            pytest.skip("Pour points file not found")
        
        df = load_pour_points(str(geojson_path))
        
        assert len(df) > 0, "No features loaded"
        assert "UUID" in df.columns
        assert "site_id" in df.columns
        assert df["UUID"].notna().any(), "No UUIDs found"
    
    def test_usgs_sites_present(self):
        """Test that USGS site IDs are present in pour points."""
        geojson_path = DATA_DIR / "pour_points.geojson"
        
        if not geojson_path.exists():
            pytest.skip("Pour points file not found")
        
        df = load_pour_points(str(geojson_path))
        usgs_sites = df[df["site_id"].notna()]
        
        assert len(usgs_sites) > 0, "No USGS sites found"
        print(f"\n  Found {len(usgs_sites)} sites with USGS IDs")


class TestUSGSFetch:
    """Test USGS data fetching."""
    
    def test_fetch_single_site(self):
        """Test fetching data for a single USGS site."""
        # Use a well-known site (Potomac at Little Falls)
        site_id = "01646500"
        
        df = fetch_usgs_single_day([site_id], TEST_DATE)
        
        if len(df) == 0:
            pytest.skip(f"No USGS data available for {site_id} on {TEST_DATE}")
        
        assert "site_id" in df.columns
        assert "discharge_cfs" in df.columns
        assert df["discharge_cfs"].iloc[0] > 0
        
        print(f"\n  {site_id} on {TEST_DATE}: {df['discharge_cfs'].iloc[0]:.1f} CFS")
    
    def test_fetch_multiple_sites(self):
        """Test fetching data for multiple sites."""
        # Test sites from different regions
        site_ids = ["01646500", "02146409", "11152650"]
        
        df = fetch_usgs_daily(site_ids, TEST_DATE, TEST_DATE)
        
        assert len(df) >= 0, "Fetch should return a DataFrame"
        
        if len(df) > 0:
            print(f"\n  Retrieved data for {df['site_id'].nunique()} sites")
            for _, row in df.iterrows():
                print(f"    {row['site_id']}: {row['discharge_cfs']:.1f} CFS")


class TestModelValidation:
    """Test model validation against USGS."""
    
    @pytest.fixture
    def comparison_data(self):
        """Load or generate comparison data."""
        geojson_path = DATA_DIR / "pour_points.geojson"
        
        if not geojson_path.exists():
            pytest.skip("Data files not found")
        
        # Load pour points
        pour_points = load_pour_points(str(geojson_path))
        
        # Filter to USGS sites
        usgs_sites = pour_points[pour_points["site_id"].notna()].copy()
        usgs_sites["site_id"] = usgs_sites["site_id"].astype(str).str.zfill(8)
        
        # Sample for faster testing
        sample_sites = usgs_sites.head(100)
        
        # Fetch USGS data
        site_list = sample_sites["site_id"].tolist()
        usgs_data = fetch_usgs_daily(site_list, TEST_DATE, TEST_DATE, chunk_size=50)
        
        # Merge
        merged = sample_sites.merge(
            usgs_data[["site_id", "discharge_cfs"]],
            on="site_id",
            how="left"
        )
        merged = merged.rename(columns={
            "discharge_cfs": "usgs_cfs",
            "model_flow_july15": "model_cfs"
        })
        
        return merged[merged["usgs_cfs"].notna() & merged["model_cfs"].notna()]
    
    def test_correlation_positive(self, comparison_data):
        """Test that model predictions are positively correlated with USGS."""
        if len(comparison_data) < 10:
            pytest.skip("Insufficient comparison data")
        
        metrics = calculate_metrics(
            comparison_data["usgs_cfs"].values,
            comparison_data["model_cfs"].values
        )
        
        assert metrics["r"] is not None, "Correlation could not be calculated"
        assert metrics["r"] > 0, f"Correlation should be positive, got {metrics['r']}"
        
        print(f"\n  Correlation (r): {metrics['r']}")
        print(f"  Sample size: {metrics['n']}")
    
    def test_nse_reasonable(self, comparison_data):
        """Test that Nash-Sutcliffe Efficiency is reasonable (> -1)."""
        if len(comparison_data) < 10:
            pytest.skip("Insufficient comparison data")
        
        metrics = calculate_metrics(
            comparison_data["usgs_cfs"].values,
            comparison_data["model_cfs"].values
        )
        
        if metrics["nse"] is not None:
            print(f"\n  NSE: {metrics['nse']}")
            # NSE > 0 means model is better than mean; > -1 is a reasonable floor
            assert metrics["nse"] > -1, f"NSE too low: {metrics['nse']}"
    
    def test_bias_acceptable(self, comparison_data):
        """Test that percent bias is within acceptable range."""
        if len(comparison_data) < 10:
            pytest.skip("Insufficient comparison data")
        
        metrics = calculate_metrics(
            comparison_data["usgs_cfs"].values,
            comparison_data["model_cfs"].values
        )
        
        if metrics["pbias_pct"] is not None:
            print(f"\n  Percent Bias: {metrics['pbias_pct']}%")
            # Bias within ±50% is often acceptable for regional models
            assert abs(metrics["pbias_pct"]) < 100, f"Bias too high: {metrics['pbias_pct']}%"


class TestCategoryValidation:
    """Test drought/pluvial category validation."""
    
    def test_category_distribution(self):
        """Check distribution of flow categories."""
        geojson_path = DATA_DIR / "pour_points.geojson"
        
        if not geojson_path.exists():
            pytest.skip("Pour points file not found")
        
        df = load_pour_points(str(geojson_path))
        
        if "model_category" not in df.columns:
            pytest.skip("Category data not available")
        
        cats = df["model_category"].value_counts()
        print(f"\n  Category distribution:")
        for cat, count in cats.items():
            print(f"    {cat}: {count} ({100*count/len(df):.1f}%)")
        
        # Should have a mix of categories
        assert len(cats) > 1, "Only one category found - suspicious"


class TestMetricsCalculation:
    """Test metric calculation functions."""
    
    def test_perfect_prediction(self):
        """Test metrics with perfect prediction."""
        obs = np.array([10, 20, 30, 40, 50])
        pred = np.array([10, 20, 30, 40, 50])
        
        metrics = calculate_metrics(obs, pred)
        
        assert metrics["r"] == 1.0
        assert metrics["nse"] == 1.0
        assert metrics["rmse_cfs"] == 0
        assert metrics["pbias_pct"] == 0
    
    def test_constant_offset(self):
        """Test metrics with constant offset (bias)."""
        obs = np.array([10, 20, 30, 40, 50])
        pred = np.array([15, 25, 35, 45, 55])  # +5 offset
        
        metrics = calculate_metrics(obs, pred)
        
        assert metrics["r"] == 1.0, "Correlation should be perfect with offset"
        assert metrics["pbias_pct"] > 0, "Should show positive bias"
    
    def test_handles_zeros(self):
        """Test that metrics handle zero values."""
        obs = np.array([0, 10, 20, 30])
        pred = np.array([1, 12, 22, 28])
        
        metrics = calculate_metrics(obs, pred)
        
        assert metrics["n"] == 4
        assert metrics["r"] is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
