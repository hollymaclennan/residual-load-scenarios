"""
Debug script to investigate the 4 issues:
1. Consumption forecast timestamp alignment
2. Renewables data availability
3. Date range limitations
4. Data point counts per scenario
"""
import logging
from datetime import datetime, timezone
import pandas as pd
from engine import ResidualLoadEngine
from metdesk_db import MetDeskDBClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use UTC-aware datetime for comparisons
now_utc = datetime.now(timezone.utc)

# Initialize engine
engine = ResidualLoadEngine()
client = MetDeskDBClient()

print("\n" + "="*80)
print("DEBUG: Issue Investigation")
print("="*80)

# ============================================================================
# 1. Investigate Consumption Timestamps
# ============================================================================
print("\n[1] CONSUMPTION DATA ALIGNMENT")
print("-" * 80)
consumption = client.get_eq_consumption()
if not consumption.empty:
    print(f"✓ Consumption data: {len(consumption)} records")
    print(f"  Date range: {consumption['utc_datetime'].min()} to {consumption['utc_datetime'].max()}")
    print(f"  Current time: {now_utc}")
    print(f"  Days ahead: {(consumption['utc_datetime'].max() - now_utc).days} days")
    print(f"\n  First 5 records:")
    print(consumption[['utc_datetime', 'consumption_mw']].head())
    print(f"\n  Last 5 records:")
    print(consumption[['utc_datetime', 'consumption_mw']].tail())
else:
    print("✗ No consumption data")

# ============================================================================
# 2. Investigate Renewable Ensemble Data
# ============================================================================
print("\n[2] RENEWABLE ENSEMBLE DATA")
print("-" * 80)
for model in ['eceps', 'ec46', 'gfsens', 'ecaifsens']:
    print(f"\n  {model.upper()}:")
    renewables = client.get_renewable_ensembles(model)
    
    if not renewables.empty:
        print(f"    ✓ Data found: {renewables.shape[0]} rows x {renewables.shape[1]} cols")
        print(f"    Date range: {renewables['utc_datetime'].min()} to {renewables['utc_datetime'].max()}")
        print(f"    Days ahead: {(renewables['utc_datetime'].max() - now_utc).days} days")
        
        # Identify column types
        wind_cols = [c for c in renewables.columns if 'wind_' in c]
        solar_cols = [c for c in renewables.columns if 'solar_' in c]
        total_cols = [c for c in renewables.columns if 'total_ren_' in c]
        
        print(f"    Wind columns: {len(wind_cols)}")
        print(f"    Solar columns: {len(solar_cols)}")
        print(f"    Total renewables columns: {len(total_cols)}")
        
        if total_cols:
            print(f"    Example total columns: {total_cols[:3]}")
    else:
        print(f"    ✗ No data available")

# ============================================================================
# 3. Full Pipeline - Check Merge & Residual
# ============================================================================
print("\n[3] FULL PIPELINE - RESIDUAL LOAD COMPUTATION")
print("-" * 80)
print("Running engine.update() for ECEPS...")
scenarios = engine.update(model="eceps")

if scenarios:
    for key, data in scenarios.items():
        if isinstance(data, pd.DataFrame):
            print(f"\n  {key}:")
            if not data.empty:
                print(f"    Shape: {data.shape}")
                print(f"    Date range: {data['utc_datetime'].min()} to {data['utc_datetime'].max()}")
                
                if key == 'residual_scenarios':
                    residual_cols = [c for c in data.columns if 'residual_' in c or c in ['ens_mean', 'ens_std', 'ens_P10', 'ens_P25', 'ens_P50', 'ens_P75', 'ens_P90']]
                    print(f"    Residual columns: {residual_cols[:5]} ... ({len(residual_cols)} total)")
                    print(f"    Sample values:")
                    print(data[['utc_datetime', 'ens_mean', 'ens_P10', 'ens_P90']].head())
            else:
                print("    Empty!")
        else:
            print(f"\n  {key}: {type(data)}")

# ============================================================================
# 4. Date Range Analysis
# ============================================================================
print("\n[4] DATE RANGE ANALYSIS")
print("-" * 80)
print(f"Current UTC time: {now_utc}")
print(f"Note: Weather forecasts typically run 3x/day (00:00, 12:00 UTC)")
print(f"Expected forecast horizon:")
print(f"  - ECEPS (eceps): 15 days")
print(f"  - Extended (ec46): 46 days")
print(f"  - GFS (gfsens): 15 days")
print(f"  - AIFS (ecaifsens): 15 days")

print("\nActual horizons from database:")
for model in ['eceps', 'ec46', 'gfsens', 'ecaifsens']:
    ren = client.get_renewable_ensembles(model)
    if not ren.empty:
        days_ahead = (ren['utc_datetime'].max() - now_utc).days
        print(f"  {model:12s}: {days_ahead:2d} days (until {ren['utc_datetime'].max().strftime('%Y-%m-%d %H:%M')})")

print("\n" + "="*80)
