from metdesk_db import MetDeskDBClient
import pandas as pd

client = MetDeskDBClient()

print("=" * 80)
print("Testing EQ Consumption Fetch")
print("=" * 80)
consumption = client.get_eq_consumption()
print(f"Consumption shape: {consumption.shape}")
print(f"Consumption columns: {consumption.columns.tolist()}")
if not consumption.empty:
    print(f"Date range: {consumption['utc_datetime'].min()} to {consumption['utc_datetime'].max()}")
    print(f"Sample data:\n{consumption.head()}")

print("\n" + "=" * 80)
print("Testing Renewable Ensemble Fetch (ECEPS)")
print("=" * 80)
ren_ens = client.get_renewable_ensembles("eceps", None)
print(f"Renewable ensemble shape: {ren_ens.shape}")
print(f"Renewable columns (first 10): {ren_ens.columns.tolist()[:10]}")
if not ren_ens.empty:
    print(f"Date range: {ren_ens['utc_datetime'].min()} to {ren_ens['utc_datetime'].max()}")
    print(f"Sample data:\n{ren_ens.iloc[:, :6].head()}")

print("\n" + "=" * 80)
print("Testing Merge")
print("=" * 80)
if not consumption.empty and not ren_ens.empty:
    merged = consumption.merge(ren_ens, on="utc_datetime", how="inner")
    print(f"Merged shape: {merged.shape}")
    if merged.empty:
        print("ERROR: No overlapping data!")
        print(f"Consumption datetime range: {consumption['utc_datetime'].min()} to {consumption['utc_datetime'].max()}")
        print(f"Renewables datetime range: {ren_ens['utc_datetime'].min()} to {ren_ens['utc_datetime'].max()}")
    else:
        print(f"Merge successful, {len(merged)} rows")
