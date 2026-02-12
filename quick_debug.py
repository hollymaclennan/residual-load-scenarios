"""
Quick debug to check the main issues
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import text
from config import DB_CONNECTION_STRING, METDESK_TABLE, COUNTRY
from sqlalchemy import create_engine
import pandas as pd

logging.basicConfig(level=logging.WARNING)

now_utc = datetime.now(timezone.utc)
engine = create_engine(
    DB_CONNECTION_STRING,
    pool_size=3,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=3600,
)

print("\n" + "="*80)
print("QUICK ISSUE CHECK")
print("="*80)

# Issue 1: Consumption timestamp alignment
print("\n[1] EQ CONSUMPTION DATE RANGE")
query = text("""
    SELECT MIN(utc_datetime) as min_date, MAX(utc_datetime) as max_date, COUNT(*) as record_count
    FROM silver.eq_consumption
    WHERE LOWER(country) = 'fr'
""")
with engine.connect() as conn:
    result = conn.execute(query).fetchone()
    min_d, max_d, cnt = result
    print(f"  Records: {cnt}")
    print(f"  Range: {min_d} to {max_d}")
    # Convert to timezone-aware for comparison if needed
    max_d_aware = max_d.replace(tzinfo=timezone.utc) if max_d and max_d.tzinfo is None else max_d
    days_ahead = (max_d_aware - now_utc).days if max_d_aware else 0
    print(f"  Days ahead: {days_ahead} days")

# Issue 2 & 3: Renewable data availability per model
print("\n[2] & [3] RENEWABLE DATA BY MODEL")
for model in ['eceps', 'ec46', 'gfsens', 'ecaifsens']:
    query = text(f"""
        SELECT COUNT(*) as wind_count, COUNT(DISTINCT utc_datetime) as unique_hours,
               MIN(utc_datetime) as min_date, MAX(utc_datetime) as max_date,
               COUNT(DISTINCT issue) as num_issues
        FROM {METDESK_TABLE}
        WHERE location = :location
          AND model = :model
          AND element = 'wind'
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"location": COUNTRY, "model": model}).fetchone()
            if result and result[0] > 0:
                wind_cnt, hrs, min_d, max_d, issues = result
                max_d_aware = max_d.replace(tzinfo=timezone.utc) if max_d and max_d.tzinfo is None else max_d
                days_ahead = (max_d_aware - now_utc).days if max_d_aware else 0
                print(f"  {model:12s}: {hrs:5d} hours, {days_ahead:2d} days ahead, {issues} issues")
            else:
                print(f"  {model:12s}: No data")
    except Exception as e:
        print(f"  {model:12s}: Error - {e}")

# Issue 4: Check which percentiles are currently computed
print("\n[4] CURRENT RESIDUAL PERCENTILES")
print("  Currently computing: P10, P25, P50, P75, P90")
print("  Need to add: P0-P100 (suggest P0, P5, P10, ... P95, P100)")

engine.dispose()
print("\n" + "="*80)
