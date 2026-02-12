from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING
import pandas as pd

engine = create_engine(DB_CONNECTION_STRING)

print("=" * 80)
print("Debugging EQ Consumption")
print("=" * 80)

with engine.connect() as conn:
    # Check raw column values
    print("\n1. Checking what columns have data:")
    result = conn.execute(text("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(consumption_act) as act_count,
            COUNT(da_consumption_fcst) as da_count,
            COUNT(consumption_fcst_latest) as latest_count
        FROM silver.eq_consumption
        WHERE country = 'FR'
    """))
    row = result.fetchone()
    print(f"   Total rows: {row[0]}")
    print(f"   consumption_act: {row[1]} non-null")
    print(f"   da_consumption_fcst: {row[2]} non-null")
    print(f"   consumption_fcst_latest: {row[3]} non-null")
    
    # Check date range
    print("\n2. Checking date ranges:")
    result = conn.execute(text("""
        SELECT MIN(utc_datetime), MAX(utc_datetime) FROM silver.eq_consumption WHERE country='FR'
    """))
    min_dt, max_dt = result.fetchone()
    print(f"   Date range: {min_dt} to {max_dt}")
    
    # Sample data
    print("\n3. Sample data (with any non-null consumption):")
    result = conn.execute(text("""
        SELECT utc_datetime, consumption_act, da_consumption_fcst, consumption_fcst_latest
        FROM silver.eq_consumption
        WHERE country='FR'
        ORDER BY utc_datetime DESC
        LIMIT 10
    """))
    for row in result:
        print(f"   {row[0]}: act={row[1]}, da={row[2]}, latest={row[3]}")
