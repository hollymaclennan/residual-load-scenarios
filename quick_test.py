from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING, COUNTRY
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(DB_CONNECTION_STRING)

# Test 1: Quick check of MetDesk structure
print("=" * 80)
print("TEST 1: MetDesk structure check")
print("=" * 80)

with engine.connect() as conn:
    # Check if table exists and has data
    result = conn.execute(text("""
        SELECT COUNT(*) FROM silver.metdesk_forecasts LIMIT 1
    """))
    total_count = result.scalar()
    print(f"Total metdesk_forecasts rows: {total_count}")
    
    # Get unique locations (quick check)
    result = conn.execute(text("""
        SELECT DISTINCT location FROM silver.metdesk_forecasts LIMIT 20
    """))
    locations = [row[0] for row in result]
    print(f"Available locations: {locations}")
    
    # If FR doesn't exist, find what does
    result = conn.execute(text("""
        SELECT location, COUNT(*) as cnt FROM silver.metdesk_forecasts 
        GROUP BY location LIMIT 10
    """))
    print("\nCounts by location:")
    for row in result:
        print(f"  {row[0]}: {row[1]:,} records")

# Test 2: EQ Consumption 
print("\n" + "=" * 80)
print("TEST 2: EQ Consumption check")
print("=" * 80)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*) FROM silver.eq_consumption WHERE country='FR'
    """))
    count = result.scalar()
    print(f"EQ Consumption rows for FR: {count}")
    
    if count > 0:
        result = conn.execute(text("""
            SELECT utc_datetime, consumption_fcst_latest 
            FROM silver.eq_consumption 
            WHERE country='FR' AND consumption_fcst_latest IS NOT NULL
            ORDER BY utc_datetime DESC 
            LIMIT 5
        """))
        rows = result.fetchall()
        print(f"Sample consumption data (latest non-null):")
        for row in rows:
            print(f"  {row[0]}: {row[1]}")
