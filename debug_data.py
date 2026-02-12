from sqlalchemy import create_engine, text, inspect
from config import DB_CONNECTION_STRING
import pandas as pd

engine = create_engine(DB_CONNECTION_STRING)

# Check columns in eq_consumption
print("=" * 80)
print("EQ Consumption table:")
inspector = inspect(engine)
cols = inspector.get_columns('eq_consumption', schema='silver')
print("Columns:", [c['name'] for c in cols])

# Try to fetch some data
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM silver.eq_consumption LIMIT 5"))
    df = pd.DataFrame(result.fetchall(), columns=[c['name'] for c in cols])
    print("\nSample data:")
    print(df)
    print(f"\nTotal rows: {conn.execute(text('SELECT COUNT(*) FROM silver.eq_consumption')).scalar()}")

# Check metdesk data
print("\n" + "=" * 80)
print("MetDesk forecasts (for FR):")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT DISTINCT element, model
        FROM silver.metdesk_forecasts
        WHERE location = 'FR'
        LIMIT 10
    """))
    print("Available element/model combinations:")
    for row in result:
        print(f"  {row[0]}/{row[1]}")
    
    # Check if we have recent data
    result = conn.execute(text("""
        SELECT COUNT(*) as count
        FROM silver.metdesk_forecasts
        WHERE location = 'FR'
        AND model = 'eceps'
        AND element IN ('wind', 'solar')
    """))
    print(f"\nECEPS wind/solar records: {result.scalar()}")
