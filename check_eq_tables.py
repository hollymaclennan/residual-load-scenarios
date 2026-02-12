from sqlalchemy import create_engine, inspect
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)
inspector = inspect(engine)

for table in ['eq_consumption', 'eq_residual_load', 'eq_wind_solar']:
    print(f"\n{'='*60}")
    print(f"Columns in {table}:")
    try:
        cols = inspector.get_columns(table, schema='silver')
        for col in cols:
            print(f"  {col['name']}: {col['type']}")
    except Exception as e:
        print(f"  Error: {e}")
