from sqlalchemy import create_engine, text, inspect
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)
inspector = inspect(engine)

# List tables with 'fr' or 'french' in them
print("Looking for French forecast tables...")
tables = inspector.get_table_names(schema='silver')
matching = [t for t in tables if 'fr' in t.lower() or 'enappsys' in t.lower()]

print(f"\nMatching tables ({len(matching)}):")
for table in sorted(matching):
    print(f"  {table}")

# Check enappsys_fr_demand if it exists
if 'enappsys_fr_demand' in tables:
    print("\n" + "=" * 60)
    print("Columns in enappsys_fr_demand:")
    cols = inspector.get_columns('enappsys_fr_demand', schema='silver')
    for col in cols:
        print(f"  {col['name']}: {col['type']}")
