from sqlalchemy import create_engine, inspect
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)
inspector = inspect(engine)

# Get all tables in silver schema
tables = inspector.get_table_names(schema='silver')
print(f"Total tables in silver: {len(tables)}")

# Look for EQ or forecast related  
matching = [t for t in tables if any(x in t.lower() for x in ['eq', 'demand', 'forecast', 'metdesk'])]
print("\nMatching tables:")
for table in sorted(matching):
    print(f"  {table}")

# Check columns in metdesk_forecasts specifically  
print("\n" + "="*60)
print("Columns in metdesk_forecasts:")
cols = inspector.get_columns('metdesk_forecasts', schema='silver')
for col in cols:
    print(f"  {col['name']}")
    
print("\nTotal columns:", len(cols))
