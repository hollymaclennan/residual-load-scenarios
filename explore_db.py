from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)

# Query to find all tables
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
    """))
    
    print("Available tables:")
    for schema, table in result:
        print(f"  {schema}.{table}")
        
    # Now look at silver.metdesk_forecasts to see what's there
    print("\n" + "="*80)
    print("Sample from silver.metdesk_forecasts:")
    result2 = conn.execute(text("""
        SELECT DISTINCT element, model, location 
        FROM silver.metdesk_forecasts 
        LIMIT 20
    """))
    for row in result2:
        print(f"  element={row[0]}, model={row[1]}, location={row[2]}")
        
    # Look for EQ data
    print("\n" + "="*80)
    print("Looking for EQ/demand tables...")
    result3 = conn.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name ILIKE '%eq%' OR table_name ILIKE '%demand%'
        ORDER BY table_schema, table_name
    """))
    for schema, table in result3:
        print(f"  {schema}.{table}")
