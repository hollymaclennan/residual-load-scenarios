from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)

with engine.connect() as conn:
    # Check if FR data exists
    r = conn.execute(text('SELECT COUNT(*) FROM silver.eq_consumption WHERE country="FR"'))
    count_fr = r.scalar()
    print(f"EQ consumption rows for FR: {count_fr}")
    
    # Sample some data
    r = conn.execute(text("""
        SELECT utc_datetime, consumption_fcst_latest 
        FROM silver.eq_consumption 
        WHERE country='FR' 
        LIMIT 5
    """))
    for row in r:
        print(f"  {row[0]}: {row[1]}")
