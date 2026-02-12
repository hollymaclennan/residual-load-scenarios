from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)

with engine.connect() as conn:
    # Check locations
    result = conn.execute(text("""
        SELECT DISTINCT location
        FROM silver.metdesk_forecasts
        LIMIT 20
    """))
    locations = [row[0] for row in result]
    print("Available locations in metdesk_forecasts:", locations)
    
    # Check if FR exists
    result = conn.execute(text("""
        SELECT COUNT(*) 
        FROM silver.metdesk_forecasts
        WHERE location = 'FR'
    """))
    count = result.scalar()
    print(f"Rows with location='FR': {count}")
