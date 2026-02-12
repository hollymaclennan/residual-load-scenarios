from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING, COUNTRY

engine = create_engine(DB_CONNECTION_STRING)

try:
    with engine.connect() as conn:
        # Check if FR data exists
        print(f"Checking for {COUNTRY} data in metdesk_forecasts...")
        result = conn.execute(text(f"""
            SELECT location, model, element, COUNT(*) as cnt
            FROM silver.metdesk_forecasts
            WHERE location = '{COUNTRY}'
            GROUP BY location, model, element
            LIMIT 20
        """))
        
        rows = result.fetchall()
        if rows:
            print(f"Found {len(rows)} different location/model/element combinations:")
            for row in rows:
                print(f"  {row[0]}/{row[1]}/{row[2]}: {row[3]} records")
        else:
            print(f"No data found for location='{COUNTRY}'")
            # Check what locations DO exist
            print("\nAvailable locations:")
            result = conn.execute(text("""
                SELECT DISTINCT location
                FROM silver.metdesk_forecasts
                LIMIT 10
            """))
            for row in result:
                print(f"  {row[0]}")
                
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
