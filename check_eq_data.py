from sqlalchemy import create_engine, text
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)

with engine.connect() as conn:    
    # Check what's in eq_residual_load
    result = conn.execute(text("""
        SELECT DISTINCT tag, country, COUNT(*) as count
        FROM silver.eq_residual_load
        GROUP BY tag, country
        ORDER BY tag, country
    """))
    
    print("eq_residual_load data:")
    print("="*60)
    for row in result:
        print(f"  tag={row[0]}, country={row[1]}, rows={row[2]}")
    
    # Check some sample data
    print("\n\nSample from eq_residual_load (last 5 rows):")
    print("="*60)
    result2 = conn.execute(text("""
        SELECT utc_datetime, residual_load, e00, e01, e02, tag, country
        FROM silver.eq_residual_load
        ORDER BY utc_datetime DESC
        LIMIT 5
    """))
    for row in result2:
        print(f"  {row[0]}: RL={row[1]}, e00={row[2]}, e01={row[3]}, e02={row[4]}, tag={row[5]}, country={row[6]}")
        
    # Check eq_consumption
    print("\n\nSample from eq_consumption (for FR):")
    print("="*60)
    result3 = conn.execute(text("""
        SELECT utc_datetime, consumption_act, da_consumption_fcst, consumption_fcst_latest, country
        FROM silver.eq_consumption
        WHERE country='FR'
        ORDER BY utc_datetime DESC
        LIMIT 5
    """))
    for row in result3:
        print(f"  {row[0]}: actual={row[1]}, da_fcst={row[2]}, latest={row[3]}, country={row[4]}")
        
    # Check eq_wind_solar
    print("\n\nSample from eq_wind_solar (for FR):")  
    print("="*60)
    result4 = conn.execute(text("""
        SELECT utc_datetime, production_acc, data_type, production_fcst_latest, country
        FROM silver.eq_wind_solar
        WHERE country='FR'
        ORDER BY utc_datetime DESC
        LIMIT 10
    """))
    for row in result4:
        print(f"  {row[0]}: acc={row[1]}, type={row[2]}, latest={row[4]}")
