#!/usr/bin/env python
from sqlalchemy import create_engine, text, inspect
from config import DB_CONNECTION_STRING

engine = create_engine(DB_CONNECTION_STRING)
inspector = inspect(engine)

schemas = inspector.get_schema_names()
print("Schemas:", ', '.join(schemas))

# Check for silver and public schemas
for schema in ['silver', 'public']:
    if schema in schemas:
        tables = inspector.get_table_names(schema=schema)
        print(f"\nTables in {schema}:")
        for table in sorted(tables)[:20]:
            print(f"  {table}")
