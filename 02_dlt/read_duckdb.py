import duckdb

conn = duckdb.connect("./kafka_pipeline.duckdb", read_only=True)

# List tables
print(conn.execute("SHOW TABLES").fetchall())

# Query (adjust schema/table to match what dlt created)
df = conn.execute("SELECT * FROM kafka_messages.postgres1_inventory_customers LIMIT 10").fetchdf()
print(df)

conn.close()
