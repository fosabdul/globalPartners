import awswrangler as wr

sql = "SELECT * FROM global_partners_gold.kpi_summary_gold"

df = wr.athena.read_sql_query(
    sql,
    database="global_partners_gold",
    workgroup="primary"  # change if you use a different workgroup name
)

print(df)
