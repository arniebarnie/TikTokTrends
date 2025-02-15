import awswrangler as wr

# Get all partition values
partitions = wr.athena.read_sql_query(
    sql = """
    SELECT DISTINCT profile, processed_at 
    FROM metadata
    """,
    database = "tiktok_analytics"
)

# Drop each partition
for _, row in partitions.iterrows():
    drop_sql = f"""
        ALTER TABLE metadata 
        DROP PARTITION (profile='{row['profile']}', processed_at='{row['processed_at']}')
    """
    wr.athena.start_query_execution(
        sql = drop_sql,
        database = "tiktok_analytics"
    )
    print(f"Dropped partition: profile={row['profile']}, processed_at={row['processed_at']}") 