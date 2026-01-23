import sqlglot

# Example CTE with DISTINCT ON
cte_sql = """
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum as max_dd_huisarts,
            nhgnr
        FROM alle_hb
        WHERE uitslag_waarde='huisarts'
        ORDER BY patient_id, nhgnr, datum DESC
"""

# Parse and generate Snowflake SQL
parsed = sqlglot.parse_one(cte_sql, read="postgres")
converted = parsed.sql(dialect="snowflake", pretty=True)
print(converted)