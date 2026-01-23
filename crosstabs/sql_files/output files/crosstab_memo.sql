-- CTEs extracted from crosstab query
WITH alle_hb AS
    (
        SELECT
            patient_id,
            memo,
            datum,
            uitslag_waarde
        FROM vumc_fh_meetwaarden_filter
        WHERE nhgnr=2815
        ORDER BY patient_id, memo, uitslag_waarde, datum desc
    ),
    grouping_uitslagen AS
    (
        SELECT
            patient_id,
            memo,
            datum,
            array_agg(distinct uitslag_waarde) FILTER (WHERE uitslag_waarde IS NOT NULL) as uitslagen
        FROM alle_hb
        GROUP BY patient_id, memo, datum
    ),
prepare AS (
SELECT distinct on (patient_id, memo)
        patient_id,
        memo||'_db',
        ARRAY_CONSTRUCT(CASE WHEN array_length(uitslagen, 1) > 1 THEN 1 ELSE 0 END::varchar,
                datum::varchar, uitslagen::varchar] as dubbel
    FROM grouping_uitslagen
    ORDER BY patient_id, memo, datum desc
)
-- Call snowflake pivot macro
{{snowflake_pivot(['patient_id', 'cvhb_db'],"memo||'_db'", 'distinct', 3,none, [])}}

SELECT * FROM draaitabel_ct