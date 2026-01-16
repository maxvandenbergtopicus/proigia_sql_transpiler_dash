-- Prepare CTE from original query
WITH prepare AS (
SELECT
        patient_id,
        categorie,
        array_to_string(ARRAY_CONSTRUCT(
            count(*)::varchar,
            max(voorschrijfdatum)::varchar,
            min(voorschrijfdatum)::varchar,
            array_agg(distinct atc_code)::varchar,
            max(actueel)::varchar),';') as resultaat
    FROM proigia_medicatie_filter_seg2
    GROUP BY patient_id, categorie
    ORDER BY patient_id, categorie
)
-- Call snowflake pivot macro
{{snowflake_pivot(['a04_', 'a05_'],'resultaat', 'categorie', 5,4, ['patient_id'])}}

SELECT * FROM draaitabel_ct