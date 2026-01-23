SELECT * FROM crosstab($$
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
    )
    SELECT distinct on (patient_id, memo)
        patient_id,
        memo||'_db',
        ARRAY[CASE WHEN array_length(uitslagen, 1) > 1 THEN 1 ELSE 0 END::varchar,
                datum::varchar, uitslagen::varchar] as dubbel
    FROM grouping_uitslagen
    ORDER BY patient_id, memo, datum desc
    $$,$$
    SELECT
        memo||'_db'
    FROM indelingen.nhg_labcodes
    JOIN vumc_fh_labcodes USING (nhgnr)
    WHERE nhgnr=2815
    ORDER BY memo
    $$) as ct(
        patient_id bigint,
        cvhb_db varchar[]);	