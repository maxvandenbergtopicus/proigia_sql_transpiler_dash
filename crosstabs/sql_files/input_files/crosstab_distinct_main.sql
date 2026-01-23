   SELECT * FROM crosstab($$
    WITH alle_kz AS
    (
        SELECT
            patient_id,
            memo,
            datum,
            uitslag_waarde
        FROM proigia_meetwaarden_filter_seg2
        WHERE omschrijving ILIKE '%deeln%keten%'
        ORDER BY patient_id, memo, uitslag_waarde, datum desc
    ),
    grouping_uitslagen AS
    (
        SELECT
            patient_id,
            memo,
            datum,
            array_agg(distinct uitslag_waarde) FILTER (WHERE uitslag_waarde IS NOT NULL) as uitslagen
        FROM alle_kz
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
        JOIN proigia_labcodes_seg2 USING (nhgnr)
        WHERE omschrijving ILIKE '%deeln%keten%'
        ORDER BY memo
    $$) AS ct(
        patient_id bigint,
        afkz_db varchar[],
        askz_db varchar[],
        clkz_db varchar[],
        cokz_db varchar[],
        cvkz_db varchar[],
        dckz_db varchar[],
        dekz_db varchar[],
        dmkz_db varchar[],
        gzkz_db varchar[],
        nfkz_db varchar[],
        obkz_db varchar[],
        oskz_db varchar[],
        ozkz_db varchar[],
        pakz_db varchar[],
        skkz_db varchar[],
        uikz_db varchar[]
    )