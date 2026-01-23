    SELECT * FROM crosstab($$
    WITH alle_hb AS
    (
        SELECT
            patient_id,
            memo,
            datum,
            uitslag_waarde
        FROM proigia_meetwaarden_filter_seg2
        WHERE omschrijving ilike '%hoofdbehandelaar%'
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
    JOIN proigia_labcodes_seg2 USING (nhgnr)
    WHERE omschrijving ilike '%hoofdbehandelaar%'
    ORDER BY memo
    $$) AS ct(
        patient_id bigint,
        adhb_db varchar[],
        afhb_db varchar[],
        ashb_db varchar[],
        clhb_db varchar[],
        cohb_db varchar[],
        cvhb_db varchar[],
        czhb_db varchar[],
        dchb_db varchar[],
        dehb_db varchar[],
        dmhb_db varchar[],
        gzhb_db varchar[],
        nfhb_db varchar[],
        obhb_db varchar[],
        oshb_db varchar[],
        ozhb_db varchar[],
        pahb_db varchar[],
        skhb_db varchar[],
        uihb_db varchar[]
    );