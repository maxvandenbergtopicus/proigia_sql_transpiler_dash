SELECT * FROM crosstab($$
    WITH alle_hb AS
    (
        SELECT
            patient_id,
            nhgnr,
            datum,
            uitslag_raw,
            uitslag_waarde
        FROM proigia_meetwaarden_filter_seg2
        WHERE omschrijving ilike '%hoofdbehandelaar%'
    ),
    max_datum_niet_ha AS
    (
        SELECT
            patient_id,
            nhgnr,
            max(datum) as max_dd_niet_huisarts
        FROM alle_hb
        WHERE (uitslag_waarde <> 'huisarts' or uitslag_waarde IS NULL)
        GROUP BY patient_id, nhgnr
    ),
    min_datum_wel_ha AS
    (
        SELECT
            patient_id,
            nhgnr,
            min(datum) as min_dd_huisarts
        FROM alle_hb
        LEFT JOIN max_datum_niet_ha USING (patient_id, nhgnr)
        WHERE uitslag_waarde = 'huisarts'
        AND (datum >= max_dd_niet_huisarts or max_dd_niet_huisarts IS NULL)
        GROUP BY patient_id, nhgnr
    )
    SELECT
        patient_id,
        nhgnr,
        ARRAY[min_dd_huisarts, max_dd_niet_huisarts]
    FROM min_datum_wel_ha
    LEFT JOIN max_datum_niet_ha USING (patient_id, nhgnr)
    ORDER BY patient_id, nhgnr
    $$,$$
    SELECT nhgnr FROM indelingen.nhg_labcodes
    JOIN proigia_labcodes_seg2 USING (nhgnr)
    WHERE omschrijving ilike '%hoofdbehandelaar%'
    ORDER BY nhgnr
    $$) AS (
        patient_id bigint,
        dmhb date[],
        ashb date[],
        cohb date[],
        gzhb date[],
        cvhb date[],
        ozhb date[],
        dchb date[],
        uihb date[],
        skhb date[],
        nfhb date[],
        obhb date[],
        afhb date[],
        oshb date[],
        clhb date[],
        pahb date[],
        czhb date[],
        dehb date[],
        adhb date[]);