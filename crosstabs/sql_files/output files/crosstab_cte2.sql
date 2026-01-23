-- CTEs extracted from crosstab query
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
    ),
prepare AS (
SELECT
        patient_id,
        nhgnr,
        ARRAY_CONSTRUCT(min_dd_huisarts, max_dd_niet_huisarts) as waardes
    FROM min_datum_wel_ha
    LEFT JOIN max_datum_niet_ha USING (patient_id, nhgnr)
    ORDER BY patient_id, nhgnr
)
-- Call snowflake pivot macro
{{snowflake_pivot(['dmhb', 'ashb', 'cohb', 'gzhb', 'cvhb', 'ozhb', 'dchb', 'uihb', 'skhb', 'nfhb', 'obhb', 'afhb', 'oshb', 'clhb', 'pahb', 'czhb', 'dehb', 'adhb'],'waardes', 'nhgnr', 2,none, ['patient_id'])}}

SELECT * FROM draaitabel_ct