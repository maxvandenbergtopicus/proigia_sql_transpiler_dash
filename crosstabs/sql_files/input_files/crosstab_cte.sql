	
	SELECT * FROM crosstab($$
    WITH kalenderjaar AS
    (
		SELECT CASE WHEN '${peildatum}'::varchar ILIKE '%-01-01'
		 THEN (EXTRACT(year FROM '${peildatum}'::date) - 1)
			ELSE EXTRACT(year FROM '${peildatum}'::date)
			END as kalenderjaar
    ),
	alle_hb AS
    (
        SELECT
            patient_id,
            nhgnr,
            datum,
            uitslag_raw,
            uitslag_waarde
        FROM proigia_meetwaarden_filter_seg2
        WHERE omschrijving ilike '%hoofdbehandelaar%'
	    ORDER BY patient_id, nhgnr, uitslag_waarde, datum DESC
    ),
    --doorverw
    max_datum_huisarts AS
    (
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum as max_dd_huisarts,
            nhgnr
        FROM alle_hb
        WHERE uitslag_waarde='huisarts'
        ORDER BY patient_id, nhgnr, datum DESC
    ),
    min_datum_specialist_na_ha AS
    (
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum as min_dd_specialist,
            nhgnr
        FROM alle_hb
        LEFT JOIN max_datum_huisarts USING (patient_id, nhgnr)
        WHERE uitslag_waarde='specialist'
        AND datum > max_dd_huisarts
        ORDER BY patient_id, nhgnr, datum ASC
    ),
    doorverwijzing AS
    (
        SELECT
            patient_id,
            1::varchar as doorverwijzing,
            nhgnr
        FROM min_datum_specialist_na_ha mds
	    CROSS JOIN kalenderjaar
        WHERE EXTRACT(year FROM min_dd_specialist) = kalenderjaar
    ),
    max_datum_specialist AS
    (
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum AS max_dd_specialist,
            nhgnr
        FROM alle_hb
        WHERE uitslag_waarde='specialist'
        ORDER BY patient_id, nhgnr, datum DESC
    ),
    min_datum_huisarts_na_spec AS
    (
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum as min_dd_huisarts,
            nhgnr
        FROM alle_hb
        LEFT JOIN max_datum_specialist USING (patient_id, nhgnr)
        WHERE uitslag_waarde='huisarts'
        AND datum > max_dd_specialist
        ORDER BY patient_id, nhgnr, datum ASC
    ),
    terugverwijzing AS
    (
        SELECT
            patient_id,
            1::varchar as terugverwijzing,
            nhgnr
        FROM min_datum_huisarts_na_spec
        CROSS JOIN kalenderjaar
        WHERE EXTRACT(year FROM min_dd_huisarts) = kalenderjaar
    ),
    --tdverw
    max_datum_specialist_voor_spec AS
    (
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum AS max_dd_specialist,
            min_dd_specialist,
            nhgnr
        FROM alle_hb ah
        LEFT JOIN min_datum_specialist_na_ha USING (patient_id, nhgnr)
        WHERE uitslag_waarde='specialist'
        AND datum < min_dd_specialist
        ORDER BY patient_id, nhgnr, datum DESC
    ),
    min_datum_huisarts_tussen AS
    (
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum AS min_dd_huisarts,
            nhgnr
        FROM alle_hb ah
        LEFT JOIN max_datum_specialist_voor_spec USING (patient_id, nhgnr)
        WHERE uitslag_waarde='huisarts'
        AND datum > max_dd_specialist
        ORDER BY patient_id, nhgnr, datum ASC
    ),
    tdverwijzing AS
    (
        SELECT
            patient_id,
            1::varchar AS terug_doorverwijzing,
            nhgnr
        FROM min_datum_huisarts_tussen
        LEFT JOIN max_datum_specialist_voor_spec USING (patient_id, nhgnr)
        CROSS JOIN kalenderjaar
        WHERE EXTRACT(year FROM min_dd_huisarts) = kalenderjaar
    ),
	--dtverw
    max_datum_huisarts_voor_ha AS
	(
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum AS max_dd_huisarts,
            min_dd_huisarts,
            nhgnr
        FROM alle_hb ah
        LEFT JOIN min_datum_huisarts_na_spec USING (patient_id, nhgnr)
        WHERE uitslag_waarde='huisarts'
        AND datum < min_dd_huisarts
        ORDER BY patient_id, nhgnr, datum DESC
    ),
	min_datum_specialist_tussen AS 
    (
        SELECT distinct on (patient_id, nhgnr)
            patient_id,
            datum AS min_dd_specialist,
            nhgnr
        FROM alle_hb ah
        LEFT JOIN max_datum_huisarts_voor_ha USING (patient_id, nhgnr)
        WHERE uitslag_waarde='specialist'
        AND datum > max_dd_huisarts
        ORDER BY patient_id, nhgnr, datum ASC
    ),
    dtverwijzing AS
    (
        SELECT
            patient_id,
            1::varchar as door_terugverwijzing,
            nhgnr
        FROM min_datum_specialist_tussen mds
	    CROSS JOIN kalenderjaar
        WHERE EXTRACT(year FROM min_dd_specialist) = kalenderjaar
    ),
    verwijzingen_niet_leeg AS
    (
        SELECT
            patient_id,
            nhgnr,
            COALESCE(doorverwijzing,0::varchar) as doorverwijzing,
	        COALESCE(terugverwijzing,0::varchar) as terugverwijzing,
	        COALESCE(terug_doorverwijzing,0::varchar) as terug_doorverwijzing,
	        COALESCE(door_terugverwijzing,0::varchar) as door_terugverwijzing
        FROM doorverwijzing d
        FULL OUTER JOIN terugverwijzing t USING (patient_id, nhgnr)
        FULL OUTER JOIN tdverwijzing td USING (patient_id, nhgnr)
        FULL OUTER JOIN dtverwijzing dt USING (patient_id, nhgnr)
    )
    SELECT
        patient_id,
        nhgnr,
        ARRAY[doorverwijzing, terugverwijzing, terug_doorverwijzing, door_terugverwijzing]
    FROM verwijzingen_niet_leeg
    ORDER BY patient_id, nhgnr
    $$,$$
    SELECT nhgnr FROM indelingen.nhg_labcodes
    JOIN proigia_labcodes_seg2 USING (nhgnr)
    WHERE omschrijving ilike '%hoofdbehandelaar%'
    ORDER BY nhgnr
    $$) AS (
       patient_id bigint,
       dm_verw varchar[],
       as_verw varchar[],
       co_verw varchar[],
       gz_verw varchar[],
       cv_verw varchar[],
       oz_verw varchar[],
       dc_verw varchar[],
       ui_verw varchar[],
       sk_verw varchar[],
       nf_verw varchar[],
       ob_verw varchar[],
       af_verw varchar[],
       os_verw varchar[],
       cl_verw varchar[],
       pa_verw varchar[],
       cz_verw varchar[],
       de_verw varchar[],
       ad_verw varchar[]
);