    SELECT * FROM crosstab(
    $$
    WITH ficat_series AS
    (
        SELECT
            patient_id,
            generate_series(1,50)::varchar AS fi_cat
        FROM proigia_patienten_gegevens
        UNION
        SELECT
            patient_id, 
            'tot'::varchar AS fi_cat
        FROM proigia_patienten_gegevens
    )
    SELECT
        patient_id,
        fi_cat,
        CASE 
            WHEN fi_cat = 'tot' 
            THEN
                CASE WHEN nrs IS NULL 
                THEN 0
                ELSE array_length(nrs,1)
                END
            WHEN get_number(fi_cat) = ANY(nrs) 
            THEN 1 
            ELSE 0
            END AS fi_cat_ind
        FROM ficat_series
        LEFT JOIN proigia_fi_score_filter USING (patient_id)
        ORDER BY patient_id, fi_cat
    $$,
    $$
    SELECT generate_series(1,50)::varchar
    UNION ALL
    SELECT 'tot'