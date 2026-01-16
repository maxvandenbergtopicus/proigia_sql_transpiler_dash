CREATE VIEW proigia_medicatie_seg2 AS
SELECT * FROM crosstab($$
    SELECT
        patient_id,
        categorie,
        ARRAY[
            count(*)::varchar,
            max(voorschrijfdatum)::varchar,
            min(voorschrijfdatum)::varchar,
            array_agg(distinct atc_code)::varchar,
            max(actueel)::varchar] as resultaat
    FROM proigia_medicatie_filter_seg2
    GROUP BY patient_id, categorie
    ORDER BY patient_id, categorie
$$,$$
    SELECT atc FROM proigia_atc_codes_seg2 ORDER BY atc
$$) as
(
    patient_id bigint,
    a04_ varchar[],
    a05_ varchar[]
);