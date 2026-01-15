    SELECT * FROM crosstab($$
    SELECT
        patient_id,
        name,
        ARRAY[min(begindatum)::varchar,
                max(einddatum)::varchar,
                max(status_datum)::varchar,
                max(status_vlag)::varchar,
                array_agg(distinct icpc)::varchar,
                max(begindatum)::varchar,
                min(einddatum)::varchar,
                min(begindatum_nonactief)::varchar,
                max(einddatum_nonactief)::varchar,
                max(begindatum_nonactief)::varchar,
                min(einddatum_nonactief)::varchar] as waardes
    FROM bewegingsapparaat_indic_53530250_episode_filter
    JOIN bewegingsapparaat_indic_53530250_icpc_codes ON icpc ILIKE searchstring
    GROUP BY patient_id, name
    ORDER BY patient_id, name
    $$,$$
        SELECT name FROM bewegingsapparaat_indic_53530250_icpc_codes ORDER BY name;
    $$) as ct(
        patient_id bigint,
        l02_ varchar[],
        l03_ varchar[]
    );