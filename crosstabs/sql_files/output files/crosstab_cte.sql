-- CTEs extracted from crosstab query
WITH kalenderjaar AS (
SELECT
  CASE
    WHEN CAST('${peildatum}' AS VARCHAR) ILIKE '%-01-01'
    THEN (
      DATE_PART(YEAR, CAST('${peildatum}' AS DATE)) - 1
    )
    ELSE DATE_PART(YEAR, CAST('${peildatum}' AS DATE))
  END AS kalenderjaar
),
alle_hb AS (
SELECT
  patient_id,
  nhgnr,
  datum,
  uitslag_raw,
  uitslag_waarde
FROM proigia_meetwaarden_filter_seg2
WHERE
  omschrijving ILIKE '%hoofdbehandelaar%'
ORDER BY
  patient_id,
  nhgnr,
  uitslag_waarde,
  datum DESC
),
max_datum_huisarts AS (
SELECT
  patient_id,
  max_dd_huisarts,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS max_dd_huisarts,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum DESC) AS _row_number
  FROM alle_hb
  WHERE
    uitslag_waarde = 'huisarts'
) AS _t
WHERE
  _row_number = 1
),
min_datum_specialist_na_ha AS (
SELECT
  patient_id,
  min_dd_specialist,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS min_dd_specialist,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum ASC) AS _row_number
  FROM alle_hb
  LEFT JOIN max_datum_huisarts
    USING (patient_id, nhgnr)
  WHERE
    uitslag_waarde = 'specialist' AND datum > max_dd_huisarts
) AS _t
WHERE
  _row_number = 1
),
doorverwijzing AS (
SELECT
  patient_id,
  CAST(1 AS VARCHAR) AS doorverwijzing,
  nhgnr
FROM min_datum_specialist_na_ha AS mds
CROSS JOIN kalenderjaar
WHERE
  DATE_PART(YEAR, min_dd_specialist) = kalenderjaar
),
max_datum_specialist AS (
SELECT
  patient_id,
  max_dd_specialist,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS max_dd_specialist,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum DESC) AS _row_number
  FROM alle_hb
  WHERE
    uitslag_waarde = 'specialist'
) AS _t
WHERE
  _row_number = 1
),
min_datum_huisarts_na_spec AS (
SELECT
  patient_id,
  min_dd_huisarts,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS min_dd_huisarts,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum ASC) AS _row_number
  FROM alle_hb
  LEFT JOIN max_datum_specialist
    USING (patient_id, nhgnr)
  WHERE
    uitslag_waarde = 'huisarts' AND datum > max_dd_specialist
) AS _t
WHERE
  _row_number = 1
),
terugverwijzing AS (
SELECT
  patient_id,
  CAST(1 AS VARCHAR) AS terugverwijzing,
  nhgnr
FROM min_datum_huisarts_na_spec
CROSS JOIN kalenderjaar
WHERE
  DATE_PART(YEAR, min_dd_huisarts) = kalenderjaar
),
max_datum_specialist_voor_spec AS (
SELECT
  patient_id,
  max_dd_specialist,
  min_dd_specialist,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS max_dd_specialist,
    min_dd_specialist AS min_dd_specialist,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum DESC) AS _row_number
  FROM alle_hb AS ah
  LEFT JOIN min_datum_specialist_na_ha
    USING (patient_id, nhgnr)
  WHERE
    uitslag_waarde = 'specialist' AND datum < min_dd_specialist
) AS _t
WHERE
  _row_number = 1
),
min_datum_huisarts_tussen AS (
SELECT
  patient_id,
  min_dd_huisarts,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS min_dd_huisarts,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum ASC) AS _row_number
  FROM alle_hb AS ah
  LEFT JOIN max_datum_specialist_voor_spec
    USING (patient_id, nhgnr)
  WHERE
    uitslag_waarde = 'huisarts' AND datum > max_dd_specialist
) AS _t
WHERE
  _row_number = 1
),
tdverwijzing AS (
SELECT
  patient_id,
  CAST(1 AS VARCHAR) AS terug_doorverwijzing,
  nhgnr
FROM min_datum_huisarts_tussen
LEFT JOIN max_datum_specialist_voor_spec
  USING (patient_id, nhgnr)
CROSS JOIN kalenderjaar
WHERE
  DATE_PART(YEAR, min_dd_huisarts) = kalenderjaar
),
max_datum_huisarts_voor_ha AS (
SELECT
  patient_id,
  max_dd_huisarts,
  min_dd_huisarts,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS max_dd_huisarts,
    min_dd_huisarts AS min_dd_huisarts,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum DESC) AS _row_number
  FROM alle_hb AS ah
  LEFT JOIN min_datum_huisarts_na_spec
    USING (patient_id, nhgnr)
  WHERE
    uitslag_waarde = 'huisarts' AND datum < min_dd_huisarts
) AS _t
WHERE
  _row_number = 1
),
min_datum_specialist_tussen AS (
SELECT
  patient_id,
  min_dd_specialist,
  nhgnr
FROM (
  SELECT
    patient_id AS patient_id,
    datum AS min_dd_specialist,
    nhgnr AS nhgnr,
    ROW_NUMBER() OVER (PARTITION BY patient_id, nhgnr ORDER BY patient_id, nhgnr, datum ASC) AS _row_number
  FROM alle_hb AS ah
  LEFT JOIN max_datum_huisarts_voor_ha
    USING (patient_id, nhgnr)
  WHERE
    uitslag_waarde = 'specialist' AND datum > max_dd_huisarts
) AS _t
WHERE
  _row_number = 1
),
dtverwijzing AS (
SELECT
  patient_id,
  CAST(1 AS VARCHAR) AS door_terugverwijzing,
  nhgnr
FROM min_datum_specialist_tussen AS mds
CROSS JOIN kalenderjaar
WHERE
  DATE_PART(YEAR, min_dd_specialist) = kalenderjaar
),
verwijzingen_niet_leeg AS (
SELECT
  patient_id,
  nhgnr,
  COALESCE(doorverwijzing, CAST(0 AS VARCHAR)) AS doorverwijzing,
  COALESCE(terugverwijzing, CAST(0 AS VARCHAR)) AS terugverwijzing,
  COALESCE(terug_doorverwijzing, CAST(0 AS VARCHAR)) AS terug_doorverwijzing,
  COALESCE(door_terugverwijzing, CAST(0 AS VARCHAR)) AS door_terugverwijzing
FROM doorverwijzing AS d
FULL OUTER JOIN terugverwijzing AS t
  USING (patient_id, nhgnr)
FULL OUTER JOIN tdverwijzing AS td
  USING (patient_id, nhgnr)
FULL OUTER JOIN dtverwijzing AS dt
  USING (patient_id, nhgnr)
),
prepare AS (
SELECT
  patient_id,
  nhgnr,
  ARRAY_CONSTRUCT(doorverwijzing, terugverwijzing, terug_doorverwijzing, door_terugverwijzing) AS waardes
FROM verwijzingen_niet_leeg
ORDER BY
  patient_id,
  nhgnr
)
-- Call snowflake pivot macro
{{snowflake_pivot(['dm_verw', 'as_verw', 'co_verw', 'gz_verw', 'cv_verw', 'oz_verw', 'dc_verw', 'ui_verw', 'sk_verw', 'nf_verw', 'ob_verw', 'af_verw', 'os_verw', 'cl_verw', 'pa_verw', 'cz_verw', 'de_verw', 'ad_verw'],'waardes', 'nhgnr', 4,none, ['patient_id'])}}

SELECT * FROM draaitabel_ct