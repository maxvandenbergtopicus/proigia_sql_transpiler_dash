-- CTEs extracted from crosstab query
WITH alle_kz AS (
SELECT
  patient_id,
  memo,
  datum,
  uitslag_waarde
FROM proigia_meetwaarden_filter_seg2
WHERE
  omschrijving ILIKE '%deeln%keten%'
ORDER BY
  patient_id,
  memo,
  uitslag_waarde,
  datum DESC
),
grouping_uitslagen AS (
SELECT
  patient_id,
  memo,
  datum,
  ARRAY_AGG(DISTINCT uitslag_waarde) AS uitslagen
FROM alle_kz
GROUP BY
  patient_id,
  memo,
  datum
),
prepare AS (
SELECT
  patient_id,
  _col,
  dubbel
FROM (
  SELECT
    patient_id AS patient_id,
    memo || '_db' AS _col,
    ARRAY_TO_STRING(
      ARRAY_CONSTRUCT(
        CAST(CASE WHEN ARRAY_SIZE(uitslagen) > 1 THEN 1 ELSE 0 END AS VARCHAR),
        CAST(datum AS VARCHAR),
        CAST(uitslagen AS VARCHAR)
      ),
      ';'
    ) AS dubbel,
    ROW_NUMBER() OVER (PARTITION BY patient_id, memo ORDER BY patient_id, memo, datum DESC) AS _row_number
  FROM grouping_uitslagen
) AS _t
WHERE
  _row_number = 1
)
-- Call snowflake pivot macro
{{snowflake_pivot(['afkz_db', 'askz_db', 'clkz_db', 'cokz_db', 'cvkz_db', 'dckz_db', 'dekz_db', 'dmkz_db', 'gzkz_db', 'nfkz_db', 'obkz_db', 'oskz_db', 'ozkz_db', 'pakz_db', 'skkz_db', 'uikz_db'],'dubbel', '_col', 3,none, ['patient_id'])}}

SELECT * FROM draaitabel_ct