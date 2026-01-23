-- CTEs extracted from crosstab query
WITH alle_hb AS (
SELECT
  patient_id,
  memo,
  datum,
  uitslag_waarde
FROM proigia_meetwaarden_filter_seg2
WHERE
  omschrijving ILIKE '%hoofdbehandelaar%'
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
FROM alle_hb
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
    ARRAY_CONSTRUCT(
      CAST(CASE WHEN ARRAY_SIZE(uitslagen) > 1 THEN 1 ELSE 0 END AS VARCHAR),
      CAST(datum AS VARCHAR),
      CAST(uitslagen AS VARCHAR)
    ) AS dubbel,
    ROW_NUMBER() OVER (PARTITION BY patient_id, memo ORDER BY patient_id, memo, datum DESC) AS _row_number
  FROM grouping_uitslagen
) AS _t
WHERE
  _row_number = 1
)
-- Call snowflake pivot macro
{{snowflake_pivot(['adhb_db', 'afhb_db', 'ashb_db', 'clhb_db', 'cohb_db', 'cvhb_db', 'czhb_db', 'dchb_db', 'dehb_db', 'dmhb_db', 'gzhb_db', 'nfhb_db', 'obhb_db', 'oshb_db', 'ozhb_db', 'pahb_db', 'skhb_db', 'uihb_db'],'dubbel', '_col', 3,none, ['patient_id'])}}

SELECT * FROM draaitabel_ct