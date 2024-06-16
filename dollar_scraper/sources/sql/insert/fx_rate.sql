INSERT INTO {final_table}
(   
    value,
    unit,
    reference_currency,
    country,
    since,
    until,
    source
)
SELECT
    s.value,
    s.unit,
    s.reference_currency,
    s.country,
    s.source_date as since,
    NULL::DATE as until,
    s.source
FROM
    {staging_table} AS s
        LEFT JOIN
    {final_table} AS f
        ON f.country  = s.country
            AND f.unit = s.unit
            AND f.reference_currency = s.reference_currency
            AND f.source = s.source
            AND f.until IS NULL
            AND f.deleted_at IS NULL
WHERE
    f.country IS NULL;