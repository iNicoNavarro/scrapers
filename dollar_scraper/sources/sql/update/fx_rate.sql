UPDATE {final_table} f
SET
    until = s.source_date,
    updated_at=GETDATE()
FROM

    {staging_table} AS s
        JOIN
    {final_table} AS f1
        ON s.country = f1.country
            AND s.reference_currency = f1.reference_currency
            AND s.unit = f1.unit
            AND s.source = f1.source
            AND f1.until IS NULL
            AND f1.deleted_at IS NULL
WHERE
    f.country = f1.country
        AND f.reference_currency = f1.reference_currency
        AND f.unit = f1.unit
        AND s.source = f1.source
        AND f.until IS NULL
        AND f.deleted_at IS NULL
        AND s.value != f1.value
        AND f.source != 'Foodology';