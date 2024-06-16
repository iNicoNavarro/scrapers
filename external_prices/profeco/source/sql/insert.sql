INSERT INTO {final_table} 
(
    country,
    city_code,
    city,
    category_code,
    category,
    name,
    price,
    created_at,
    updated_at,
    deleted_at
)
SELECT
    staging.country,
    staging.city_code,
    staging.city,
    staging.category_code,
    staging.category,
    staging.name,
    staging.price,
    GETDATE() as created_at,
    GETDATE() as updated_at,
    NULL::TIMESTAMP as deleted_at
FROM
    {staging_table}  AS staging
        left JOIN
    {final_table} AS mmss
        ON staging."no"  = mmss."no"
        AND staging.country  = mmss.country
        AND staging.shipment_date  = mmss.shipment_date
        AND mmss.deleted_at IS NULL
where mmss."no" IS NULL;
