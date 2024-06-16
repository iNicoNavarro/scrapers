UPDATE {final_table} f
SET
    deleted_at = GETDATE(),
    updated_at = GETDATE()
FROM
    {final_table} AS mmss
        LEFT JOIN
    {staging_table} AS staging
        ON mmss."no"  = staging."no"
        AND mmss.country = staging.country
        AND coalesce(mmss.transfer_from_code, '') = coalesce(staging.transfer_from_code, '')
        AND coalesce(mmss.transfer_to_code, '') = coalesce(staging.transfer_to_code, '')
        AND coalesce(mmss.receipt_date, '2000-01-01') = coalesce(staging.receipt_date, '2000-01-01')
        AND coalesce(mmss.shipment_date, '2000-01-01') = coalesce(staging.shipment_date, '2000-01-01')
        AND coalesce(mmss.completely_received, true) = coalesce(staging.completely_received, true)
        AND coalesce(mmss.shortcut_dimension, '') = coalesce(staging.shortcut_dimension, '')
        AND coalesce(mmss.status, '') = coalesce(staging.status, '')
        AND coalesce(mmss.shipping_advice, '') = coalesce(staging.shipping_advice, '')
WHERE 
    f."no" = mmss."no"
    AND f.country = mmss.country
    AND mmss.deleted_at IS NULL
    AND f.deleted_at IS NULL  
    AND staging.country IS NULL
    AND mmss.country = '{country}'
