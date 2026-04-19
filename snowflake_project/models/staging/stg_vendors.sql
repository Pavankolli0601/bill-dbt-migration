WITH source AS (
    SELECT * FROM {{ ref('vendors') }}
),

renamed AS (
    SELECT
        vendor_id,
        vendor_name,
        UPPER(category)                 AS vendor_category,
        UPPER(payment_method)           AS preferred_payment_method,
        risk_tier,
        CURRENT_TIMESTAMP()             AS _loaded_at
    FROM source
)

SELECT * FROM renamed