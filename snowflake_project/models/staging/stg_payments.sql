WITH source AS (
    SELECT * FROM {{ ref('payments') }}
),

renamed AS (
    SELECT
        payment_id,
        invoice_id,
        CAST(amount AS DECIMAL(12,2))   AS payment_amount,
        UPPER(method)                   AS payment_method,
        LOWER(status)                   AS payment_status,
        CAST(processed_at AS DATE)      AS processed_at,
        reconciled,
        CURRENT_TIMESTAMP()             AS _loaded_at
    FROM source
    WHERE LOWER(status) != 'failed'
)

SELECT * FROM renamed