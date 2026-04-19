WITH source AS (
    SELECT * FROM {{ ref('invoices') }}
),

renamed AS (
    SELECT
        invoice_id,
        vendor_id,
        CAST(amount AS DECIMAL(12,2))                    AS invoice_amount,
        UPPER(currency)                                  AS currency_code,
        LOWER(status)                                    AS invoice_status,
        CAST(created_date AS DATE)                       AS invoice_created_date,
        CAST(due_date AS DATE)                           AS invoice_due_date,
        payment_terms,
        category                                         AS expense_category,
        DATEDIFF('day', created_date, due_date)          AS payment_terms_days,
        CURRENT_TIMESTAMP()                              AS _loaded_at
    FROM source
    WHERE LOWER(status) != 'void'
)

SELECT * FROM renamed