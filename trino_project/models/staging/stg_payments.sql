-- Trino dialect migration from Snowflake
-- Changes made:
--   CURRENT_TIMESTAMP()  →  current_timestamp

WITH source AS (
    SELECT * FROM (
        VALUES
            ('PAY000001','INV000001', 5000.00, 'ACH',           'completed', date '2026-01-28', true),
            ('PAY000002','INV000002', 7500.00, 'WIRE',          'completed', date '2026-01-05', true),
            ('PAY000003','INV000003', 3200.00, 'CARD',          'completed', date '2026-02-28', false),
            ('PAY000004','INV000004',12000.00, 'INTERNATIONAL', 'completed', date '2025-11-28', true),
            ('PAY000005','INV000005', 8900.00, 'ACH',           'failed',    date '2026-03-28', false)
    ) AS t(payment_id, invoice_id, amount, method, status, processed_at, reconciled)
),

renamed AS (
    SELECT
        payment_id,
        invoice_id,
        CAST(amount AS DECIMAL(12,2))  AS payment_amount,
        upper(method)                  AS payment_method,
        lower(status)                  AS payment_status,
        CAST(processed_at AS DATE)     AS processed_at,
        reconciled,
        current_timestamp              AS _loaded_at
    FROM source
    WHERE lower(status) != 'failed'
)

SELECT * FROM renamed