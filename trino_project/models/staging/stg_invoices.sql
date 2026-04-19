-- Trino dialect migration from Snowflake
-- Changes made:
--   DATEDIFF('day', a, b)  →  date_diff('day', a, b)
--   CURRENT_TIMESTAMP()    →  current_timestamp
--   CAST syntax remains standard SQL (compatible)

WITH source AS (
    SELECT * FROM (
        VALUES
            ('INV000001','V0001', 5000.00, 'USD', 'pending', date '2026-01-01', date '2026-01-31', 'NET30', 'Software'),
            ('INV000002','V0002', 7500.00, 'USD', 'overdue', date '2025-12-01', date '2025-12-31', 'NET30', 'Marketing'),
            ('INV000003','V0003', 3200.00, 'USD', 'pending', date '2026-02-01', date '2026-03-01', 'NET30', 'Logistics'),
            ('INV000004','V0004',12000.00, 'USD', 'overdue', date '2025-11-01', date '2025-12-01', 'NET30', 'Legal'),
            ('INV000005','V0005', 8900.00, 'USD', 'pending', date '2026-03-01', date '2026-03-31', 'NET30', 'Office')
    ) AS t(invoice_id, vendor_id, amount, currency, status,
           created_date, due_date, payment_terms, category)
),

renamed AS (
    SELECT
        invoice_id,
        vendor_id,
        CAST(amount AS DECIMAL(12,2))                          AS invoice_amount,
        upper(currency)                                        AS currency_code,
        lower(status)                                          AS invoice_status,
        CAST(created_date AS DATE)                             AS invoice_created_date,
        CAST(due_date AS DATE)                                 AS invoice_due_date,
        payment_terms,
        category                                               AS expense_category,
        date_diff('day', created_date, due_date)               AS payment_terms_days,
        -- ^ Snowflake: DATEDIFF('day', a, b) → Trino: date_diff('day', a, b)
        current_timestamp                                      AS _loaded_at
        -- ^ Snowflake: CURRENT_TIMESTAMP() → Trino: current_timestamp (no parentheses)
    FROM source
    WHERE lower(status) != 'void'
)

SELECT * FROM renamed