-- Trino dialect migration from Snowflake
-- Changes made:
--   DATEDIFF('day', a, b)  →  date_diff('day', a, b)
--   CURRENT_DATE()         →  current_date (no parentheses)

WITH base AS (
    SELECT * FROM {{ ref('int_invoice_payment_match') }}
    WHERE invoice_status IN ('pending', 'overdue')
),

aged AS (
    SELECT
        vendor_id,
        expense_category,
        count(*)                                                      AS invoice_count,
        sum(invoice_amount)                                           AS total_outstanding,
        sum(CASE
            WHEN date_diff('day', invoice_due_date, current_date) BETWEEN 0 AND 30
            THEN invoice_amount ELSE 0 END)                           AS bucket_0_30,
        -- ^ Snowflake: DATEDIFF + CURRENT_DATE() → Trino: date_diff + current_date
        sum(CASE
            WHEN date_diff('day', invoice_due_date, current_date) BETWEEN 31 AND 60
            THEN invoice_amount ELSE 0 END)                           AS bucket_31_60,
        sum(CASE
            WHEN date_diff('day', invoice_due_date, current_date) BETWEEN 61 AND 90
            THEN invoice_amount ELSE 0 END)                           AS bucket_61_90,
        sum(CASE
            WHEN date_diff('day', invoice_due_date, current_date) > 90
            THEN invoice_amount ELSE 0 END)                           AS bucket_90_plus,
        current_timestamp                                             AS report_generated_at
    FROM base
    GROUP BY 1, 2
)

SELECT * FROM aged