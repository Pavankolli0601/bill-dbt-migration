-- Trino dialect migration from Snowflake
-- Changes made:
--   DATEDIFF('day', a, b)  →  date_diff('day', a, b)
--   ZEROIFNULL(x)          →  coalesce(x, 0)
--   USING (col)            →  ON a.col = b.col  (Trino limitation)

WITH invoices AS (
    SELECT * FROM {{ ref('stg_invoices') }}
),
payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),
matched AS (
    SELECT
        i.invoice_id,
        i.vendor_id,
        i.invoice_amount,
        i.invoice_status,
        i.invoice_created_date,
        i.invoice_due_date,
        i.expense_category,
        i.payment_terms,
        p.payment_id,
        p.payment_method,
        p.payment_status,
        p.processed_at,
        date_diff('day', i.invoice_due_date, p.processed_at)  AS days_to_pay,
        CASE
            WHEN p.processed_at <= i.invoice_due_date THEN 'on_time'
            WHEN p.processed_at >  i.invoice_due_date THEN 'late'
            ELSE 'unpaid'
        END                                                    AS payment_timeliness,
        coalesce(i.invoice_amount - p.payment_amount, 0)       AS payment_variance
    FROM invoices i
    LEFT JOIN payments p ON i.invoice_id = p.invoice_id
)
SELECT * FROM matched