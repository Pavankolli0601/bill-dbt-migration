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
        DATEDIFF('day', i.invoice_due_date, p.processed_at)  AS days_to_pay,
        CASE
            WHEN p.processed_at <= i.invoice_due_date THEN 'on_time'
            WHEN p.processed_at >  i.invoice_due_date THEN 'late'
            ELSE 'unpaid'
        END                                                   AS payment_timeliness,
        ZEROIFNULL(i.invoice_amount - p.payment_amount)       AS payment_variance
    FROM invoices i
    LEFT JOIN payments p USING (invoice_id)
)

SELECT * FROM matched