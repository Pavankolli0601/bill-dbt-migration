WITH base AS (
    SELECT * FROM {{ ref('int_invoice_payment_match') }}
    WHERE invoice_status IN ('pending', 'overdue')
),

aged AS (
    SELECT
        vendor_id,
        expense_category,
        COUNT(*)                                                     AS invoice_count,
        SUM(invoice_amount)                                          AS total_outstanding,
        SUM(CASE
            WHEN DATEDIFF('day', invoice_due_date, CURRENT_DATE()) BETWEEN 0 AND 30
            THEN invoice_amount ELSE 0 END)                          AS bucket_0_30,
        SUM(CASE
            WHEN DATEDIFF('day', invoice_due_date, CURRENT_DATE()) BETWEEN 31 AND 60
            THEN invoice_amount ELSE 0 END)                          AS bucket_31_60,
        SUM(CASE
            WHEN DATEDIFF('day', invoice_due_date, CURRENT_DATE()) BETWEEN 61 AND 90
            THEN invoice_amount ELSE 0 END)                          AS bucket_61_90,
        SUM(CASE
            WHEN DATEDIFF('day', invoice_due_date, CURRENT_DATE()) > 90
            THEN invoice_amount ELSE 0 END)                          AS bucket_90_plus,
        CURRENT_TIMESTAMP()                                          AS report_generated_at
    FROM base
    GROUP BY 1, 2
)

SELECT * FROM aged