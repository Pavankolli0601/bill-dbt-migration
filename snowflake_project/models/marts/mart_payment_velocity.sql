WITH base AS (
    SELECT * FROM {{ ref('int_invoice_payment_match') }}
    WHERE payment_id IS NOT NULL
),

velocity AS (
    SELECT
        payment_method,
        expense_category,
        COUNT(*)                        AS total_payments,
        AVG(days_to_pay)                AS avg_days_to_pay,
        MIN(days_to_pay)                AS min_days_to_pay,
        MAX(days_to_pay)                AS max_days_to_pay,
        SUM(invoice_amount)             AS total_amount_processed,
        SUM(CASE WHEN payment_timeliness = 'on_time'
            THEN 1 ELSE 0 END)          AS on_time_count,
        SUM(CASE WHEN payment_timeliness = 'late'
            THEN 1 ELSE 0 END)          AS late_count,
        ROUND(
            SUM(CASE WHEN payment_timeliness = 'on_time'
                THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        )                               AS on_time_pct,
        CURRENT_TIMESTAMP()             AS report_generated_at
    FROM base
    GROUP BY 1, 2
)

SELECT * FROM velocity