-- Trino dialect migration from Snowflake
-- Changes made:
--   CURRENT_TIMESTAMP()  →  current_timestamp

WITH base AS (
    SELECT * FROM {{ ref('int_invoice_payment_match') }}
    WHERE payment_id IS NOT NULL
),

velocity AS (
    SELECT
        payment_method,
        expense_category,
        count(*)                          AS total_payments,
        avg(days_to_pay)                  AS avg_days_to_pay,
        min(days_to_pay)                  AS min_days_to_pay,
        max(days_to_pay)                  AS max_days_to_pay,
        sum(invoice_amount)               AS total_amount_processed,
        sum(CASE WHEN payment_timeliness = 'on_time'
            THEN 1 ELSE 0 END)            AS on_time_count,
        sum(CASE WHEN payment_timeliness = 'late'
            THEN 1 ELSE 0 END)            AS late_count,
        round(
            sum(CASE WHEN payment_timeliness = 'on_time'
                THEN 1.0 ELSE 0 END) * 100.0 / count(*), 2
        )                                 AS on_time_pct,
        current_timestamp                 AS report_generated_at
    FROM base
    GROUP BY 1, 2
)

SELECT * FROM velocity