WITH vendor_spend AS (
    SELECT * FROM {{ ref('int_vendor_spend') }}
),

ranked AS (
    SELECT
        vendor_id,
        vendor_name,
        vendor_category,
        preferred_payment_method,
        risk_tier,
        total_invoices,
        total_spend,
        avg_invoice_amount,
        first_invoice_date,
        last_invoice_date,
        RANK() OVER (
            PARTITION BY vendor_category
            ORDER BY total_spend DESC
        )                               AS spend_rank_in_category,
        ROUND(total_spend * 100.0 / SUM(total_spend) OVER (), 2)
                                        AS pct_of_total_spend,
        CURRENT_TIMESTAMP()             AS report_generated_at
    FROM vendor_spend
)

SELECT * FROM ranked