WITH invoices AS (
    SELECT * FROM {{ ref('stg_invoices') }}
),

vendors AS (
    SELECT * FROM {{ ref('stg_vendors') }}
),

spend AS (
    SELECT
        v.vendor_id,
        v.vendor_name,
        v.vendor_category,
        v.preferred_payment_method,
        v.risk_tier,
        COUNT(i.invoice_id)             AS total_invoices,
        SUM(i.invoice_amount)           AS total_spend,
        AVG(i.invoice_amount)           AS avg_invoice_amount,
        MIN(i.invoice_created_date)     AS first_invoice_date,
        MAX(i.invoice_created_date)     AS last_invoice_date
    FROM vendors v
    LEFT JOIN invoices i USING (vendor_id)
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM spend