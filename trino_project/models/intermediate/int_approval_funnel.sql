-- Trino dialect migration from Snowflake
-- Changes made:
--   DATEDIFF('day', a, b)  →  date_diff('day', a, b)
--   USING (col)            →  ON a.col = b.col  (Trino limitation)

WITH approvals AS (
    SELECT * FROM {{ ref('stg_approvals') }}
),
invoices AS (
    SELECT * FROM {{ ref('stg_invoices') }}
),
funnel AS (
    SELECT
        a.approval_id,
        a.invoice_id,
        a.approver_id,
        a.approval_status,
        a.approved_at,
        a.approval_threshold,
        i.invoice_amount,
        i.expense_category,
        i.vendor_id,
        date_diff('day', i.invoice_created_date, a.approved_at) AS days_to_approve,
        CASE
            WHEN i.invoice_amount > a.approval_threshold THEN true
            ELSE false
        END                                                      AS exceeds_threshold
    FROM approvals a
    LEFT JOIN invoices i ON a.invoice_id = i.invoice_id
)
SELECT * FROM funnel