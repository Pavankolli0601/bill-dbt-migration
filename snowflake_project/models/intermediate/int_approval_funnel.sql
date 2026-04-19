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
        DATEDIFF('day', i.invoice_created_date, a.approved_at)  AS days_to_approve,
        CASE
            WHEN i.invoice_amount > a.approval_threshold THEN TRUE
            ELSE FALSE
        END                                                       AS exceeds_threshold
    FROM approvals a
    LEFT JOIN invoices i USING (invoice_id)
)

SELECT * FROM funnel