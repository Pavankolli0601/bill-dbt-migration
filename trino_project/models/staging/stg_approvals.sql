-- Trino dialect migration from Snowflake
-- Changes made:
--   CURRENT_TIMESTAMP()  →  current_timestamp

WITH source AS (
    SELECT * FROM (
        VALUES
            ('APR000001','INV000001','EMP001','approved', date '2026-01-03', 10000),
            ('APR000002','INV000002','EMP002','approved', date '2025-12-03', 10000),
            ('APR000003','INV000003','EMP003','rejected', date '2026-02-03',  5000),
            ('APR000004','INV000004','EMP004','approved', date '2025-11-03', 25000),
            ('APR000005','INV000005','EMP005','approved', date '2026-03-03', 10000)
    ) AS t(approval_id, invoice_id, approver_id, status, approved_at, threshold)
),

renamed AS (
    SELECT
        approval_id,
        invoice_id,
        approver_id,
        lower(status)            AS approval_status,
        CAST(approved_at AS DATE) AS approved_at,
        threshold                AS approval_threshold,
        current_timestamp        AS _loaded_at
    FROM source
)

SELECT * FROM renamed