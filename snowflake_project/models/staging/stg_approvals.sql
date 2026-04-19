WITH source AS (
    SELECT * FROM {{ ref('approvals') }}
),

renamed AS (
    SELECT
        approval_id,
        invoice_id,
        approver_id,
        LOWER(status)                   AS approval_status,
        CAST(approved_at AS DATE)       AS approved_at,
        threshold                       AS approval_threshold,
        CURRENT_TIMESTAMP()             AS _loaded_at
    FROM source
)

SELECT * FROM renamed