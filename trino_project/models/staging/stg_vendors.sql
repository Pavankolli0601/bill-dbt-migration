-- Trino dialect migration from Snowflake
-- Changes made:
--   CURRENT_TIMESTAMP()  →  current_timestamp

WITH source AS (
    SELECT * FROM (
        VALUES
            ('V0001', 'Acme Software',    'Software',  'ACH',  'Low'),
            ('V0002', 'Brand Agency',     'Marketing', 'CARD', 'Medium'),
            ('V0003', 'FastShip Logistic','Logistics', 'WIRE', 'Low'),
            ('V0004', 'LegalEdge LLP',    'Legal',     'WIRE', 'High'),
            ('V0005', 'Office Plus',      'Office',    'ACH',  'Low')
    ) AS t(vendor_id, vendor_name, category, payment_method, risk_tier)
),

renamed AS (
    SELECT
        vendor_id,
        vendor_name,
        upper(category)         AS vendor_category,
        upper(payment_method)   AS preferred_payment_method,
        risk_tier,
        current_timestamp       AS _loaded_at
    FROM source
)

SELECT * FROM renamed