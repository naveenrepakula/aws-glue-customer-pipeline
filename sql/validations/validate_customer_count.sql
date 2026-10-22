-- Jira Ticket: COADP-7
-- Purpose: Validate customer counts between
--          the curated table and reporting view
-- Environment: DEV

WITH source_count AS (
    SELECT COUNT(*) AS total
    FROM coadp_dev_db.curated_customers
),
view_count AS (
    SELECT SUM(total_customers) AS total
    FROM coadp_dev_db.vw_customer_country_summary
)
SELECT
    s.total AS source_customers,
    v.total AS view_customers,
    CASE
        WHEN s.total = v.total THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status
FROM source_count s
CROSS JOIN view_count v;
