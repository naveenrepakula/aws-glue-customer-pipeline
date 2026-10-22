-- Jira Ticket: COADP-7
-- Purpose: Create reusable country-wise customer summary view
-- Environment: DEV

CREATE OR REPLACE VIEW
    coadp_dev_db.vw_customer_country_summary AS

SELECT
    country,
    COUNT(*) AS total_customers
FROM coadp_dev_db.curated_customers
GROUP BY country;
