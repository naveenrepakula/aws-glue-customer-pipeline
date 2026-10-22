-- Jira Ticket: COADP-7
-- Purpose: Calculate total customers by country
-- Source: coadp_dev_db.curated_customers

SELECT
    country,
    COUNT(*) AS total_customers
FROM coadp_dev_db.curated_customers
GROUP BY country
ORDER BY total_customers DESC;
