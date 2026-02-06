/*
Quality checks for gold

Script Purpose:
  This script performs quality checks to validate the integrity, consistency,
  and accuracy of the Gold Layer. Theses checks ensure:
  - Uniqueness of surrogate keys in the dimension tables.
  - Uniqueness of surrogate keys in the dimension tables.
  - Referential integrity between fact and dimension tables.
  - Validation of relationships in the data model for analytical purposes.
*/

-- ========================================================================
-- Checking 'gold.dim_products'
-- ========================================================================
-- Check for Uniqueness of Product Key in gold.dim_customers
-- Expectation: No results

SELECT product_key, count(*)
FROM gold.dim_products
GROUP BY product_id
HAVING count(*) > 1;

-- ========================================================================
-- Checking 'gold.dim_customers'
-- ========================================================================
-- Check for Uniqueness of Customers Key in gold.dim_customers
-- Expectation: No results

SELECT customer_key, count(*)
FROM gold.dim_customers
GROUP BY customer_id
HAVING count(*) > 1;

-- ========================================================================
-- Checking 'gold.fact_sales'
-- ========================================================================
-- Check the data model connectivity between fact and dimensions

SELECT * FROM `integration-projeto.gold.fact_sales` s
LEFT JOIN  gold.dim_products p
  ON s.product_key = p.product_key
LEFT JOIN gold.dim_customers c
  ON s.customer_key = c.customer_key
WHERE s.customer_key IS NULL OR s.product_key IS NULL;
