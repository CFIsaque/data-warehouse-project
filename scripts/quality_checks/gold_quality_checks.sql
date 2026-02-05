/*
Quality checks for gold
*/

SELECT product_id, count(*)
FROM gold.dim_products
GROUP BY product_id
HAVING count(*) > 1;



SELECT customer_id, count(*)
FROM gold.dim_customers
GROUP BY customer_id
HAVING count(*) > 1;



SELECT * FROM `integration-projeto.gold.fact_sales` s
LEFT JOIN  gold.dim_products p
  ON s.product_key = p.product_key
LEFT JOIN gold.dim_customers c
  ON s.customer_key = c.customer_key
WHERE s.customer_key IS NULL OR s.product_key IS NULL;
