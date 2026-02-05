/*
Script pourpose
*/



CREATE VIEW gold.dim_customers AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
  c.cst_id AS customer_id,
  c.cst_key AS customer_number,
  c.cst_firstname AS first_name,
  c.cst_lastname AS last_name,

  CASE 
    WHEN c.cst_gndr != 'n/a' THEN c.cst_gndr
    ELSE b.gen
    END AS gender,
  c.cst_marital_status AS marital_status,
  b.bdate AS birthdate,
  c.cst_create_date AS create_date

FROM silver.crm_customer_info c
LEFT JOIN silver.erp_cust_az12 b
  ON cst_key = cid;








CREATE VIEW gold.dim_products AS

SELECT 
  ROW_NUMBER () OVER (ORDER BY p.prd_start_dt, p.prd_key) AS product_key,
  p.prd_id AS product_id,
  p.prd_key AS product_number,
  p.prd_nm AS product_name,
  p.cat_id AS category_id,
  c.cat AS category,
  c.subcat AS subcategory,
  c.maintenance AS maintenance,
  p.prd_cost AS cost,
  p.prd_line AS product_line,
  p.prd_start_dt AS start_date

FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 c
  ON p.cat_id = c.id
WHERE p.prd_end_dt IS NULL; -- Filter out all historical data







CREATE VIEW gold.fact_sales AS

SELECT
  s.sls_ord_num AS order_number,
  p.product_key,
  c.customer_key,
  s.sls_order_dt AS order_date,
  s.sls_ship_dt AS ship_date,
  s.sls_due_dt AS due_date,
  s.sls_sales AS sales_amount,
  s.sls_quantity AS quantity,
  s.sls_price AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_products p
  ON s.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers c
  ON s.sls_cust_id = c.customer_id;
