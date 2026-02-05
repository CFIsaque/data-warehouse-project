/*
----------------------------------------------------------------------
Quality Checks
----------------------------------------------------------------------
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, and standarization
  across the 'bronze' schemas. It includes checks for:
  - Null or duplicate primary keys
  - Unwanted spaces in string columns
  - Data consistency between related fields

Usage Notes:
  - Run these checks after data loading bronze layer
  - Investigate and resolve any discrepancies found during the checks

----------------------------------------------------------------------
*/

-------------------crm_customer_info----------------------


--- checks for null and duplicates in the primary key ---

SELECT cst_id, count(*) as cont_key
FROM bronze.crm_customer_info
GROUP BY cst_id
HAVING cont_key > 1 OR cst_id IS NULL;

----- checks for NULL values spaces in strings columns --- 

SELECT cst_firstname
FROM bronze.crm_customer_info
WHERE cst_firstname IS NULL

SELECT cst_lastname
FROM bronze.crm_customer_info
WHERE cst_lastname IS NULL

SELECT cst_marital_status
FROM bronze.crm_customer_info
GROUP BY cst_marital_status

SELECT cst_gndr
FROM bronze.crm_customer_info
GROUP BY cst_gndr

----- checks for unwanted spaces in strings columns --- 

SELECT cst_firstname
FROM bronze.crm_customer_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM bronze.crm_customer_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_marital_status
FROM bronze.crm_customer_info
WHERE cst_marital_status != TRIM(cst_marital_status)

SELECT cst_gndr
FROM bronze.crm_customer_info
WHERE cst_gndr != TRIM(cst_gndr)

----- checks for quality in date column --- 

SELECT column_name, data_type
FROM `integration-projeto.bronze.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'crm_customer_info' AND column_name = 'cst_create_date';


-------------------crm_prd_info----------------------

--- checks for null and duplicates in the primary key ---

SELECT prd_id, count(*) as cont_key
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING cont_key > 1 OR prd_id IS NULL;

----- checks for NULL values spaces in strings columns --- 

SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key IS NULL

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm IS NULL

SELECT prd_line, count(*)
FROM bronze.crm_prd_info
GROUP BY prd_line


----- checks for unwanted spaces in strings columns --- 

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT prd_line
FROM bronze.crm_prd_info
WHERE prd_line != TRIM(prd_line)


----- checks for null and negative in in values columns ------

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

----- checks for quality in date column --- 

SELECT column_name, data_type
FROM `integration-projeto.bronze.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'crm_prd_info' AND column_name = 'prd_start_dt';


SELECT column_name, data_type
FROM `integration-projeto.bronze.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'crm_prd_info' AND column_name = 'prd_end_dt';

SELECT prd_start_dt, prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-------------------crm_sales_details----------------------

--- checks for null in the sls_cust_id, sls_prd_key and sls_ord_num  ---

SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key IS NULL;

SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL;

SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL; 


----- checks for quality in INT columns --- 

SELECT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
OR sls_sales IS NULL
OR sls_sales < 0
OR sls_quantity IS NULL
OR sls_quantity < 0
OR sls_price IS NULL
OR sls_price < 0;

----- checks for quality in date column --- 

SELECT column_name, data_type
FROM `integration-projeto.bronze.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'crm_sales_details' AND column_name = 'sls_order_dt';

SELECT column_name, data_type
FROM `integration-projeto.bronze.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'crm_sales_details' AND column_name = 'sls_ship_dt';

SELECT column_name, data_type
FROM `integration-projeto.bronze.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'crm_sales_details' AND column_name = 'sls_due_dt';

SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LENGTH(CAST(sls_order_dt AS STRING)) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101;


SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt 
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101;


SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LENGTH(CAST(sls_due_dt AS STRING)) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101;

SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt;

-------------------erp_erp_px_cat_g1v2----------------------

------------------quality id --------------

SELECT DISTINCT id
FROM bronze.erp_px_cat_g1v2;

SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id);

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2
ORDER BY subcat;

SELECT subcat
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

-------------------erp_cust_az12----------------------

-------- quality in cid column -------


SELECT cid, count(cid) AS t
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING t > 1 OR cid IS NULL;


----- checks for quality in string column --- 

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

SELECT gen
FROM bronze.erp_cust_az12
WHERE gen != TRIM(gen);

SELECT gen, TRIM(gen) 
FROM bronze.erp_cust_az12
WHERE TRIM(gen) = ''

----- checks for quality in date column --- 

SELECT column_name, data_type
FROM `integration-projeto.bronze.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'erp_cust_az12' AND column_name = 'bdate';


SELECT bdate
FROM bronze.erp_cust_az12
WHERE EXTRACT(YEAR FROM bdate) <= 1924
OR EXTRACT(YEAR FROM bdate) > 2026;





