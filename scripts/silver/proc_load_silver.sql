/*
-----------------------------------------------------------
Stored Procedure: Load Silver Layer (Bronze -> Silver)
-----------------------------------------------------------

Script purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) 
  processe to populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
  - Truncates Silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Exemple:

CALL `integration-projeto.silver.load_silver`();

-----------------------------------------------------------
*/


CREATE OR REPLACE PROCEDURE `integration-projeto.silver.load_silver`()
BEGIN

------ Time variables --------

  DECLARE start_time TIMESTAMP;
  DECLARE end_time TIMESTAMP;
  DECLARE duration_seconds INT64;


--------------------------- CRM TABLES --------------------

  SELECT 'Loading Silver Layer' AS log_message; 

  SELECT 'Loading CRM Tables' AS log_message;

  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: silver.crm_customer_info' AS log_message;

  TRUNCATE TABLE silver.crm_customer_info;

  SELECT '>> Inserting Data Into: silver.crm_customer_info' AS log_message;

  INSERT INTO silver.crm_customer_info 
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, 
 cst_gndr, cst_create_date, ingestion_timestamp)

  SELECT cst_id, cst_key,
  (CASE
      WHEN cst_firstname IS NULL THEN 'n/a'
      ELSE TRIM(cst_firstname)
      END) AS cst_firstname,
  (CASE
      WHEN cst_lastname IS NULL THEN 'n/a'
      ELSE TRIM(cst_lastname)
      END) AS cst_lastname,
  (CASE
      WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
      WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
      ELSE 'n/a'
      END) AS cst_marital_status,
  (CASE
      WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
      WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
      ELSE 'n/a'
      END) AS cst_gndr,
  cst_create_date,
  CURRENT_TIMESTAMP()

  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
  FROM bronze.crm_customer_info
  WHERE cst_id IS NOT NULL) t 
  WHERE flag_last = 1;

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' AS log_message, duration_seconds AS value;


  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: silver.crm_prd_info' AS log_message;
  TRUNCATE TABLE silver.crm_prd_info;

  SELECT '>> Inserting Data Into: silver.crm_prd_infoo' AS log_message;
  INSERT INTO silver.crm_prd_info

(prd_id, cat_id, prd_key, prd_nm, prd_cost, 
prd_line, prd_start_dt, prd_end_dt, ingestion_timestamp)

  SELECT prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS pr_key,
  prd_nm,
  IFNULL(prd_cost, 0) AS prd_cost,
  CASE 
    WHEN TRIM(prd_line) = 'M' THEN 'Mountain'
    WHEN TRIM(prd_line) = 'R' THEN 'Road'
    WHEN TRIM(prd_line) = 'T' THEN 'Touring'
    WHEN TRIM(prd_line) = 'S' THEN 'Other sales'
    ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as prd_end_dt,
    CURRENT_TIMESTAMP()

  FROM bronze.crm_prd_info;

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds AS value;

  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: silver.crm_sales_details' AS log_message;
  TRUNCATE TABLE silver.crm_sales_details;
  SELECT '>> Inserting Data Into: bronze.crm_sales_details' AS log_message;

  INSERT INTO silver.crm_sales_details

  (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
  sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price, ingestion_timestamp)

  SELECT sls_ord_num, sls_prd_key, sls_cust_id,
  CASE
    WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
    ELSE PARSE_DATE('%Y%m%d', CAST(sls_order_dt AS STRING))
    END AS sls_order_dt,
  CASE
    WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) !=8 THEN NULL
    ELSE PARSE_DATE('%Y%m%d', CAST(sls_ship_dt AS STRING))
    END AS sls_ship_dt,
  CASE
    WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
    ELSE PARSE_DATE('%Y%m%d', CAST(sls_due_dt AS STRING)) 
    END AS sls_due_dt,
  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
    END AS sls_sales,
  sls_quantity,
  CASE
    WHEN sls_price IS NULL OR sls_price <= 0
    THEN CAST((sls_sales / sls_quantity) AS INT64) 
    ELSE sls_price
    END AS sls_price,
  CURRENT_TIMESTAMP() 
  FROM bronze.crm_sales_details;

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds AS value;

  -------------- ERP TABLES --------------------

  SET start_time = CURRENT_TIMESTAMP();

  SELECT 'Loading ERP Tables' AS log_message;

  SELECT '>> Truncating Table: silver.erp_cust_az12' AS log_message;
  TRUNCATE TABLE silver.erp_cust_az12;

  SELECT '>> Inserting Data Into: silver.erp_cust_az12' AS log_message;
  INSERT INTO silver.erp_cust_az12
  (cid, bdate, gen, ingestion_timestamp)

  SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
    END AS cid,
  CASE
    WHEN bdate > CURRENT_DATE() THEN NULL
    ELSE bdate
    END AS bdate,
  CASE
    WHEN TRIM(gen) IN ('M', 'Male') THEN 'Male'
    WHEN TRIM(gen) IN ('F', 'Female') THEN 'Female'
    ELSE 'n/a'
    END AS gen,
  CURRENT_TIMESTAMP()
  FROM bronze.erp_cust_az12;

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds as value;

  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: silver.erp_px_cat_g1v2' AS log_message;
  TRUNCATE TABLE silver.erp_px_cat_g1v2;

  SELECT '>> Inserting Data Into: silver.erp_px_cat_g1v2' AS log_message;
  INSERT INTO silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance, ingestion_timestamp)

  SELECT id, cat, subcat,
  CASE
    WHEN maintenance IS FALSE THEN 'No'
    ELSE 'Yes'
    END AS maintenance,
  CURRENT_TIMESTAMP() AS ingestion_timestamp
  FROM bronze.erp_px_cat_g1v2;

  SELECT 'Silver Layer loaded sucessfully' AS log_message; 

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds as value;

EXCEPTION WHEN ERROR THEN

  SELECT 'Failed Load Silver Layer' AS status,
    @@error.message AS error_message,
    @@error.statement_text AS failed_statement;

END;
