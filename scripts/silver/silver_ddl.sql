CREATE OR REPLACE TABLE silver.crm_customer_info(
  cst_id INT64,
  cst_key STRING,
  cst_firstname STRING,
  cst_lastname STRING,
  cst_marital_status STRING,
  cst_gndr STRING,
  cst_create_date DATE,
  ingestion_timestamp TIMESTAMP 
);

CREATE OR REPLACE TABLE silver.crm_prd_info(
  prd_id       INT64,
  prd_key      STRING,
  prd_nm       STRING,
  prd_cost     INT64,
  prd_line     STRING,
  prd_start_dt DATE,
  prd_end_dt   DATE,
  ingestion_timestamp TIMESTAMP 
);

CREATE OR REPLACE TABLE silver.crm_sales_details(
  sls_ord_num  STRING,
  sls_prd_key  STRING,
  sls_cust_id  INT64,
  sls_order_dt INT64,
  sls_ship_dt  INT64,
  sls_due_dt   INT64,
  sls_sales    INT64,
  sls_quantity INT64,
  sls_price    INT64,
  ingestion_timestamp TIMESTAMP 
);


CREATE OR REPLACE TABLE silver.erp_cust_az12 (
  cid STRING,
  bdate DATE,
  gen STRING,
  ingestion_timestamp TIMESTAMP  
);

CREATE OR REPLACE TABLE silver.erp_px_cat_g1v2 (
  id STRING,
  cat STRING,
  subcat STRING,
  maintenance BOOL,
  ingestion_timestamp TIMESTAMP  
);
