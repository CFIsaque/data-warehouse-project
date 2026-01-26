/*
------------------------------------------------------------------
Stored Procedure: Load Bronze Layer (Source -> Bronze)
-----------------------------------------------------------------
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from CSV files in the Cloud Storage.
  It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'LOAD DATA INTO' command to load data from the csv files to bronze tables.

Paramanters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Exemple: 
    CALL `integration-projeto.bronze.load_bronze`();
--------------------------------------------------------------
*/


CREATE OR REPLACE PROCEDURE `integration-projeto.bronze.load_bronze`()
BEGIN

--- Time variables -----

  DECLARE start_time TIMESTAMP;
  DECLARE end_time TIMESTAMP;
  DECLARE duration_seconds INT64; 


--------------------------- CRM TABLES --------------------

  SELECT 'Loading Bronze Layer' AS log_message; 

  SELECT 'Loading CRM Tables' AS log_message;

  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: bronze.crm_customer_info' AS log_message;
  TRUNCATE TABLE bronze.crm_customer_info;

  SELECT '>> Inserting Data Into: bronze.crm_customer_info' AS log_message;
  LOAD DATA INTO bronze.crm_customer_info
  FROM FILES (
    format = 'CSV',
    uris = ['gs://ingestion-bucket-crm/cust_info.csv'],
    skip_leading_rows = 1
  );

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' AS log_message, duration_seconds AS value;


  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: bronze.crm_prd_info' AS log_message;
  TRUNCATE TABLE bronze.crm_prd_info;

  SELECT '>> Inserting Data Into: bronze.crm_prd_infoo' AS log_message;
  LOAD DATA INTO bronze.crm_prd_info
  FROM FILES (
    format = 'CSV',
    uris = ['gs://ingestion-bucket-crm/prd_info.csv'],
    skip_leading_rows = 1
  );

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds AS value;

  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: bronze.crm_sales_details' AS log_message;
  TRUNCATE TABLE bronze.crm_sales_details;

  SELECT '>> Inserting Data Into: bronze.crm_sales_details' AS log_message;
  LOAD DATA INTO bronze.crm_sales_details
  FROM FILES (
    format = 'CSV',
    uris = ['gs://ingestion-bucket-crm/sales_details.csv'],
    skip_leading_rows = 1
  );

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds AS value;

  SET start_time = CURRENT_TIMESTAMP();

  -------------- ERP TABLES --------------------

  SELECT 'Loading ERP Tables' AS log_message;

  SELECT '>> Truncating Table: bronze.erp_cust_az12' AS log_message;
  TRUNCATE TABLE bronze.erp_cust_az12;

  SELECT '>> Inserting Data Into: bronze.erp_cust_az12' AS log_message;
  LOAD DATA INTO bronze.erp_cust_az12
  FROM FILES (
    format = 'CSV',
    uris = ['gs://ingestion-bucket-crm/CUST_AZ12.csv'],
    skip_leading_rows = 1
  );

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds as value;


  SET start_time = CURRENT_TIMESTAMP();

  SELECT '>> Truncating Table: bronze.erp_px_cat_g1v2' AS log_message;
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;

  SELECT '>> Inserting Data Into: bronze.erp_px_cat_g1v2' AS log_message;
  LOAD DATA INTO bronze.erp_px_cat_g1v2
  FROM FILES (
    format = 'CSV',
    uris = ['gs://ingestion-bucket-crm/PX_CAT_G1V2.csv'],
    skip_leading_rows = 1
  );

  SELECT 'Bronze Layer loaded sucessfully' AS log_message; 

  SET end_time = CURRENT_TIMESTAMP();
  SET duration_seconds = TIMESTAMP_DIFF(end_time, start_time, SECOND);

  SELECT 'Load time (seconds)' as log_message, duration_seconds as value;



EXCEPTION WHEN ERROR THEN

  SELECT 'Failed Load Bronze Layer' AS status,
    @@error.message AS error_message,
    @@error.statement_text AS failed_statement;

END;
