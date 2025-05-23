--=================================
--DDL Script: Create Bronze Tables
--=================================
-- This script creates tables in the 'bronze' schema, dropping existing tables if they already exist. 
-- NVARCHAR data types are used at this initial layer to ensure all raw data is captured without loss or truncation.
  
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
	cst_id NVARCHAR(50),
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date NVARCHAR(50)
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
	prd_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost NVARCHAR(50),
	prd_line NVARCHAR(50),
	prd_start_dt NVARCHAR(50),
	prd_end_dt NVARCHAR(50)
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_dt NVARCHAR(50),
	sls_ship_dt NVARCHAR(50),
	sls_due_dt NVARCHAR(50),
	sls_sales NVARCHAR(50),
	sls_quantity NVARCHAR(50),
	sls_price NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erm_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erm_cust_az12;
GO

CREATE TABLE bronze.erm_cust_az12 (
	cid NVARCHAR(50),
	bdate NVARCHAR(50),
	gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erm_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erm_loc_a101;
GO

CREATE TABLE bronze.erm_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erm_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erm_px_cat_g1v2;
GO

CREATE TABLE bronze.erm_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
