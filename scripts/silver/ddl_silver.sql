/*
script purpose: 
    created tables for silver schema added a new column 'dwh_cerate_date' for each table and also changed the
    datatypes of some columns in some tables with transformations respectively.
*/

IF OBJECT_ID('silver.crm_cust_info', 'u') IS NOT NULL
	DROP TABLE silver.crm_cust_info;

create table silver.crm_cust_info (
	cst_id int,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(10),
	cst_create_date DATE,
	dwh_create_date DATETIME DEFAULT getdate()
);

IF OBJECT_ID('silver.crm_prd_info', 'u') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

create table silver.crm_prd_info (
	prd_id int,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost int,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	dwh_create_date DATETIME DEFAULT getdate()
);

IF OBJECT_ID('silver.crm_sales_info', 'u') IS NOT NULL
	DROP TABLE silver.crm_sales_info;

create table silver.crm_sales_info (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id int,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME DEFAULT getdate()
);

IF OBJECT_ID('silver.erp_cust_az12', 'u') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;

create table silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT getdate()
);

IF OBJECT_ID('silver.erp_loc_a102', 'u') IS NOT NULL
	DROP TABLE silver.erp_loc_a102;

create table silver.erp_loc_a102 (
	cid NVARCHAR(50),
	cntry VARCHAR(50),
	dwh_create_date DATETIME DEFAULT getdate()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'u') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
create table silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenence NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT getdate()
)

