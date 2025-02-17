/*
======================
script purpose:
  it creates a stored procedure for inseritng data into the tables form a csv file
  turcating all the rows in table if exists and bulk inerting into table.
  Also checks duration of loading the data into each table.
  try and catch blocks for error handling.
*/
create or alter procedure bronze.load_bronze AS 
begin
	BEGIN TRY
		DECLARE @starttime datetime, @endtime datetime;
		print '===================';
		print 'loading bronze layer';
		print '===================';
		PRINT 'LOADING CRM TABLES';
		PRINT '--------------------';
		SET @starttime = getdate();
		print'>>TRUNCATING bronze.crm_cust_info TABLE';
		PRINT '-------------------';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>>INSERTING  bronze.crm_cust_info TABLE';
		PRINT '-------------------';
		print ''
		BULK INSERT [bronze].[crm_cust_info] 
		FROM 'C:\Users\manish reddy\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' 
		with 
		(firstrow = 2,
		fieldterminator=',',
		tablock);
		SET @endtime = getdate();
		print '>>duration:' + cast(datediff(second,@starttime,@endtime) as NVARCHAR)+ 'seconds';
		print '-----------------------------------------------';
		print'>>TRUNCATING bronze.crm_prd_info TABLE';
		PRINT '-------------------';
		set @starttime= getdate();
		TRUNCATE TABLE [bronze].[crm_prd_info];
		PRINT '>>INSERTING  bronze.crm_prd_info TABLE';
		PRINT '-------------------';
		BULK INSERT [bronze].[crm_prd_info] 
		FROM 'C:\Users\manish reddy\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv' 
		with 
		(firstrow = 2,
		fieldterminator=',',
		tablock);
		SET @endtime = getdate();
		print '>>duration:' + cast(datediff(second,@starttime,@endtime) as NVARCHAR)+ 'seconds';
		print '-----------------------------------------------';
		print'>>TRUNCATING bronze.crm_prd_info TABLE';
		PRINT '-------------------';
		set @starttime= getdate();
		print'>>TRUNCATING bronze.crm_sales_info TABLE';
		PRINT '-------------------';
		TRUNCATE TABLE [bronze].[crm_sales_info];
		PRINT '>>INSERTING  bronze.crm_sales_info TABLE';
		PRINT '-------------------';
		BULK INSERT [bronze].[crm_sales_info]
		FROM 'C:\Users\manish reddy\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv' 
		with 
		(firstrow = 2,
		fieldterminator=',',
		tablock);
		SET @endtime = getdate();
		print '>>duration:' + cast(datediff(second,@starttime,@endtime) as NVARCHAR)+'seconds';
		print '-----------------------------------------------';
		PRINT '-------------------';
		print '===================';
		PRINT 'LOADING ERP TABLES';
		PRINT '--------------------';
		set @starttime= getdate();
		print'>>TRUNCATING bronze.erp_cust_az12 TABLE';
		PRINT '-------------------';
		TRUNCATE TABLE [bronze].[erp_cust_az12];
		print'>>inserting bronze.erp_cust_az12 TABLE';
		PRINT '-------------------';
		BULK INSERT [bronze].[erp_cust_az12]
		FROM 'C:\Users\manish reddy\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with 
		(firstrow = 2,
		fieldterminator=',',
		tablock);
		SET @endtime = getdate();
		print '>>duration:' + cast(datediff(second,@starttime,@endtime) as NVARCHAR)+ 'seconds';
		print '-----------------------------------------------';
		set @starttime= getdate();
		print'>>TRUNCATING bronze.erp_loc_a102 TABLE';
		PRINT '-------------------';
		TRUNCATE TABLE [bronze].[erp_loc_a102];
		print'>>INSERTING VALUES INTO bronze.erp_loc_a102 TABLE';
		PRINT '-------------------';
		BULK INSERT [bronze].[erp_loc_a102]
		FROM 'C:\Users\manish reddy\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with 
		(firstrow = 2,
		fieldterminator=',',
		tablock);
		SET @endtime = getdate();
		print '>>duration:' + cast(datediff(second,@starttime,@endtime) as NVARCHAR) + 'seconds';
		print '-----------------------------------------------';
		set @starttime= getdate();
		print'>>TRUNCATING bronze.erp_PXCAT_G1V2 TABLE';
		PRINT '-------------------';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
		print'>>INSERING VALUES INTO bronze.erp_PXCAT_G1V2 TABLE';
		PRINT '-------------------';
		BULK INSERT [bronze].[erp_px_cat_g1v2]
		FROM "C:\Users\manish reddy\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
		with 
		(firstrow = 2,
		fieldterminator=',',
		tablock);
		SET @endtime = getdate();
		print '>>duration:' + cast(datediff(second,@starttime,@endtime) as NVARCHAR)+  'seconds';
		print '-----------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT '================================';
	END CATCH
end





