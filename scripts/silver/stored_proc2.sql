/*
  script purpose: 
    created a stored procedure which cleans, standardizes, and transform the data of each bronze layer
    table and insert into the silver layer tables. And added the duration for each task also used try and catch for error handling.
*/
create or alter procedure silver.load_bronze as
begin 
	begin try
		declare @starttime datetime, @endtime datetime, @batchstarttime datetime, @batchendtime datetime;
		set @batchstarttime = getdate();
		print '-------------------------'
		set @starttime = getdate();
		print '>>> TRUNCATING INTO TABLE';
		Truncate table silver.crm_cust_info
		print '>>> INSERTING INTO TABLE';
		insert into silver.crm_cust_info
		(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim (cst_lastname) as cst_lastname,
		case
		when trim(cst_marital_status)= 'S' then 'single'
		when trim(cst_marital_status) = 'M' then 'married'
		else 'n/a'
		end cst_martial_status,
		case
		when trim(cst_gndr) = 'F' then 'female'
		when trim(cst_gndr) = 'M' then 'male'
		else 'n/a'
		end cst_gndr,
		cst_create_date
		from (select *,
		row_number() over(partition by	cst_id order by cst_create_date DESC)as flag_last
		from bronze.crm_cust_info where cst_id is not null
		)k where flag_last=1;
		set @endtime = getdate();
		print 'duration:' + cast(datediff(second,@starttime,@endtime) as Nvarchar) + 'seconds' ;
		print '################';
		set @starttime = getdate();
		print '>>> TRUNCATING INTO TABLE';
		truncate table silver.crm_prd_info
		print '>>> INSERTING  INTO TABLE';
		insert into silver.crm_prd_info
		(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		select
		prd_id,
		replace(substring(prd_key,1,5),'-','_') cat_id,
		substring(prd_key,7,len(prd_key))as prd_key,
		trim(prd_nm) as prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case 
		when prd_line = 'R' then 'Road'
		when prd_line = 'S' then 'Other Sales'
		when prd_line = 'M' then 'Mountain'
		else 'n/a'
		end prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt )-1 as date) as prd_end_dt
		from bronze.crm_prd_info
		set @endtime = getdate();
		print 'duration:' + cast(datediff(second,@starttime,@endtime) as Nvarchar) + 'seconds' ;
		print '>>>>>>>>>>>>>>>>>>>';
		set @starttime = getdate();
		print '>>> TRUNCATING INTO TABLE';
		truncate table silver.crm_sales_info
		print '>>> INSERTING INTO TABLE';
		insert into silver.crm_sales_info(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_price,
		sls_quantity
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
		when sls_order_dt =0 or len(sls_order_dt)!=8 then null
		else cast(cast(sls_order_dt as varchar) as date) 
		end as sls_order_dt,
		case 
		when sls_ship_dt =0 or len(sls_ship_dt)!=8 then null
		else cast(cast(sls_ship_dt as varchar) as date) 
		end as sls_ship_dt,
		case 
		when sls_due_dt =0 or len(sls_due_dt)!=8 then null
		else cast(cast(sls_due_dt as varchar) as date) 
		end as sls_due_dt,
		case 
		when sls_sales is null or sls_sales<=0 or  sls_sales != sls_quantity * sls_price then sls_quantity * abs(sls_price)
		else sls_sales
		end as sls_sales,
		case
		when sls_price is null or sls_price<=0 then sls_sales / sls_quantity
		else sls_price
		end as sls_price,
		sls_quantity
		from bronze.crm_sales_info
		set @endtime = getdate();
		print 'duration:' + cast(datediff(second,@starttime,@endtime) as Nvarchar) + 'seconds' ;
		print '>>>>>>>>>>>>>>>>>>>';
		set @starttime = getdate();
		print '>>> TRUNCATING INTO TABLE';
		truncate table silver.erp_cust_az12
		print '>>> INSERTING INTO TABLE';
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		select
		case 
		when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
		end as cid,
		case 
		when bdate > getdate() then NULL
		else bdate
		end as bdate,
		case 
		when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
		when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
		else 'n/a'
		end as gen

		from bronze.erp_cust_az12
		set @endtime = getdate();
		print 'duration:' + cast(datediff(second,@starttime,@endtime) as Nvarchar) + 'seconds' ;
		print '>>>>>>>>>>>>>>>>>>>>';
		set @starttime = getdate();
		print '>>> TRUNCATING INTO TABLE';
		truncate table silver.erp_loc_a102
		print '>>> INSERTING INTO TABLE';
		insert into silver.erp_loc_a102(
		cid,
		cntry
		)
		select
		replace(cid, '-','') as cid,
		case 
		when trim(cntry) in ('DE','Germany') then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) is null or cntry = '' then 'n/a'
		else trim(cntry)
		end as cntry

		from bronze.erp_loc_a102
		set @endtime = getdate();
		print 'duration:' + cast(datediff(second,@starttime,@endtime) as Nvarchar) + 'seconds' ;
		print '>>>>>>>>>>>>>>>>>>>>';
		set @starttime = getdate();
		print '>>> TRUNCATING INTO TABLE';
		truncate table silver.erp_px_cat_g1v2
		print '>>> INSERTING INTO TABLE';
		insert into [silver].[erp_px_cat_g1v2]
		(
		id,
		cat,
		subcat,
		maintenence
		)
		select 
		id,
		trim(cat) as cat,
		trim(subcat) as cat,
		trim(maintenence) as maintenence
		from bronze.erp_px_cat_g1v2
		set @endtime = getdate();
		print 'duration:' + cast(datediff(second,@starttime,@endtime) as Nvarchar) + 'seconds' ;
	end try
	begin catch
		PRINT '================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT '================================';
	end catch
	set @batchendtime = getdate();
	print 'silver layer duration:' +cast(datediff(second,@batchstarttime,@batchendtime) as nvarchar)+ 'seconds'
end


    
