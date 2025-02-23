/*
  script purpose:
      it creates 3 different views for sales, products, and customers which integrates erm and crp tables together with the help of
      foreign keys.
*/
if object_id('gold.cust_dimen', 'V') is not null
	drop view gold.cust_dimen
create view gold.cust_dimen as
  select 
  ROW_NUMBER() OVER (ORDER BY(cst_id)) as customer_key, 
  ci.cst_id as customer_id,
  ci.cst_key as customer_num,
  ci.cst_firstname as first_name,
  ci.cst_lastname as last_name,
  li.cntry as country,
  case 
  when ci.cst_gndr != 'n/a' then ci.cst_gndr
  else COALESCE(ei.gen, 'n/a')
  end as gender,
  ei.bdate as birth_date,
  ci.cst_marital_status as marital_status,
  ci.cst_create_date as create_date
 
  from silver.crm_cust_info as ci
  left join silver.erp_cust_az12 as ei
  on ci.cst_key = ei.cid
  left join silver.erp_loc_a102 as li
  on ci.cst_key = li.cid
  ----------
  if object_id('gold.pro_dimen', 'V') is not null
	drop view gold.pro_dimen
create view gold.pro_dimen as
	 select
	 row_number() over(order by prd_id,prd_start_dt) as product_key,
	 prd_id as product_id,
	 cat_id as category_id,
	 ev.cat as category,
	 ev.subcat as sub_category,
	 ev.maintenence,
	 prd_key as product_num,
	 prd_nm as product_name,
	 prd_cost as product_cost,
	 prd_line as product_line,
	 prd_start_dt as product_start_date
	 from silver.crm_prd_info as pv
	 left join silver.erp_px_cat_g1v2 as ev
	 on pv.cat_id = ev.id
	 where pv.prd_end_dt is NULL

 ---------------
 if object_id('gold.sales_fact', 'V') is not null
	drop view gold.sales_fact
 create view gold.sales_fact as
	select
	sls_ord_num as order_number,
	cu.customer_key,
	pr.product_key,
	sls_order_dt as order_date,
	sls_ship_dt as shipping_date,
	sls_due_dt as due_date, 
	sls_sales,
	sls_quantity,
	sls_price
	from silver.crm_sales_info as sd
	left join gold.cust_dimen as cu
	on sd.sls_cust_id = cu.customer_id
	left join gold.pro_dimen as pr
	on sd.sls_prd_key = pr.product_num
