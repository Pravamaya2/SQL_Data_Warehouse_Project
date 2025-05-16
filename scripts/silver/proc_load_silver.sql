/*
========================================================================
Stored Procedure:Load Silver Layer (Bronze -> Silver)
========================================================================
Script Purpose:
  This is stored procedure performs the ETL (Extract, Transform, Load) proces to populate the 'silver'
  Schema tables from the 'bronze' table.
Action Performed:
  - Truncates silver Tables.
  - Inserts transformed and cleaned data from bronze layer to silver layer tables.
Parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
  Exec Silver.load_silver;
==========================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_Start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '==================================';
	PRINT 'Loading Silver Layer';
	PRINT '==================================';

	PRINT '----------------------------------';
	PRINT 'Loading CRM TABLE'
	PRINT '----------------------------------';

	--Loading silver.crm_cust_info
	SET @start_time = GETDATE();
	PRINT '==================================';
	PRINT '>> Truncating the Table silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info
	Print '>>Insert into silver.crm_cust_info';
	PRINT '==================================';
	INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)

	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
		 ELSE 'N/A'
	END cst_marital_status,

	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
		 ELSE 'N/A'
	END cst_gndr,
	cst_create_date
	FROM
		(SELECT *,
			row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
			from bronze.crm_cust_info)x
	where flag_last =1; 
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
	PRINT '------------------------';

	-- Loading silver.crm_prd_info
	SET @start_time = GETDATE();
	PRINT '==================================';
	PRINT '>> Truncating the Table silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting data into silver.crm_prd_info';
	PRINT '==================================';

	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)

	SELECT
		prd_id,
		REPLACE (SUBSTRING(prd_key, 1,5), '-','_') AS cat_id, -- Extract category ID
		SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,		  -- Extract Product Key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'MOUNTAIN'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'
		END prd_line, -- Map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
			LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
			AS DATE)
			AS prd_end_dt -- Calculate end date as one day before the next start date
		from bronze.crm_prd_info

	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
	PRINT '------------------------';

	PRINT '==================================';
	PRINT '>> Truncating the Table silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting data into silver.crm_sales_details';
	PRINT '==================================';

	SET @start_time = GETDATE();

	INSERT INTO silver.crm_sales_details(
	  sls_ord_num,
	  sls_prd_key,
	  sls_cust_id,
	  sls_order_dt,
	  sls_ship_dt,
	  sls_due_dt,
	  sls_sales,
	  sls_quantity,
	  sls_price
	  )

	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
	
		CASE WHEN sls_order_dt =0 or len(sls_order_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt =0 or len(sls_ship_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt =0 or len(sls_due_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
	
		CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price,		-- Derive price if original is invalid
	
		sls_quantity,

		CASE WHEN sls_sales IS NULL or sls_sales <=0 or sls_sales != sls_quantity* ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales	 -- Recalculate the sales if original price is NULL or -ve or 0
	
		FROM bronze.crm_sales_details

	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
	PRINT '------------------------';

	PRINT '==================================';
	PRINT '>> Truncating the Table silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting data into silver.erp_cust_az12';
	PRINT '==================================';

	SET @start_time = GETDATE();
	INSERT INTO silver.erp_cust_az12(
		CID,
		bdate,
		gen
		)

	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) -- Removed prefix NAS from CID
		 ELSE CID
	END AS CID,

	CASE WHEN bdate >GETDATE() THEN NULL		-- Converted the future BDATE as NULL
		ELSE bdate
	END AS bdate,

	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'   -- Normalize gender values and handle the unknown cases
		 WHEN UPPER(TRIM(gen)) IN ( 'M', 'Male') THEN 'Male'
		 ELSE 'N/A'
	END AS gen
	FROM bronze.erp_cust_az12

	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
	PRINT '------------------------';


	PRINT '==================================';
	PRINT '>> Truncating the Table silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting data into silver.erp_loc_a101';
	PRINT '==================================';

	SET @start_time = GETDATE();
	INSERT INTO silver.erp_loc_a101(
		cid,
		CNTRY
		)
	SELECT REPLACE(cid, '-', '') cid,		-- Removed '-' from the cid
		   CASE WHEN trim(cntry)='DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' or CNTRY IS NULL THEN 'N/A'
				ELSE TRIM(cntry)
		   END AS cntry						-- Normalize and handle missing or Blank country codes
		from bronze.erp_loc_a101
	
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
	PRINT '------------------------';

	PRINT '==================================';
	PRINT '>> Truncating the Table silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting data into silver.erp_px_cat_g1v2';
	PRINT '==================================';
	SET @start_time = GETDATE();

	INSERT INTO silver.erp_px_cat_g1v2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)

		select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		from bronze.erp_px_cat_g1v2

	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' +CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
	PRINT '------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '============================================';
		PRINT 'LOADING SILVER LAYER IS COMPLETED'
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================';
	END TRY
	BEGIN CATCH
			PRINT '============================================';
			PRINT 'Error occured During loading Silver layer';
			PRINT 'Error Message' +ERROR_MESSAGE();
			PRINT 'Error Message' +CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' +CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '============================================';
		END CATCH
	END;
