/*
-- ===================================================================
--  DDL Script : CReate Gold Views
-- ===================================================================
Script Purpose:
  This Purpose:
    The script creates views for the Gold layer in the data warehouse.
    The Gold Layer represents the final dimension and Fact tables(Star Schema)
    Each View performs transformations and combines data from Silver layer to produce 
    a clean, enriched, and business - ready dataset.

Usage:
  - These views can be quried directly from analytics and reporting.
-- ==================================================================
*/
-- =================================================================
-- Create Dimension: gold.dim_customers
-- =================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
  DROP VIEW gold.dim_customers;

CREATE VIEW gold.dim_customers 
AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id  AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	CASE WHEN ci.cst_gndr!= 'N/A' THEN ci.cst_gndr  -- CRM is the Master for gender Info
		 ELSE COALESCE(ca.GEN, 'N/A')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
	FROM SILVER.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON		  ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON		  ci.cst_key=la.CID



-- =================================================================
-- Create Dimension: gold.dim_product
-- =================================================================
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
  DROP VIEW gold.dim_product;
GO
CREATE VIEW gold.dim_product
AS
Select
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
	
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- Filterout all historicla data


-- =================================================================
-- Create Fact Table: gold.fact_sales
-- =================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
  DROP VIEW gold.fact_sales;
  
  CREATE VIEW gold.fact_sales
AS
SELECT 
  sd.sls_ord_num AS order_number,
  pr.product_key,
  cs.customer_key,
  sd.sls_order_dt AS order_date,
  sd.sls_ship_dt AS ship_date,
  sd.sls_due_dt AS due_date,
  sd.sls_sales AS sales_amount,
  sd.sls_quantity as quantity,
  sd.sls_price AS price
  FROM silver.crm_sales_details sd
  LEFT JOIN gold.dim_product pr
  ON sd.sls_prd_key = pr.product_number
  LEFT JOIN gold.dim_customers cs
  ON sd.sls_cust_id = cs.customer_id

