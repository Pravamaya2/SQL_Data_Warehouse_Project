
print '===================================================';
prinnt '1. Creating a Stagging table(if it doen't exist)';
print '===================================================';
  
IF OBJECT_ID('staging.crm_cust_info', 'U') IS NULL
BEGIN
    CREATE TABLE staging.crm_cust_info (
        -- Use the same schema as bronze.crm_cust_info
        CustomerID NVARCHAR(50),
        Name NVARCHAR(100),
        Email NVARCHAR(100),
        -- add all other columns here...
        UpdateDate DATETIME -- optional, for better incremental handling
    );
END;

print '===================================================';
prinnt '1. Truncate and Load into the Stagging table';
print '===================================================';
  
TRUNCATE TABLE staging.crm_cust_info;

BULK INSERT staging.crm_cust_info
FROM 'C:\Dataset\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW=2,
    FIELDTERMINATOR =',',
    TABLOCK
);

print '===================================================';
prinnt 'Merge the Incremental data to bronze layer';
print '===================================================';

MERGE bronze.crm_cust_info AS target
USING staging.crm_cust_info AS source
ON target.CustomerID = source.CustomerID  -- adjust key as needed

WHEN MATCHED THEN
    UPDATE SET
        target.Name = source.Name,
        target.Email = source.Email
        -- add other column updates as needed

WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerID, Name, Email)  -- list all columns
    VALUES (source.CustomerID, source.Name, source.Email);
