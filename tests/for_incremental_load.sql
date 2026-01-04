CREATE OR ALTER PROCEDURE bronze.usp_load_crm_cust_info_incremental
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @last_loaded_date DATE;

    BEGIN TRY
        /*----------------------------------------------------
          1. Ensure staging table exists
        ----------------------------------------------------*/
        IF OBJECT_ID('bronze.stg_crm_cust_info', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.stg_crm_cust_info (
                cst_id INT,
                cst_key NVARCHAR(50),
                cst_firstname NVARCHAR(50),
                cst_lastname NVARCHAR(50),
                cst_marital_status NVARCHAR(50),
                cst_gndr NVARCHAR(50),
                cst_create_date DATE
            );
        END;

        /*----------------------------------------------------
          2. Truncate staging table
        ----------------------------------------------------*/
        TRUNCATE TABLE bronze.stg_crm_cust_info;

        /*----------------------------------------------------
          3. Load CSV into staging
        ----------------------------------------------------*/
        BULK INSERT bronze.stg_crm_cust_info
        FROM 'C:\Dataset\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        /*----------------------------------------------------
          4. Get last loaded date from target
        ----------------------------------------------------*/
        SELECT @last_loaded_date = MAX(cst_create_date)
        FROM bronze.crm_cust_info;

        IF @last_loaded_date IS NULL
            SET @last_loaded_date = '1900-01-01';

        /*----------------------------------------------------
          5. Incremental insert (new records only)
        ----------------------------------------------------*/
        INSERT INTO bronze.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            s.cst_id,
            s.cst_key,
            s.cst_firstname,
            s.cst_lastname,
            s.cst_marital_status,
            s.cst_gndr,
            s.cst_create_date
        FROM bronze.stg_crm_cust_info s
        WHERE s.cst_create_date > @last_loaded_date;

    END TRY
    BEGIN CATCH
        -- Error handling
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @error_severity INT = ERROR_SEVERITY();
        DECLARE @error_state INT = ERROR_STATE();

        RAISERROR (
            'Incremental load failed: %s',
            @error_severity,
            @error_state,
            @error_message
        );
    END CATCH;
END;
GO
