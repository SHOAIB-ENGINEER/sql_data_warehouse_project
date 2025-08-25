/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
    BEGIN
        DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
        BEGIN TRY 
        SET @BATCH_START_TIME = GETDATE();
        PRINT '=====================================';
        PRINT 'LOADING THE BRONZE LAYER';
        PRINT '=====================================';

        PRINT '-------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '-------------------------------------';

        SET @START_TIME = GETDATE();
        PRINT '>> TRUNCATING TABLE 1 : bronze.crm_cust_info '
        TRUNCATE table bronze.crm_cust_info
        PRINT '>>>INSERTING DATA INTO TABLE : bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM "E:\yt download\data warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
        WITH (
            FIRSTROW = 2 ,
            FIELDTERMINATOR =',',
            TABLOCK 
        );
        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME)AS NVARCHAR)+ 'SECONDS';
        PRINT '--------------------'

        SET @START_TIME = GETDATE();
        PRINT '>>TRNCATING TABLE 2 : bronze.crm_prd_info'
        TRUNCATE table bronze.crm_prd_info;
        PRINT '>>>INSERTING DATA INTO TABLE : bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM "E:\yt download\data warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
        WITH (
            FIRSTROW=2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME)AS NVARCHAR)+ 'SECONDS';
        PRINT '--------------------'

        SET @START_TIME = GETDATE();
        ALTER TABLE bronze.crm_sales_details ALTER COLUMN sls_ord_num NVARCHAR(50);
        PRINT '>> TRUNCATING TABLE 3 : bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>>> INSERTING DATA INTO TABLE : bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM "E:\yt download\data warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR= ',',
            TABLOCK
        );
        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME)AS NVARCHAR)+ 'SECONDS';
        PRINT '--------------------'

        
        PRINT '-------------------------------------';
        PRINT 'LOADING ERP TABLES';
        PRINT '-------------------------------------';
        SET @START_TIME = GETDATE();
        PRINT '>> TRUNCATING TABLE 4 : bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>>> INSERTING DATA INTO TABLE : bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM "E:\yt download\data warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
        WITH ( 
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME)AS NVARCHAR)+ 'SECONDS';
        PRINT '--------------------'

        SET @START_TIME = GETDATE();
        PRINT '>>TRUNCATING TABLE 5 : bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>>>INSERTING DATA INTO TABLE : bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM "E:\yt download\data warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
        WITH ( 
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
         );
         SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME)AS NVARCHAR)+ 'SECONDS';
        PRINT '--------------------'

        SET @START_TIME = GETDATE();
         PRINT '>>  TRUNCATE TABLE 6 : bronze.erp_px_cat_glv2'
         TRUNCATE TABLE bronze.erp_px_cat_glv2;
         PRINT '>>> INSERTING INTO TABLE : bronze.erp_px_cat_glv2'
         BULK INSERT bronze.erp_px_cat_glv2
         FROM "E:\yt download\data warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
         WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
            SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME)AS NVARCHAR)+ 'SECONDS';
        PRINT '--------------------'
        SET @BATCH_END_TIME = GETDATE();
        PRINT 'LOAD DURATION BATCH TIME '+ CAST (DATEDIFF (SECOND,@BATCH_START_TIME,@BATCH_END_TIME)AS NVARCHAR)+ 'SECONDS';
        END TRY
            BEGIN CATCH 
            PRINT '=========================================='
            PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
            PRINT 'ERROR MESSAGE' + ERROR_MESSAGE ();
            PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER () AS NVARCHAR);
            PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE () AS NVARCHAR)
            PRINT '=========================================='
            END CATCH
     END        


EXEC  bronze.load_bronze 
