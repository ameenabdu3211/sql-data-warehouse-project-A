
-- =========================================================================================
-- Stored Procedure Name: bronze.loud_bronze
-- Purpose: Loads raw CSV data from the source CRM and ERM into the Bronze Layer.
-- This procedure performs bulk insertions after truncating the existing data.
-- It includes duration logging and basic error handling.
-- =========================================================================================
CREATE OR ALTER PROCEDURE bronze.loud_bronze AS
BEGIN  
  DECLARE @start_time DATETIME, @end_time DATETIME,  @batch_start_time DATETIME, @batch_end_time DATETIME;
 
  SET @batch_start_time = GETDATE()

  BEGIN TRY
    PRINT '=================================='
    PRINT 'louding Bronze Layer'
    PRINT '=================================='

    -- Load CRM  tables
    PRINT '----------------------------------'
    PRINT 'louding CRM Tables'
    PRINT '----------------------------------'

    SET @start_time = GETDATE();
    PRINT 'TRUNCATE TABLE : bronze.crm_cust_info'
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT 'BULK INSERT : bronze.crm_cust_info'
    -- Load customer info data from CSV
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\user\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
      FIRSTROW = 2,  -- Skip header
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT CONCAT('>>> Load Duration:', CAST(DATEDIFF(MILLISECOND, @start_time,@end_time) AS NVARCHAR), ' milli second')
    PRINT '######################################'	 

    SET @start_time = GETDATE();
    PRINT 'TRUNCATE TABLE : bronze.crm_prd_info'
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT 'BULK INSERT : bronze.crm_prd_info'
    -- Load product info data
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\user\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT CONCAT('>>> Load Duration:', CAST(DATEDIFF(MILLISECOND, @start_time,@end_time) AS NVARCHAR), ' milli second')
    PRINT '######################################'	

    SET @start_time = GETDATE();
    PRINT 'TRUNCATE TABLE : bronze.crm_sales_details'
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT 'BULK INSERT : bronze.crm_sales_details'
    -- Load sales details data
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\user\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK  
    );
    SET @end_time = GETDATE();
    PRINT CONCAT('>>> Load Duration:', CAST(DATEDIFF(MILLISECOND, @start_time,@end_time) AS NVARCHAR), ' milli second')
    PRINT '######################################'	 

    -- Load ERM tables
    PRINT '----------------------------------'
    PRINT 'louding ERM Tables'
    PRINT '----------------------------------'

    SET @start_time = GETDATE();
    PRINT 'TRUNCATE TABLE : bronze.erm_cust_az12'
    TRUNCATE TABLE bronze.erm_cust_az12;

    PRINT 'BULK INSERT : bronze.erm_cust_az12'
    -- Load ERM customer data
    BULK INSERT bronze.erm_cust_az12
    FROM 'C:\Users\user\Desktop\SQL\sql-data-warehouse-project\datasets\source_erm\CUST_AZ12.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT CONCAT('>>> Load Duration:', CAST(DATEDIFF(MILLISECOND, @start_time,@end_time) AS NVARCHAR), ' milli second')
    PRINT '######################################'	 

    SET @start_time = GETDATE();
    PRINT 'TRUNCATE TABLE : bronze.erm_loc_a101'
    TRUNCATE TABLE bronze.erm_loc_a101;

    PRINT 'BULK INSERT : bronze.erm_loc_a101'
    -- Load ERM location data
    BULK INSERT bronze.erm_loc_a101
    FROM 'C:\Users\user\Desktop\SQL\sql-data-warehouse-project\datasets\source_erm\LOC_A101.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT CONCAT('>>> Load Duration:', CAST(DATEDIFF(MILLISECOND, @start_time,@end_time) AS NVARCHAR), ' milli second')
    PRINT '######################################'	 

    SET @start_time = GETDATE();
    PRINT 'TRUNCATE TABLE : bronze.erm_px_cat_g1v2'
    TRUNCATE TABLE bronze.erm_px_cat_g1v2;

    PRINT 'BULK INSERT : bronze.erm_px_cat_g1v2'
    -- Load ERM price category data
    BULK INSERT bronze.erm_px_cat_g1v2
    FROM 'C:\Users\user\Desktop\SQL\sql-data-warehouse-project\datasets\source_erm\PX_CAT_G1V2.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT CONCAT('>>> Load Duration:', CAST(DATEDIFF(MILLISECOND, @start_time,@end_time) AS NVARCHAR), ' milli second')

    PRINT '###########################################################'
    SET @batch_end_time = GETDATE()
    PRINT 'Loading Bronze Layer is completed'
    PRINT CONCAT('>>> Total Load Duration :', CAST(DATEDIFF(MILLISECOND, @batch_start_time,@batch_end_time) AS NVARCHAR), ' milli second')

  END TRY
  BEGIN CATCH
    PRINT '=============================================='
    PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
    PRINT CONCAT('Error Message: ', ERROR_MESSAGE())
    PRINT CONCAT('Error Number: ', CAST(ERROR_NUMBER() AS NVARCHAR))
    PRINT CONCAT('Error State: ', CAST(ERROR_STATE() AS NVARCHAR))
    PRINT '==============================================='
  END CATCH

END
