/*
======================================================================================
Stored Procedure: Load Silver Layer
======================================================================================
Script purpose:
  This stored procedure loads data into the 'silver' schema from data in the 'bronze' schema.
  The data has been transformed into clean data, ready for business use.
  It truncates each table in the 'silver' schema before loading data.
  It also calculates time elapsed for each table loaded.

This stored procedure has no parameters required and does not return any values.

To execute it, run the following code:
  CALL silver.load_silver_layer();
======================================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver_layer()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '=============================================';

    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---';

    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating silver.crm_cust_info';
        TRUNCATE silver.crm_cust_info;
        
        -- Clean bronze.crm_cust_info and insert into silver.crm_cust_info
        INSERT INTO silver.crm_cust_info (
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
            TRIM(cst_firstname) AS cst_firstname,   -- Remove any unnecessary spaces
            TRIM(cst_lastname) AS cst_lastname,     -- Remove any unnecessary spaces
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,  -- Converting codes to descriptive values
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,    -- Converting codes to descriptive values
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last  -- Flagging any duplicate customer_id
            FROM
                bronze.crm_cust_info
        )t 
        WHERE flag_last = 1;

        end_time := clock_timestamp();
        RAISE NOTICE 'silver.crm_cust_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.crm_cust_info: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating silver.crm_prd_info';
        TRUNCATE silver.crm_prd_info;
        
        -- Clean bronze.crm_prd_info and insert into silver.crm_prd_info
        INSERT INTO silver.crm_prd_info (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,  -- Converting NULL values to 0
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Sport'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,    -- Converting codes to descriptive values
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt    -- Calculating the end date as one day before the next start date
        FROM
            bronze.crm_prd_info;
        
        end_time := clock_timestamp();
        RAISE NOTICE 'silver.crm_prd_info: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.crm_prd_info: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating silver.crm_sales_details';
        TRUNCATE silver.crm_sales_details;
        
        -- Clean bronze.crm_sales_details and insert into silver.crm_sales_details
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
            CASE
                WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
            END AS sls_order_dt,    -- Converting text to date format
            CASE
                WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
            END AS sls_ship_dt, -- Converting text to date format
            CASE
                WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
            END AS sls_due_dt,  -- Converting text to date format
            CASE
                WHEN sls_sales IS NULL OR sls_sales = 0 THEN ABS(sls_price) * sls_quantity
                WHEN ABS(sls_sales) != ABS(sls_price) AND sls_quantity = 1 THEN ABS(sls_price)
                WHEN ABS(sls_sales) = ABS(sls_price) and sls_quantity > 1 THEN ABS(sls_price) * sls_quantity
                ELSE ABS(sls_sales)
            END AS sls_sales,   -- Removing NULL values, zero values and replacing values that do not equal sls_price * sls_quantity
            sls_quantity,
            CASE
                WHEN sls_price IS NULL THEN ABS(sls_sales) / sls_quantity
                ELSE ABS(sls_price)
            END AS sls_price    -- Removing NULL values
        FROM 
            bronze.crm_sales_details;

        end_time := clock_timestamp();
        RAISE NOTICE 'silver.crm_sales_details: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.crm_sales_details: %', SQLERRM;
    END;

    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '---';

    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating silver.erp_cust_az12';
        TRUNCATE silver.erp_cust_az12;
        
        -- Clean bronze.erp_cust_az12 and insert into silver.erp_cust_az12
        INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
                ELSE cid
            END AS cid, -- Handle invalid values
            CASE
                WHEN bdate > CURRENT_DATE THEN NULL
                ELSE bdate
            END AS bdate,   -- Handle invalid birth dates
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen  -- Handle invalid values
        FROM
            bronze.erp_cust_az12;

        end_time := clock_timestamp();
        RAISE NOTICE 'silver.erp_cust_az12 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN 
            RAISE WARNING 'Error loading silver.erp_cust_az12: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    BEGIN
        start_time := clock_timestamp();
        
        RAISE NOTICE 'Truncating silver.erp_loc_a101';
        TRUNCATE silver.erp_loc_a101;
        
        -- Clean bronze.erp_loc_a101 and insert into silver.erp_loc_a101
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT DISTINCT
            REPLACE(cid, '-', '') AS cid,   -- Change customer id to correct format
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) in ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry    -- Handle missing values, NULL values and country abbreviations/codes
        FROM
            bronze.erp_loc_a101;

        end_time := clock_timestamp();
        RAISE NOTICE 'silver.erp_loc_a101 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.erp_loc_a101: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating silver.erp_px_cat_g1v2';
        TRUNCATE silver.erp_px_cat_g1v2;
        
        -- Clean bronze.erp_px_cat_g1v2 and insert into silver.erp_px_cat_g1v2
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM
            bronze.erp_px_cat_g1v2;
            
        end_time := clock_timestamp();
        RAISE NOTICE 'silver.erp_px_cat_g1v2 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading silver.erp_px_cat_g1v2: %', SQLERRM;
    END;
    RAISE NOTICE '---';
    
    batch_end_time := clock_timestamp();

    RAISE NOTICE 'Loading Silver Layer is completed';
    RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
END;
$$;
