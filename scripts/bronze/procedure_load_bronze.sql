/*
======================================================================================
Stored Procedure: Load Bronze Layer
======================================================================================
Script purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It truncates each table in the 'bronze' schema before loading data.
  It also calculates time elapsed for each table loaded.

This stored procedure has no parameters required and does not return any values.

To execute it, run the following code:
  CALL bronze.load_bronze_layer();
======================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze_layer()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=============================================';

    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---';

    -- Load data to crm_cust_info
    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating bronze.crm_cust_info';
        TRUNCATE bronze.crm_cust_info;

        RAISE NOTICE 'Inserting data into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        FROM 'C:\pgsql-access-folder\source_crm\cust_info.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE 'bronze.crm_cust_info duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading bronze.crm_cust_info: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    -- Load data to crm_prd_info
    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating bronze.crm_prd_info';
        TRUNCATE bronze.crm_prd_info;

        RAISE NOTICE 'Inserting data into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        FROM 'C:\pgsql-access-folder\source_crm\prd_info.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE 'bronze.crm_prd_info duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading bronze.crm_prd_info: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    -- Load data to crm_sales_details
    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating bronze.crm_sales_details';
        TRUNCATE bronze.crm_sales_details;

        RAISE NOTICE 'Inserting data into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        FROM 'C:\pgsql-access-folder\source_crm\sales_details.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE 'bronze.crm_sales_details duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading bronze.crm_sales_details: %', SQLERRM;
    END;
    
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '---';

    -- Load data to erp_cust_az12
    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating bronze.erp_cust_az12';
        TRUNCATE bronze.erp_cust_az12;

        RAISE NOTICE 'Inserting data into: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12 (cid, bdate, gen)
        FROM 'C:\pgsql-access-folder\source_erp\CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE 'bronze.erp_cust_az12 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading bronze.erp_cust_az12: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    -- Load data to erp_loc_a101
    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating bronze.erp_loc_a101';
        TRUNCATE bronze.erp_loc_a101;

        RAISE NOTICE 'Inserting data into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101 (cid, cntry)
        FROM 'C:\pgsql-access-folder\source_erp\LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE 'bronze.erp_loc_a101 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading bronze.erp_loc_a101: %', SQLERRM;
    END;
    RAISE NOTICE '---';

    -- Load data to erp_px_cat_g1v2
    BEGIN
        start_time := clock_timestamp();

        RAISE NOTICE 'Truncating bronze.erp_px_cat_g1v2';
        TRUNCATE bronze.erp_px_cat_g1v2;

        RAISE NOTICE 'Inserting data into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        FROM 'C:\pgsql-access-folder\source_erp\PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        RAISE NOTICE 'bronze.erp_px_cat_g1v2 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error loading bronze.erp_px_cat_g1v2: %', SQLERRM;
    END;
    RAISE NOTICE '---';
    
    batch_end_time := clock_timestamp();

    RAISE NOTICE 'Loading Bronze Layer is completed';
    RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
END;
$$;
