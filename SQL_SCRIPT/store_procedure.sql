-- STORE PROCEDURES FOR ORCHESTRAION 

CREATE OR ALTER PROCEDURE Integration.usp_Load_DIM_PRODUCT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RowsInserted INT = 0, @RowsUpdated INT = 0;

    BEGIN TRY
      BEGIN TRAN;

      -- temp table to capture MERGE actions
      IF OBJECT_ID('tempdb..#mergeActions') IS NOT NULL DROP TABLE #mergeActions;
      CREATE TABLE #mergeActions ([action] NVARCHAR(10));

      MERGE Integration.TBL_DIM_PRODUCT AS tgt
      USING (
        SELECT 
           s.PRODUCT_NUMBER,
           l.PRODUCT_NAME,
           s.BASE_PRODUCT_NUMBER,
           s.PRODUCT_COLOR_CODE,
           s.PRODUCT_SIZE_CODE,
           s.PRODUCT_BRAND_CODE,
           s.PRODUCTION_COST,
           s.INTRODUCTION_DATE,
           s.DISCONTINUED_DATE
        FROM stg.PRODUCT s
        LEFT JOIN Integration.TBL_DIM_PRODUCT_NAME_LKP l
           ON s.PRODUCT_NUMBER = l.PRODUCT_NUMBER
      ) AS src
      ON tgt.PRODUCT_NUMBER = src.PRODUCT_NUMBER

      WHEN MATCHED THEN
        UPDATE SET
          tgt.PRODUCT_NAME = src.PRODUCT_NAME,
          tgt.BASE_PRODUCT_NUMBER = src.BASE_PRODUCT_NUMBER,
          tgt.PRODUCT_COLOR_CODE = src.PRODUCT_COLOR_CODE,
          tgt.PRODUCT_SIZE_CODE  = src.PRODUCT_SIZE_CODE,
          tgt.PRODUCT_BRAND_CODE = src.PRODUCT_BRAND_CODE,
          tgt.PRODUCTION_COST    = src.PRODUCTION_COST,
          tgt.INTRODUCTION_DATE  = src.INTRODUCTION_DATE,
          tgt.DISCONTINUED_DATE  = src.DISCONTINUED_DATE,
          tgt.SOURCE_ID          = 'INTERNAL DB',
          tgt.UPDATE_DATE        = GETDATE()

      WHEN NOT MATCHED BY TARGET THEN
        INSERT (
          PRODUCT_NUMBER, PRODUCT_NAME, BASE_PRODUCT_NUMBER,
          PRODUCT_COLOR_CODE, PRODUCT_SIZE_CODE, PRODUCT_BRAND_CODE,
          PRODUCTION_COST, INTRODUCTION_DATE, DISCONTINUED_DATE,
          SOURCE_ID, DATA_DATE, UPDATE_DATE
        )
        VALUES (
          src.PRODUCT_NUMBER, src.PRODUCT_NAME, src.BASE_PRODUCT_NUMBER,
          src.PRODUCT_COLOR_CODE, src.PRODUCT_SIZE_CODE, src.PRODUCT_BRAND_CODE,
          src.PRODUCTION_COST, src.INTRODUCTION_DATE, src.DISCONTINUED_DATE,
          'INTERNAL DB', CAST(GETDATE() AS DATE), GETDATE()
        )
      OUTPUT $action INTO #mergeActions;

      SET @RowsInserted = (SELECT COUNT(1) FROM #mergeActions WHERE [action] = 'INSERT');
      SET @RowsUpdated  = (SELECT COUNT(1) FROM #mergeActions WHERE [action] = 'UPDATE');

      -- Insert audit row
      INSERT INTO dbo.TBL_LOAD_AUDIT
      (JOB_NAME, START_TIME, END_TIME, ROWS_INSERTED, ROWS_UPDATED, STATUS, NOTES)
      VALUES ('usp_Load_DIM_PRODUCT', SYSUTCDATETIME() , SYSUTCDATETIME(), @RowsInserted, @RowsUpdated, 'SUCCESS', NULL);

      COMMIT;
      -- Return counts so ADF can pick them up if needed
      SELECT @RowsInserted AS RowsInserted, @RowsUpdated AS RowsUpdated;

    END TRY
    BEGIN CATCH
      ROLLBACK;
      DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
      INSERT INTO Integration.TBL_LOAD_AUDIT (JOB_NAME, START_TIME, END_TIME, ROWS_INSERTED, ROWS_UPDATED, STATUS, NOTES)
      VALUES ('usp_Load_DIM_PRODUCT', SYSUTCDATETIME(), SYSUTCDATETIME(), 0, 0, 'FAILED', @ErrMsg);
      THROW;
    END CATCH
END
GO


---FOR ORDER TABLE

CREATE OR ALTER PROCEDURE Integration.sp_Load_TBL_DIM_ORDER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LoadDate DATETIME = GETDATE();

    BEGIN TRY
        BEGIN TRANSACTION;

        -------------------------
        -- A) EXPIRE changed active rows
        -- (mark END_EFFECTIVE_DATE on existing active rows if SCD2 columns changed)
        -------------------------
        UPDATE tgt
        SET 
            tgt.END_EFFECTIVE_DATE = @LoadDate,
            tgt.UPDATE_DATE = @LoadDate,
            tgt.CHANGE_INFO = 'U'
        FROM Integration.TBL_DIM_ORDER AS tgt
        INNER JOIN stg.vw_order_source AS src
            ON tgt.ORDER_DETAIL_CODE = src.ORDER_DETAIL_CODE
           AND tgt.ORDER_NUMBER = src.ORDER_NUMBER
        WHERE tgt.END_EFFECTIVE_DATE IS NULL
          AND (
               ISNULL(tgt.BRANCH_CODE, '') <> ISNULL(src.BRANCH_CODE, '')
            OR ISNULL(tgt.WAREHOUSE_BRANCH_CODE, '') <> ISNULL(src.WAREHOUSE_BRANCH_CODE, '')
            OR ISNULL(tgt.COUNTRY_CODE, '') <> ISNULL(src.COUNTRY_CODE, '')
            OR ISNULL(tgt.WAREHOUSE_CITY, '') <> ISNULL(src.WAREHOUSE_CITY, '')
            OR ISNULL(tgt.QUANTITY, -999999) <> ISNULL(src.QUANTITY, -999999)
            OR ISNULL(tgt.UNIT_COST, -999999.9999) <> ISNULL(src.UNIT_COST, -999999.9999)
            OR ISNULL(tgt.UNIT_PRICE, -999999.9999) <> ISNULL(src.UNIT_PRICE, -999999.9999)
            OR ISNULL(tgt.UNIT_SALE_PRICE, -999999.9999) <> ISNULL(src.UNIT_SALE_PRICE, -999999.9999)
            OR ISNULL(CONVERT(VARCHAR(10),tgt.ORDER_CLOSE_DATE,120),'1900-01-01') 
                <> ISNULL(CONVERT(VARCHAR(10),src.ORDER_CLOSE_DATE,120),'1900-01-01')
          );

        -------------------------
        -- B) INSERT new rows (new business keys OR versions for changed keys)
        -- If an active row exists (END_EFFECTIVE_DATE IS NULL) we do NOT insert;
        -- if active row DOES NOT exist (either never existed, or we expired it above),
        -- insert a new row using DATA_DATE from previous active row if present.
        -------------------------
        INSERT INTO Integration.TBL_DIM_ORDER
        (
            ORDER_DETAIL_CODE, ORDER_NUMBER, BRANCH_CODE, WAREHOUSE_BRANCH_CODE, COUNTRY_CODE,
            WAREHOUSE_CITY, SHIP_DATE, QUANTITY, UNIT_COST, UNIT_PRICE, UNIT_SALE_PRICE,
            ORDER_DATE, ORDER_CLOSE_DATE,
            SOURCE_ID, DATA_DATE, UPDATE_DATE,
            BEGIN_EFFECTIVE_DATE, END_EFFECTIVE_DATE, VERSION_NUM, CHANGE_INFO
        )
        SELECT
            src.ORDER_DETAIL_CODE,
            src.ORDER_NUMBER,
            src.BRANCH_CODE,
            src.WAREHOUSE_BRANCH_CODE,
            src.COUNTRY_CODE,
            src.WAREHOUSE_CITY,
            src.SHIP_DATE,
            src.QUANTITY,
            src.UNIT_COST,
            src.UNIT_PRICE,
            src.UNIT_SALE_PRICE,
            src.ORDER_DATE,
            src.ORDER_CLOSE_DATE,
            'ORDER' AS SOURCE_ID,
            -- preserve DATA_DATE from previous active row if it exists, else use load date
            COALESCE(prev.DATA_DATE, CONVERT(date,@LoadDate)) AS DATA_DATE,
            @LoadDate AS UPDATE_DATE,
            @LoadDate AS BEGIN_EFFECTIVE_DATE,
            NULL AS END_EFFECTIVE_DATE,
            COALESCE(prev.VERSION_NUM,0) + 1 AS VERSION_NUM,
            'I' AS CHANGE_INFO
        FROM stg.vw_order_source src
        LEFT JOIN Integration.TBL_DIM_ORDER prev
            ON prev.ORDER_DETAIL_CODE = src.ORDER_DETAIL_CODE
           AND prev.ORDER_NUMBER = src.ORDER_NUMBER
           AND prev.END_EFFECTIVE_DATE IS NULL
        WHERE prev.ORDER_DETAIL_CODE IS NULL
           -- prev is null means either totally new business key, or we expired it above for change

        ;

        -------------------------
        -- C) PASS-THROUGH UPDATE: For active rows that are NOT changing in SCD2 fields,
        -- update pass-through fields (SHIP_DATE, ORDER_DATE) and UPDATE_DATE
        -------------------------
        UPDATE tgt
        SET 
            tgt.SHIP_DATE = src.SHIP_DATE,
            tgt.ORDER_DATE = src.ORDER_DATE,
            tgt.UPDATE_DATE = @LoadDate
        FROM Integration.TBL_DIM_ORDER tgt
        INNER JOIN stg.vw_order_source src
            ON tgt.ORDER_DETAIL_CODE = src.ORDER_DETAIL_CODE
           AND tgt.ORDER_NUMBER = src.ORDER_NUMBER
        WHERE tgt.END_EFFECTIVE_DATE IS NULL
          AND NOT (
               ISNULL(tgt.BRANCH_CODE, '') <> ISNULL(src.BRANCH_CODE, '')
            OR ISNULL(tgt.WAREHOUSE_BRANCH_CODE, '') <> ISNULL(src.WAREHOUSE_BRANCH_CODE, '')
            OR ISNULL(tgt.COUNTRY_CODE, '') <> ISNULL(src.COUNTRY_CODE, '')
            OR ISNULL(tgt.WAREHOUSE_CITY, '') <> ISNULL(src.WAREHOUSE_CITY, '')
            OR ISNULL(tgt.QUANTITY, -999999) <> ISNULL(src.QUANTITY, -999999)
            OR ISNULL(tgt.UNIT_COST, -999999.9999) <> ISNULL(src.UNIT_COST, -999999.9999)
            OR ISNULL(tgt.UNIT_PRICE, -999999.9999) <> ISNULL(src.UNIT_PRICE, -999999.9999)
            OR ISNULL(tgt.UNIT_SALE_PRICE, -999999.9999) <> ISNULL(src.UNIT_SALE_PRICE, -999999.9999)
            OR ISNULL(CONVERT(VARCHAR(10),tgt.ORDER_CLOSE_DATE,120),'1900-01-01') 
                <> ISNULL(CONVERT(VARCHAR(10),src.ORDER_CLOSE_DATE,120),'1900-01-01')
          );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
	ROLLBACK;
	DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
      INSERT INTO Integration.TBL_LOAD_AUDIT (JOB_NAME, START_TIME, END_TIME, ROWS_INSERTED, ROWS_UPDATED, STATUS, NOTES)
      VALUES ('sp_Load_TBL_DIM_ORDER', SYSUTCDATETIME(), SYSUTCDATETIME(), 0, 0, 'FAILED', @ErrMsg);
      THROW; 
    END CATCH
END;
GO


---FOR COUNTRY TABLE

CREATE PROCEDURE Integration.usp_MergeDimCountryLkp
AS
BEGIN
    SET NOCOUNT ON;

    MERGE Integration.TBL_DIM_COUNTRY_LKP AS tgt
    USING (
        SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE
        FROM stg.COUNTRY
    ) AS src
    ON tgt.COUNTRY_CODE = src.COUNTRY_CODE

    -- If record exists, update it (SCD1 overwrite)
    WHEN MATCHED THEN
        UPDATE SET 
            tgt.COUNTRY_ID = src.COUNTRY_ID,
            tgt.COUNTRY_NAME = src.COUNTRY_NAME,
            tgt.UPDATE_DATE = GETDATE()

    -- If record is new, insert it
    WHEN NOT MATCHED THEN
        INSERT (COUNTRY_KEY, COUNTRY_CODE, COUNTRY_ID, COUNTRY_NAME, SOURCE_ID, DATA_DATE, UPDATE_DATE)
        VALUES (
            (SELECT ISNULL(MAX(COUNTRY_KEY),0)+1 FROM Integration.TBL_DIM_COUNTRY_LKP),
            src.COUNTRY_CODE,
            src.COUNTRY_ID,
            src.COUNTRY_NAME, 
            'LKP_FILE',
            GETDATE(),
            GETDATE()
        );
END;
GO


CREATE PROCEDURE Integration.usp_MergeDimReturnLkp
AS
BEGIN
    SET NOCOUNT ON;
	MERGE Integration.TBL_DIM_RETURN_REASON_LKP AS tgt
USING (
    SELECT RETURN_REASON_CODE, REASON_DESCRIPTION_EN AS RETURN_REASON_DESC
    FROM stg.RETURN_REASON
) AS src
ON tgt.RETURN_REASON_CODE = src.RETURN_REASON_CODE

-- If record exists, update it (SCD1 overwrite)
WHEN MATCHED THEN
    UPDATE SET tgt.RETURN_REASON_DESC = src.RETURN_REASON_DESC,
               tgt.UPDATE_DATE = GETDATE()

-- If record is new, insert it
WHEN NOT MATCHED THEN
    INSERT (RETURN_REASON_CODE, RETURN_REASON_DESC, SOURCE_ID, DATA_DATE, UPDATE_DATE)
    VALUES (src.RETURN_REASON_CODE, src.RETURN_REASON_DESC, 'LKP_FILE',GETDATE() , GETDATE());
END;
GO

--FOR ORDER METHOD

CREATE PROCEDURE Integration.usp_MergeDimOrderMethodLkp
AS
BEGIN
    SET NOCOUNT ON;
MERGE integration.TBL_DIM_ORDER_METHOD_LKP AS tgt
USING (
    SELECT ORDER_METHOD_CODE AS METHOD_CODE, ORDER_METHOD_EN AS METHOD_NAME
    FROM stg.ORDER_METHOD
) AS src
ON tgt.METHOD_CODE = src.METHOD_CODE

-- If record exists, update it (SCD1 overwrite)
WHEN MATCHED THEN
    UPDATE SET tgt.METHOD_NAME = src.METHOD_NAME,
               tgt.UPDATE_DATE = GETDATE()

-- If record is new, insert it
WHEN NOT MATCHED THEN
    INSERT (METHOD_CODE, METHOD_NAME, SOURCE_ID, DATA_DATE, UPDATE_DATE)
    VALUES (src.METHOD_CODE, src.METHOD_NAME, 'LKP_FILE',GETDATE() , GETDATE());
END;
GO

--FOR RETAILER

CREATE PROCEDURE Integration.usp_MergeDimRetailerlkp
AS
BEGIN
    SET NOCOUNT ON;
MERGE Integration.TBL_DIM_RETAILER_LKP AS tgt
USING (
    SELECT RETAILER_NAME, RETAILER_SITE_CODE,RETAILER_CONTACT_CODE
    FROM stg.RETAILER
) AS src
ON tgt.RETAILER_SITE_CODE = src.RETAILER_SITE_CODE

-- If record exists, update it (SCD1 overwrite)
WHEN MATCHED THEN
    UPDATE SET tgt.RETAILER_CONTACT_CODE = src.RETAILER_CONTACT_CODE,
               tgt.UPDATE_DATE = GETDATE()

-- If record is new, insert it
WHEN NOT MATCHED THEN
    INSERT (RETAILER_NAME, RETAILER_SITE_CODE,RETAILER_CONTACT_CODE, SOURCE_ID, DATA_DATE, UPDATE_DATE)
    VALUES (src.RETAILER_NAME, src.RETAILER_SITE_CODE,src.RETAILER_CONTACT_CODE, 'LKP_FILE',GETDATE() , GETDATE());
END;
GO

--FOR WAREHOUSE

CREATE PROCEDURE Integration.usp_MergeDimWarehouseLkp
AS
BEGIN
    SET NOCOUNT ON;
MERGE Integration.TBL_DIM_WAREHOUSE_LKP AS tgt
USING(
SELECT BRANCH_CODE,ADDRESS1 AS ADDRESS,CITY,POSTAL_ZONE AS POSTAL_CODE,COUNTRY_CODE,WAREHOUSE_BRANCH_CODE
FROM stg.WAREHOUSE) AS src
ON tgt.BRANCH_CODE=src.BRANCH_CODE

WHEN MATCHED THEN 
		UPDATE SET tgt.ADDRESS=src.ADDRESS,
		           tgt.CITY=src.CITY,
				   tgt.POSTAL_CODE=src.POSTAL_CODE,
				   tgt.COUNTRY_CODE=src.COUNTRY_CODE,
					tgt.UPDATE_DATE=GETDATE()
WHEN NOT MATCHED THEN
       INSERT (BRANCH_CODE,ADDRESS,CITY,POSTAL_CODE,COUNTRY_CODE,WAREHOUSE_BRANCH_CODE,SOURCE_ID,DATA_DATE,UPDATE_DATE)
	   VALUES (src.BRANCH_CODE,src.ADDRESS,src.CITY,src.POSTAL_CODE,src.COUNTRY_CODE,src.WAREHOUSE_BRANCH_CODE,'LKP_FILE',GETDATE(),GETDATE());
END;
GO

---FOR PRODUCT NAME LOOKUP
CREATE PROCEDURE Integration.usp_MergeDimProductNameLkp
AS
BEGIN
    SET NOCOUNT ON;
MERGE Integration.TBL_DIM_PRODUCT_NAME_LKP AS tgt
USING (
    SELECT PRODUCT_NUMBER, PRODUCT_NAME,PRODUCT_DESCRIPTION
    FROM stg.PRODUCT_NAME_LOOKUP
) AS src
ON tgt.PRODUCT_NUMBER = src.PRODUCT_NUMBER

-- If record exists, update it (SCD1 overwrite)
WHEN MATCHED THEN
    UPDATE SET tgt.PRODUCT_NAME = src.PRODUCT_NAME,
	           tgt.PRODUCT_DESCRIPTION=src.PRODUCT_DESCRIPTION,
               tgt.UPDATE_DATE = GETDATE()

-- If record is new, insert it
WHEN NOT MATCHED THEN
    INSERT (PRODUCT_NUMBER, PRODUCT_NAME,PRODUCT_DESCRIPTION, SOURCE_ID, DATA_DATE, UPDATE_DATE)
    VALUES (src.PRODUCT_NUMBER, src.PRODUCT_NAME,src.PRODUCT_DESCRIPTION, 'LKP_FILE',GETDATE() , GETDATE());
END;

---FOR RETURN REASON
CREATE PROCEDURE Integration.usp_MergeDimReturnReasonlkp
AS
BEGIN
    SET NOCOUNT ON;
MERGE Integration.TBL_DIM_RETURN_REASON_LKP AS tgt
USING (
    SELECT RETURN_REASON_CODE, REASON_DESCRIPTION_EN AS RETURN_REASON_DESC
    FROM stg.RETURN_REASON
) AS src
ON tgt.RETURN_REASON_CODE = src.RETURN_REASON_CODE

-- If record exists, update it (SCD1 overwrite)
WHEN MATCHED THEN
    UPDATE SET tgt.RETURN_REASON_DESC = src.RETURN_REASON_DESC,
               tgt.UPDATE_DATE = GETDATE()

-- If record is new, insert it
WHEN NOT MATCHED THEN
    INSERT (RETURN_REASON_CODE, RETURN_REASON_DESC, SOURCE_ID, DATA_DATE, UPDATE_DATE)
    VALUES (src.RETURN_REASON_CODE, src.RETURN_REASON_DESC, 'LKP_FILE',GETDATE() , GETDATE());
END;
GO

--
CREATE PROCEDURE dbo.usp_Load_Lookup
  @LookupTable SYSNAME,
  @StagingTable SYSNAME
AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @sql NVARCHAR(MAX) = N'
    MERGE ' + QUOTENAME(@LookupTable) + ' AS tgt
    USING ' + QUOTENAME(@StagingTable) + ' AS src
    ON tgt.[CODE] = src.[CODE]  -- adapt column names
    WHEN MATCHED THEN
      UPDATE SET tgt.[NAME] = src.[NAME], tgt.UPDATE_DATE = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT ([CODE],[NAME],SOURCE_ID,DATA_DATE,UPDATE_DATE)
      VALUES (src.[CODE], src.[NAME], ''INTERNAL DB'', CAST(GETDATE() AS DATE), GETDATE());
  ';
  EXEC sp_executesql @sql;
END
GO

---FOR FAILURE LOG SP

CREATE TABLE Integration.TBL_LOAD_AUDIT (
    AuditId       INT IDENTITY(1,1) PRIMARY KEY,
    JobName       NVARCHAR(100),
    StartTime     DATETIME2,
    EndTime       DATETIME2,
    Status        NVARCHAR(20),
    ErrorMessage  NVARCHAR(MAX),
    RunId         UNIQUEIDENTIFIER,
    PipelineName  NVARCHAR(100),
    TableName     NVARCHAR(100)
);


CREATE PROCEDURE Integration.usp_Log_Pipeline_Failure
    @JobName NVARCHAR(100),
    @PipelineName NVARCHAR(100),
    @RunId UNIQUEIDENTIFIER,
    @TableName NVARCHAR(100),
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    INSERT INTO Integration.TBL_LOAD_AUDIT (JobName, StartTime, EndTime, Status, ErrorMessage, RunId, PipelineName, TableName)
    VALUES (@JobName, SYSUTCDATETIME(), SYSUTCDATETIME(), 'FAILED', @ErrorMessage, @RunId, @PipelineName, @TableName);
END
