CREATE   PROCEDURE dbo.usp_incremental_load_warehouse
AS
BEGIN
    SET NOCOUNT ON;

    /* =========================================
       1. VALIDATE CUSTOMER REFERENCES
       ========================================= */

    IF EXISTS (
        SELECT 1
        FROM dbo.stg_fact_sales AS f
        LEFT JOIN dbo.dim_customer AS c
            ON f.CustomerID = c.CustomerID
        WHERE c.CustomerKey IS NULL
    )
        THROW 50001, 'Unknown CustomerID in daily batch', 1;


    /* =========================================
       2. VALIDATE PRODUCT REFERENCES
       ========================================= */

    IF EXISTS (
        SELECT 1
        FROM dbo.stg_fact_sales AS f
        LEFT JOIN dbo.dim_product AS p
            ON f.ProductID = p.ProductID
        WHERE p.ProductKey IS NULL
    )
        THROW 50002, 'Unknown ProductID in daily batch', 1;


    BEGIN TRY
        BEGIN TRANSACTION;

        /* =========================================
           3. DIM DATE
           Insert new dates only
           ========================================= */

        MERGE INTO dbo.dim_date AS target
        USING dbo.stg_dim_date AS source
            ON target.DateKey = source.DateKey

        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                DateKey,
                FullDate,
                Day,
                Month,
                MonthName,
                Quarter,
                Year
            )
            VALUES
            (
                source.DateKey,
                source.FullDate,
                source.Day,
                source.Month,
                source.MonthName,
                source.Quarter,
                source.Year
            );


        /* =========================================
           4. VALIDATE DATE REFERENCES
           ========================================= */

        IF EXISTS (
            SELECT 1
            FROM dbo.stg_fact_sales AS f
            LEFT JOIN dbo.dim_date AS d
                ON f.OrderDate = d.FullDate
            WHERE d.DateKey IS NULL
        )
            THROW 50003, 'Unknown OrderDate in daily batch', 1;


        /* =========================================
           5. FACT SALES
           INSERT new / UPDATE existing
           ========================================= */

        MERGE INTO dbo.fact_sales AS target

        USING
        (
            SELECT
                f.OrderID,
                c.CustomerKey,
                p.ProductKey,
                d.DateKey,
                f.Quantity,
                f.UnitCost,
                f.UnitPrice,
                f.DiscountRate,
                f.GrossAmount,
                f.DiscountAmount,
                f.NetAmount,
                f.CostAmount,
                f.ProfitAmount,
                f.ReturnAmount

            FROM dbo.stg_fact_sales AS f

            INNER JOIN dbo.dim_customer AS c
                ON f.CustomerID = c.CustomerID

            INNER JOIN dbo.dim_product AS p
                ON f.ProductID = p.ProductID

            INNER JOIN dbo.dim_date AS d
                ON f.OrderDate = d.FullDate

        ) AS source

        ON target.OrderID = source.OrderID
        AND target.ProductKey = source.ProductKey

        WHEN MATCHED THEN
            UPDATE SET
                CustomerKey = source.CustomerKey,
                DateKey = source.DateKey,
                Quantity = source.Quantity,
                UnitCost = source.UnitCost,
                UnitPrice = source.UnitPrice,
                DiscountRate = source.DiscountRate,
                GrossAmount = source.GrossAmount,
                DiscountAmount = source.DiscountAmount,
                NetAmount = source.NetAmount,
                CostAmount = source.CostAmount,
                ProfitAmount = source.ProfitAmount,
                ReturnAmount = source.ReturnAmount

        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                OrderID,
                CustomerKey,
                ProductKey,
                DateKey,
                Quantity,
                UnitCost,
                UnitPrice,
                DiscountRate,
                GrossAmount,
                DiscountAmount,
                NetAmount,
                CostAmount,
                ProfitAmount,
                ReturnAmount
            )
            VALUES
            (
                source.OrderID,
                source.CustomerKey,
                source.ProductKey,
                source.DateKey,
                source.Quantity,
                source.UnitCost,
                source.UnitPrice,
                source.DiscountRate,
                source.GrossAmount,
                source.DiscountAmount,
                source.NetAmount,
                source.CostAmount,
                source.ProfitAmount,
                source.ReturnAmount
            );

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;

    END CATCH;
END;

GO