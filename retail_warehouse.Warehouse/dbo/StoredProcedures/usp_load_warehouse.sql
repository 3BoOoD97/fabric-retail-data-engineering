CREATE   PROCEDURE dbo.usp_load_warehouse
AS
BEGIN

    /* =========================================
       1. DIM CUSTOMER
       ========================================= */

    MERGE dbo.dim_customer AS target
    USING dbo.stg_dim_customer AS source

    ON target.CustomerID = source.CustomerID

    WHEN MATCHED THEN
        UPDATE SET
            target.Gender = source.Gender,
            target.Region = source.Region,
            target.City = source.City,
            target.Age = source.Age,
            target.CustomerSegment = source.CustomerSegment,
            target.SignUpDate = source.SignUpDate

    WHEN NOT MATCHED THEN
        INSERT
        (
            CustomerID,
            Gender,
            Region,
            City,
            Age,
            CustomerSegment,
            SignUpDate
        )
        VALUES
        (
            source.CustomerID,
            source.Gender,
            source.Region,
            source.City,
            source.Age,
            source.CustomerSegment,
            source.SignUpDate
        );


    /* =========================================
       2. DIM PRODUCT
       ========================================= */

    MERGE dbo.dim_product AS target
    USING dbo.stg_dim_product AS source

    ON target.ProductID = source.ProductID

    WHEN MATCHED THEN
        UPDATE SET
            target.ProductName = source.ProductName,
            target.CategoryName = source.CategoryName

    WHEN NOT MATCHED THEN
        INSERT
        (
            ProductID,
            ProductName,
            CategoryName
        )
        VALUES
        (
            source.ProductID,
            source.ProductName,
            source.CategoryName
        );


    /* =========================================
       3. DIM DATE
       ========================================= */

    MERGE dbo.dim_date AS target
    USING dbo.stg_dim_date AS source

    ON target.DateKey = source.DateKey

    WHEN NOT MATCHED THEN
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
       4. FACT SALES
       ========================================= */

    MERGE dbo.fact_sales AS target

    USING
    (
        SELECT
            s.OrderID,
            c.CustomerKey,
            p.ProductKey,
            d.DateKey,

            s.Quantity,
            s.UnitCost,
            s.UnitPrice,
            s.DiscountRate,
            s.GrossAmount,
            s.DiscountAmount,
            s.NetAmount,
            s.CostAmount,
            s.ProfitAmount,
            s.ReturnAmount

        FROM dbo.stg_fact_sales s

        JOIN dbo.dim_customer c
            ON s.CustomerID = c.CustomerID

        JOIN dbo.dim_product p
            ON s.ProductID = p.ProductID

        JOIN dbo.dim_date d
            ON s.OrderDate = d.FullDate

    ) AS source

    ON  target.OrderID = source.OrderID
    AND target.ProductKey = source.ProductKey

    WHEN MATCHED THEN
        UPDATE SET
            target.CustomerKey = source.CustomerKey,
            target.DateKey = source.DateKey,
            target.Quantity = source.Quantity,
            target.UnitCost = source.UnitCost,
            target.UnitPrice = source.UnitPrice,
            target.DiscountRate = source.DiscountRate,
            target.GrossAmount = source.GrossAmount,
            target.DiscountAmount = source.DiscountAmount,
            target.NetAmount = source.NetAmount,
            target.CostAmount = source.CostAmount,
            target.ProfitAmount = source.ProfitAmount,
            target.ReturnAmount = source.ReturnAmount

    WHEN NOT MATCHED THEN
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

END;

GO